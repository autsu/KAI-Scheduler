/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package scheduler

import (
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	kubeaischedulerver "github.com/NVIDIA/KAI-scheduler/pkg/apis/client/clientset/versioned"
	schedcache "github.com/NVIDIA/KAI-scheduler/pkg/scheduler/cache"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/cache/usagedb"
	api "github.com/NVIDIA/KAI-scheduler/pkg/scheduler/cache/usagedb/api"
	usagedbapi "github.com/NVIDIA/KAI-scheduler/pkg/scheduler/cache/usagedb/api"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/conf"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/conf_util"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/framework"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/log"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/metrics"
)

type Scheduler struct {
	cache           schedcache.Cache
	config          *conf.SchedulerConfiguration
	schedulerParams *conf.SchedulerParams
	schedulePeriod  time.Duration
	mux             *http.ServeMux
}

func NewScheduler(
	config *rest.Config,
	schedulerConf *conf.SchedulerConfiguration,
	schedulerParams *conf.SchedulerParams,
	mux *http.ServeMux,
) (*Scheduler, error) {
	kubeClient, kubeAiSchedulerClient := newClients(config)

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("Failed to create discovery client: %v", err)
	}

	usageDBClient, err := getUsageDBClient(schedulerConf.UsageDBConfig)
	if err != nil {
		return nil, fmt.Errorf("error getting usage db client: %v", err)
	}

	var usageDBParams *api.UsageParams
	if schedulerConf.UsageDBConfig != nil {
		usageDBParams = schedulerConf.UsageDBConfig.GetUsageParams()
	}

	schedulerCacheParams := &schedcache.SchedulerCacheParams{
		KubeClient:                  kubeClient,
		KAISchedulerClient:          kubeAiSchedulerClient,
		UsageDBParams:               usageDBParams,
		UsageDBClient:               usageDBClient,
		SchedulerName:               schedulerParams.SchedulerName,
		NodePoolParams:              schedulerParams.PartitionParams,
		RestrictNodeScheduling:      schedulerParams.RestrictSchedulingNodes,
		DetailedFitErrors:           schedulerParams.DetailedFitErrors,
		ScheduleCSIStorage:          schedulerParams.ScheduleCSIStorage,
		FullHierarchyFairness:       schedulerParams.FullHierarchyFairness,
		NumOfStatusRecordingWorkers: schedulerParams.NumOfStatusRecordingWorkers,
		UpdatePodEvictionCondition:  schedulerParams.UpdatePodEvictionCondition,
		DiscoveryClient:             discoveryClient,
	}

	scheduler := &Scheduler{
		config:          schedulerConf,
		schedulerParams: schedulerParams,
		cache:           schedcache.New(schedulerCacheParams),
		schedulePeriod:  schedulerParams.SchedulePeriod,
		mux:             mux,
	}

	return scheduler, nil
}

// +GangSchedulingStep:1 调度器主入口 - 启动调度循环
// Run 启动调度器的主循环，这是 Gang 调度的起点
// 功能：
// 1. 启动 Cache，监听 Kubernetes 资源变化（Pod、PodGroup、Node、Queue 等）
// 2. 等待 Cache 同步完成，确保有完整的集群状态
// 3. 启动定期调度循环，每个调度周期执行一次 runOnce
func (s *Scheduler) Run(stopCh <-chan struct{}) {
	// 启动 Cache，开始监听 K8s 资源
	s.cache.Run(stopCh)

	// 等待 Cache 同步完成，确保获取到所有 PodGroup、Pod、Node 等资源
	s.cache.WaitForCacheSync(stopCh)

	// 启动定期调度循环，默认每秒执行一次
	go func() {
		wait.Until(s.runOnce, s.schedulePeriod, stopCh)
	}()
}

// +GangSchedulingStep:2 单次调度周期执行
// runOnce 执行一次完整的调度周期，这是 Gang 调度的核心流程
// 每个调度周期的流程：
// 1. 打开 Session（获取集群快照）
// 2. 依次执行 Actions（Allocate、Consolidate、Reclaim、Preempt、StaleGangEviction）
// 3. 关闭 Session（清理资源）
func (s *Scheduler) runOnce() {
	// 生成唯一的 Session ID，用于日志追踪
	sessionId := generateSessionID(6)
	log.InfraLogger.SetSessionID(string(sessionId))

	log.InfraLogger.V(1).Infof("Start scheduling ...")
	scheduleStartTime := time.Now()
	defer log.InfraLogger.V(1).Infof("End scheduling ...")

	defer metrics.UpdateE2eDuration(scheduleStartTime)

	// +GangSchedulingStep:3 打开调度会话，获取集群快照
	// OpenSession 会：
	// 1. 从 Cache 获取集群快照（所有 PodGroup、Pod、Node、Queue 的当前状态）
	// 2. 初始化插件（Proportion、Topology、GPU 等）
	// 3. 注册插件的回调函数（用于 Gang 约束检查、资源分配等）
	ssn, err := framework.OpenSession(s.cache, s.config, s.schedulerParams, sessionId, s.mux)
	if err != nil {
		log.InfraLogger.Errorf("Error while opening session, will try again next cycle. \nCause: %+v", err)
		return
	}
	defer framework.CloseSession(ssn)

	// +GangSchedulingStep:4 执行调度 Actions
	// Actions 执行顺序（Gang 调度主要在 Allocate Action 中）：
	// 1. Allocate - 为待调度的 PodGroup 分配资源（Gang 调度的核心）
	// 2. Consolidate - 整合已运行的工作负载，减少碎片
	// 3. Reclaim - 回收超配额队列的资源
	// 4. Preempt - 高优先级作业抢占低优先级作业
	// 5. StaleGangEviction - 清理不满足 minMember 的 PodGroup
	actions, _ := conf_util.GetActionsFromConfig(s.config)
	for _, action := range actions {
		log.InfraLogger.SetAction(string(action.Name()))
		metrics.SetCurrentAction(string(action.Name()))
		actionStartTime := time.Now()

		// 执行 Action（Allocate Action 是 Gang 调度的入口）
		action.Execute(ssn)

		metrics.UpdateActionDuration(string(action.Name()), metrics.Duration(actionStartTime))
	}
	log.InfraLogger.RemoveActionLogger()
}

func newClients(config *rest.Config) (kubernetes.Interface, kubeaischedulerver.Interface) {
	k8cClientConfig := rest.CopyConfig(config)

	// Force protobuf serialization for k8s built-in resources
	k8cClientConfig.AcceptContentTypes = "application/vnd.kubernetes.protobuf"
	k8cClientConfig.ContentType = "application/vnd.kubernetes.protobuf"
	return kubernetes.NewForConfigOrDie(k8cClientConfig), kubeaischedulerver.NewForConfigOrDie(config)
}

func getUsageDBClient(dbConfig *usagedbapi.UsageDBConfig) (usagedbapi.Interface, error) {
	resolver := usagedb.NewClientResolver(nil)
	return resolver.GetClient(dbConfig)
}

func generateSessionID(l int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := []byte(str)
	var result []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for range l {
		result = append(result, bytes[r.Intn(len(bytes))])
	}

	return string(result)
}

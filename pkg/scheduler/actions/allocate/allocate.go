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

package allocate

import (
	"time"

	"golang.org/x/exp/maps"

	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/actions/common"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/actions/utils"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/framework"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/log"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/metrics"
)

type allocateAction struct {
}

func New() *allocateAction {
	return &allocateAction{}
}

func (alloc *allocateAction) Name() framework.ActionType {
	return framework.Allocate
}

// +GangSchedulingStep:5 Allocate Action 执行 - Gang 调度的核心入口
// Execute 是 Allocate Action 的主函数，负责为所有待调度的 PodGroup 分配资源
// Gang 调度的关键流程：
// 1. 按队列优先级和公平性排序所有待调度的 PodGroup
// 2. 逐个尝试为 PodGroup 分配资源
// 3. 使用 Statement 事务机制，确保 Gang 的原子性（要么全部分配成功，要么全部回滚）
func (alloc *allocateAction) Execute(ssn *framework.Session) {
	log.InfraLogger.V(2).Infof("Enter Allocate ...")
	defer log.InfraLogger.V(2).Infof("Leaving Allocate ...")

	// +GangSchedulingStep:6 初始化作业队列，按优先级排序
	// NewJobsOrderByQueues 会：
	// 1. 过滤掉非 Pending 状态的 PodGroup
	// 2. 过滤掉未就绪的 PodGroup（例如队列不存在）
	// 3. 按队列的公平性、优先级、作业优先级等排序
	// 这确保了高优先级和资源不足的队列优先得到调度
	jobsOrderByQueues := utils.NewJobsOrderByQueues(ssn, utils.JobsOrderInitOptions{
		FilterNonPending:  true, // 只处理 Pending 状态的 PodGroup
		FilterUnready:     true, // 过滤未就绪的 PodGroup
		MaxJobsQueueDepth: ssn.GetJobsDepth(framework.Allocate),
	})
	jobsOrderByQueues.InitializeWithJobs(ssn.ClusterInfo.PodGroupInfos)

	log.InfraLogger.V(2).Infof("There are <%d> PodGroupInfos and <%d> Queues in total for scheduling",
		jobsOrderByQueues.Len(), ssn.CountLeafQueues())

	// +GangSchedulingStep:7 遍历所有待调度的 PodGroup
	// 按优先级顺序处理每个 PodGroup，尝试为其分配资源
	for !jobsOrderByQueues.IsEmpty() {
		// 弹出优先级最高的 PodGroup
		job := jobsOrderByQueues.PopNextJob()

		// +GangSchedulingStep:8 创建 Statement 事务
		// Statement 提供类似数据库事务的机制：
		// - Checkpoint: 创建回滚点
		// - Allocate: 虚拟分配资源（不实际修改 K8s）
		// - Commit: 提交所有分配（创建 BindRequest）
		// - Rollback: 回滚到 Checkpoint（Gang 调度失败时使用）
		stmt := ssn.Statement()
		alreadyAllocated := job.GetNumAllocatedTasks() > 0

		// +GangSchedulingStep:9 尝试为 PodGroup 分配资源（Gang 调度的核心）
		// attemptToAllocateJob 会检查 minMember 约束，确保 Gang 语义
		if ok, pipelined := attemptToAllocateJob(ssn, stmt, job); ok {
			// 分配成功，提交 Statement
			metrics.IncPodgroupScheduledByAction()

			// +GangSchedulingStep:10 提交 Statement，创建 BindRequest
			// Commit 会为所有分配的 Pod 创建 BindRequest CRD
			// Binder 组件会监听 BindRequest 并执行实际的 Pod 绑定
			err := stmt.Commit()
			if err == nil && !pipelined && !alreadyAllocated {
				setLastStartTimestamp(job)
			}

			// 如果还有任务未分配（部分分配），重新加入队列
			if err == nil && podgroup_info.HasTasksToAllocate(job, true) {
				jobsOrderByQueues.PushJob(job)
				continue
			}
		} else {
			// +GangSchedulingStep:11 分配失败，丢弃 Statement
			// 这确保了 Gang 的原子性：如果无法满足 minMember，不会分配任何 Pod
			stmt.Discard()
		}
	}
}

// +GangSchedulingStep:12 尝试为单个 PodGroup 分配资源
// attemptToAllocateJob 是 Gang 调度的关键函数，负责：
// 1. 计算 PodGroup 需要的资源
// 2. 调用 common.AllocateJob 进行实际分配
// 3. 处理 Pipeline 模式（资源预留）
func attemptToAllocateJob(ssn *framework.Session, stmt *framework.Statement, job *podgroup_info.PodGroupInfo) (allocated, pipelined bool) {
	queue := ssn.ClusterInfo.Queues[job.Queue]

	// 计算待分配任务的初始资源需求
	resReq := podgroup_info.GetTasksToAllocateInitResource(job, ssn.PodSetOrderFn, ssn.TaskOrderFn, true, ssn.ClusterInfo.MinNodeGPUMemory)
	log.InfraLogger.V(3).Infof("Attempting to allocate job: <%v/%v> of queue <%v>, resources: <%v>",
		job.Namespace, job.Name, queue.Name, resReq)

	// +GangSchedulingStep:13 调用 common.AllocateJob 进行资源分配
	// 这是 Gang 调度的核心逻辑，会检查 minMember 约束
	nodes := maps.Values(ssn.ClusterInfo.Nodes)
	if !common.AllocateJob(ssn, stmt, nodes, job, false) {
		log.InfraLogger.V(3).Infof("Could not allocate resources for job: <%v/%v> of queue <%v>",
			job.Namespace, job.Name, job.Queue)
		return false, false
	}

	// 处理 Pipeline 模式（某些 Pod 需要等待其他 Pod 释放资源）
	pipelined = false
	if job.ShouldPipelineJob() {
		log.InfraLogger.V(3).Infof(
			"Some tasks were pipelined, setting all job to be pipelined for job: <%v/%v>",
			job.Namespace, job.Name)
		err := stmt.ConvertAllAllocatedToPipelined(job.UID)
		if err != nil {
			log.InfraLogger.Errorf(
				"Failed to covert tasks from allocated to pipelined for job: <%v/%v>, error: <%v>",
				job.Namespace, job.Name, err)
			return false, false
		}
		pipelined = true
	} else {
		log.InfraLogger.V(3).Infof("Succesfully allocated resources for job: <%v/%v>",
			job.Namespace, job.Name)
	}

	return true, pipelined
}

func setLastStartTimestamp(job *podgroup_info.PodGroupInfo) {
	timeNow := time.Now()
	job.LastStartTimestamp = &timeNow
}

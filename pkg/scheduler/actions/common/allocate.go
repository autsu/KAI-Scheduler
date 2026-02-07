// Copyright 2025 NVIDIA CORPORATION
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"fmt"
	"sort"

	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/common_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/node_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/pod_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/podgroup_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/api/podgroup_info/subgroup_info"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/framework"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/gpu_sharing"
	"github.com/NVIDIA/KAI-scheduler/pkg/scheduler/log"
)

// +GangSchedulingStep:14 AllocateJob - Gang 调度的核心实现
// 这是 Gang 调度最关键的函数，负责：
// 1. 获取待分配的任务列表
// 2. 检查是否满足队列容量限制
// 3. 递归处理 SubGroupSet（支持层级 Gang 调度）
// 4. 确保满足 minMember 约束（Gang 语义的核心）
func AllocateJob(ssn *framework.Session, stmt *framework.Statement, nodes []*node_info.NodeInfo,
	job *podgroup_info.PodGroupInfo, isPipelineOnly bool) bool {
	// 作业分配前的预处理（插件回调）
	ssn.PreJobAllocation(job)

	// +GangSchedulingStep:15 获取待分配的任务列表
	// GetTasksToAllocate 会：
	// 1. 按 PodSet 和 Task 优先级排序
	// 2. 过滤已分配的任务
	// 3. 返回需要调度的 Pod 列表
	tasksToAllocate := podgroup_info.GetTasksToAllocate(job, ssn.PodSetOrderFn, ssn.TaskOrderFn, !isPipelineOnly)

	// +GangSchedulingStep:16 检查队列容量限制
	// IsJobOverQueueCapacityFn 会检查：
	// 1. 是否超过队列的 Limit（硬限制）
	// 2. 是否超过队列的 Quota（配额）
	// 3. 是否可以使用超配额资源（基于公平性）
	// 这是 Gang 调度的第一道关卡
	result := ssn.IsJobOverQueueCapacityFn(job, tasksToAllocate)
	if !result.IsSchedulable {
		if !isPipelineOnly {
			job.AddJobFitError(common_info.NewJobFitErrorWithQueueContext(
				job.Name, podgroup_info.DefaultSubGroup, job.Namespace,
				result.Reason, result.Message, result.Details))
		}
		return false
	}

	// +GangSchedulingStep:17 递归分配 SubGroupSet
	// 支持层级 Gang 调度，例如：
	// PodGroup (minMember=2)
	//   ├─ decode (minMember=2)
	//   │   ├─ decode-workers (minMember=4)
	//   │   └─ decode-leaders (minMember=1)
	//   └─ prefill (minMember=2)
	//       ├─ prefill-workers (minMember=4)
	//       └─ prefill-leaders (minMember=1)
	return allocateSubGroupSet(ssn, stmt, nodes, job, job.RootSubGroupSet, tasksToAllocate, isPipelineOnly)
}

// +GangSchedulingStep:18 分配 SubGroupSet（支持层级 Gang）
// allocateSubGroupSet 处理层级 Gang 调度：
// 1. 根据拓扑约束选择节点子集
// 2. 为每个节点子集尝试分配
// 3. 使用 Checkpoint/Rollback 确保原子性
func allocateSubGroupSet(ssn *framework.Session, stmt *framework.Statement, nodes []*node_info.NodeInfo,
	job *podgroup_info.PodGroupInfo, subGroupSet *subgroup_info.SubGroupSet, tasksToAllocate []*pod_info.PodInfo,
	isPipelineOnly bool,
) bool {
	// +GangSchedulingStep:19 根据拓扑约束选择节点子集
	// SubsetNodesFn 会根据拓扑约束（例如同一机架、同一区域）
	// 将节点分组，确保 Pod 可以满足拓扑要求
	nodeSets, err := ssn.SubsetNodesFn(job, &subGroupSet.SubGroupInfo, subGroupSet.GetAllPodSets(), tasksToAllocate, nodes)
	if err != nil {
		log.InfraLogger.Errorf(
			"Failed to run SubsetNodes on job <%s/%s>: %v", job.Namespace, job.Name, err)
		return false
	}

	// +GangSchedulingStep:20 尝试在每个节点子集上分配
	// 使用 Checkpoint/Rollback 机制确保 Gang 的原子性
	for _, nodeSet := range nodeSets {
		// 创建 Checkpoint，如果分配失败可以回滚
		cp := stmt.Checkpoint()

		// 尝试在这个节点子集上分配所有任务
		if allocateSubGroupSetOnNodes(ssn, stmt, nodeSet, job, subGroupSet, tasksToAllocate, isPipelineOnly) {
			// 分配成功，返回 true
			return true
		}

		// 分配失败，回滚到 Checkpoint
		// 这确保了 Gang 的原子性：要么全部成功，要么全部回滚
		if err := stmt.Rollback(cp); err != nil {
			log.InfraLogger.Errorf("Failed to rollback statement in session %v, err: %v", ssn.ID, err)
		}
	}

	// 所有节点子集都无法满足要求，返回 false
	return false
}

// +GangSchedulingStep:21 在指定节点集上分配 SubGroupSet
// allocateSubGroupSetOnNodes 递归处理层级结构：
// 1. 先处理子 SubGroupSet（递归）
// 2. 再处理子 PodSet
// 3. 确保每一层都满足 minMember 约束
func allocateSubGroupSetOnNodes(ssn *framework.Session, stmt *framework.Statement, nodes node_info.NodeSet,
	job *podgroup_info.PodGroupInfo, subGroupSet *subgroup_info.SubGroupSet, tasksToAllocate []*pod_info.PodInfo,
	isPipelineOnly bool,
) bool {
	// +GangSchedulingStep:22 递归处理子 SubGroupSet
	// 例如：先处理 decode 和 prefill 这两个顶层 SubGroup
	// 每个 SubGroup 必须满足自己的 minMember 约束
	for _, childSubGroupSet := range orderedSubGroupSets(ssn, subGroupSet.GetChildGroups()) {
		podSets := childSubGroupSet.GetAllPodSets()
		subGroupTasks := filterTasksForPodSets(podSets, tasksToAllocate)

		// 递归调用，处理子 SubGroupSet
		if !allocateSubGroupSet(ssn, stmt, nodes, job, childSubGroupSet, subGroupTasks, isPipelineOnly) {
			// 子 SubGroupSet 分配失败，整个分配失败
			// 这确保了层级 Gang 的语义
			return false
		}
	}

	// +GangSchedulingStep:23 处理子 PodSet（叶子节点）
	// 例如：处理 decode-workers 和 decode-leaders
	// 每个 PodSet 必须满足自己的 minMember 约束
	for _, podSet := range orderedPodSets(ssn, subGroupSet.GetChildPodSets()) {
		podSetTasks := filterTasksForPodSet(podSet, tasksToAllocate)

		// 为 PodSet 分配资源
		if !allocatePodSet(ssn, stmt, nodes, job, podSet, podSetTasks, isPipelineOnly) {
			// PodSet 分配失败，整个分配失败
			return false
		}
	}

	// 所有子 SubGroupSet 和 PodSet 都分配成功
	return true
}

// +GangSchedulingStep:24 为 PodSet 分配资源
// allocatePodSet 处理单个 PodSet 的分配：
// 1. 根据拓扑约束选择节点子集
// 2. 尝试在节点子集上分配所有任务
// 3. 使用 Checkpoint/Rollback 确保原子性
func allocatePodSet(ssn *framework.Session, stmt *framework.Statement, nodes node_info.NodeSet,
	job *podgroup_info.PodGroupInfo, podSet *subgroup_info.PodSet, tasksToAllocate []*pod_info.PodInfo,
	isPipelineOnly bool,
) bool {
	// 构造 PodSet 映射
	podSets := map[string]*subgroup_info.PodSet{
		podSet.GetName(): podSet,
	}

	// +GangSchedulingStep:25 根据拓扑约束选择节点子集
	// 例如：如果 PodSet 要求在同一机架，会将节点按机架分组
	nodeSets, err := ssn.SubsetNodesFn(job, &podSet.SubGroupInfo, podSets, tasksToAllocate, nodes)
	if err != nil {
		log.InfraLogger.Errorf(
			"Failed to run SubsetNodes on job <%s/%s>: %v", job.Namespace, job.Name, err)
		return false
	}

	// +GangSchedulingStep:26 尝试在每个节点子集上分配任务
	for _, nodeSet := range nodeSets {
		// 创建 Checkpoint
		cp := stmt.Checkpoint()

		// 尝试在这个节点子集上分配所有任务
		if allocateTasksOnNodeSet(ssn, stmt, nodeSet, job, tasksToAllocate, isPipelineOnly) {
			// 分配成功
			return true
		}

		// 分配失败，回滚
		if err := stmt.Rollback(cp); err != nil {
			log.InfraLogger.Errorf("Failed to rollback statement in session %v, err: %v", ssn.ID, err)
		}
	}

	// 所有节点子集都无法满足要求
	return false
}

// +GangSchedulingStep:27 在节点集上分配所有任务
// allocateTasksOnNodeSet 遍历所有任务，逐个分配：
// 1. 为每个任务查找合适的节点
// 2. 如果任何一个任务分配失败，整个分配失败
// 3. 这确保了 Gang 的原子性：要么全部成功，要么全部失败
func allocateTasksOnNodeSet(ssn *framework.Session, stmt *framework.Statement, nodes node_info.NodeSet,
	job *podgroup_info.PodGroupInfo, tasksToAllocate []*pod_info.PodInfo, isPipelineOnly bool) bool {
	// +GangSchedulingStep:28 遍历所有待分配的任务
	// 这是 Gang 调度的关键：必须为所有任务都找到节点
	for index, task := range tasksToAllocate {
		// 为单个任务分配节点
		success := allocateTask(ssn, stmt, nodes, task, isPipelineOnly)
		if !success {
			// +GangSchedulingStep:29 任务分配失败，记录错误信息
			// handleFailedTaskAllocation 会：
			// 1. 记录失败原因（资源不足、节点不满足要求等）
			// 2. 如果是 Gang 调度，说明需要多少个 Pod 才能满足 minMember
			// 3. 提供详细的调试信息
			handleFailedTaskAllocation(job, task, index)

			// 返回 false，触发上层的 Rollback
			// 这确保了 Gang 的原子性
			return false
		}
	}

	// 所有任务都分配成功
	return true
}

// +GangSchedulingStep:30 为单个任务分配节点
// allocateTask 是最底层的分配函数：
// 1. 执行预谓词检查（Pre-Predicate）
// 2. 对节点进行评分排序
// 3. 遍历节点，找到第一个满足要求的节点
// 4. 执行虚拟分配（不实际修改 K8s）
func allocateTask(ssn *framework.Session, stmt *framework.Statement, nodes []*node_info.NodeInfo,
	task *pod_info.PodInfo, isPipelineOnly bool) (success bool) {
	// 获取任务所属的 PodGroup
	job := ssn.ClusterInfo.PodGroupInfos[task.Job]
	if job == nil {
		log.InfraLogger.Errorf("Failed to find job <%s> in session <%s>", task.Job, ssn.ID)
		return false
	}

	// +GangSchedulingStep:31 执行预谓词检查
	// PrePredicateFn 会检查：
	// 1. Pod 的基本配置是否正确
	// 2. 是否有必需的资源声明
	// 3. 是否满足基本的调度要求
	err := ssn.PrePredicateFn(task, job)
	if err != nil {
		log.InfraLogger.V(6).Infof("pre-predicates failed on task %s/%s. Error: %v",
			task.Namespace, task.Name, err)

		fitErrors := common_info.NewFitErrors()
		fitErrors.SetError(err.Error())
		job.AddTaskFitErrors(task, fitErrors)
		return false
	}

	log.InfraLogger.V(6).Infof("Looking for best node for task - Task: <%s/%s>, init requested: <%v>.",
		task.Namespace, task.Name, task.ResReq)

	// +GangSchedulingStep:32 对节点进行评分排序
	// OrderedNodesByTask 会：
	// 1. 执行谓词过滤（Predicate）：过滤不满足要求的节点
	// 2. 对剩余节点评分（Score）：根据资源利用率、亲和性等打分
	// 3. 按分数排序，返回最优节点列表
	orderedNodes := ssn.OrderedNodesByTask(nodes, task)

	// +GangSchedulingStep:33 遍历节点，尝试分配
	for _, node := range orderedNodes {
		// 检查节点是否满足要求（再次验证）
		if !ssn.FittingNode(task, node, !isPipelineOnly) {
			continue
		}

		// +GangSchedulingStep:34 尝试将任务分配到节点
		// allocateTaskToNode 会：
		// 1. 处理 GPU 共享（如果需要）
		// 2. 执行虚拟分配（更新 Statement 中的状态）
		// 3. 触发插件的 AllocateFunc 回调
		success = allocateTaskToNode(ssn, stmt, task, node, isPipelineOnly)
		if success {
			// 分配成功，跳出循环
			break
		}

		log.InfraLogger.V(6).Infof("Failed to allocate or pipeline task: <%v/%v> to node: %v",
			task.Namespace, task.Name, node.Name)
	}

	if success {
		log.InfraLogger.V(6).Infof("Allocation succeeded for task: <%v/%v>", task.Namespace, task.Name)
	} else {
		log.InfraLogger.V(6).Infof("Failed statement allocate for task: <%v/%v>", task.Namespace, task.Name)
	}

	return success
}

// +GangSchedulingStep:35 将任务分配到节点
// allocateTaskToNode 根据任务类型选择分配方式：
// 1. GPU 共享任务：使用特殊的 GPU 分配逻辑
// 2. 普通任务：直接绑定或 Pipeline
func allocateTaskToNode(ssn *framework.Session, stmt *framework.Statement, task *pod_info.PodInfo, node *node_info.NodeInfo, isPipelineOnly bool) bool {
	// +GangSchedulingStep:36 处理 GPU 共享任务
	// 如果任务请求分数 GPU 或指定 GPU 内存，使用特殊的分配逻辑
	if task.IsFractionRequest() || task.IsMemoryRequest() {
		return gpu_sharing.AllocateFractionalGPUTaskToNode(ssn, stmt, task, node, isPipelineOnly)
	}

	// +GangSchedulingStep:37 处理普通任务
	// 检查节点是否有足够的资源
	if taskAllocatable := node.IsTaskAllocatable(task); !isPipelineOnly && taskAllocatable {
		// 资源充足，直接绑定
		return bindTaskToNode(ssn, stmt, task, node)
	}

	// 资源不足，尝试 Pipeline（等待其他 Pod 释放资源）
	return pipelineTaskToNode(ssn, stmt, task, node, !isPipelineOnly)
}

// +GangSchedulingStep:38 绑定任务到节点（虚拟分配）
// bindTaskToNode 执行虚拟绑定：
// 1. 调用 stmt.Allocate 更新 Statement 中的状态
// 2. 虚拟扣减节点资源
// 3. 记录操作，用于后续的 Commit 或 Rollback
// 注意：这里不会实际修改 Kubernetes，只是在内存中模拟
func bindTaskToNode(ssn *framework.Session, stmt *framework.Statement, task *pod_info.PodInfo, node *node_info.NodeInfo) bool {
	log.InfraLogger.V(6).Infof("Binding Task <%v/%v> to node <%v>, requires: %v GPUs",
		task.Namespace, task.Name, node.Name, task.ResReq)

	// +GangSchedulingStep:39 执行虚拟分配
	// stmt.Allocate 会：
	// 1. 虚拟扣减节点资源（CPU、内存、GPU 等）
	// 2. 更新任务状态为 Allocated
	// 3. 记录操作到 Statement 的操作列表
	// 4. 触发插件的 AllocateFunc 回调（例如更新队列使用量）
	if err := stmt.Allocate(task, node.Name); err != nil {
		log.InfraLogger.Errorf("Failed to bind Task %v on %v in Session %v, err: %v", task.UID, node.Name, ssn.ID, err)
		return false
	}
	return true
}

// +GangSchedulingStep:40 Pipeline 任务到节点
// pipelineTaskToNode 用于资源预留场景：
// 1. 当前节点资源不足
// 2. 但预期其他 Pod 会被驱逐，释放资源
// 3. 将任务标记为 Pipelined，等待资源释放
func pipelineTaskToNode(ssn *framework.Session, stmt *framework.Statement, task *pod_info.PodInfo, node *node_info.NodeInfo, updateTasksIfExistsOnNode bool) bool {
	log.InfraLogger.V(6).Infof("Pipelining Task <%v/%v> to node <%v> requires: %v GPUs",
		task.Namespace, task.Name, node.Name, task.ResReq)

	// 执行 Pipeline 操作
	if err := stmt.Pipeline(task, node.Name, updateTasksIfExistsOnNode); err != nil {
		log.InfraLogger.V(6).Infof("Failed to pipeline Task %v on %v in Session %v", task.UID, node.Name, ssn.ID)
		return false
	}
	return true
}

// +GangSchedulingStep:41 处理任务分配失败
// handleFailedTaskAllocation 记录详细的失败信息：
// 1. 如果是 Gang 调度，说明需要多少个 Pod 才能满足 minMember
// 2. 记录失败原因（资源不足、节点不满足要求等）
// 3. 提供详细的调试信息，帮助用户理解为什么调度失败
func handleFailedTaskAllocation(job *podgroup_info.PodGroupInfo, unschedulableTask *pod_info.PodInfo, numSchedulableTasks int) {
	// 获取任务的失败原因
	allocationError, found := job.TasksFitErrors[unschedulableTask.UID]

	if !found {
		allocationError = common_info.NewFitErrors()
		allocationError.SetError(common_info.DefaultPodError)
	}

	// +GangSchedulingStep:42 检查是否是 Gang 调度
	// isGangScheduling 检查 PodGroup 的任何 SubGroup 是否有 minMember > 1
	gangScheduling := isGangScheduling(job)

	// 获取任务所属的 SubGroup
	taskSubGroupName := podgroup_info.DefaultSubGroup
	if len(unschedulableTask.SubGroupName) != 0 {
		taskSubGroupName = unschedulableTask.SubGroupName
	}
	taskSubGroup := job.GetSubGroups()[taskSubGroupName]

	// +GangSchedulingStep:43 根据是否是 Gang 调度，生成不同的错误信息
	// 如果不是 Gang 调度，或者已经满足 minMember，只记录简单的错误
	if !gangScheduling || taskSubGroup.GetNumActiveUsedTasks() >= int(taskSubGroup.GetMinAvailable()) {
		job.AddSimpleJobFitError(
			podgroup_info.PodSchedulingErrors,
			fmt.Sprintf("Resources were not found for pod %s/%s due to: %s",
				unschedulableTask.Namespace, unschedulableTask.Name, allocationError.Error()))
		return
	}

	// +GangSchedulingStep:44 Gang 调度失败，生成详细的错误信息
	// 说明需要多少个 Pod 才能满足 minMember，以及为什么无法调度更多 Pod
	if len(job.GetSubGroups()) == 1 && taskSubGroup.GetName() == podgroup_info.DefaultSubGroup {
		// 单个 SubGroup 的情况
		job.AddSimpleJobFitError(
			podgroup_info.PodSchedulingErrors,
			fmt.Sprintf("Resources were found for %d pods while %d are required for gang scheduling. "+
				"Additional pods cannot be scheduled due to: %s",
				numSchedulableTasks, taskSubGroup.GetMinAvailable(), allocationError.Error()))
		return
	}

	// 多个 SubGroup 的情况
	job.AddSimpleJobFitError(
		podgroup_info.PodSchedulingErrors,
		fmt.Sprintf("Resources were found for %d pods from all sub-groups while sub-group %s requires %d pods for gang scheduling. "+
			"Additional pods cannot be scheduled in this sub-group due to: %s",
			numSchedulableTasks, taskSubGroup.GetName(), taskSubGroup.GetMinAvailable(), allocationError.Error()))
}

// +GangSchedulingStep:45 检查是否是 Gang 调度
// isGangScheduling 判断 PodGroup 是否需要 Gang 调度：
// 只要有任何一个 SubGroup 的 minMember > 1，就认为是 Gang 调度
func isGangScheduling(job *podgroup_info.PodGroupInfo) bool {
	for _, subGroup := range job.GetSubGroups() {
		if subGroup.GetMinAvailable() > 1 {
			return true
		}
	}
	return false
}

func filterTasksForPodSet(podSet *subgroup_info.PodSet, tasks []*pod_info.PodInfo) []*pod_info.PodInfo {
	return filterTasksForPodSets(map[string]*subgroup_info.PodSet{podSet.GetName(): podSet}, tasks)
}

func filterTasksForPodSets(podSets map[string]*subgroup_info.PodSet, tasks []*pod_info.PodInfo) []*pod_info.PodInfo {
	var result []*pod_info.PodInfo
	for _, task := range tasks {
		subGroupName := task.SubGroupName
		if len(subGroupName) == 0 {
			subGroupName = podgroup_info.DefaultSubGroup
		}
		if _, found := podSets[subGroupName]; found {
			result = append(result, task)
		}
	}
	return result
}

func orderedSubGroupSets(ssn *framework.Session, subGroupSets []*subgroup_info.SubGroupSet) []*subgroup_info.SubGroupSet {
	result := append([]*subgroup_info.SubGroupSet{}, subGroupSets...)
	sort.Slice(result, func(i, j int) bool {
		return ssn.SubGroupSetOrderFn(result[i], result[j])
	})
	return result
}

func orderedPodSets(ssn *framework.Session, podSets []*subgroup_info.PodSet) []*subgroup_info.PodSet {
	result := append([]*subgroup_info.PodSet{}, podSets...)
	sort.Slice(result, func(i, j int) bool {
		return ssn.PodSetOrderFn(result[i], result[j])
	})
	return result
}

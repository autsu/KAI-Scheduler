# Gang 调度代码追踪指南

## 概述

我已经在 KAI Scheduler 的 Gang 调度核心代码中添加了详细的注释和步骤标记。通过搜索 `+GangSchedulingStep`，你可以按顺序追踪整个 Gang 调度的执行流程。

## 如何使用

### 1. 搜索步骤标记

在项目根目录执行：

```bash
# 搜索所有 Gang 调度步骤
grep -r "+GangSchedulingStep" pkg/scheduler/

# 或使用 ripgrep（更快）
rg "\+GangSchedulingStep" pkg/scheduler/
```

### 2. 按步骤顺序查看

步骤编号从 1 到 45，按执行顺序标记。你可以按数字顺序查看：

```bash
# 查看特定步骤
grep -r "+GangSchedulingStep:1" pkg/scheduler/
grep -r "+GangSchedulingStep:2" pkg/scheduler/
# ... 依此类推
```

## Gang 调度完整流程（45 个步骤）

### 阶段 1：调度器启动和循环（步骤 1-4）

| 步骤 | 文件 | 函数 | 说明 |
|------|------|------|------|
| **Step 1** | `pkg/scheduler/scheduler.go` | `Run()` | 调度器主入口，启动调度循环 |
| **Step 2** | `pkg/scheduler/scheduler.go` | `runOnce()` | 单次调度周期执行 |
| **Step 3** | `pkg/scheduler/scheduler.go` | `runOnce()` | 打开调度会话，获取集群快照 |
| **Step 4** | `pkg/scheduler/scheduler.go` | `runOnce()` | 执行调度 Actions |

**关键点**：
- 调度器每秒执行一次 `runOnce()`
- 每次执行都会获取集群的完整快照（PodGroup、Pod、Node、Queue）
- Actions 按顺序执行：Allocate → Consolidate → Reclaim → Preempt → StaleGangEviction

### 阶段 2：Allocate Action 执行（步骤 5-13）

| 步骤 | 文件 | 函数 | 说明 |
|------|------|------|------|
| **Step 5** | `pkg/scheduler/actions/allocate/allocate.go` | `Execute()` | Allocate Action 执行入口 |
| **Step 6** | `pkg/scheduler/actions/allocate/allocate.go` | `Execute()` | 初始化作业队列，按优先级排序 |
| **Step 7** | `pkg/scheduler/actions/allocate/allocate.go` | `Execute()` | 遍历所有待调度的 PodGroup |
| **Step 8** | `pkg/scheduler/actions/allocate/allocate.go` | `Execute()` | 创建 Statement 事务 |
| **Step 9** | `pkg/scheduler/actions/allocate/allocate.go` | `Execute()` | 尝试为 PodGroup 分配资源 |
| **Step 10** | `pkg/scheduler/actions/allocate/allocate.go` | `Execute()` | 提交 Statement，创建 BindRequest |
| **Step 11** | `pkg/scheduler/actions/allocate/allocate.go` | `Execute()` | 分配失败，丢弃 Statement |
| **Step 12** | `pkg/scheduler/actions/allocate/allocate.go` | `attemptToAllocateJob()` | 尝试为单个 PodGroup 分配资源 |
| **Step 13** | `pkg/scheduler/actions/allocate/allocate.go` | `attemptToAllocateJob()` | 调用 common.AllocateJob |

**关键点**：
- Statement 提供事务机制，确保 Gang 的原子性
- 如果分配失败，Statement 会被丢弃，不会创建任何 BindRequest
- 如果分配成功，Statement 会被提交，为所有 Pod 创建 BindRequest

### 阶段 3：AllocateJob 核心逻辑（步骤 14-26）

| 步骤 | 文件 | 函数 | 说明 |
|------|------|------|------|
| **Step 14** | `pkg/scheduler/actions/common/allocate.go` | `AllocateJob()` | Gang 调度的核心实现 |
| **Step 15** | `pkg/scheduler/actions/common/allocate.go` | `AllocateJob()` | 获取待分配的任务列表 |
| **Step 16** | `pkg/scheduler/actions/common/allocate.go` | `AllocateJob()` | 检查队列容量限制 |
| **Step 17** | `pkg/scheduler/actions/common/allocate.go` | `AllocateJob()` | 递归分配 SubGroupSet |
| **Step 18** | `pkg/scheduler/actions/common/allocate.go` | `allocateSubGroupSet()` | 分配 SubGroupSet（支持层级 Gang） |
| **Step 19** | `pkg/scheduler/actions/common/allocate.go` | `allocateSubGroupSet()` | 根据拓扑约束选择节点子集 |
| **Step 20** | `pkg/scheduler/actions/common/allocate.go` | `allocateSubGroupSet()` | 尝试在每个节点子集上分配 |
| **Step 21** | `pkg/scheduler/actions/common/allocate.go` | `allocateSubGroupSetOnNodes()` | 在指定节点集上分配 SubGroupSet |
| **Step 22** | `pkg/scheduler/actions/common/allocate.go` | `allocateSubGroupSetOnNodes()` | 递归处理子 SubGroupSet |
| **Step 23** | `pkg/scheduler/actions/common/allocate.go` | `allocateSubGroupSetOnNodes()` | 处理子 PodSet（叶子节点） |
| **Step 24** | `pkg/scheduler/actions/common/allocate.go` | `allocatePodSet()` | 为 PodSet 分配资源 |
| **Step 25** | `pkg/scheduler/actions/common/allocate.go` | `allocatePodSet()` | 根据拓扑约束选择节点子集 |
| **Step 26** | `pkg/scheduler/actions/common/allocate.go` | `allocatePodSet()` | 尝试在每个节点子集上分配任务 |

**关键点**：
- 支持层级 Gang 调度（SubGroups）
- 使用 Checkpoint/Rollback 机制确保原子性
- 每一层都必须满足 minMember 约束

### 阶段 4：任务分配（步骤 27-40）

| 步骤 | 文件 | 函数 | 说明 |
|------|------|------|------|
| **Step 27** | `pkg/scheduler/actions/common/allocate.go` | `allocateTasksOnNodeSet()` | 在节点集上分配所有任务 |
| **Step 28** | `pkg/scheduler/actions/common/allocate.go` | `allocateTasksOnNodeSet()` | 遍历所有待分配的任务 |
| **Step 29** | `pkg/scheduler/actions/common/allocate.go` | `allocateTasksOnNodeSet()` | 任务分配失败，记录错误信息 |
| **Step 30** | `pkg/scheduler/actions/common/allocate.go` | `allocateTask()` | 为单个任务分配节点 |
| **Step 31** | `pkg/scheduler/actions/common/allocate.go` | `allocateTask()` | 执行预谓词检查 |
| **Step 32** | `pkg/scheduler/actions/common/allocate.go` | `allocateTask()` | 对节点进行评分排序 |
| **Step 33** | `pkg/scheduler/actions/common/allocate.go` | `allocateTask()` | 遍历节点，尝试分配 |
| **Step 34** | `pkg/scheduler/actions/common/allocate.go` | `allocateTask()` | 尝试将任务分配到节点 |
| **Step 35** | `pkg/scheduler/actions/common/allocate.go` | `allocateTaskToNode()` | 将任务分配到节点 |
| **Step 36** | `pkg/scheduler/actions/common/allocate.go` | `allocateTaskToNode()` | 处理 GPU 共享任务 |
| **Step 37** | `pkg/scheduler/actions/common/allocate.go` | `allocateTaskToNode()` | 处理普通任务 |
| **Step 38** | `pkg/scheduler/actions/common/allocate.go` | `bindTaskToNode()` | 绑定任务到节点（虚拟分配） |
| **Step 39** | `pkg/scheduler/actions/common/allocate.go` | `bindTaskToNode()` | 执行虚拟分配 |
| **Step 40** | `pkg/scheduler/actions/common/allocate.go` | `pipelineTaskToNode()` | Pipeline 任务到节点 |

**关键点**：
- 对节点进行评分排序，选择最优节点
- 支持 GPU 共享和普通任务
- 虚拟分配不会实际修改 Kubernetes，只在内存中模拟

### 阶段 5：错误处理和 Gang 检查（步骤 41-45）

| 步骤 | 文件 | 函数 | 说明 |
|------|------|------|------|
| **Step 41** | `pkg/scheduler/actions/common/allocate.go` | `handleFailedTaskAllocation()` | 处理任务分配失败 |
| **Step 42** | `pkg/scheduler/actions/common/allocate.go` | `handleFailedTaskAllocation()` | 检查是否是 Gang 调度 |
| **Step 43** | `pkg/scheduler/actions/common/allocate.go` | `handleFailedTaskAllocation()` | 根据是否是 Gang 调度，生成不同的错误信息 |
| **Step 44** | `pkg/scheduler/actions/common/allocate.go` | `handleFailedTaskAllocation()` | Gang 调度失败，生成详细的错误信息 |
| **Step 45** | `pkg/scheduler/actions/common/allocate.go` | `isGangScheduling()` | 检查是否是 Gang 调度 |

**关键点**：
- 提供详细的错误信息，说明为什么 Gang 调度失败
- 区分 Gang 调度和普通调度的错误信息
- 帮助用户理解需要多少个 Pod 才能满足 minMember

## 核心文件列表

Gang 调度的核心代码分布在以下文件中：

1. **pkg/scheduler/scheduler.go**
   - 调度器主入口
   - 调度循环
   - 步骤：1-4

2. **pkg/scheduler/actions/allocate/allocate.go**
   - Allocate Action 实现
   - 作业队列管理
   - 步骤：5-13

3. **pkg/scheduler/actions/common/allocate.go**
   - Gang 调度核心逻辑
   - SubGroupSet 和 PodSet 处理
   - 任务分配和错误处理
   - 步骤：14-45

## Gang 调度的关键机制

### 1. Statement 事务机制

```go
// 创建 Statement
stmt := ssn.Statement()

// 创建 Checkpoint（回滚点）
cp := stmt.Checkpoint()

// 尝试分配
if allocate(...) {
    // 成功，提交
    stmt.Commit()
} else {
    // 失败，回滚
    stmt.Rollback(cp)
}
```

**作用**：
- 确保 Gang 的原子性
- 要么全部分配成功，要么全部回滚
- 不会出现部分 Pod 被调度的情况

### 2. 层级 Gang 调度

```
PodGroup (minMember=2)
  ├─ decode (minMember=2)
  │   ├─ decode-workers (minMember=4)
  │   └─ decode-leaders (minMember=1)
  └─ prefill (minMember=2)
      ├─ prefill-workers (minMember=4)
      └─ prefill-leaders (minMember=1)
```

**验证顺序**：
1. 检查根 PodGroup 的 minMember（至少 2 个顶层 SubGroup）
2. 检查 decode 的 minMember（至少 2 个子 SubGroup）
3. 检查 decode-workers 的 minMember（至少 4 个 Pod）
4. 检查 decode-leaders 的 minMember（至少 1 个 Pod）
5. 检查 prefill 的 minMember（至少 2 个子 SubGroup）
6. 检查 prefill-workers 的 minMember（至少 4 个 Pod）
7. 检查 prefill-leaders 的 minMember（至少 1 个 Pod）

### 3. 拓扑感知

```go
// 根据拓扑约束选择节点子集
nodeSets := ssn.SubsetNodesFn(job, subGroup, podSets, tasks, nodes)

// 尝试在每个节点子集上分配
for _, nodeSet := range nodeSets {
    if allocate(nodeSet) {
        return true
    }
}
```

**作用**：
- 支持拓扑约束（例如同一机架、同一区域）
- 将节点按拓扑分组
- 确保 Pod 满足拓扑要求

## 调试技巧

### 1. 查看 Gang 调度日志

```bash
# 查看调度器日志
kubectl logs -n kai-scheduler deployment/kai-scheduler -f | grep -E "Gang|minMember|Allocate"
```

### 2. 查看 PodGroup 状态

```bash
# 查看 PodGroup
kubectl get podgroup -A

# 查看详细信息
kubectl describe podgroup <podgroup-name> -n <namespace>
```

### 3. 查看失败原因

```bash
# 查看 PodGroup 的 status.conditions
kubectl get podgroup <podgroup-name> -n <namespace> -o yaml | grep -A 20 status
```

## 常见问题

### Q1: 为什么我的 PodGroup 一直处于 Pending 状态？

**可能原因**：
1. 集群资源不足，无法满足 minMember
2. 队列配额不足
3. 拓扑约束无法满足
4. 节点亲和性配置错误

**调试方法**：
- 查看 PodGroup 的 status.conditions
- 搜索步骤 44 的代码，查看错误信息生成逻辑
- 检查队列配额和集群资源

### Q2: 如何理解 Checkpoint/Rollback 机制？

**答案**：
- Checkpoint 创建一个回滚点，记录当前状态
- 如果后续分配失败，Rollback 会恢复到 Checkpoint 时的状态
- 这确保了 Gang 的原子性：要么全部成功，要么全部失败
- 参考步骤 20 的代码

### Q3: 层级 Gang 调度是如何工作的？

**答案**：
- 从根 PodGroup 开始，递归处理每一层 SubGroup
- 每一层都必须满足自己的 minMember 约束
- 如果任何一层失败，整个分配失败
- 参考步骤 17-23 的代码

## 总结

通过搜索 `+GangSchedulingStep`，你可以：

1. **按顺序追踪**：从步骤 1 到步骤 45，完整了解 Gang 调度流程
2. **理解关键机制**：Statement 事务、层级 Gang、拓扑感知
3. **调试问题**：根据步骤编号，快速定位问题所在
4. **学习代码**：每个步骤都有详细的注释，解释了代码的作用

建议按照以下顺序学习：
1. 先看步骤 1-13，了解整体流程
2. 再看步骤 14-26，理解 Gang 调度的核心逻辑
3. 最后看步骤 27-45，了解任务分配的细节

祝你学习愉快！


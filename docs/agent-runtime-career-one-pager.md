# Agent Runtime 学习与职业发展一页纸

## 定位

这份路线适合有基础设施、分布式系统、存储或平台工程背景的开发者。目标不是成为 Prompt Engineer，而是逐步转向：

```text
Agent Developer -> Agent Runtime Engineer -> Agent Platform Architect
```

已有的工程经验可以迁移到 Runtime、Workflow、状态恢复、工具执行、人工审批、安全控制和平台化能力上。学习重点应放在如何把 Agent 做成可靠的生产系统，而不是只关注提示词技巧。

## 核心判断

LangGraph 不只是 Node / Edge / State / Pregel，而是 Agent Runtime。

类比：

```text
Java -> JVM
Flink -> Runtime
Airflow -> Scheduler
Temporal -> Workflow Runtime
LangGraph -> Agent Runtime
```

未来 2~3 个月的学习重点：

- Durable Execution：任务中断后可以恢复，而不是从头重跑。
- Persistence：checkpoint 是为了 fault tolerance、resume 和 long running workflow。
- Thread：`thread_id` 是 Workflow Instance ID，类似 Airflow DagRun / Temporal WorkflowID。
- Interrupt：危险操作前暂停，等待人工审批，再继续执行。
- Subgraph：把复杂任务拆成数据库、Kubernetes、存储、监控等子 Agent。
- Multi Agent：用 Supervisor 编排多个专业 Agent 完成 RCA 或自动恢复。

## 三个月路线

### Month 1：掌握 Agent Runtime 基础

目标：理解 LangGraph 的运行时能力，而不是只会写 demo。

必须完成：

- Durable Execution demo
- Persistence + checkpointer demo
- `thread_id` resume demo
- `interrupt()` 人工审批 demo
- Subgraph demo

本月不要投入：

- Pregel 源码
- Prompt Engineering
- 复杂 RAG
- Agent 论文

### Month 2：做 Database RCA Agent

目标：把 Agent 用到真实基础架构诊断。

输入：

```text
数据库延迟高
```

自动诊断：

```text
实例状态 -> 慢查询 -> iostat -> 监控指标 -> Kubernetes -> 综合分析
```

输出：

- Compaction
- 热点 Key / 热点请求
- 内存或缓存压力
- IO 瓶颈
- CPU 饱和
- 网络抖动
- Pod / Node 资源争用

交付标准：

- 有 Planner / Executor / Reporter。
- 工具边界清晰。
- 状态可持久化。
- 任务可恢复。
- RCA 报告有 evidence、confidence、recommended actions。

### Month 3：做 Kubernetes Node Maintenance Agent

目标：从诊断走向带审批的自动恢复。

流程：

```text
发现异常节点 -> 诊断 -> 人工审批 -> Drain -> Pod 迁移 -> 恢复 -> 验证
```

必须具备：

- 所有危险操作前 `interrupt()`。
- approve / reject / timeout 三条路径。
- Drain / Taint / Evict 动作可重入。
- 记录审批结果、审批时间、执行动作和恢复结果。
- 输出恢复后验证报告。

## 项目组合

优先做这三个项目：

1. Resource Eviction Approval Agent  
   用来掌握 interrupt、human approval、resume。

2. Database RCA Agent  
   用来掌握 Plan & Execute、tool calling、multi-step diagnosis、structured report。

3. Kubernetes Node Maintenance Agent  
   用来掌握 multi-agent、危险操作审批、幂等执行、审计和恢复验证。

这三个项目完成后，再研究 Temporal、LangSmith、OpenTelemetry、Agent Platform、多租户 Runtime、权限和执行沙箱。

## 能力栈地图

需要补齐的不是 prompt 技巧，而是下面这些平台能力：

- Workflow：状态机、长任务、重试、恢复、分支。
- Runtime：checkpoint、resume、interrupt、thread、durable execution。
- Tooling：数据库、Kubernetes、监控系统、Linux command、workflow scheduler。
- Safety：审批、权限、幂等、dry-run、审计。
- Observability：日志、trace、metrics、执行历史、证据链。
- Architecture：Supervisor、Subgraph、Multi Agent、Agent Platform。

## 每周自检

每周结束时检查：

- 这周有没有一个能跑的 demo？
- 它有没有 checkpoint？
- 它能不能用同一个 `thread_id` resume？
- 它有没有清晰的 state？
- 它有没有真实工具或可替换 mock？
- 危险动作前有没有人工审批？
- 结果能不能映射到数据库、Kubernetes 或节点健康类问题？
- 这周的产出是否能放进未来的 Agent Platform？

## 最终目标

三个月后，应该能够清楚地设计并实现：

```text
Airflow + Temporal + 状态机 + LLM
```

这种形态的基础架构 Agent Runtime，而不是只会写聊天机器人。

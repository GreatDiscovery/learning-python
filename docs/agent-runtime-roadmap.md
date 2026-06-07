# Agent Runtime 2~3 个月学习路线

## 1. 背景与目标

这份路线面向有基础设施、分布式系统、存储或平台工程背景的开发者，例如：

- Redis 或其他数据库系统
- RocksDB
- Kubernetes
- Airflow 或其他 workflow scheduler
- Java / Go / Python
- 分布式系统
- 存储引擎
- 故障诊断

学习 Agent 的主线不应该是 Prompt Engineering，而应该是 Runtime、状态、恢复、调度、工具执行和生产化平台能力。

目标路径：

```text
Agent Developer
↓
Agent Runtime Engineer
↓
Agent Platform Architect
```

## 2. 对 LangGraph 的正确理解

不要把 LangGraph 只理解成：

```text
Node
Edge
State
Pregel
```

这些只是实现和建模细节。更重要的是把它理解成：

```text
Agent Runtime
```

类比：

```text
Java -> JVM
Flink -> Runtime
Airflow -> Scheduler
LangGraph -> Agent Runtime
```

核心能力：

- Durable Execution
- Persistence
- Interrupt
- Human In The Loop
- Resume
- Long Running Workflow
- Multi Agent

重点官方文档：

- [LangGraph Overview](https://docs.langchain.com/oss/python/langgraph/overview)
- [Durable Execution](https://docs.langchain.com/oss/python/langgraph/durable-execution)
- [Persistence](https://docs.langchain.com/oss/python/langgraph/persistence)
- [Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)
- [Subgraphs](https://docs.langchain.com/oss/python/langgraph/use-subgraphs)

## 3. Month 1：Agent Runtime 基础

目标：理解 LangGraph 为什么是 Agent Runtime，而不是聊天机器人框架。

### Week 1：Durable Execution / Persistence / Thread

学习目标：

- 理解 Durable Execution：执行到一半进程挂掉，恢复后继续执行，而不是从头重跑。
- 理解 Persistence：`graph.compile(checkpointer=...)` 不是为了聊天记忆，而是为了 checkpoint、fault tolerance、resume。
- 理解 Thread：`thread_id` 本质是 Workflow Instance ID，类似 Airflow DagRun 或 Temporal WorkflowID。

不要做：

- 不要碰 Pregel 源码。
- 不要陷入 Node / Edge / State 的语法细节。

交付物：

- 写一个最小 LangGraph workflow。
- 加 checkpointer。
- 模拟执行中断或进程重启。
- 用同一个 `thread_id` 恢复执行。

### Week 2：Human In The Loop / Interrupt

学习目标：

- 理解 `interrupt()` 的语义：暂停当前 workflow，等待外部输入，然后继续执行。
- 把人工审批建模为 runtime 级能力，而不是 prompt 技巧。

标准流程：

```text
Agent
↓
发现危险操作
↓
interrupt()
↓
等待人工确认
↓
resume
↓
继续执行
```

实践项目：Resource Eviction Approval Agent。

流程：

```text
分析服务状态
↓
准备执行高风险操作
↓
interrupt()
↓
人工批准
↓
继续执行
```

交付物：

- 一个带人工审批的资源操作 Agent demo。
- 明确记录 interrupt 前后的 state。
- 明确区分 approve / reject 两条路径。

### Week 3：Agent Pattern

目标：研究工作流，不研究 Prompt。

重点模式：

```text
ReAct:
Thought -> Tool -> Observation -> Thought
```

```text
Reflection:
生成答案 -> 自我检查 -> 修正
```

```text
Plan & Execute:
Planner -> Task1 / Task2 / Task3 -> Executor
```

理解重点：

- ReAct 解决工具调用闭环。
- Reflection 解决结果校验和修正。
- Plan & Execute 解决复杂任务拆解，是 Deep Research 的核心模式之一。

交付物：

- 用数据库 RCA 场景实现一个 Plan & Execute demo。
- Planner 只负责生成诊断计划。
- Executor 负责执行实例状态、慢查询、系统指标等工具。
- 最终输出结构化 RCA 结论。

### Week 4：Subgraph

目标：理解 Subgraph 如何表达复杂系统中的子诊断域。

结构示例：

```text
Main Agent
├── Database Agent
├── Kubernetes Agent
├── Storage Agent
└── Monitoring Agent
```

类比：

```text
Airflow SubDAG
```

但 LangGraph 的 Subgraph 更适合表达有状态、可恢复、可组合的 Agent workflow。

交付物：

- Main Agent 负责路由和汇总。
- Database Agent 负责实例状态、慢查询、热点请求、缓存压力分析。
- Kubernetes Agent 负责 Pod / Node / Event / Resource 分析。
- Monitoring Agent 负责指标查询和趋势判断。

## 4. Month 2：Database RCA Agent

目标：真正做一个能解决基础架构问题的 Agent。

输入：

```text
数据库延迟高
```

自动诊断链路：

```text
实例状态
↓
慢查询
↓
iostat
↓
监控指标
↓
综合分析
```

输出候选原因：

- Compaction
- 热点 Key / 热点请求
- 内存或缓存压力
- IO 瓶颈
- CPU 饱和
- 网络抖动
- Pod / Node 资源争用

建议设计：

```text
Supervisor
├── Database Inspector
├── IO Inspector
├── CPU Inspector
├── Memory Inspector
├── Kubernetes Inspector
└── Report Writer
```

交付物：

- 工具层：数据库、Kubernetes、监控系统、节点命令查询 mock 或通用接入。
- Runtime 层：checkpoint、thread_id、resume。
- Workflow 层：Plan & Execute + Reflection。
- 输出层：结构化 RCA 报告。

RCA 报告建议格式：

```text
Summary
Evidence
Timeline
Root Cause Candidates
Confidence
Recommended Actions
Risk
Need Human Approval
```

## 5. Month 3：Kubernetes Node Maintenance Agent

目标：从诊断 Agent 进化到带审批和执行能力的恢复 Agent。

### 项目 1：Kubernetes Node Health Agent

输入：

```text
节点异常
```

自动分析：

```text
NPD
↓
SMART
↓
dmesg
↓
Pod 分布
↓
资源与故障历史
↓
分析
```

输出：

- 建议维护或替换节点
- 建议 Drain
- 建议 Taint
- 建议继续观察
- 建议人工介入

### 项目 2：Node Maintenance Agent

结合 workflow scheduler：

```text
发现异常节点
↓
诊断
↓
人工审批
↓
Drain
↓
Pod 迁移
↓
恢复
↓
验证
```

重点能力：

- Interrupt：危险操作前暂停审批。
- Resume：审批后继续，不重跑前面的诊断。
- Persistence：保存诊断证据、审批结果、执行状态。
- Idempotency：Drain / Taint / Evict 等动作必须可重入。
- Audit：记录审批结果、审批时间、执行动作和恢复结果。

交付物：

- 一个完整 Node Maintenance workflow。
- 至少支持 approve / reject / timeout 三种审批结果。
- 所有危险操作前必须 interrupt。
- 输出恢复后验证报告。

## 6. 必读开源仓库与课程

### 官方仓库

GitHub：

```text
langchain-ai/langgraph
```

重点关注：

- persistence
- interrupt
- subgraph
- multi-agent

仓库地址：

- [langchain-ai/langgraph](https://github.com/langchain-ai/langgraph)

### LangGraph 101

重点关注：

- 基础建模
- Agent
- Multi Agent

建议用法：

- 不要只跑 demo。
- 把 demo 改成 Redis / K8s / Node Health 场景。

### Deep Agents

来源：LangGraph Academy。

重点关注：

- Deep Research 背后的 Plan & Execute。
- 多步骤工具调用。
- 结果反思与修正。
- 长任务状态管理。

### Interrupt Template

仓库：

- [langgraph-interrupt-workflow-template](https://github.com/KirtiJha/langgraph-interrupt-workflow-template)

重点学习：

```text
Pause
Resume
Human Approval
```

## 7. 暂时不要学的东西

优先级低：

```text
Pregel 源码
Prompt Engineering
复杂 RAG
Agent 论文
```

原因：这些内容短期内对基础架构 Agent 落地收益不高。当前阶段更应该把时间投到 runtime、checkpoint、interrupt、resume、workflow、tooling、idempotency 和 observability 上。

## 8. 三个月后的进阶方向

完成 Database RCA Agent、Kubernetes Node Health Agent、Node Maintenance Agent 后，再开始系统研究：

- Temporal
- LangSmith
- OpenTelemetry
- Agent Platform
- 多租户 Agent Runtime
- Agent 权限与审计
- Agent 执行沙箱
- Agent 任务队列与调度

到这个阶段，LangGraph 会更像：

```text
Airflow + Temporal + 状态机 + LLM
```

而不是聊天机器人框架。

## 9. 每周检查清单

每周结束时检查：

- 有没有一个能跑的 demo？
- 有没有 checkpoint？
- 有没有 thread_id？
- 有没有 resume？
- 有没有结构化 state？
- 有没有清晰的工具边界？
- 有没有危险操作审批？
- 有没有执行日志和审计信息？
- 有没有把结果映射回真实 Redis / K8s / Node 问题？

## 10. 最小项目结构建议

后续代码可以按下面方式组织：

```text
learning-python/
├── docs/
│   └── agent-runtime-roadmap.md
├── test/
│   └── ai/
│       ├── langgraph_test.py
│       ├── durable_execution_demo.py
│       ├── redis_eviction_agent.py
│       ├── redis_rca_agent.py
│       └── k8s_node_health_agent.py
└── notes/
    ├── langgraph-runtime-notes.md
    ├── interrupt-notes.md
    └── production-agent-platform-notes.md
```

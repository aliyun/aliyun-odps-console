# MaxC CLI 实施说明（2026-03）

这份文档服务于当前仓库开发。

- [design.md](./design.md) 负责表达产品愿景、路线图和目标接口形态。
- [product-positioning.md](./product-positioning.md) 负责回答“这个 CLI 最该先成为什么产品”。
- [base-api-roadmap.md](./base-api-roadmap.md) 负责回答“基础 API 下一步先补什么”。
- 本文负责定义“当前能做什么、不能做什么、真实 MaxCompute 怎么映射”。
- 两者冲突时，编码和联调阶段以本文为准。

## 1. 当前开发基线

| 范围 | 状态 | 说明 |
| --- | --- | --- |
| `query` | 已增强 | 支持真实 MaxCompute / mock fallback、`cost`、`explain`、cursor 分页 |
| `job submit/status/wait/result/cancel/list` | 已实现 | 真实 backend 直接映射 MaxCompute instance |
| `meta list-tables/describe/search/search-columns/partitions` | 已增强 | `describe` 已补 owner / 时间 / table_type / partition_columns 等字段 |
| `data sample/profile` | 已实现 | 基于真实表 schema 和样例数据 |
| `agent context` | 已实现 | 输出当前项目、可用表、可用 skill |
| `agent skill` | 已实现 | 当前支持 builtin query / meta.describe / data.sample |
| `skill list/info` | 已实现 | 本地 skill 目录 |
| `@natural` | 规划中 | Q2，未实现 |
| `agent plan` / `agent run` | 规划中 | Q2，未实现 |
| `Human-in-the-Loop` 审批流 | 规划中 | 当前用文档约束和审计日志替代 |
| `Skill Registry install/publish` | 未开始 | Q2/Q3 |
| `auth` / `resource` / `data upload/download` | 未开始 | 不在当前开发基线 |
| `meta.lineage` 真实血缘 | 未开始 | 当前返回空数组占位 |

## 2. 真实 MaxCompute 对接约定

### 2.1 环境变量

当前真实 backend 优先从环境变量读取连接信息：

- `ALIBABA_CLOUD_ACCESS_KEY_ID`
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`
- `MAXCOMPUTE_PROJECT`
- `MAXCOMPUTE_ENDPOINT`
- `MAXCOMPUTE_REGION`（可选）
- `MAXCOMPUTE_TUNNEL_ENDPOINT`（可选）

如果这些变量不完整，CLI 自动回退到 mock backend。

### 2.2 字段映射

| 设计抽象 | 当前实现 | 来源 |
| --- | --- | --- |
| `job_id` | MaxCompute instance id | `instance.id` |
| `submitted_at` | 实例开始时间 | `instance.start_time` |
| `completed_at` | 实例结束时间 | `instance.end_time` |
| `bytes_scanned` | 输入数据量 | `task_cost.input_size` 或 `SQLCost.input_size` |
| `task_cost_cpu` | CPU 成本 | `instance.get_task_cost().cpu_cost` |
| `task_cost_memory` | Memory 成本 | `instance.get_task_cost().memory_cost` |
| `estimated_input_size_bytes` | 预估输入大小 | `task_cost.input_size` 或 `SQLCost.input_size` |
| `cost_cu` | 当前为空 | MaxCompute 未直接暴露统一 CU 口径 |
| `logview` | 调试链接 | `instance.get_logview_address()` |

## 3. 成本模型说明

当前不要把 `cost_cu` 当成 MaxCompute 的原生能力。

- 对真实 MaxCompute，优先使用 `task_cost_cpu`、`task_cost_memory`、`estimated_input_size_bytes`
- `query --dry-run` 当前走 `execute_sql_cost`，适合做预估
- `--cost-check` 只有在 backend 提供统一成本口径时才适合启用
- 如果后续业务需要统一 `CU` 抽象，可以把 `cost_cu` 作为兼容字段补回来

## 4. Phase A 已落地能力

当前已经实现并经过 mock 测试覆盖的基础 API 增强包括：

- `maxc query cost "SELECT ..."`
- `maxc query explain "SELECT ..."`
- `maxc query --page-size N --cursor <token>`
- `maxc meta search-columns <keyword>`
- richer `maxc meta describe`

## 5. 文档化的已知缺口

这些内容当前做不到，必须显式记录，而不是隐含在设计图里：

- MaxCompute 真实血缘 API 还没接入，`meta.lineage` 目前只能占位
- `@natural` 依赖独立 AgentAPI / NL2SQL 服务，不能当作纯 CLI 内建能力默认存在
- `agent plan` / `agent run` 依赖明确的计划 DSL、审批模型和外部副作用策略，当前只适合保留接口位
- `Human-in-the-Loop` 需要单独的交互协议和审计模型，不应只靠一个 CLI 示例图表达
- Skill 的成本 guard 需要区分“预执行可判断”和“执行后可观测”，不能只写一个抽象字段
- 真实 backend 的 `query explain` 当前是 `execute_sql_cost` + query outline 的结构化包装，不是完整执行计划树
- 真实 backend 预执行阶段拿不到 `task_cost_cpu` / `task_cost_memory`，只能返回 `estimated_input_size_bytes`、复杂度和 UDF 数量
- `--cursor` 当前是 CLI 侧 offset token，不是 MaxCompute 原生 server-side cursor
- `meta search-columns` 当前通过遍历可见表 schema 实现，在超大 catalog 下可能偏慢

## 6. 建议补齐的后续文档

为了让后续开发更稳，建议继续补三份文档：

1. `docs/agent-api.md`
   只描述 `@natural`、`agent plan`、`agent run` 依赖的外部 AgentAPI 协议。
2. `docs/skill-contract.md`
   只描述 Skill 输入输出契约、guard 语义、builtin target 和 compose 规则。
3. `docs/cost-policy.md`
   只描述成本模型、审批阈值和 `cost_cu` 与 `task_cost_*` 的映射策略。

产品定位和基础 API 路线图已经单独沉淀在：

- [product-positioning.md](./product-positioning.md)
- [base-api-roadmap.md](./base-api-roadmap.md)

## 7. 当前开发原则

- 先保证 `query / job / meta / data` 在真实 MaxCompute 上可用
- 再把 `agent context / agent skill` 做成稳定骨架
- 需要外部服务、审批流、统一计费抽象的能力，一律先写清接口和约束，再实现

# MaxC CLI 基础 API 路线图

## 1. 路线图目标

这份路线图只讨论不依赖 `maxc agent` 的基础能力。

目标不是“让 CLI 看起来更像 agent”，而是让它更适合被：

- 外部 LLM 调用
- CI / Bot 调用
- shell / Python 自动化调用
- 人工交互调试

## 2. 总体原则

### 2.1 先做读路径，再做高级编排

优先顺序：

1. `query`
2. `job`
3. `meta`
4. `data`
5. `auth`
6. 其它高级能力

### 2.2 先做观测性，再做智能性

基础 CLI 的第一职责不是“替你思考”，而是“把系统状态清晰地交给会思考的外部 agent”。

因此优先级应当向这些能力倾斜：

- 结构化输出
- 失败原因
- 成本/风险预估
- 权限和范围边界
- 大结果集处理

### 2.3 优先构建低风险高频场景

最优先服务的场景是：

- 只读分析
- 事故排查
- CI 审查

而不是：

- 自动写表
- 自动审批
- 多系统副作用编排

## 3. 当前能力快照

| 模块 | 当前状态 | 可直接支撑的场景 | 主要缺口 |
| --- | --- | --- | --- |
| `query` | 已增强 | 只读分析、快速验证 SQL、预估成本、分页读取 | 导出 / 文件输出 |
| `job` | 可用 | 长查询、异步执行 | 失败诊断、阶段进度、重试提示 |
| `meta` | 已增强 | 找表、找列、看 schema、看 richer describe | freshness、真实 lineage |
| `data` | 基本可用 | 采样、快速理解数据 | 分区采样、diff、质量规则 |
| `auth` | 缺失 | 无 | whoami、can-i |
| `agent context` | 可用 | 给外部 agent 注入上下文 | 需要更精简的上下文摘要 |

## 4. P0：必须完成

这些能力决定 `maxc cli` 能不能成为真实可用的 Agent 执行底座。

### 4.1 查询层

#### `query explain`

建议新增独立命令：

```bash
maxc query explain "SELECT ..."
```

至少返回：

- SQL operation type
- tables used
- estimated input size
- task cost estimate
- warnings

原因：

- 外部 agent 需要在执行前判断风险
- CI 需要在执行前判定是否允许
- 不能把这部分信息只埋在 `query --dry-run` 的副结果里

#### `query cost`

建议新增独立命令：

```bash
maxc query cost "SELECT ..."
```

返回统一的成本对象：

- `estimated_input_size_bytes`
- `task_cost_cpu`
- `task_cost_memory`
- `cost_model`

其中 `cost_model` 应明确说明：

- `maxcompute_native`
- `maxc_derived`

这样后续即使补 `cost_cu`，也不会和原生字段混淆。

#### 分页与大结果处理

建议补：

- `--page-size`
- `--cursor`
- `--output`
- `--output-format`

原因：

- 结构化查询只靠 `max_rows` 不足以支撑真实分析场景
- 外部 agent 不应该被迫一次吃下整个结果集

### 4.2 任务层

#### 统一任务状态模型

现在的 `job` 已可用，但还缺少“足够好用的诊断层”。

建议 `job status` / `job wait` 输出补齐：

- `stage`
- `retryable`
- `failure_reason`
- `logview`
- `task_summary`

#### 失败诊断

建议新增：

```bash
maxc job diagnose <job_id>
```

至少归一化三类问题：

- SQL 语义错误
- 权限错误
- 资源 / 配额 / 超时错误

### 4.3 元数据层

#### 列级搜索

建议新增：

```bash
maxc meta search-columns "gmv"
```

原因：

- 外部 agent 首先是在找字段，不是在找表
- 只做表级搜索会明显影响分析效率

#### 最新分区与新鲜度

建议新增：

```bash
maxc meta latest-partition <table>
maxc meta freshness <table>
```

这是事故排查和日报守护的核心基础能力。

#### 更丰富的 describe

建议把这些字段稳定输出：

- owner
- created_at
- updated_at
- table_type
- row_count_source
- partition_columns

### 4.4 数据层

#### 分区采样

建议 `data sample` 支持：

- `--partition`
- `--columns`
- `--rows`

#### 轻量 profile

当前 `data profile` 只是起点，建议逐步补：

- null ratio
- distinct count
- top values
- numeric min/max
- partition-scoped profile

## 5. P1：强烈建议尽快补

### 5.1 权限感知

建议新增：

```bash
maxc auth whoami
maxc auth can-i --table xxx --operation SELECT
```

原因：

- 外部 agent 在执行前必须知道自己是谁
- 权限提示不能总等后端报错

### 5.2 真实 lineage

建议补齐：

```bash
maxc meta lineage <table>
```

只要进入事故排查和影响分析场景，lineage 很快就会成为高频能力。

### 5.3 diff 类能力

建议新增：

- `data diff`
- `schema diff`
- `partition diff`

这类命令特别适合 CI 和巡检机器人。

## 6. P2：先明确接口，不急着实现

这些能力要保留设计，但不建议抢在 P0/P1 之前开工：

- `@natural`
- `agent plan`
- `agent run`
- Human-in-the-Loop 审批流
- Skill Registry

理由：

- 它们对基础 API 的质量依赖极高
- 它们引入的交互复杂度远高于基础命令
- 现在先做容易把仓库拉向“概念很大、底层很薄”

## 7. 和杀手场景的映射

| 杀手场景 | 最关键的基础能力 |
| --- | --- |
| 只读分析助手 | `meta search`、`describe`、`sample`、`query`、分页 |
| 数据事故排查助手 | `latest-partition`、`freshness`、`job status`、`job diagnose`、`lineage` |
| SQL/数据开发 CI 审查器 | `query explain`、`query cost`、`auth can-i`、`schema diff` |

如果一个新增命令无法明显增强这三类场景中的至少一类，优先级就不应太高。

## 8. 推荐实施顺序

### Phase A：把只读分析做厚

当前状态（2026-03-20）：

- 已实现：`query explain`
- 已实现：`query cost`
- 已实现：分页 / cursor
- 已实现：列级搜索
- 已实现：更丰富的 describe
- 未实现：`--output`、`--output-format`

- `query explain`
- `query cost`
- 分页 / cursor
- 列级搜索
- 更丰富的 describe

### Phase B：把事故排查做深

- latest partition
- freshness
- job diagnose
- task summary
- 真实 lineage

### Phase C：把自动化接入做稳

- auth whoami / can-i
- diff 系列
- 导出 / 文件输出
- 更稳定的 machine-readable contract

### Phase D：再进入 agent 能力

- `@natural`
- `agent plan`
- `agent run`
- 审批流

## 9. 完成判定

基础 API 达到“可做杀手应用”的最低标准，应至少满足：

1. 外部 agent 可以独立完成一次只读分析闭环
2. Bot 可以独立完成一次数据事故的一线排查
3. CI 可以独立阻断明显危险的 SQL 变更
4. 用户不需要理解 MaxCompute 底层对象细节，也能消费大部分 CLI 输出

在达到这四点之前，不建议把主要精力转去做高级 agent 能力。

# MaxC CLI 实施说明（2026-03）

这份文档描述当前仓库“已经实现什么、没有实现什么、真实 MaxCompute 如何接入”。

- [design.md](./design.md) 负责表达产品定位和目标接口
- [product-positioning.md](./product-positioning.md) 负责回答“为什么先做工具层”
- [roadmap.md](./roadmap.md) 负责回答“下一步优先做什么”
- 本文负责定义当前代码的真实行为；如果文档之间冲突，以本文和实际代码为准

## 1. 当前开发基线

| 范围 | 状态 | 说明 |
| --- | --- | --- |
| `query` | 已增强 | 支持真实 MaxCompute、`cost`、`explain`、cursor 分页 |
| `job submit/status/wait/result/cancel/list/diagnose` | 已实现 | 已补 `stage` / `retryable` / `failure_reason` / `logview` / `task_summary` |
| `meta list-tables/describe/search/search-columns/partitions/latest-partition/freshness/lineage` | 已增强 | `lineage` 在真实 backend 返回明确的 unsupported 契约 |
| `data sample/profile` | 已实现 | `sample` 支持 `--partition` / `--columns` / `--rows`，`profile` 支持 `--partition` |
| `auth login` | 已实现 | 支持写入配置文件，并可选远程校验 |
| `auth whoami/can-i` | 已实现 | `whoami` 输出脱敏身份摘要，`can-i` 支持表级 `SELECT` 预检 |
| `diff schema/partition/data` | 已实现 | `data` 为 keyed snapshot compare |
| `agent context` | 已实现 | 输出当前项目、backend、安全约束和本地上下文摘要，不列举 tables |
| `cache build/status/clear/save-semantic/get-semantic` | 已实现 | 覆盖元数据缓存与语义缓存 |
| `@natural` | 规划中 | 未实现 |
| `agent plan` / `agent run` | 已移除 | 当前工作树不再暴露这些命令 |
| `skill list/info` / `agent skill` | 已移除 | Skill 文档改为外部 Agent 直接读取 `skills/use-maxc-cli/` 发布产物 |
| `meta.lineage` 真实血缘 | 未开始 | 真实 backend 返回 `supported=false` 的明确占位结果 |

## 2. 安装与依赖

当前基础依赖已经包含：

- `pyodps`
- `pandas`
- `PyYAML`

仓库内安装：

```bash
python -m pip install -e .
```

发布后安装：

```bash
python -m pip install maxc-cli
```

## 3. 真实 MaxCompute 对接约定

### 3.1 登录与配置来源

当前真实 backend 支持两类来源：

1. 环境变量
2. `maxc auth login` 写入的配置文件 `auth` 段

环境变量优先级高于配置文件。

`auth login` 默认写入：

```text
~/.maxc/config.yaml
```

也可以通过顶层 `--config` 指定写入目标。

### 3.2 环境变量

当前实现接受这些主变量：

- `ALIBABA_CLOUD_ACCESS_KEY_ID`
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`
- `MAXCOMPUTE_PROJECT`
- `MAXCOMPUTE_ENDPOINT`
- `MAXCOMPUTE_REGION`（可选）
- `MAXCOMPUTE_TUNNEL_ENDPOINT`（可选）

兼容别名：

- `ODPS_ACCESS_ID`
- `ODPS_ACCESS_KEY`
- `ODPS_ACCESS_KEY_SECRET`
- `ACCESS_KEY_ID`
- `ACCESS_KEY_SECRET`
- `ODPS_PROJECT`
- `ODPS_ENDPOINT`
- `odps_endpoint`
- `ALIBABA_CLOUD_REGION`
- `ODPS_TUNNEL_ENDPOINT`

### 3.3 配置文件格式

`auth login` 当前写入的关键结构：

```yaml
auth:
  access_id: "<access_key_id>"
  secret_access_key: "<access_key_secret>"
  project: "<project>"
  endpoint: "http://service.<region>.maxcompute.aliyun.com/api"
  region_name: "<region>"
  tunnel_endpoint: "<optional_tunnel_endpoint>"

default_project: "<project>"
default_region: "<region>"

backend:
  type: auto
```

### 3.4 backend 选择规则

- 当前工作树以真实 MaxCompute 为目标
- `backend.type=auto` 且认证配置完整时，走 `odps`
- 缺少认证时，CLI 返回结构化引导或校验失败，不再回退到运行时 mock backend

## 4. 认证命令语义

### 4.1 `auth login`

- 支持参数传入或 `--from-env`
- 缺少必填字段时，在交互终端中会提示补齐
- `--no-validate` 只保存，不做远程校验
- 默认会把 YAML 文件权限尽量收敛到 `0600`

### 4.2 `auth whoami`

`auth whoami --json` 的规范化输出位于 `data.identity`。

关键字段：

- `authenticated`
- `configured`
- `validation_status`
- `backend=odps`
- `identity_source=environment | config_file | mixed | unknown`
- `principal_display` 为远端 DisplayName 或 access_id 脱敏值

当前实现会在配置存在时执行远端 security `whoami` 探测：

- `authenticated=true` 且 `validation_status=verified`：远端探测成功
- `authenticated=false` 且 `configured=true`：配置存在，但远端探测失败
- `authenticated=false` 且 `configured=false`：缺少必需认证配置

### 4.3 `auth can-i`

- 当前只支持表级 `SELECT` 预检
- 检查方式是：
  - 表元数据可见性
  - `SELECT * FROM table LIMIT 0` 的 SQLCost 探测

## 5. 真实 MaxCompute 字段映射

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

## 6. 已落地能力清单

- `maxc auth login`
- `maxc auth whoami`
- `maxc auth can-i --table <table> --operation SELECT`
- `maxc query cost "SELECT ..."`
- `maxc query explain "SELECT ..."`
- `maxc query --page-size N --cursor <token>`
- `maxc query --output file --output-format json|csv|ndjson|table`
- `maxc meta search-columns <keyword>`
- richer `maxc meta describe`
- `maxc meta latest-partition <table>`
- `maxc meta freshness <table>`
- `maxc meta lineage <table>`
- `maxc diff schema <left_table> <right_table>`
- `maxc diff partition <left_table> <right_table>`
- `maxc diff data <left_table> <right_table> --keys id`
- `maxc data sample <table> --partition <spec> --columns <col1,col2> --rows <n>`
- `maxc data profile <table> --partition <spec>`
- `maxc job diagnose <job_id>`

## 7. 已知缺口

- MaxCompute 真实血缘 API 还没接入，`meta.lineage` 当前通过 `supported=false`、`coverage=unsupported`、`limitation` 明确表达限制
- `@natural` 依赖外部 AgentAPI / NL2SQL 服务，当前不在 CLI 内建能力里
- 真实 backend 的 `query explain` 当前是 `execute_sql_cost` + query outline 的结构化包装，不是完整执行计划树
- 真实 backend 预执行阶段拿不到 `task_cost_cpu` / `task_cost_memory`，只能返回 `estimated_input_size_bytes`、复杂度和 UDF 数量
- `--cursor` 当前是 CLI 侧 offset token，不是 MaxCompute 原生 server-side cursor
- `meta search-columns` 当前通过遍历可见表 schema 实现，在超大 catalog 下可能偏慢
- `meta latest-partition` 在真实 backend 中优先尝试 `get_max_partition`；若不可用则退化为遍历可见分区推断
- `meta freshness` 当前使用统一启发式阈值：`<=36h` 视为 `fresh`，`<=72h` 视为 `lagging`，更久视为 `stale`
- `auth whoami` 优先返回真实 security `whoami` 的 DisplayName；拿不到时才退回 access_id 脱敏摘要
- `auth login` 会把 AccessKey 明文写入本地 YAML；虽然 CLI 会尝试收敛权限，但这仍然是需要用户接受的本地存储模型
- 环境变量优先于 `auth login` 保存的配置；如果 shell 中已有变量，它们会覆盖配置文件值
- `diff schema`、`diff partition`、`diff data` 当前都只比较同一 project 下两张表
- `diff data` 当前是 keyed snapshot compare：每侧最多读取 `--rows` 行，只适合只读快照比对，不是全表 exhaustive diff
- `data sample --partition` 在真实 backend 当前直接下推为只读 SQL 采样；如果分区语义需要更严格预检，仍需后续增强
- `job diagnose` 当前主要基于 task result 文本做错误归类；更细粒度的执行计划级诊断仍可继续增强

## 8. 当前开发原则

- 先保证 `query / job / meta / data / auth / diff / cache` 在真实 MaxCompute 上可用
- `auth login` 是 bootstrap 命令，不应依赖当前 backend 已经可用
- Skill 文档由外部 Agent 直接读取发布后的 `use-maxc-cli` skill 目录
- 需要外部服务、审批流、统一计费抽象的能力，一律先写清接口和约束，再实现

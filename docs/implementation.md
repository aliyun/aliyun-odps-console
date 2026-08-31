# MaxC CLI 实施说明（2026-08）

这份文档描述当前仓库”已经实现什么、没有实现什么、真实 MaxCompute 如何接入”。

- [design.md](./design.md) 负责表达产品定位和目标接口
- [product-positioning.md](./product-positioning.md) 负责回答“为什么先做工具层”
- [roadmap.md](./roadmap.md) 负责回答“下一步优先做什么”
- 本文负责定义当前代码的真实行为；如果文档之间冲突，以本文和实际代码为准

## 1. 当前开发基线

| 范围 | 状态 | 说明 |
| --- | --- | --- |
| `query` | 已增强 | 支持真实 MaxCompute、`cost`、`explain`、cursor 分页 |
| `job submit/status/wait/result/cancel/list/diagnose` | 已实现 | 已补 `stage` / `retryable` / `failure_reason` / `logview` / `task_summary` |
| `meta list-tables/describe/search/search-columns/partitions/latest-partition/freshness/list-projects/list-schemas/semantic` | 已实现 | 覆盖两层/三层命名空间与本地语义元数据 |
| `data sample/profile/upload/download` | 已实现 | Tunnel 上传下载；下载默认保护已有本地文件，显式 `--overwrite` 才替换 |
| `auth login/login-external` | 已实现 | OAuth 为公共云交互式首选；也支持 AK/STS、环境变量和外部凭证进程 |
| `auth whoami/can-i` | 已实现 | `whoami` 输出脱敏身份摘要；`can-i` 使用 MaxCompute permission API 检查对象权限 |
| `agent context/doctor/manifest` | 已实现 | 本地上下文、可选在线就绪检查、实时 parser 契约清单 |
| `agent skill` | 已实现 | Skill 名为 `alibabacloud-maxcompute-cli`，支持 install/update/uninstall/list/diff/path |
| `cache build/build-status/status/clear` | 已实现 | 覆盖元数据缓存 |
| `@natural` | 规划中 | 未实现 |
| `agent plan` / `agent run` | 已移除 | 当前工作树不再暴露这些命令 |

## 2. 安装与依赖

当前基础依赖已经包含：

- `pyodps`
- `PyYAML`

独立 Python 入口要求 Python 3.9 或更高版本。公共云首选 Alibaba Cloud
CLI 3.3.19 或更高版本提供的 `aliyun maxc` 入口。

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

当前真实 backend 支持这些来源：

1. OAuth（公共云交互式首选）
2. Alibaba Cloud CLI profile 或运行时注入的环境变量 / STS
3. 外部凭证进程（例如 NCS）
4. 直接 AK/SK 配置

如果配置文件显式设置了认证 provider，运行时会抑制认证环境变量并给出警告，
防止静默切换身份。只有未显式选择 provider，或用户运行
`auth login --from-env` 选择环境变量路径时，环境变量才参与认证解析。

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

直接 AK/SK 登录写入的关键结构如下。OAuth token、STS 和外部凭证进程也
写在同一个 `auth` 段中；这些字段属于敏感状态，不应手工复制或输出：

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

- 支持 `--oauth`、参数传入或 `--from-env`
- 公共云交互式登录优先使用 `aliyun maxc auth login --oauth --json`
- OAuth 使用 Authorization Code + PKCE，并在后续调用中自动刷新和交换临时 STS
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

- 支持 Table、Project、Schema、Function、Resource 和 Instance 对象类型；
  实际 action 组合以 `auth can-i --help` 为准
- 接受 `table`、`schema.table` 和 `project.schema.table` 等表标识，并支持
  显式 `--project` / `--schema`
- 通过 schema-aware MaxCompute `checkPermission` API 检查，不执行探测 SQL

### 4.4 Agent 就绪检查

- `agent context --json` 是严格的本地命令，只报告版本、Python 版本、配置、
  能力和 `auth_status`；`network_checked=false` 时不能据此声称远端可达
- `agent manifest --json` 从当前运行版本的 parser 生成命令、参数、认证/
  网络要求和副作用清单，是 Agent 命令发现的运行时真值
- `agent doctor --online --json` 执行实时身份检查；远端数据操作应以
  `data.ready=true` 为就绪条件

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
- `maxc data sample <table> --partition <spec> --columns <col1,col2> --rows <n>`
- `maxc data profile <table> --partition <spec>`
- `maxc data upload <table> --file <path> --dry-run`
- `maxc data download <table> --output <path>`（已有文件默认失败；显式
  `--overwrite` 才原子替换）
- `maxc job diagnose <job_id>`
- `maxc agent context --json`
- `maxc agent manifest --json`
- `maxc agent doctor --online --json`

## 7. 已知缺口

- MaxCompute 真实血缘 API 还没接入。backend 内保留显式 unsupported
  占位，但当前公共 parser 不暴露 `meta lineage` 命令
- `@natural` 依赖外部 AgentAPI / NL2SQL 服务，当前不在 CLI 内建能力里
- 真实 backend 的 `query explain` 当前是 `execute_sql_cost` + query outline 的结构化包装，不是完整执行计划树
- 真实 backend 预执行阶段拿不到 `task_cost_cpu` / `task_cost_memory`，只能返回 `estimated_input_size_bytes`、复杂度和 UDF 数量
- `--cursor` 当前是 CLI 侧 offset token，不是 MaxCompute 原生 server-side cursor
- `meta search-columns` 当前通过遍历可见表 schema 实现，在超大 catalog 下可能偏慢
- `meta latest-partition` 在真实 backend 中优先尝试 `get_max_partition`；若不可用则退化为遍历可见分区推断
- `meta freshness` 当前使用统一启发式阈值：`<=36h` 视为 `fresh`，`<=72h` 视为 `lagging`，更久视为 `stale`
- `auth whoami` 优先返回真实 security `whoami` 的 DisplayName；拿不到时才退回 access_id 脱敏摘要
- 直接 AK/SK 登录会把 AccessKey 写入本地 YAML；虽然 CLI 会尝试收敛权限，
  但这仍是需要用户接受的本地存储模型。公共云交互式登录应优先 OAuth
- 显式保存的认证 provider 不会被 shell 中的认证环境变量静默覆盖；CLI 会把
  被抑制的变量作为 warning 暴露。要切到环境变量认证需显式运行
  `auth login --from-env`
- `data sample --partition` 在真实 backend 当前直接下推为只读 SQL 采样；如果分区语义需要更严格预检，仍需后续增强
- `job diagnose` 当前主要基于 task result 文本做错误归类；更细粒度的执行计划级诊断仍可继续增强

## 8. Phase 1 改进（2026-04）

本节记录 Phase 1 引入的能力增强，补充 Section 1 的功能矩阵。

### 8.1 结构化 SuggestedAction

`agent_hints.actions[]` 从 string 数组升级为 `SuggestedAction` 对象数组：

```json
{
  "id": "meta.describe",
  "title": "Describe table",
  "command": "maxc meta describe my_table --json",
  "executable": true,
  "placeholders": {},
  "args_schema": {}
}
```

- `action_ids` 和 `next_actions` 保留为派生字段，向后兼容
- 消费者应优先读 `actions[]`，对旧格式消费者 `next_actions` 保持不变

### 8.2 服务端只读模式与 --force

所有查询和 job 命令先执行客户端 SQL 形状检查，`data.safety` 记录实际决策；
客户端不注入 `odps.sql.read.only`：

- `policy_decision=allowed`：操作被允许
- `policy_decision=blocked`，`reason=WRITE_OPERATION_REQUIRES_FORCE`：写操作被阻断

`--force` 仅在用户对精确语句、project、schema、目标和影响明确授权后，
放行一条正向识别的数据面 DDL/DML。多语句、未知或过程式 SQL，以及权限、
账号、会话和项目等管理类操作即使带 `--force` 也会失败关闭。
前置 `SET` 也属于远端执行上下文：项目安全、访问控制和脱敏参数始终阻断；
强制写入仅接受已审查的语句级 SQL/运行时参数。

### 8.3 新输出格式

`--format` 升级为全局标志（所有命令共用）：

| 格式 | 说明 |
|------|------|
| `json` | Envelope v2.0 完整 JSON（等价于 `--json`） |
| `markdown` | 人类可读 markdown，用于展示 |
| `brief` | 最小化单行摘要，用于 token 受限场景 |

### 8.4 精细化错误码

Phase 1 新增错误码（取代泛化 `NOT_FOUND`）：

| code | 场景 |
|------|------|
| `SCHEMA_NOT_FOUND` | Schema 不存在；`error.did_you_mean` 提供候选 |
| `TABLE_NOT_FOUND` | 表不存在；`error.did_you_mean` 提供候选 |
| `COLUMN_NOT_FOUND` | 列引用不存在；`error.available` 列出可用列 |
| `WRITE_OPERATION_REQUIRES_FORCE` | 写操作被只读模式阻断 |

这些错误在 `error` 中附带 `context`、`did_you_mean`、`available` 等富文本字段，Agent 可直接用于构建修复建议。

## 9. 当前开发原则

- 先保证 `query / job / meta / data / auth / cache` 在真实 MaxCompute 上可用
- `auth login` 是 bootstrap 命令，不应依赖当前 backend 已经可用
- Skill 源随包发布，并安装为 `alibabacloud-maxcompute-cli`；公共云渲染入口
  使用 `aliyun maxc`
- 需要外部服务、审批流、统一计费抽象的能力，一律先写清接口和约束，再实现

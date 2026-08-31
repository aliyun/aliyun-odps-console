# Envelope v2.0 规范

> maxc-cli 普通 JSON 响应的标准格式，供 Agent 和下游消费者参考。
> CSV/NDJSON 行流和 `job wait --stream` 生命周期流不为每条记录包一层
> Envelope，见第 7 节。

## 1. 顶层结构

```json
{
  "version": "2.0",
  "command": "meta describe",       // 归一化后的命令路径（空格分隔）
  "status": "success",              // "success" | "pending" | "failure"
  "data": { ... },                  // 命令结果（按命令类型归一化）
  "metadata": { ... },              // 上下文元数据
  "error": null,                    // ErrorPayload | null
  "agent_hints": { ... }           // Agent 提示
}
```

## 2. data 归一化映射

每个命令的 `data` 均经过 `_normalize_data()` 归一化，保证结构一致。

| command | data 顶层 key | 说明 |
|------------|---------------|------|
| `query` | `result` + `pagination` | rows/schema/row_count + has_more/next_cursor |
| `query cost` | `analysis` | 成本估算 |
| `query explain` | `analysis` | 执行计划 |
| `job status` | `job` | 单个 job 信息 |
| `job wait` | `result` + `pagination` 或 `job` | 等待结果 |
| `job result` | `result` + `pagination` | 获取 job 结果 |
| `job cancel` | `job` | 取消后 job 信息 |
| `job list` | `jobs` + `pagination` | job 列表 |
| `job diagnose` | `diagnosis` | 诊断信息 |
| `auth whoami` | `identity` + `auth_options` | 身份信息 |
| `auth login` | `identity` + `persistence` | 登录结果 |
| `auth login-external` | `identity` + `persistence` | 外部凭证进程登录 |
| `auth can-i` | `authorization` | 权限检查 |
| `meta list-tables` | `tables` + `pagination` | 表列表 |
| `meta list-projects` | `projects` + `pagination` | 项目列表 |
| `meta list-schemas` | `schemas` + `pagination` | Schema 列表 |
| `meta search` | `search` + `pagination` | 搜索结果 |
| `meta search-columns` | `search` + `pagination` | 列搜索结果 |
| `meta describe` | `table` | 表详情 |
| `meta partitions` | `table` + `partitions` | 分区列表 |
| `meta latest-partition` | `partition` | 最新分区 |
| `meta freshness` | `freshness` | 数据新鲜度 |
| `data sample` | `sample` | 采样数据 |
| `data profile` | `profile` | 数据画像 |
| `data upload` | 扁平结果 | 上传行数、分区、overwrite/create_partition 等 |
| `data download` | 扁平结果 | 输出路径、行数、列、truncated 等 |
| `agent context` | `context` | 环境上下文 |

## 3. pagination 结构

列表类命令统一使用分页结构：

```json
{
  "pagination": {
    "total": 1234,
    "has_more": false
  }
}
```

查询类命令使用 cursor 分页：

```json
{
  "pagination": {
    "has_more": true,
    "next_cursor": "eyJvZmZzZXQiOjEwMH0="
  }
}
```

## 4. error 结构

```json
{
  "error": {
    "code": "PERMISSION_DENIED",
    "message": "Access denied to table xxx",
    "suggestion": "Check your permissions with maxc auth can-i",
    "recoverable": false,
    "recovery_steps": [
      "Check the table and operation with: maxc auth can-i --table <table> --operation SELECT --json",
      "Verify your project access with: maxc auth whoami --json",
      "Contact your project administrator for access."
    ]
  }
}
```

### 错误码一览

| code | recoverable | 说明 |
|------|-------------|------|
| `PERMISSION_DENIED` | false | 权限不足 |
| `QUOTA_EXCEEDED` | true | 配额超限 |
| `SQL_ERROR` | false | SQL 语法错误 |
| `COST_LIMIT_EXCEEDED` | false | 成本超阈值 |
| `NOT_FOUND` | false | 资源不存在 |
| `SCHEMA_NOT_FOUND` | false | Schema 不存在 |
| `TABLE_NOT_FOUND` | false | 表不存在 |
| `COLUMN_NOT_FOUND` | false | 列引用不存在 |
| `READ_ONLY_VIOLATION` | false | 服务端只读模式阻断写操作，客户端 `--force` 无法绕过 |
| `WRITE_OPERATION_REQUIRES_FORCE` | false | 客户端写门禁检测到 DDL/DML，但当前未提供 `--force`；该错误本身不代表用户授权 |
| `UNSUPPORTED_SQL_OPERATION` | false | SQL 形状无法被证明为只读，也不在数据面 DDL/DML 正向允许列表中；`--force` 不会绕过未知、过程式、权限、会话控制或管理类 SQL |
| `CSV_PARSE_ERROR` | false | 上传文件值无法转换为目标列类型 |
| `UPLOAD_COMMIT_OUTCOME_UNKNOWN` | false | Tunnel commit 请求开始后结果无法确认；先核验目标数据，禁止盲目重传 |
| `VALIDATION_ERROR` | false | 参数校验失败 |
| `FEATURE_UNAVAILABLE` | false | 功能不可用 |
| `BACKEND_CONNECTION_ERROR` | true | 连接失败 |
| `JOB_TIMEOUT` | true | 任务超时 |
| `EXECUTION_FAILED` | true | 默认错误码 |

精细化错误码（`SCHEMA_NOT_FOUND`、`TABLE_NOT_FOUND`、
`COLUMN_NOT_FOUND`）可在 `error` 中附带上下文：

```json
{
  "error": {
    "code": "TABLE_NOT_FOUND",
    "message": "Table 'my_table' not found in schema 'my_schema'",
    "suggestion": "Use maxc meta search to find the correct table name",
    "context": {"schema": "my_schema", "table": "my_table"},
    "did_you_mean": ["my_table_v2", "my_table_bak"],
    "available": ["table1", "table2"],
    "recoverable": false,
    "recovery_steps": [
      "Search for the table with: maxc meta search my_table --json",
      "List tables in the schema: maxc meta list-tables --schema my_schema --json"
    ]
  }
}
```

## 5. agent_hints 结构

```json
{
  "agent_hints": {
    "actions": [
      {
        "id": "meta.describe",
        "title": "Describe table",
        "command": "maxc --user-agent <user_agent> meta describe my_table --json",
        "executable": false,
        "placeholders": {"user_agent": "<user_agent>"},
        "args_schema": {
          "user_agent": {
            "type": "string",
            "description": "Reuse the User-Agent generated once for the current Agent session.",
            "pattern": "^AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/[0-9a-f]{32}$"
          }
        },
        "effect": "read",
        "confirmation_required": false,
        "agent_allowed": true
      }
    ],
    "action_ids": ["meta.describe"],
    "warnings": ["Large result set truncated to 100 rows"],
    "insights": ["Table xxx is partitioned by ds (daily)"]
  }
}
```

### SuggestedAction 对象 schema

`actions[]` 数组中每个元素为 `SuggestedAction` 对象：

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | dot-notation 命令 ID（如 `meta.describe`），供程序化路由使用 |
| `title` | string | 人类可读标题 |
| `command` | string | 建议的完整 CLI 命令（通常含 `--json`）；是否可执行还要看后续安全字段 |
| `executable` | bool | `true` 表示命令已解析且允许直接执行；模板、禁止 Agent 执行或需要确认的动作均为 `false` |
| `placeholders` | object | 未填充的占位符及其说明 |
| `args_schema` | object | 命令参数的结构化 schema（供程序化调用） |
| `effect` | string | 动作影响分类，例如 `read`、`local_write`、`remote_write` |
| `confirmation_required` | bool | 执行前是否必须取得用户确认 |
| `agent_allowed` | bool | Agent 是否被允许执行该动作 |

`actions[]` 是权威来源。`action_ids` 标识全部结构化动作；
`next_actions` 只是向后兼容的安全子集。

### next_actions 格式

- `action_ids`: `actions[]` 中全部动作的稳定 dot-notation ID。
- `next_actions`: 仅包含同时满足 `executable=true`、
  `agent_allowed=true`、`confirmation_required=false` 的命令字符串。
- `actions[].placeholders` 使用 `<angular_brackets>`（如 `<keyword>`,
  `<job_id>`, `<user_agent>`）；存在任何未解析占位符时，该动作的
  `executable=false`，不会进入 `next_actions`。
- 表名和 job ID 等上下文会在证据唯一时自动填充。调用云 API 的动作还必须
  使用当前 Agent 会话的 User-Agent；缺失时运行时注入 `<user_agent>` 模板。

> **设计说明**: 不要因为动作出现在 `actions[]` 或 `action_ids` 中就执行它。
> Agent 必须检查动作的执行、权限、确认和影响字段。

### action_id → maxc 命令映射

| action_id | 生成的 CLI 命令 |
|-----------|----------------|
| `query` | `maxc --user-agent <user_agent> query <sql> --json` |
| `query.paginate` | `maxc --user-agent <user_agent> query <sql> --cursor <next_cursor> --json` |
| `query.cost` | `maxc --user-agent <user_agent> query cost <sql> --json` |
| `job.wait` | `maxc --user-agent <user_agent> job wait <job_id> --json` |
| `meta.describe` | `maxc --user-agent <user_agent> meta describe <table_name> --json` |
| `meta.search` | `maxc --user-agent <user_agent> meta search <keyword> --json` |
| `data.sample` | `maxc --user-agent <user_agent> data sample <table_name> --json` |
| `auth.can-i` | `maxc --user-agent <user_agent> auth can-i --table <t> --operation SELECT --json` |

完整映射见 `models.py` 中的 `_format_next_action()`。

## 6. safety 块

`query` 和 `job` 相关命令的 `data` 中包含 `safety` 字段，描述当前安全策略决策（客户端强制执行）：

```json
{
  "safety": {
    "mode": "read_only",
    "force": false,
    "allowed_operations": ["SELECT"],
    "effective_hints": {},
    "policy_decision": "allowed"
  }
}
```

写操作被阻断时（`policy_decision=blocked`）：

```json
{
  "safety": {
    "mode": "read_only",
    "force": false,
    "policy_decision": "blocked",
    "reason": "WRITE_OPERATION_REQUIRES_FORCE"
  }
}
```

| 字段 | 说明 |
|------|------|
| `mode` | 当前安全模式：`read_only` \| `force` |
| `force` | 是否使用 `--force` 放行一条已明确授权且被正向识别的数据面 DDL/DML |
| `allowed_operations` | 当前模式下允许的操作类型列表 |
| `effective_hints` | 实际注入到 MaxCompute 的 SET 参数。已审查的 key 只有在值符合对应布尔、数字或枚举域时才保留值；未知参数或越界值仅保留 key，值显示为 `<redacted>` |
| `policy_decision` | `allowed` \| `blocked` |
| `reason` | 仅在 `blocked` 时出现，对应错误码 |

`job.wait` 和 `job.result` 只观察已提交作业，不会重新对原 SQL 执行写入策略判定。
因此它们的 `safety` 块使用 `scope=result_observation`，并将当前非变更操作记为
`JOB_WAIT` 或 `JOB_RESULT`；原 SQL 仍保留在 `metadata.sql_executed`。

## 7. 输出格式

### 输出建议

对 Agent，统一使用 `--json`：

```bash
maxc meta describe my_table --json
```

普通 `--json` 响应输出一个 Envelope v2.0，适合机器/Agent 消费。
`--format csv`、`--format ndjson`、查询文件输出的 CSV/NDJSON，以及
`job wait --stream` 是专用流格式；其中的行或生命周期事件不是 Envelope。
`job wait --stream` 会用一条自包含的终态 NDJSON 记录结束。

## 8. metadata 常见字段

```json
{
  "metadata": {
    "job_id": "202604151234_abcd",
    "project": "my_project",
    "elapsed_ms": 1234,
    "region": "cn-hangzhou",
    "config_sources": ["/home/user/.maxc/config.yaml"],
    "state_dir": "/home/user/.maxc",
    "job_mode": "remote"
  }
}
```

## 9. 版本兼容性

- `version` 字段固定为 `"2.0"`
- `command` 为人类可读格式（空格分隔）
- Envelope 顶层 `status` 固定为 `success`、`pending` 或 `failure`；Job、cache
  等命令自己的生命周期状态位于对应 `data` 字段或流式事件中
- 新增字段不改变现有字段语义，保证向后兼容

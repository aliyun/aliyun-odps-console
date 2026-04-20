# Envelope v2.0 规范

> maxc-cli 所有命令的标准输出格式，供 Agent 和下游消费者参考。

## 1. 顶层结构

```json
{
  "version": "2.0",
  "command": "meta describe",       // 归一化后的命令路径（空格分隔）
  "command_id": "meta.describe",    // 原始 dot-notation 命令 ID
  "status": "success",              // "success" | "error"
  "data": { ... },                  // 命令结果（按命令类型归一化）
  "metadata": { ... },              // 上下文元数据
  "error": null,                    // ErrorPayload | null
  "agent_hints": { ... }           // Agent 提示
}
```

## 2. data 归一化映射

每个命令的 `data` 均经过 `_normalize_data()` 归一化，保证结构一致。

| command_id | data 顶层 key | 说明 |
|------------|---------------|------|
| `query` | `result` + `pagination` | rows/schema/row_count + has_more/next_cursor |
| `query.cost` | `analysis` | 成本估算 |
| `query.explain` | `analysis` | 执行计划 |
| `job.status` | `job` | 单个 job 信息 |
| `job.wait` | `result` + `pagination` 或 `job` | 等待结果 |
| `job.result` | `result` + `pagination` | 获取 job 结果 |
| `job.cancel` | `job` | 取消后 job 信息 |
| `job.list` | `jobs` + `pagination` | job 列表 |
| `job.diagnose` | `diagnosis` | 诊断信息 |
| `auth.whoami` | `identity` + `auth_options` | 身份信息 |
| `auth.login` | `identity` + `persistence` | 登录结果 |
| `auth.login-external` | `identity` + `persistence` 或 `accounts` | NCS 登录 |
| `auth.can-i` | `authorization` | 权限检查 |
| `meta.list-tables` | `tables` + `pagination` | 表列表 |
| `meta.list-projects` | `projects` + `pagination` | 项目列表 |
| `meta.list-schemas` | `schemas` + `pagination` | Schema 列表 |
| `meta.search` | `search` + `pagination` | 搜索结果 |
| `meta.search-columns` | `search` + `pagination` | 列搜索结果 |
| `meta.describe` | `table` | 表详情 |
| `meta.partitions` | `table` + `partitions` | 分区列表 |
| `meta.latest-partition` | `partition` | 最新分区 |
| `meta.freshness` | `freshness` | 数据新鲜度 |
| `data.sample` | `sample` | 采样数据 |
| `data.profile` | `profile` | 数据画像 |
| `project.*` | `project` | 项目操作 |
| `diff.*` | `diff` | 对比结果 |
| `agent.context` | `context` | 环境上下文 |

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
| `SCHEMA_NOT_FOUND` | false | Schema 不存在（Phase 1 新增） |
| `TABLE_NOT_FOUND` | false | 表不存在（Phase 1 新增） |
| `COLUMN_NOT_FOUND` | false | 列引用不存在（Phase 1 新增） |
| `WRITE_OPERATION_REQUIRES_FORCE` | false | 写操作被只读模式阻断（Phase 1 新增） |
| `VALIDATION_ERROR` | false | 参数校验失败 |
| `FEATURE_UNAVAILABLE` | false | 功能不可用 |
| `BACKEND_CONNECTION_ERROR` | true | 连接失败 |
| `JOB_TIMEOUT` | true | 任务超时 |
| `EXECUTION_FAILED` | true | 默认错误码 |

Phase 1 新增的精细化错误码（`SCHEMA_NOT_FOUND`、`TABLE_NOT_FOUND`、`COLUMN_NOT_FOUND`）在 `error` 中附带富文本上下文：

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
        "command": "maxc meta describe my_table --json",
        "executable": true,
        "placeholders": {},
        "args_schema": {}
      }
    ],
    "action_ids": ["meta.describe"],
    "next_actions": ["maxc meta describe my_table --json"],
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
| `command` | string | 可直接执行的完整 CLI 命令（含 `--json`） |
| `executable` | bool | `true` 表示命令可直接执行；`false` 表示含未填充占位符 |
| `placeholders` | object | 未填充的占位符及其说明（`executable=false` 时非空） |
| `args_schema` | object | 命令参数的结构化 schema（供程序化调用） |

`action_ids` 和 `next_actions` 均从 `actions[]` 派生，保持向后兼容。

### next_actions 格式

- `action_ids`: dot-notation 格式（程序化解析用），自动从 `next_actions` 转换
  - `"maxc meta describe"` → `"meta.describe"`
  - `"maxc job wait"` → `"job.wait"`
  - `"meta.describe"` → `"meta.describe"` (已是 dot-notation)
- `next_actions`: 可直接在 shell 执行的 `maxc` 命令（含 `--json`），通过 `_format_next_action()` 动态模板化
- 占位符使用 `<angular_brackets>`（如 `<keyword>`, `<job_id>`）
- 上下文变量自动填充：table_name → 当前表名，job_id → 当前任务 ID 等

> **设计说明**: `actions[]` 是权威来源；`action_ids` 和 `next_actions` 为派生字段，
> 保持对旧版消费者的向后兼容。

### action_id → maxc 命令映射

| action_id | 生成的 CLI 命令 |
|-----------|----------------|
| `query` | `maxc query <sql> --json` |
| `query.paginate` | `maxc query <sql> --cursor <next_cursor> --json` |
| `query.cost` | `maxc query cost <sql> --json` |
| `job.wait` | `maxc job wait <job_id> --json` |
| `meta.describe` | `maxc meta describe <table_name> --json` |
| `meta.search` | `maxc meta search <keyword> --json` |
| `data.sample` | `maxc data sample <table_name> --json` |
| `auth.can-i` | `maxc auth can-i --table <t> --operation SELECT --json` |

完整映射见 `models.py` 中的 `_format_next_action()`。

## 6. safety 块（Phase 1 新增）

`query` 和 `job` 相关命令的 `data` 中包含 `safety` 字段，描述当前安全策略决策：

```json
{
  "safety": {
    "mode": "read_only",
    "force": false,
    "allowed_operations": ["SELECT"],
    "effective_hints": {"odps.sql.read.only": "true"},
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
    "allowed_operations": ["SELECT"],
    "effective_hints": {"odps.sql.read.only": "true"},
    "policy_decision": "blocked",
    "reason": "WRITE_OPERATION_REQUIRES_FORCE"
  }
}
```

| 字段 | 说明 |
|------|------|
| `mode` | 当前安全模式：`read_only` \| `force` |
| `force` | 是否通过 `--force` 绕过只读限制 |
| `allowed_operations` | 当前模式下允许的操作类型列表 |
| `effective_hints` | 实际注入到 MaxCompute 的 SET 参数 |
| `policy_decision` | `allowed` \| `blocked` |
| `reason` | 仅在 `blocked` 时出现，对应错误码 |

## 7. 输出格式（Phase 1 新增）

### --format 全局标志

`--format` 现在是全局标志（位于子命令之前），适用于所有命令：

```bash
maxc --format json meta describe my_table   # 结构化 JSON（等价于 --json）
maxc --format markdown meta describe my_table   # 人类可读 markdown
maxc --format brief meta describe my_table      # 最小化单行输出
```

| 格式 | 说明 | 适用场景 |
|------|------|---------|
| `json` | Envelope v2.0 完整 JSON | 机器/Agent 消费 |
| `markdown` | 人类可读 markdown 表格/代码块 | 展示给用户 |
| `brief` | 最小化单行摘要 | token 受限场景 |

`--json` 是 `--format json` 的简写，保持向后兼容。

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
- `command_id` 保持 dot-notation（程序化解析用）
- `command` 为人类可读格式（空格分隔）
- 新增字段不改变现有字段语义，保证向后兼容

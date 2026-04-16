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
| `VALIDATION_ERROR` | false | 参数校验失败 |
| `FEATURE_UNAVAILABLE` | false | 功能不可用 |
| `BACKEND_CONNECTION_ERROR` | true | 连接失败 |
| `JOB_TIMEOUT` | true | 任务超时 |
| `EXECUTION_FAILED` | true | 默认错误码 |

## 5. agent_hints 结构

```json
{
  "agent_hints": {
    "action_ids": ["meta.search", "meta.list-tables"],
    "next_actions": [
      "maxc meta search <keyword> --json",
      "maxc meta list-tables --json"
    ],
    "warnings": ["Large result set truncated to 100 rows"],
    "insights": ["Table xxx is partitioned by ds (daily)"]
  }
}
```

### next_actions 格式

- `action_ids`: dot-notation 格式（程序化解析用），自动从 `next_actions` 转换
  - `"maxc meta describe"` → `"meta.describe"`
  - `"maxc job wait"` → `"job.wait"`
  - `"meta.describe"` → `"meta.describe"` (已是 dot-notation)
- `next_actions`: 可直接在 shell 执行的 `maxc` 命令（含 `--json`），通过 `_format_next_action()` 动态模板化
- 占位符使用 `<angular_brackets>`（如 `<keyword>`, `<job_id>`）
- 上下文变量自动填充：table_name → 当前表名，job_id → 当前任务 ID 等

> **设计说明**: `app.py` 中 `next_actions` 使用 `"maxc ..."` 前缀格式，
> `_render_agent_hints()` 负责：
> 1. 提取 `action_ids` (dot-notation) 供程序化使用
> 2. 通过 `_format_next_action()` 生成完整 CLI 命令（含动态参数 + `--json`）

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

## 6. metadata 常见字段

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

## 7. 版本兼容性

- `version` 字段固定为 `"2.0"`
- `command_id` 保持 dot-notation（程序化解析用）
- `command` 为人类可读格式（空格分隔）
- 新增字段不改变现有字段语义，保证向后兼容

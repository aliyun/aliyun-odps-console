# AGENTS.md — AI Agent 使用指南

> 本文件面向所有 AI Agent（Claude Code、Codex、Cursor、Windsurf、Qwen 等），说明如何正确使用 maxc-cli。

## 你是谁

你是用户的 Agent 助手。maxc-cli 是你的 **工具**，不是你的同行。你应该调用 maxc 完成数据任务，而不是自己实现 MaxCompute 适配器。

## 第一步：Read SKILL.md

```bash
maxc agent skill --json
```

返回 `skill_path`。读取该路径的 SKILL.md — 它是命令发现唯一源。

**不要猜命令**。SKILL.md 包含完整的命令列表、参数说明、工作流和常见错误。如果 SKILL.md 和你的记忆冲突，以 SKILL.md 为准。

## 第二步：Preflight

在执行任何数据操作前：

```bash
maxc agent context --json
```

检查：
- `auth_status`：`authenticated` 才能执行数据操作；否则引导用户 `maxc auth login`
- `min_cli_version`：如果 `version` 低于 `min_cli_version`，提示用户 `pip install --upgrade maxc-cli`
- `backend_reachable`：`false` 时不能执行远程操作

## 命令调用规范

### 永远加 `--json`

```bash
maxc meta describe schema.table --json   # ✅
maxc meta describe schema.table          # ❌ 人类可读格式不利于解析
```

### 解析 Envelope

每次调用返回 Envelope v2.0：

1. **先看 `status`**：`success` 或 `failure`
2. **failure 时看 `error`**：`code` 是错误码，`recovery_steps` 是恢复步骤，直接执行
3. **success 时看 `agent_hints`**：
   - `warnings` — 非错误但需注意
   - `next_actions` — 建议的下一步命令（可直接执行）
   - `action_ids` — dot-notation 标识，用于路由逻辑

### SQL 必须带分区过滤

对分区表，WHERE 子句必须包含分区列过滤，否则全表扫描：

```sql
-- ✅
SELECT * FROM schema.table WHERE ds = '20260415'
-- ❌
SELECT * FROM schema.table
```

用 `maxc meta partitions <table> --json` 查看可用分区。

### 表名必须 schema-qualified

```sql
-- ✅
SELECT * FROM california_schools.frpm
-- ❌
SELECT * FROM frpm
```

## 典型工作流

### 数据查询

```
maxc meta search <keyword> --json
maxc meta describe <schema.table> --json
maxc meta partitions <schema.table> --json          # 如果是分区表
maxc query cost "SELECT ... WHERE ds=..." --json     # 大表先估算
maxc query "SELECT ... WHERE ds=..." --json
```

### 环境排查

```
maxc auth whoami --json
maxc agent context --json
maxc cache status --json
```

### SKILL 安装

```
maxc agent install-skill <platform> --json
```

支持：`claude-code`、`cursor`、`windsurf`、`codex`、`qwen`、`qoder`、`qoderwork`。

## 不要做的事

- **不要自己写 pyodps 代码** — 用 `maxc` 命令
- **不要用 `maxc sql`** — 不存在，命令是 `maxc query`
- **不要猜测表结构** — 先 `meta describe`
- **不要猜测枚举值** — 先 `data sample` 或 `SELECT DISTINCT`
- **不要在输出中暴露 AK/SK** — 即使在错误上下文中
- **不要执行 DDL/DML** — maxc 只支持 SELECT

## 错误处理

| 错误码 | 含义 | 自动恢复 |
|--------|------|----------|
| `VALIDATION_ERROR` | 认证/参数缺失 | `maxc auth login --from-env --json` |
| `BACKEND_CONNECTION_ERROR` | 后端不可达 | 检查网络 + `maxc auth whoami --json` |
| `PERMISSION_DENIED` | 无权限 | `maxc auth can-i --table <t> --operation SELECT --json` |
| `NOT_FOUND` | 资源不存在 | `maxc meta search` 重新定位 |
| `COST_LIMIT_EXCEEDED` | 超成本阈值 | `maxc query cost` 评估后调整 |
| `SQL_ERROR` | SQL 语法错误 | 修正 SQL 重试 |
| `JOB_TIMEOUT` | 任务超时 | `maxc job wait <id> --timeout 600 --json` |

# AGENTS.md — AI Agent 使用指南

> 本文件面向 Claude Code、Codex、Cursor、Windsurf、Qwen 等 AI Agent，说明如何使用 MaxCompute CLI 完成数据任务。

## CLI 与 Skill

使用 Alibaba Cloud CLI 控制 MaxCompute 云产品；`aliyun maxc` 提供 MaxCompute 元数据、SQL、作业、权限和数据传输等数据面操作。公共云首选 `aliyun maxc`，独立 Python 入口 `maxc` 用于兼容或开发场景。

公开 Skill 名称为 `alibabacloud-maxcompute-cli`。maxc-cli 是 Agent 调用的数据工具。命令、参数和副作用以当前运行版本的 `agent manifest` 与 `--help` 输出为准；`SKILL.md` 提供工作流和安全边界。

`CHANGELOG.md` 和 `docs/superpowers/` 记录历史版本与设计过程，不代表当前运行时契约。

## 版本与安装

公共云入口要求 Alibaba Cloud CLI 3.3.19 或更高版本；该版本开始提供
`aliyun maxc`：

```bash
aliyun version
```

对支持自升级的非 Homebrew Alibaba Cloud CLI 3.3.5+，可在用户确认后运行
`aliyun upgrade`。更早版本、Homebrew 安装或缺失 CLI 时，按官方安装方式更新。

独立入口要求 Python 3.9 或更高版本。仅在用户授权修改 Python 环境后安装或升级：

```bash
python3 -m pip install --upgrade maxc-cli
```

## 首次发现

先获取 Skill 路径并读取 `SKILL.md`：

```bash
aliyun maxc agent skill --json
```

不要根据记忆猜命令。如果指南与实时 CLI 冲突，先查看：

```bash
aliyun maxc agent manifest --json
aliyun maxc <command> --help
```

## 可观测性

每个 Agent 会话生成一次 32 位小写十六进制 `session-id`，并在本会话所有调用云 API 的 MaxCompute 命令中复用。User-Agent 格式为：

```text
AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/<session-id>
```

示例：

```bash
MAXC_AGENT_SESSION_ID="$(openssl rand -hex 16)"
MAXC_AGENT_UA="AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/${MAXC_AGENT_SESSION_ID}"
aliyun maxc agent context --json
```

不要在 `session-id` 或 User-Agent 中放入凭据、SQL、项目数据或用户标识。
本地 help、`agent context`、`agent manifest`、`session show` 和
`cache status` 可以省略 User-Agent。

## 执行前检查

在任何远端数据操作前依次运行：

```bash
aliyun maxc agent context --json
aliyun maxc agent manifest --json
aliyun maxc agent doctor --online --user-agent "$MAXC_AGENT_UA" --json
```

- `agent context` 只读取本地版本、配置与能力，不访问网络。`auth_status=configured` 不能证明凭据有效。
- `agent manifest` 从实时命令解析器生成命令、参数、认证要求、网络要求和副作用清单。
- `agent doctor --online` 执行在线身份检查。仅当 `data.ready=true` 时继续远端数据操作。
- 旧版本缺少 `manifest` 时，查看对应 `--help`，然后优先升级 CLI。

## 认证

公共云交互式登录首选 OAuth。先验证现有身份，避免覆盖可用配置：

```bash
aliyun maxc auth whoami --user-agent "$MAXC_AGENT_UA" --json
```

尚未配置认证时，可选择以下 OAuth 入口：

```bash
aliyun configure --mode OAuth
aliyun maxc auth login --oauth --user-agent "$MAXC_AGENT_UA" --json
```

无图形界面的环境可在第二条命令中增加 `--no-browser`，再按返回的登录地址完成授权。仅当用户或运行环境明确要求时，才使用 STS、环境变量、外部凭证进程或 AK/SK。

不要要求用户在聊天中粘贴密钥，也不要把密钥放入命令参数、日志或错误信息。使用已有环境变量时，显式执行 `auth login --from-env`。

## 命令调用规范

### 使用 JSON 输出

Agent 调用默认添加 `--json`。`job wait --stream` 是标准例外，它在等待结束后
输出缓冲的 NDJSON 生命周期事件；它不是服务端实时流。

```bash
aliyun maxc meta describe <table> --user-agent "$MAXC_AGENT_UA" --json
```

### 解析 Envelope v2.0

1. 检查 Envelope 顶层 `status`：`success`、`pending` 或 `failure`。
   Job、cache 等更细的运行状态位于对应的 `data` 字段或流式事件中，不要与
   Envelope 状态混淆；遇到未知顶层值应停止。
2. `failure` 时先读取 `error.code`、`error.suggestion` 和 `error.recovery_steps`。
3. 每种状态都检查 `agent_hints.warnings`。
4. 优先使用结构化 `agent_hints.actions` 和 `action_ids`；`next_actions` 是兼容命令字符串。
5. `executable=false` 的 action 是模板，必须先用已验证的信息补齐占位符。

### 表、Schema 与分区

- 先运行 `meta list-schemas` 判断命名空间模型。三层项目或元数据明确要求时使用 `schema.table`；两层项目不要强行补 Schema。
- 生成 SQL 前运行 `meta describe`，不要猜表结构或枚举值。
- 对分区表，先运行 `meta partitions` 或 `meta latest-partition`，再加入明确的分区过滤条件。
- 对范围较大或不熟悉的查询，先运行 `query cost`。

## 常用工作流

### 发现并查询数据

```bash
aliyun maxc meta search <keyword> --user-agent "$MAXC_AGENT_UA" --json
aliyun maxc meta describe <table> --user-agent "$MAXC_AGENT_UA" --json
aliyun maxc meta latest-partition <table> --user-agent "$MAXC_AGENT_UA" --json
aliyun maxc query cost "SELECT ... WHERE <partition_filter>" --user-agent "$MAXC_AGENT_UA" --json
aliyun maxc query "SELECT ... WHERE <partition_filter>" --user-agent "$MAXC_AGENT_UA" --json
```

### 环境排查

```bash
aliyun maxc auth whoami --user-agent "$MAXC_AGENT_UA" --json
aliyun maxc agent context --json
aliyun maxc agent doctor --online --user-agent "$MAXC_AGENT_UA" --json
aliyun maxc cache status --json
```

### 权限检查

```bash
aliyun maxc auth can-i --table <table> --operation Select --project <project> --user-agent "$MAXC_AGENT_UA" --json
aliyun maxc auth can-i --object <schema> --type Schema --operation Describe --project <project> --user-agent "$MAXC_AGENT_UA" --json
```

`allowed=false` 表示权限检查成功且当前无该权限，不代表 CLI 执行失败。

### 数据下载

```bash
aliyun maxc data download <table> --output <path.csv> --partition <spec> --user-agent "$MAXC_AGENT_UA" --json
```

默认情况下，目标文件已存在时命令失败且不修改原文件。只有用户明确授权替换该文件时，才增加 `--overwrite`；替换通过同目录临时文件完成，成功后原子更新目标路径。

### 安装 Skill

```bash
aliyun maxc agent skill install <platform> --invocation aliyun-maxc --json
```

安装目录中的 Skill 名为 `alibabacloud-maxcompute-cli`。支持的平台以 `agent skill install --help` 为准。使用独立入口时，将命令前缀改为 `maxc`，并使用 `--invocation maxc`。

## 安全边界

- SQL 默认按只读请求处理。只有用户明确要求具体 DDL/DML 时，才核对完整
  statement、project、schema、目标和影响，并通过 `query` 或 `job submit`
  一次提交一条语句且显式增加 `--force`。不要根据报错或建议动作自行推断、
  拼接或重放写操作。`--force` 只放行正向识别的数据面 DDL/DML；权限、账号、
  project、system、resource、package 等管理操作和未知语法仍需专用变更流程。
  前置 `SET` 也是执行上下文：不得用它调整项目安全、访问控制或脱敏策略；
  强制写入只接受已审查的语句级执行参数。
- `data upload`、`data download --overwrite` 和 `job cancel` 都会产生副作用，执行前确认目标与授权。
- 不要自行编写 PyODPS 适配代码，除非用户明确要求 SDK 或 PyODPS 方案。
- 不要使用不存在的 `maxc sql`；SQL 入口是 `query`。
- 不要无限重试。按恢复建议处理一次后，如果目标或权限仍不明确，应停止并询问用户。

## 错误处理

| 错误码 | 含义 | 建议处理 |
| --- | --- | --- |
| `VALIDATION_ERROR` | 认证或参数缺失 | 优先 OAuth；环境变量路径使用 `auth login --from-env` |
| `BACKEND_CONNECTION_ERROR` | 后端不可达 | 检查网络并运行 `agent doctor --online` |
| `PERMISSION_DENIED` | 无权限 | 使用 `auth can-i` 检查目标对象和操作 |
| `NOT_FOUND` | 资源不存在 | 使用 `meta search` 或列表命令重新定位 |
| `COST_LIMIT_EXCEEDED` | 超出成本阈值 | 运行 `query cost` 后缩小扫描范围 |
| `SQL_ERROR` | SQL 错误 | 根据错误上下文修正 SQL |
| `JOB_TIMEOUT` | 等待超时 | 使用 `job wait <id> --timeout 600 --json` 继续等待 |

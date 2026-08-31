# maxc-cli

使用 Alibaba Cloud CLI 控制 MaxCompute 云产品。`aliyun maxc` 提供
MaxCompute 元数据、SQL、作业、权限和数据传输等数据面操作。公共云 Skill
名称为 `alibabacloud-maxcompute-cli`。

## 快速开始

### 公共云（推荐）

```bash
# aliyun maxc 需要 Alibaba Cloud CLI >= 3.3.19
aliyun version

# OAuth 是交互式登录的首选方式，不需要把长期 AK/SK 放进命令行
aliyun maxc auth login --oauth --json

# 本地检查与在线就绪检查是两个独立步骤
aliyun maxc agent context --json
aliyun maxc agent manifest --json
aliyun maxc agent doctor --online --json

# 发现并查询数据
aliyun maxc meta search "销售" --json
aliyun maxc meta describe schema.table --json
aliyun maxc query cost "SELECT * FROM schema.table WHERE ds='20260415'" --json
aliyun maxc query "SELECT * FROM schema.table WHERE ds='20260415'" --json
```

支持自升级的非 Homebrew Alibaba Cloud CLI 3.3.5+ 可在用户确认后运行
`aliyun upgrade`；更早版本、Homebrew 安装或缺失 CLI 请按官方安装方式更新。

如果 Alibaba Cloud CLI 扩展不可用，或明确需要 PyPI 版本，可使用独立入口。
独立入口要求 Python 3.9 或更高版本：

```bash
python3 -m pip install --upgrade maxc-cli
maxc auth login --oauth --json
maxc agent doctor --online --json
```

已有 Alibaba Cloud CLI OAuth profile、环境变量、STS 或外部凭证进程时，
先运行 `auth whoami --json` 验证当前身份，不要无故覆盖现有认证。

## 命令一览

| 家族 | 命令 | 说明 |
|------|------|------|
| **query** | `query [run]`, `query cost`, `query explain` | SQL 执行、成本估算、执行计划 |
| **job** | `submit`, `status`, `wait`, `result`, `cancel`, `diagnose`, `list` | 异步任务全生命周期 |
| **meta** | `list-tables`, `describe`, `search`, `search-columns`, `partitions`, `latest-partition`, `freshness`, `list-projects`, `list-schemas`, `semantic set/get/clear/list-missing` | 元数据发现与语义管理 |
| **data** | `sample`, `profile`, `upload`, `download` | 数据采样、画像与 CSV/TSV 传输 |
| **auth** | `login`, `login-external`, `logout`, `whoami`, `can-i` | 认证与权限 |
| **session** | `set`, `show`, `unset` | 项目/Schema 切换 |
| **cache** | `build`, `build-status`, `status`, `clear` | 元数据缓存管理 |
| **agent** | `context`, `doctor`, `manifest`, `skill install/update/uninstall/list/diff/path` | Agent 就绪检查、命令发现与 Skill 管理 |

普通命令支持 `--json` 输出 Envelope v2.0 结构化响应。对 Agent 而言应优先
使用 `--json`；CSV/NDJSON 行流和 `job wait --stream` 生命周期流是明确例外，
不会为每条记录重复封装 Envelope。

## Agent 集成

### 方式 1：公共 Skill（主路径）

安装名为 `alibabacloud-maxcompute-cli` 的公共 Skill。Skill 会先检查
`aliyun maxc`，仅在该入口不可用且用户选择独立发行版时才使用 PyPI 入口。

### 方式 2：从 CLI 注册

当前 CLI 包内含同一份 Skill 源，可以注册到 Agent 平台的标准目录：

```bash
# 公共云入口会把 Skill 内的命令渲染为 aliyun maxc
aliyun maxc agent skill install codex --invocation aliyun-maxc --json

# 独立入口
maxc agent skill install codex --invocation maxc --json
```

安装目录名统一为 `alibabacloud-maxcompute-cli`。支持的平台以
`agent skill install --help` 的实时输出为准。
后续执行 `agent skill update <platform>` 或 `agent skill update --all` 时，
未显式传 `--invocation` 会分别保留每个已安装 Skill 的原入口；只有显式参数才会覆盖。

### preflight 检查

Agent 启动时生成一次 32 位小写十六进制 session ID，并在整个会话中复用：

```bash
UA="AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/<session-id>"
```

每条调用云 API 的 `aliyun maxc` 命令都追加 `--user-agent "$UA"`；本地
help、`agent context`、`agent manifest`、`session show` 和 `cache status`
可以省略。然后依次运行：

```bash
aliyun maxc agent context --json                              # 仅检查本地版本、配置和能力；不访问网络
aliyun maxc agent manifest --json                             # 从实时 parser 生成命令、参数和副作用清单
aliyun maxc agent doctor --online --user-agent "$UA" --json  # 验证身份与后端可达性
aliyun maxc agent skill --json                                # Skill 路径、名称与 min_cli_version
```

只有 `agent doctor --online` 能证明远端已就绪；不要把 `agent context` 中的
`auth_status=configured` 解读成已经通过在线认证。

## Envelope v2.0

普通 `--json` 响应遵循统一结构：

```json
{
  "version": "2.0",
  "command": "meta describe",
  "status": "success | pending | failure",
  "data": { ... },
  "metadata": { ... },
  "error": null | { "code": "...", "message": "...", "recovery_steps": [...] },
  "agent_hints": {
    "actions": [
      {
        "id": "meta.search",
        "title": "Search tables",
        "command": "maxc --user-agent <user_agent> meta search <keyword> --json",
        "executable": false,
        "placeholders": {
          "keyword": "<keyword>",
          "user_agent": "<user_agent>"
        },
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
    "action_ids": ["meta.search"],
    "insights": [...],
    "warnings": [...]
  }
}
```

- `agent_hints.actions[]`：权威的结构化 `SuggestedAction` 对象数组
- `action_ids`：全部结构化动作的稳定 dot-notation ID
- `next_actions`：兼容字段，只包含 `executable=true`、`agent_allowed=true`
  且无需确认的命令；模板或有副作用动作可能只出现在 `actions[]`
- `error.recovery_steps`：错误码对应的恢复步骤

### 输出格式

对 Agent，统一使用 `--json`：

```bash
maxc meta describe my_table --json
```

### safety 块

`query` 和 `job` 命令的 `data` 中包含 `safety` 字段，描述安全策略决策：

```json
"safety": {
  "mode": "read_only",
  "force": false,
  "allowed_operations": ["SELECT"],
  "effective_hints": {},
  "policy_decision": "allowed"
}
```

详见 [`docs/ENVELOPE_SPEC.md`](docs/ENVELOPE_SPEC.md)。

## 项目结构

```
src/maxc_cli/
├── cli.py               # argparse 命令注册
├── app.py               # MaxCApp 业务逻辑
├── models.py            # Envelope / AgentHints / QueryResult
├── exceptions.py        # ErrorPayload + 类型化异常 + recovery_steps
├── config.py            # YAML 配置加载
├── cache.py             # LocalCache (SQLite)
├── store.py             # JobStore（加锁、原子写入的本地 JSON）
├── output.py            # JSON / Markdown / brief / 人类可读渲染
├── auth_providers.py    # OAuth / AK-SK / STS / 外部进程 / 环境变量认证
├── backend/             # ODPS 后端（query / job / meta / catalog / data / auth mixin）
└── skills/              # alibabacloud-maxcompute-cli 的包内源文件与 references
```

## 文档

| 文档 | 内容 |
|------|------|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | 三层架构、核心数据流、缓存/认证架构 |
| [`docs/ENVELOPE_SPEC.md`](docs/ENVELOPE_SPEC.md) | Envelope v2.0 规范、pagination、error codes |
| [`docs/ODPS_BACKEND.md`](docs/ODPS_BACKEND.md) | ODPS 后端 API 映射、限制与回退行为 |
| [`docs/design.md`](docs/design.md) | 产品定位与命令体系 |
| [`docs/implementation.md`](docs/implementation.md) | 当前代码的真实行为和输出契约 |
| [`docs/roadmap.md`](docs/roadmap.md) | 路线图 |

运行时命令和参数以当前版本的 `agent manifest` 与 `--help` 输出为准。
`CHANGELOG.md` 和 `docs/superpowers/` 用于追溯历史版本与设计过程，不作为
当前运行时契约。

## 限制

- **查询安全**：SQL 默认按只读请求处理。只有用户明确要求具体 DDL/DML
  时，才核对完整 statement、project、schema、目标和影响，通过 `query`
  或 `job submit` 一次提交一条语句并显式增加 `--force`。`data upload`、
  `data download --overwrite` 和 `job cancel` 是独立的有副作用操作，同样需
  与影响相匹配的明确授权。权限、账号、project、system、resource、package
  等管理 SQL 和未知语法不在公共 `--force` 正向允许列表中。前置 `SET`
  同样属于执行上下文：项目安全、访问控制和脱敏参数始终被阻断，强制写入
  仅接受已审查的语句级执行参数。
- **OAuth 优先**：公共云交互式登录优先 OAuth。只有运行环境明确要求时才用
  AK/SK、STS、环境变量或外部凭证进程。直接 AK/SK 会写入
  `~/.maxc/config.yaml`（文件权限 0600）。OAuth 需要账号/组织已分配官方
  `official-cli` OAuth 应用。
  省略 `--project` 时通过 Catalog API 弹交互式 project picker（需 TTY，仅支持中国区 project）。
  CI 用 `--no-picker`；想重选已保存的 project 用 `--reselect`；非中国区用 `--catalog-endpoint` 覆盖。
  OAuth 回调始终监听 CLI 所在主机的 `127.0.0.1`；`--no-browser` 只是不自动
  打开浏览器，并不是 device-code/headless flow。SSH 场景需配置端口转发，或
  在 CLI 同机浏览器完成授权。
- **远程查询重试**：可恢复远程执行拒绝 `--retry-on`、`--max-retries` 和非默认
  `--retry-backoff`。保留首次返回的 `metadata.job_id`，先检查原任务，再人工
  判断是否重新提交。
- **上传分区**：普通 `data upload` 不创建缺失分区。只有显式
  `--create-partition` 才允许创建 `--partition` 指定的分区；这是独立元数据
  副作用，后续上传失败时可能留下空分区。
- **外部凭证进程**：只能来自可信用户级配置或用户显式选择的 `--config`；自动
  发现的 workspace 配置不得定义 `auth`。命令按 executable + argv 运行，不经
  shell，不支持管道、重定向或命令替换。
- **安全下载**：`data download` 默认拒绝覆盖已有本地文件；只有显式传入
  `--overwrite` 才会原子替换目标文件。
- **list-tables 分页**：CLI 侧 offset token，非服务端游标

## 开发

```bash
pip install -e .
pytest tests/ -m unit
```

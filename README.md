# maxc-cli

Agent-first MaxCompute CLI — 不是 Agent，而是给 Agent 调用的结构化工具层。

## 快速开始

```bash
pip install maxc-cli

# 认证
maxc auth login --from-env --json          # 从环境变量
maxc auth login --access-id ID --secret-access-key KEY --project PROJ --endpoint URL --json

# 确认就绪
maxc auth whoami --json
maxc agent context --json

# 用
maxc meta search "销售" --json
maxc meta describe schema.table --json
maxc query cost "SELECT * FROM schema.table WHERE ds='20260415'" --json
maxc query "SELECT * FROM schema.table WHERE ds='20260415'" --json
```

## 命令一览

| 家族 | 命令 | 说明 |
|------|------|------|
| **query** | `query [run]`, `query cost`, `query explain` | SQL 执行、成本估算、执行计划 |
| **job** | `submit`, `status`, `wait`, `result`, `cancel`, `diagnose`, `list` | 异步任务全生命周期 |
| **meta** | `list-tables`, `describe`, `search`, `search-columns`, `partitions`, `latest-partition`, `freshness`, `list-projects`, `list-schemas`, `semantic set/get/list-missing` | 元数据发现与语义管理 |
| **data** | `sample`, `profile` | 数据采样与画像 |
| **auth** | `login`, `login-ncs`, `whoami`, `can-i` | 认证与权限 |
| **session** | `set`, `show`, `unset` | 项目/Schema 切换 |
| **diff** | `schema`, `partition`, `data` | 表结构/分区/数据对比 |
| **cache** | `build`, `build-status`, `status`, `clear` | 元数据缓存管理 |
| **agent** | `context`, `skill`, `install-skill` | Agent 集成与 SKILL 安装 |

所有命令支持 `--json` 输出 Envelope v2.0 结构化响应。

## Agent 集成

### 方式 1：SKILL HUB（主路径）

SKILL HUB 安装 SKILL → Agent 读 SKILL.md → Agent 自己 `pip install maxc-cli`。

### 方式 2：install-skill（内网兜底）

先 pip install，再一键注册到 Agent 平台：

```bash
pip install maxc-cli
maxc agent install-skill claude-code    # 或 cursor / windsurf / codex / qwen
```

`install-skill` 从包内 `skills/` 目录拷贝 SKILL.md + references/ 到目标平台目录，写入 `.maxc-skill-version` 标记版本。同版本跳过，版本变化覆盖。

### preflight 检查

Agent 启动时应先运行：

```bash
maxc agent context --json   # 版本、认证状态、后端可达性、能力矩阵
maxc agent skill --json     # SKILL.md 路径与 min_cli_version
```

## Envelope v2.0

所有 `--json` 输出遵循统一结构：

```json
{
  "version": "2.0",
  "command": "meta.describe",
  "command_id": "meta.describe",
  "status": "success | failure",
  "data": { ... },
  "metadata": { ... },
  "error": null | { "code": "...", "message": "...", "recovery_steps": [...] },
  "agent_hints": {
    "next_actions": ["maxc meta search --json", ...],
    "action_ids": ["meta.search", ...],
    "insights": [...],
    "warnings": [...]
  }
}
```

- `action_ids`：稳定 dot-notation，用于程序化路由（`meta.describe`、`job.wait`）
- `next_actions`：可直接 copy-paste 的 CLI 命令
- `error.recovery_steps`：错误码对应的恢复步骤

详见 [`docs/ENVELOPE_SPEC.md`](docs/ENVELOPE_SPEC.md)。

## 项目结构

```
src/maxc_cli/
├── cli.py               # argparse 命令注册
├── app.py               # MaxCApp 业务逻辑
├── models.py            # Envelope / AgentHints / QueryResult
├── exceptions.py        # ErrorPayload + 9 个异常子类 + recovery_steps
├── config.py            # YAML 配置加载
├── cache.py             # LocalCache (SQLite)
├── store.py             # JobStore (SQLite)
├── output.py            # Rich / 纯文本渲染
├── auth_providers.py    # AK-SK / NCS / 环境变量认证
├── backend/             # ODPS 后端（query / meta / catalog / data / diff 五个 mixin）
└── skills/              # SKILL.md + references/ + agents/（随 pip 包安装，唯一源）
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

## 限制

- **只读**：CLI 强制 SELECT-only，不支持 DDL/DML
- **auth login**：AK/SK 明文存储于 `~/.maxc/config.yaml`（文件权限 0600）
- **list-tables 分页**：CLI 侧 offset token，非服务端游标
- **diff data**：按主键快照对比，非全量 diff

## 开发

```bash
pip install -e .
pytest tests/ -m unit    # 142 个单元测试
```

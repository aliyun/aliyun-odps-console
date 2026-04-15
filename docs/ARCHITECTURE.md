# maxc-cli Architecture

> 代码级架构文档，补充 [design.md](./design.md) 的产品视角。

## 1. 模块总览

```
src/maxc_cli/
├── __init__.py          # __version__
├── __main__.py          # python -m maxc_cli
├── cli.py               # argparse 命令定义 (1310 行)
├── app.py               # MaxCApp 业务逻辑 (3605 行)
├── models.py            # Envelope / AgentHints / QueryResult / JobInfo
├── exceptions.py        # ErrorPayload + 9 个异常子类
├── config.py            # MaxCConfig / TableDefinition / YAML 加载
├── helpers.py           # ODPS 结果转换 / 错误翻译 / profile 构建
├── auth_providers.py    # AK-SK / NCS / 环境变量认证解析
├── cache.py             # LocalCache (SQLite)
├── store.py             # JobStore (SQLite)
├── output.py            # Rich / 纯文本渲染
├── audit.py             # 审计日志
├── utils.py             # extract_table_names / deep_merge / etc.
│
├── backend/
│   ├── __init__.py      # re-export OdpsBackend
│   ├── odps.py          # OdpsBackend 组合类 (130 行)
│   ├── query.py         # QueryMixin — execute / cost / explain
│   ├── job.py           # JobMixin — status / wait / cancel / diagnose
│   ├── meta.py          # MetaMixin — list / describe / search / lineage
│   ├── data.py          # DataMixin — sample / profile
│   └── auth.py          # AuthMixin — whoami / can-i
│
└── skills/
    ├── SKILL.md         # Agent 可读技能文档 (随包安装)
    ├── references/      # 参考文档子目录
    └── agents/          # Agent 平台适配模板
```

## 2. 三层架构

```
┌───────────────────────────────────────────────────────────────┐
│  CLI Layer — cli.py                                          │
│  argparse 定义子命令、参数解析、help 文本                      │
│  调用 MaxCApp 方法 → 拿到 Envelope → output.py 渲染输出        │
│  不含业务逻辑                                                 │
└────────────────────────┬──────────────────────────────────────┘
                         │ 调用 MaxCApp 方法
┌────────────────────────▼──────────────────────────────────────┐
│  Application Layer — app.py (MaxCApp)                         │
│  • 3605 行，所有业务逻辑在此                                   │
│  • 管理 backend 生命周期 (lazy init + _should_load_backend)    │
│  • 组装 Envelope (data + metadata + agent_hints)              │
│  • 错误捕获 → ErrorPayload → 结构化错误 Envelope              │
│  • 缓存策略 / 分页 / 远程任务提交                              │
└────────────────────────┬──────────────────────────────────────┘
                         │ 调用 backend 方法
┌────────────────────────▼──────────────────────────────────────┐
│  Backend Layer — backend/*.py (OdpsBackend)                   │
│  • OdpsBackend = QueryMixin + JobMixin                        │
│                  + MetaMixin + DataMixin + AuthMixin           │
│  • 每个 Mixin 对应一个 ODPS 领域                               │
│  • 纯 pyodps 调用封装，不含 CLI 逻辑                           │
│  • 方法签名含完整 docstring (Args/Returns/Raises/Limitations)  │
└────────────────────────┬──────────────────────────────────────┘
                         │ pyodps SDK
                    ┌────▼────┐
                    │  ODPS   │
                    └─────────┘
```

## 3. 核心数据流

### 3.1 正常执行

```
用户/Agent
    │
    ▼
maxc query "SELECT ..." --json
    │
    ▼ cli.py: parse args → call app.query(sql, ...)
    │
    ▼ app.py: validate → backend.execute_query()
    │
    ▼ backend/query.py: pyodps execute_sql + wait_for_success
    │
    ▼ app.py: build Envelope (rows, schema, agent_hints)
    │
    ▼ output.py: json.dumps(envelope.to_dict())
    │
    ▼ stdout (JSON)
```

### 3.2 错误执行

```
pyodps raises OdpsError
    │
    ▼ backend: translate_odps_error() → MaxCError 子类
    │
    ▼ app.py: except MaxCError → error.to_payload()
    │
    ▼ ErrorPayload(code, message, suggestion, recoverable, recovery_steps)
    │
    ▼ Envelope(status="error", error=ErrorPayload.to_dict())
    │
    ▼ output.py → stderr (JSON)
```

## 4. 关键类

### 4.1 Envelope (models.py)

```python
@dataclass
class Envelope:
    command: str           # "query", "meta.describe", etc.
    status: str            # "success" | "error"
    data: dict             # 命令结果
    metadata: dict         # job_id, elapsed_ms, project, etc.
    agent_hints: AgentHints
    error: dict | None     # ErrorPayload.to_dict()
    version: str
```

所有命令返回统一的 Envelope 结构，保证 Agent 解析一致。

### 4.2 MaxCApp (app.py)

```python
class MaxCApp:
    # 生命周期
    __init__(config)       # 加载配置，不初始化 backend
    _ensure_backend()      # lazy init OdpsBackend (首次需要时)
    _should_load_backend   # 白名单：auth.login/session/agent.* 免加载

    # 核心业务方法 (50+)
    query(sql, ...)        → Envelope
    job_status(job_id)     → Envelope
    meta_describe(table)   → Envelope
    agent_context()        → Envelope  # 含环境就绪检查
    agent_skill()          → Envelope
    agent_commands()       → Envelope
    ...
```

### 4.3 OdpsBackend (backend/odps.py)

```python
class OdpsBackend(JobMixin, MetaMixin, DataMixin, AuthMixin):
    """MaxCompute backend — pyodps 封装层"""
    supports_remote_jobs = True

    # JobMixin 继承 QueryMixin (submit → wait → fetch)
```

### 4.4 ErrorPayload (exceptions.py)

```python
@dataclass
class ErrorPayload:
    code: str              # "PERMISSION_DENIED", "JOB_TIMEOUT", etc.
    message: str
    suggestion: str | None
    recoverable: bool
    recovery_steps: list[str]  # Agent 可执行的恢复命令列表
```

## 5. Backend 初始化策略

maxc-cli 使用 **延迟初始化** 策略：

1. `MaxCApp.__init__()` 只加载配置，**不**连接 ODPS
2. 首次调用需要 backend 的方法时，`_ensure_backend()` 初始化 `OdpsBackend`
3. 白名单 `_should_load_backend` 中的命令**跳过** backend 加载：
   - `auth.login`, `auth.login-ncs`, `auth.whoami` (部分场景)
   - `session.set/show/unset`
   - `agent.context/skill/commands`

这确保了未认证用户也能获取帮助、查看 Skill 和命令列表。

## 6. 缓存架构

```python
class LocalCache:  # cache.py (662 行)
    # SQLite 存储，位于 ~/.maxc/cache/
    # 三类缓存表：
    #   table_metadata   — 元数据搜索加速
    #   table_semantic   — NL2SQL 语义描述 (FTS5)
    #   query_sessions   — 分页查询复用
```

缓存由 `cache build` 命令构建，`meta search` 等命令优先查缓存。
缓存失效策略基于 TTL + 手动 `cache clear`。

## 7. 认证流程

```
maxc auth login --from-env
    │
    ▼ auth_providers.py: resolve_auth_connection()
    │   1. 读取环境变量 ODPS_ACCESS_ID / ODPS_ACCESS_KEY
    │   2. 回退到 config.yaml 中的 auth 段
    │   3. 支持 NCS (内部认证服务)
    │
    ▼ persist_login_config() → ~/.maxc/config.yaml
    │
    ▼ 后续命令自动读取已保存的配置
```

## 8. Agent 集成点

| 集成方式 | 路径 | 说明 |
|---------|------|------|
| SKILL.md | `src/maxc_cli/skills/SKILL.md` | 随包安装，Agent 读取技能文档 |
| `maxc agent skill` | CLI 命令 | 返回 SKILL.md 路径 + 元数据 |
| `maxc agent commands` | CLI 命令 | 返回结构化命令目录 |
| `maxc agent context` | CLI 命令 | 返回环境就绪检查 + 能力矩阵 |
| agent_hints | 每个 Envelope | next_actions 为可执行 maxc 命令 |
| recovery_steps | ErrorPayload | 错误时提供可执行的恢复步骤 |
| agent install-skill | CLI 命令 | 注册 SKILL 到各 Agent 平台目录 |

## 9. 测试分层 (规划)

| 层级 | 标记 | 依赖 | 示例 |
|------|------|------|------|
| Unit | `@pytest.mark.unit` | 无外部依赖 | Envelope 构造、config 解析 |
| Integration | `@pytest.mark.integration` | ODPS 连接 | execute_query、list_tables |
| E2E | `@pytest.mark.e2e` | 完整 CLI | `maxc meta search` 子进程 |

详见 [tests/TEST.md](./tests/TEST.md)。

## 10. 依赖关系图

```
cli.py ──→ app.py ──→ backend/*.py ──→ pyodps
  │           │            │
  │           ├──→ config.py
  │           ├──→ models.py (Envelope)
  │           ├──→ exceptions.py (ErrorPayload)
  │           ├──→ cache.py (LocalCache)
  │           ├──→ store.py (JobStore)
  │           ├──→ helpers.py
  │           └──→ auth_providers.py
  │
  └──→ output.py (渲染)
```

## 11. 版本与发布

- 版本定义: `src/maxc_cli/__init__.py` → `__version__`
- 包数据: `setup.py` 中 `package_data` 包含 `skills/**/*`
- 当前版本: **0.1.3**

# maxc-cli Architecture

> 代码级架构文档，补充 [design.md](./design.md) 的产品视角。

## 1. 模块总览

```
src/maxc_cli/
├── __init__.py          # __version__
├── __main__.py          # python -m maxc_cli
├── cli.py               # argparse 命令定义 + 实时 agent manifest
├── app.py               # MaxCApp 业务逻辑
├── models.py            # Envelope / AgentHints / QueryResult / JobInfo
├── exceptions.py        # ErrorPayload + 类型化异常层级与稳定错误码
├── config.py            # MaxCConfig / TableDefinition / YAML 加载
├── helpers.py           # ODPS 结果转换 / 错误翻译 / profile 构建
├── auth_providers.py    # OAuth / AK-SK / STS / 外部进程 / 环境变量认证解析
├── oauth.py             # OAuth Authorization Code + PKCE、刷新与 STS 交换
├── odps_runtime.py      # ODPS client 创建与 User-Agent 观测标识
├── cache.py             # LocalCache (SQLite)
├── store.py             # JobStore（跨进程锁 + 原子写入的 JSON）
├── output.py            # Rich / 纯文本渲染
├── audit.py             # 审计日志
├── utils.py             # extract_table_names / deep_merge / etc.
│
├── backend/
│   ├── __init__.py      # re-export OdpsBackend
│   ├── odps.py          # OdpsBackend 组合类 (130 行)
│   ├── query.py         # QueryMixin — execute / cost / explain
│   ├── job.py           # JobMixin — status / wait / cancel / diagnose
│   ├── meta.py          # MetaMixin — list / describe / search (client-side)
│   ├── catalog.py       # CatalogMixin — server-side FTS search (pyodps RestClient, no extra deps)
│   ├── data.py          # DataMixin — sample / profile / upload / download
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
│  • 负责用例编排与 Envelope 组装                                 │
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
    ▼ Envelope(status="failure", error=ErrorPayload.to_dict())
    │
    ▼ output.py → stdout (JSON；人类可读错误使用 stderr)
```

## 4. 关键类

### 4.1 Envelope (models.py)

```python
@dataclass
class Envelope:
    command: str           # "query", "meta.describe", etc.
    status: str            # top-level: success, pending, or failure
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
    agent_context()        → Envelope  # 严格本地上下文，不访问网络
    agent_doctor(online)   → Envelope  # 本地检查 + 可选在线身份探测
    agent_manifest()       → Envelope  # 从实时 parser 生成命令契约
    agent_skill()          → Envelope
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
   - `auth.login`, `auth.login-external`
   - `session.set/show/unset`
   - `agent.context/manifest/skill` 与 Skill 生命周期命令

这确保了未认证用户也能获取帮助、查看 Skill、读取实时 manifest 和检查本地
配置。`agent context` 不证明远端可达；在线门禁是
`agent doctor --online`。

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
aliyun maxc auth login --oauth
    │
    ▼ oauth.py: Authorization Code + PKCE
    │   1. CLI 主机 127.0.0.1 启动 loopback callback
    │   2. 浏览器完成用户授权（--no-browser 只禁止自动打开）
    │   3. OAuth token 交换临时 STS
    │   4. 后续调用自动刷新 token / STS
    │
    ▼ persist_login_config() → ~/.maxc/config.yaml
    │
    ▼ 后续命令自动读取已保存的配置
```

公共云交互式认证优先 OAuth。已有 Alibaba Cloud CLI profile、环境变量、STS
或外部凭证进程时先验证现有身份；直接 AK/SK 是兼容路径，不是默认推荐。
`--no-browser` 不改变 loopback 回调，也不是 device-code/headless flow；SSH
场景需要端口转发或 CLI 同机浏览器。外部凭证进程只允许来自可信用户级配置或
用户显式指定的 `--config`，按 executable + argv 执行且不经过 shell；自动
发现的 workspace 配置不能定义 `auth`。

## 8. Agent 集成点

| 集成方式 | 路径 | 说明 |
|---------|------|------|
| SKILL.md | `src/maxc_cli/skills/SKILL.md` | Skill 名为 `alibabacloud-maxcompute-cli`，随包安装 |
| `maxc agent skill` | CLI 命令 | 返回 SKILL.md 路径 + 元数据 |
| `agent context` | CLI 命令 | 本地版本、配置和能力摘要，不访问网络 |
| `agent manifest` | CLI 命令 | 从实时 parser 输出命令、参数与副作用清单 |
| `agent doctor --online` | CLI 命令 | 验证认证与后端可达性 |
| agent_hints | 每个 Envelope | `actions[]` 为权威结构；`next_actions` 只保留可执行、Agent 可运行且无需确认的兼容命令 |
| recovery_steps | ErrorPayload | 错误时提供可执行的恢复步骤 |
| agent skill install | CLI 命令 | 注册 SKILL 到各 Agent 平台目录 |

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
- Python 独立发行版要求 **Python 3.9+**。
- 公共云首选 **Alibaba Cloud CLI 3.3.19+** 的 `aliyun maxc` 入口。
- 包装层执行 `agent context` 或离线 `agent doctor` 时只读取本地 profile
  元数据，并通过 `ALIBABA_CLOUD_MAXC_PROFILE_CONFIGURED=1` 传递非敏感就绪提示；
  不解析或注入 AK/SK/STS。在线检查和远端命令才解析实际凭据。
- 当前版本以 `src/maxc_cli/__init__.py` 为准，不在本文重复硬编码。

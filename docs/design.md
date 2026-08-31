# maxc-cli 设计文档

> 定位：maxc-cli 是 MaxCompute 的 **Agent-first 工具层**，供外部 AI Agent（Claude Code、Codex、Cursor 等）调用。

## 一、核心定位

```
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│   maxc-cli 是供 Agent 调用的「工具箱」                             │
│                                                                    │
│   ┌──────────────────────────────────────────────────────────────┐ │
│   │  外部 AI Agent（Claude Code / Codex / Cursor / 自研）        │ │
│   │                                                              │ │
│   │  用户: "帮我查一下上月销售数据"                              │ │
│   │       ↓                                                      │ │
│   │  Agent 理解意图 → 决定调用哪些 maxc 命令                    │ │
│   │       ↓                                                      │ │
│   │  $ maxc meta search "销售"                                   │ │
│   │  $ maxc data sample dws_sale_1d                              │ │
│   │  $ maxc query "SELECT SUM(gmv) FROM dws_sale_1d..."          │ │
│   │       ↓                                                      │ │
│   │  Agent 解读 JSON 输出 → 给用户友好回答                       │ │
│   └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│   maxc-cli 只负责：                                               │
│   ✓ 执行命令，返回结构化 JSON                                     │
│   ✓ 提供 agent_hints 辅助 Agent 决策                             │
│   ✓ 提供 Skill 文档供 Agent 参考                                 │
│                                                                    │
│   maxc-cli 不负责：                                               │
│   ✗ 理解自然语言（那是外部 Agent 的事）                          │
│   ✗ 自主决策执行计划（那是外部 Agent 的事）                      │
│   ✗ 执行 Skill（Skill 只是给 Agent 的参考手册）                  │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

安装后，外部 Agent 直接调用 `maxc` 命令即可：

```bash
python -m pip install maxc-cli
maxc auth login --from-env --json
maxc auth whoami --json
```

## 二、设计原则

### 2.1 Agent-first 输出设计

```json
{
  "version": "2.0",
  "command": "query",
  "status": "success",
  "data": {
    "result": {
      "rows": [{"id": 1}],
      "schema": [{"name": "id", "type": "BIGINT", "comment": ""}],
      "row_count": 1000,
      "returned_rows": 1
    },
    "pagination": {
      "has_more": true,
      "next_cursor": "eyJvIjoxMDB9"
    },
    "safety": {
      "mode": "read_only",
      "force": false,
      "allowed_operations": ["SELECT"],
      "effective_hints": {},
      "policy_decision": "allowed"
    }
  },
  "metadata": {
    "job_id": "20260320_xxx",
    "elapsed_ms": 1230,
    "project": "my_project",
    "sql_executed": "SELECT id FROM my_table",
    "tables_used": ["my_table"]
  },
  "error": null,
  "agent_hints": {
    "actions": [
      {
        "id": "meta.describe",
        "title": "Describe table",
        "command": "maxc --user-agent <user_agent> meta describe my_table --project my_project --json",
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
      },
      {
        "id": "query.paginate",
        "title": "Next page",
        "command": "maxc --user-agent <user_agent> query 'SELECT id FROM my_table' --cursor eyJvIjoxMDB9 --project my_project --json",
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
    "action_ids": ["meta.describe", "query.paginate"],
    "warnings": ["大表全扫描，建议增加分区过滤"]
  }
}
```

这里没有 `next_actions`：两个云端动作仍缺当前会话的 User-Agent，因此是
`executable=false` 的模板。Agent 填充经过验证的占位符后才能执行；
`actions[]` 和 `action_ids` 始终是完整、权威的动作集合。

### 2.2 核心特征

- **机器可读输出**：Agent 显式使用 `--json` 获取 Envelope v2.0
- **结构化错误**：错误信息包含 `code`、`message`、`suggestion` 和 `recovery_steps`
- **agent_hints**：`actions[]` 是权威结构；`next_actions` 只包含已可执行、Agent 可运行且无需确认的兼容命令
- **安全续接**：优先使用返回的 job ID、cursor 和结构化动作继续；不要把写操作或提交失败当作可盲目重放的幂等请求
- **分页支持**：cursor 机制，复用查询结果

## 三、命令体系

```
maxc/
├── 查询执行
│   ├── maxc query [SQL]              # 执行 SQL，返回 JSON
│   │     --project    指定项目
│   │     --max-rows   结果行数限制
│   │     --cursor     分页 cursor
│   │     --dry-run    只生成执行计划
│   │
│   ├── maxc query cost [SQL]         # 预估输入量/复杂度
│   ├── maxc query explain [SQL]      # 返回结构化 explain 信息
│   └── maxc query --stdin            # 从 stdin 读取 SQL
│
├── 任务管理
│   ├── maxc job submit [SQL]         # 提交异步 SQL 作业
│   ├── maxc job status [job_id]      # 查询任务状态
│   ├── maxc job wait [job_id]        # 等待任务完成
│   ├── maxc job result [job_id]      # 获取任务结果
│   ├── maxc job cancel [job_id]      # 取消任务
│   ├── maxc job diagnose [job_id]    # 诊断失败原因
│   └── maxc job list                 # 任务列表
│
├── 元数据操作
│   ├── maxc meta list-tables         # 表列表
│   ├── maxc meta describe [table]    # 表结构详情
│   ├── maxc meta search [keyword]    # 搜索表
│   ├── maxc meta search-columns ...  # 按字段搜索
│   ├── maxc meta partitions [table]  # 分区信息
│   ├── maxc meta latest-partition    # 最新分区
│   └── maxc meta freshness [table]   # 数据新鲜度
│
├── 数据操作
│   ├── maxc data sample [table]      # 数据采样
│   ├── maxc data profile [table]     # 数据质量分析
│   ├── maxc data upload [table]      # CSV/TSV 上传（支持 dry-run）
│   └── maxc data download [table]    # 安全下载（默认不覆盖已有文件）
│
├── 缓存管理
│   ├── maxc cache build              # 构建元数据缓存
│   ├── maxc cache status             # 缓存状态
│   └── maxc cache clear              # 清除缓存
│
├── 认证管理
│   ├── maxc auth login --oauth       # 公共云交互式首选
│   ├── maxc auth whoami              # 当前身份
│   └── maxc auth can-i               # 权限检查
│
└── Agent 辅助
    ├── maxc agent context            # 本地环境上下文（不访问网络）
    ├── maxc agent manifest           # 实时命令与副作用契约
    └── maxc agent doctor --online    # 在线身份与可达性检查
```

## 四、Skill 文档

SKILL.md 随 pip 包安装，位于 `src/maxc_cli/skills/`（package_data），运行时通过 `importlib.resources` 定位。这是唯一源，不在 repo 中维护第二份副本。

Agent 平台注册通过 `maxc agent skill install <platform>` 完成，它会从安装包中拷贝 SKILL.md 和 references 到目标目录。

支持的平台及安装目录以实时
`maxc agent skill install --help` 为准；常见目标包括 `claude-code`、
`cursor`、`windsurf`、`codex`、`qwen`、`qoder`、`openclaw` 和 `hermes`。

## 五、配置体系

### 5.1 全局配置 `~/.maxc/config.yaml`

全局配置通常由 `maxc auth login` 写入。显式配置的认证 provider 优先，
认证环境变量不会静默覆盖它；要使用环境变量需选择
`auth login --from-env`。未配置 provider 时，CLI 才从环境变量补齐连接信息。

`maxc session set --project/--schema` 直接写入 `~/.maxc/config.yaml` 的 `default_project` / `default_schema`，不再使用单独的 override 文件。

当前工作树不再提供运行时 mock fallback。

```yaml
auth:
  access_id: "<access_key_id>"
  secret_access_key: "<access_key_secret>"
  project: my_project
  endpoint: http://service.cn-hangzhou.maxcompute.aliyun.com/api
  region_name: cn-hangzhou

default_project: my_project
default_region: cn-hangzhou
default_format: json

backend:
  type: auto
```

### 5.2 项目配置 `.maxc/config.yaml`

```yaml
# 项目级只覆盖上下文与安全约束。
# 如果希望继续沿用全局 login，不建议在这里硬编码 default_project。
backend:
  type: auto

# 业务上下文（供 agent context 输出）
project_context: |
  这是零售业务数仓项目
  - 事实表前缀: dwd_
  - 汇总表前缀: dws_
  - 应用层前缀: ads_
  - 主分区字段: dt (格式 yyyy-MM-dd)

# 安全配置
allowed_operations:
  - SELECT
sensitive_columns:
  - user_phone
  - id_card_no
```

默认发现顺序：

```text
~/.maxc/config.yaml
./.maxc/config.yaml
./.maxc.yaml
./.maxc
```

## 六、SQLite 缓存架构

maxc-cli 使用 SQLite 作为本地缓存，支持：

### 6.1 查询会话缓存

```sql
-- 复用分页查询结果
CREATE TABLE query_sessions (
    session_id INTEGER PRIMARY KEY,
    job_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
```

### 6.2 元数据缓存

```sql
-- 加速 meta search
CREATE TABLE table_metadata (
    project TEXT NOT NULL,
    table_name TEXT NOT NULL,
    schema_json TEXT,
    stats_json TEXT,
    cached_at TEXT NOT NULL,
    PRIMARY KEY (project, table_name)
);
```

### 6.3 语义元数据缓存

```sql
-- 支持 NL2SQL 场景
CREATE TABLE table_semantic (
    project TEXT NOT NULL,
    table_name TEXT NOT NULL,
    semantic_desc TEXT,       -- AI 生成的表描述
    use_cases TEXT,           -- 典型使用场景
    sample_questions TEXT,    -- 示例问题
    column_semantics_json TEXT,
    PRIMARY KEY (project, table_name)
);

-- FTS5 全文索引
CREATE VIRTUAL TABLE table_fts USING fts5(...);
```

## 七、与传统 CLI 的区别

| 特性 | 传统 odpscmd | maxc-cli |
|------|-------------|----------|
| 设计目标 | 人用 | Agent 用 |
| 输出格式 | 文本表格 | 结构化 JSON |
| 错误信息 | 自然语言 | 结构化 code + suggestion |
| 分页 | 无 | cursor 机制 |
| 决策辅助 | 无 | agent_hints |
| 缓存 | 无 | SQLite 本地缓存 |

## 八、后续规划

详见 [roadmap.md](./roadmap.md)

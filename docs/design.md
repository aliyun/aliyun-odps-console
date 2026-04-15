# maxc-cli 设计文档

> 定位：maxc-cli 是 MaxCompute 的 **Agent-first 工具层**，供外部 AI Agent（Claude Code、Codex、Cursor 等）调用。

## 一、核心定位

```
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│   maxc-cli 不是 Agent，而是 Agent 的「工具箱」                     │
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
  "command": "query",
  "status": "success",
  "data": {
    "rows": [...],
    "schema": [...],
    "total_rows": 1000,
    "returned_rows": 100,
    "has_more": true,
    "next_cursor": "eyJvIjoxMDB9"
  },
  "metadata": {
    "job_id": "20260320_xxx",
    "elapsed_ms": 1230,
    "project": "my_project"
  },
  "agent_hints": {
    "next_actions": [
      "query \"SELECT ...\" --cursor eyJvIjoxMDB9 --json",
      "data profile my_table --json"
    ],
    "action_ids": ["query.paginate", "data.profile"],
    "warnings": ["大表全扫描，建议增加分区过滤"],
    "insights": ["结果为空，可能原因：日期范围无数据"]
  }
}
```

### 2.2 核心特征

- **机器可读输出**：默认 JSON 格式，所有命令 `--json` 输出一致
- **结构化错误**：错误信息包含 `code`、`message`、`suggestion`
- **agent_hints**：`next_actions` 直接返回可执行命令片段，`action_ids` 保留稳定的动作标识
- **幂等设计**：相同操作多次执行结果一致
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
│   ├── maxc meta freshness [table]   # 数据新鲜度
│   └── maxc meta freshness [table]   # 数据新鲜度
│
├── 数据操作
│   ├── maxc data sample [table]      # 数据采样
│   └── maxc data profile [table]     # 数据质量分析
│
├── 差异比较
│   ├── maxc diff schema              # Schema 差异
│   ├── maxc diff partition           # 分区差异
│   └── maxc diff data                # 数据差异
│
├── 缓存管理
│   ├── maxc cache build              # 构建元数据缓存
│   ├── maxc cache status             # 缓存状态
│   ├── maxc cache clear              # 清除缓存
│   ├── maxc cache build              # 构建缓存
│   └── maxc cache clear              # 清除缓存
│
├── 认证管理
│   ├── maxc auth login               # 保存 AccessKey 登录配置
│   ├── maxc auth whoami              # 当前身份
│   └── maxc auth can-i               # 权限检查
│
└── Agent 辅助
    └── maxc agent context            # 输出环境上下文
```

## 四、Skill 文档

SKILL.md 随 pip 包安装，位于 `src/maxc_cli/skills/`（package_data），运行时通过 `importlib.resources` 定位。这是唯一源，不在 repo 中维护第二份副本。

Agent 平台注册通过 `maxc agent install-skill <platform>` 完成，它会从安装包中拷贝 SKILL.md 和 references 到目标目录。

支持的平台：
- `claude-code`：`~/.claude/plugins/maxc-cli/`
- `cursor`：`~/.cursor/skills/use-maxc-cli/`
- `windsurf`：`~/.windsurf/skills/use-maxc-cli/`
- `codex`：`$CODEX_HOME/skills/use-maxc-cli/`

## 五、配置体系

### 5.1 全局配置 `~/.maxc/config.yaml`

全局配置通常由 `maxc auth login` 写入。连接信息优先级是：

1. 环境变量
2. 配置文件中的 `auth`

project/schema 选择还会叠加：

3. `session_override.yaml` 中的 project/schema

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

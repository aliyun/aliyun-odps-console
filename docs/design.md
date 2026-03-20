## 零、文档说明（实施视角）

- 本文同时承担产品愿景、路线图和接口草案三种角色，阅读时必须区分“目标态”和“当前实现”。
- 当前仓库的开发基线、真实 MaxCompute 对接方式和已知缺口，统一记录在 [docs/implementation.md](./implementation.md)。
- 当文档抽象与 MaxCompute 原生能力不完全一致时，优先保留语义目标，再映射到真实字段。
  典型例子：统一 `cost_cu` 可在真实 backend 中下沉为 `task_cost_cpu`、`task_cost_memory`、`estimated_input_size_bytes`。

## 一、CLI 和 Skill
### 1.1 2026 Agent 演进的核心趋势
```plain
┌──────────────────────────────────────────────────────────────────┐
│  2026 Agent 演进的核心趋势：                                       │
│                                                                    │
│  ① Agent 从「工具调用」→「自主执行」                             │
│     LLM 不再只是 Chat，而是直接驱动系统完成任务                  │
│                                                                    │
│  ② 执行环境从「沙箱」→「真实系统」                              │
│     Agent 需要像人一样操作终端、文件、数据库                      │
│                                                                    │
│  ③ 能力组合从「单工具」→「技能组合（Skill）」                   │
│     复杂任务需要多步骤、有状态、可复用的能力单元                  │
│                                                                    │
│  结论：CLI 是 Agent 的「手」，Skill 是 Agent 的「脑」             │
│        两者合一，MaxC 才能真正 Agent-First                        │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 1.2 CLI 与 Skill 的本质定义
```plain
┌──────────────────────────────────────────────────────────────────┐
│  CLI（Agent CLI）                                                 │
│                                                                  │
│  不是传统的「人用命令行」                                         │
│  而是「Agent 驱动的可编程执行界面」                              │
│                                                                  │
│  核心特征：                                                       │
│  ├── 机器可读输出（JSON/NDJSON）                                 │
│  ├── 非交互式执行（--non-interactive）                           │
│  ├── 结构化退出码（成功/失败/需要人工介入）                      │
│  ├── 流式日志输出（Agent 可实时感知进度）                        │
│  ├── 可组合管道（Unix 哲学）                                     │
│  └── 幂等设计（Agent 重试友好）                                  │
│                                                                  │
│  类比：OpenAI Codex CLI、GitHub CLI(gh)、AWS CLI v2             │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│  Skill（Agent Skill）                                            │
│                                                                  │
│  不是简单的「API 调用」                                           │
│  而是「有状态、可组合、语义化的能力单元」                        │
│                                                                  │
│  核心特征：                                                       │
│  ├── 语义描述（LLM 可理解能做什么）                             │
│  ├── 有状态（多步骤执行，保持中间状态）                          │
│  ├── 可组合（Skill 可以调用其他 Skill）                         │
│  ├── 可测试（有明确的输入/输出契约）                             │
│  ├── 可发现（注册到 Skill Registry）                             │
│  └── 跨框架（不绑定任何 Agent 框架）                            │
│                                                                  │
│  类比：Semantic Kernel Skill、Google A2A Skill、                 │
│        Copilot Studio Skill                                      │
└──────────────────────────────────────────────────────────────────┘
```

---

## 二、MaxCompute CLI for Agent（maxc CLI）
### 2.1 设计哲学
```plain
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│   传统 CLI 设计原则：「人读输出」                                  │
│   maxc CLI 设计原则：「Agent 读输出，人也能读」                   │
│                                                                    │
│   Human-First CLI：$ odpscmd -e "SELECT count(*) FROM t"         │
│                    输出：+------------+                           │
│                          | _c0        |                           │
│                          +------------+                           │
│                          | 1234567    |                           │
│                          +------------+                           │
│                                                                   │
│   Agent-First CLI：$ maxc query "SELECT count(*) FROM t" --json  │
│                    输出：{                                         │
│                            "status": "success",                   │
│                            "rows": [{"_c0": 1234567}],           │
│                            "metadata": {                          │
│                              "elapsed_ms": 230,                   │
│                              "bytes_scanned": 10240,             │
│                              "task_cost_cpu": 5,                 │
│                              "task_cost_memory": 56,             │
│                              "cost_cu": null                     │
│                            }                                      │
│                          }                                        │
│                                                                   │
└────────────────────────────────────────────────────────────────────┘
```

### 2.2 maxc CLI 完整命令体系（目标态视图）
```plain
maxc/
├── 认证管理
│   ├── maxc auth login          # 交互式登录（人用）
│   ├── maxc auth token          # 输出临时 Token（Agent用）
│   └── maxc auth whoami --json  # 当前身份信息
│
├── 查询执行（核心）
│   ├── maxc query [SQL]                    # 执行 SQL
│   │     --project    指定项目
│   │     --format     json|csv|ndjson|table
│   │     --max-rows   结果行数限制
│   │     --timeout    超时设置
│   │     --async      异步提交，返回 job_id
│   │     --dry-run    只生成执行计划不执行
│   │     --cost-check 超过费用阈值则中止
│   │
│   ├── maxc query --file query.sql         # 从文件读取SQL
│   ├── maxc query --stdin                  # 从 stdin 读取SQL（管道）
│   └── maxc query @natural "上月销售额"   # 自然语言转SQL执行
│
├── 任务管理
│   ├── maxc job submit [SQL]     # 提交异步任务
│   ├── maxc job status [job_id]  # 查询任务状态
│   ├── maxc job wait [job_id]    # 等待任务完成（流式进度）
│   ├── maxc job result [job_id]  # 获取任务结果
│   ├── maxc job cancel [job_id]  # 取消任务
│   └── maxc job list             # 任务列表
│
├── 元数据操作
│   ├── maxc meta list-tables     # 表列表
│   ├── maxc meta describe [table]# 表结构
│   ├── maxc meta search [keyword]# 语义搜索表/字段
│   ├── maxc meta lineage [table] # 血缘关系
│   └── maxc meta partitions [t]  # 分区信息
│
├── 数据操作
│   ├── maxc data upload          # 数据上传
│   ├── maxc data download        # 数据下载
│   ├── maxc data sample [table]  # 数据采样
│   └── maxc data profile [table] # 数据质量分析
│
├── 资源管理
│   ├── maxc resource quota       # 查看配额
│   ├── maxc resource usage       # 资源使用情况
│   └── maxc resource topk        # TopK 耗费任务
│
└── Agent 专属命令（新增核心）
    ├── maxc agent run [goal]     # 自然语言目标，自主执行
    ├── maxc agent plan [goal]    # 只输出执行计划不执行
    ├── maxc agent skill [name]   # 调用注册的 Skill
    └── maxc agent context        # 输出当前环境上下文（给LLM用）
```

#### 2.2.1 当前开发基线（2026-03）

当前仓库不按上面的“全量命令树”同时开工，而是按可落地性分层：

- 已优先落地：`query`（含 `cost` / `explain` / cursor 分页）、`job`、`meta`（含 `search-columns` / richer `describe`）、`data sample/profile`、`agent context`、`agent skill`、`skill list/info`
- 已接真实 backend：真实 MaxCompute 查询、实例状态追踪、表结构读取、数据采样
- 仅保留骨架或路线图：`@natural`、`agent plan`、`agent run`、`Human-in-the-Loop`、`Skill Registry install/publish`
- 暂未实现：`auth`、`resource`、`data upload/download`、真实 `lineage API`

### 2.3 Agent-Native 关键设计细节
#### 2.3.1 结构化输出规范
```json
// 所有命令统一输出格式（--json 模式）
{
  "version": "1.0",
  "command": "query",
  "status": "success | failure | partial | pending",
  
  // 执行结果
  "data": {
    "rows": [...],
    "schema": [
      {"name": "col1", "type": "string", "comment": "用户ID"}
    ],
    "total_rows": 1000,
    "returned_rows": 100,
    "has_more": true,
    "next_cursor": "eyJvZmZzZXQiOjEwMH0="
  },
  
  // 执行元数据（Agent 决策依据）
  "metadata": {
    "job_id": "20260320_xxx",
    "elapsed_ms": 1230,
    "bytes_scanned": 10485760,
    "task_cost_cpu": 14,
    "task_cost_memory": 144,
    "estimated_input_size_bytes": 10485760,
    "cost_cu": null,
    "project": "my_project",
    "sql_executed": "SELECT ..."
  },
  
  // 错误信息（结构化，Agent可理解）
  "error": {
    "code": "SYNTAX_ERROR | PERMISSION_DENIED | QUOTA_EXCEEDED",
    "message": "Column 'user_id' not found in table 'orders'",
    "suggestion": "Did you mean 'buyer_id'? Available columns: [...]",
    "recoverable": true
  },
  
  // Agent 行动建议（新增核心字段）
  "agent_hints": {
    "next_actions": ["retry", "check_schema", "reduce_scope"],
    "warnings": ["大表全扫描，建议增加分区过滤"],
    "insights": ["结果为空，可能原因：日期范围无数据"]
  }
}
```

> 实施说明：
> 1. `cost_cu` 不是 MaxCompute 原生字段，真实 backend 当前优先输出 `task_cost_cpu`、`task_cost_memory` 和 `estimated_input_size_bytes`。
> 2. `cost_cu` 可以保留为兼容字段；只有当上层补齐统一计费映射时才建议填充。

#### 2.3.2 流式输出（NDJSON）
```bash
# Agent 实时感知长任务进度
$ maxc job wait job_20260101_xxx --stream

# 输出流（每行一个 JSON 事件）：
{"type":"started",    "ts":"2026-01-01T10:00:00Z", "job_id":"xxx"}
{"type":"progress",   "ts":"2026-01-01T10:00:05Z", "percent":20, "stage":"map"}
{"type":"progress",   "ts":"2026-01-01T10:00:10Z", "percent":60, "stage":"reduce"}
{"type":"log",        "ts":"2026-01-01T10:00:12Z", "level":"warn", "msg":"内存使用率 85%"}
{"type":"completed",  "ts":"2026-01-01T10:00:15Z", "rows":10000, "task_cost_cpu":14, "task_cost_memory":144, "estimated_input_size_bytes":10485760}
```

#### 2.3.3 幂等与重试设计
```bash
# 幂等性：相同操作多次执行结果一致
$ maxc query "SELECT ..." --idempotency-key "task-20260101-001"

# 自动重试配置
$ maxc query "SELECT ..." \
    --retry-on "QUOTA_EXCEEDED,TRANSIENT_ERROR" \
    --max-retries 3 \
    --retry-backoff exponential

# 退出码规范（Agent CI/CD 感知）
# 0  = 成功
# 1  = 执行失败（可重试）
# 2  = 权限错误（不可重试，需人工介入）
# 3  = 配额超限（等待后重试）
# 4  = SQL错误（需要修正SQL）
# 5  = 费用超限（需审批）
```

#### 2.3.4 管道组合（Unix 哲学）
```bash
# Agent 可以像写 shell 脚本一样组合 maxc 命令

# 场景1：查询结果传给下一个工具
$ maxc query "SELECT user_id, gmv FROM dws_sale" --format ndjson \
  | jq '.data.rows[] | select(.gmv > 10000)' \
  | python3 send_to_crm.py

# 场景2：多步骤数据处理管道
$ maxc meta search "用户留存" --json \
  | jq -r '.data[0].table_name' \
  | xargs -I {} maxc data sample {} --rows 100 --json \
  | maxc agent analyze "分析这批数据的质量问题"

# 场景3：CI 场景，检查 SQL 并执行
$ maxc query --file daily_report.sql \
    --dry-run \
    --cost-check 100 \   # 仅当 backend 提供统一成本口径时启用
  && maxc query --file daily_report.sql --async
```

> 当前真实 MaxCompute backend 建议先用 `--dry-run` 获取 `SQLCost` / `task cost` 元数据，再由上层策略判断是否继续执行。

#### 2.3.5 自然语言模式（Agent 核心）

> 路线图状态：本节属于 Q2 能力设计，当前不应作为 Q1 开发阻塞项。

```bash
# @ 前缀 = 自然语言模式
$ maxc query @natural "统计上个月各省份的 GMV，按降序排列"

# 输出：
{
  "status": "success",
    "nl_to_sql": {
    "input": "统计上个月各省份的 GMV，按降序排列",
    "generated_sql": "SELECT province, SUM(gmv) AS total_gmv FROM dws_sale_province_1d WHERE dt BETWEEN '2025-12-01' AND '2025-12-31' GROUP BY province ORDER BY total_gmv DESC",
    "confidence": 0.95,
    "tables_used": ["dws_sale_province_1d"],
    "explanation": "使用省份日粒度销售表，聚合上月GMV"
  },
  "data": {
    "rows": [
      {"province": "广东", "total_gmv": 98234567.0},
      {"province": "浙江", "total_gmv": 76123456.0}
    ]
  }
}
```

```plain
┌────────────────────────────────────────────────────────────────────┐
│           maxc query @natural "上月各省 GMV 排名" 的完整链路        │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ① CLI 接收命令                                                    │
│     maxc query @natural "上月各省 GMV 排名"                        │
│     └── 识别 @natural 前缀 → 走 AgentAPI 路由                    │
│                                                                    │
│  ② CLI 读取本地上下文                                              │
│     └── 读取 .maxc 配置：project / 业务语义 / 敏感字段定义        │
│                                                                    │
│  ③ CLI → AgentAPI（HTTP POST）                                    │
│     POST https://maxc-agent.aliyuncs.com/agent/v1/query           │
│     Authorization: Bearer <token>                                  │
│     {                                                              │
│       "question": "上月各省 GMV 排名",                            │
│       "project": "retail_dw",                                     │
│       "context": {                                                 │
│         "project_desc": "零售业务数仓，GMV=支付金额不含退款",     │
│         "preferred_tables": ["dws_sale_province_1d"]              │
│       },                                                           │
│       "mode": "nl2sql_and_execute",                               │
│       "stream": true                                               │
│     }                                                              │
│                                                                    │
│  ④ AgentAPI 内部处理（流式返回）                                   │
│     data: {"type":"thinking","content":"分析问题，定位相关表..."}  │
│     data: {"type":"schema_lookup","tables":["dws_sale_province"]} │
│     data: {"type":"sql_generated","sql":"SELECT province,SUM(gmv)…"}│
│     data: {"type":"executing","job_id":"20260201_xxx"}            │
│     data: {"type":"result","rows":[...],"total":31}               │
│     data: {"type":"insight","content":"广东 GMV 最高，达 9.8亿"} │
│     data: {"type":"done"}                                         │
│                                                                    │
│  ⑤ CLI 接收流 → 格式化为 Agent-Native 输出                        │
│     {                                                              │
│       "status": "success",                                        │
│       "nl_to_sql": { "sql": "SELECT ...", "confidence": 0.96 },  │
│       "data": { "rows": [...] },                                  │
│       "insight": "广东 GMV 最高，达 9.8亿",                      │
│       "agent_hints": { "next_actions": ["drill_down","export"] } │
│     }                                                             │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘

```

```bash
# 执行计划模式（Agent 先看计划再决定是否执行）
$ maxc agent plan "帮我找出最近30天没有下单的用户，发送到营销系统"

# 输出执行计划：
{
  "goal": "找出最近30天没有下单的用户，发送到营销系统",
  "plan": [
    {
      "step": 1,
      "action": "meta_search",
      "description": "搜索用户表和订单表",
      "command": "maxc meta search '用户 订单'"
    },
    {
      "step": 2,
      "action": "query",
      "description": "生成休眠用户 SQL",
      "estimated_cost_cu": 12.5,
      "estimated_rows": 50000,
      "requires_approval": false
    },
    {
      "step": 3,
      "action": "skill_call",
      "description": "调用营销系统 Skill",
      "skill": "marketing.push_users",
      "requires_approval": true,
      "reason": "写操作，需要人工确认"
    }
  ],
  "total_estimated_cost_cu": 12.5,
  "risk_level": "medium",
  "human_approval_required": true
}
```

> 说明：本节示例中的 `estimated_cost_cu` / `total_estimated_cost_cu` 是规划层抽象字段。
> 如果直接对接真实 MaxCompute，可以替换为 `estimated_task_cost_cpu`、`estimated_task_cost_memory`，或统一的 `estimated_cost` 对象。

```plain
┌────────────────────────────────────────────────────────────────────┐
│           $ maxc agent plan "找出上月流失用户，推送营销系统"        │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Step 0：CLI 本地预处理                                       │ │
│  │                                                              │ │
│  │  ① 读取 .maxc 配置                                          │ │
│  │     project / 业务语义描述 / 敏感字段 / 安全规则             │ │
│  │                                                              │ │
│  │  ② 读取本地 Skill Registry                                  │ │
│  │     已安装哪些 Skill → 作为 context 传给 AgentAPI           │ │
│  │                                                              │ │
│  │  ③ 构建 AgentAPI 请求体                                     │ │
│  │     POST /agent/v1/plan                                     │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                           │                                        │
│                           ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Step 1：AgentAPI — 意图理解（Intent Parsing）               │ │
│  │                                                              │ │
│  │  输入：goal + project_context + available_skills            │ │
│  │                                                              │ │
│  │  LLM 推理：                                                  │ │
│  │  ┌────────────────────────────────────────────────────────┐ │ │
│  │  │ 目标分解：                                              │ │ │
│  │  │  Sub-goal 1：定义「流失用户」                           │ │ │
│  │  │  Sub-goal 2：查询上月流失用户数据                       │ │ │
│  │  │  Sub-goal 3：将用户数据推送到营销系统                   │ │ │
│  │  │                                                        │ │ │
│  │  │ 操作类型识别：                                          │ │ │
│  │  │  READ  → Sub-goal 1, 2                                 │ │ │
│  │  │  WRITE → Sub-goal 3（外部系统写入，高风险）            │ │ │
│  │  └────────────────────────────────────────────────────────┘ │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                           │                                        │
│                           ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Step 2：AgentAPI — 数据环境探查（Context Grounding）        │ │
│  │                                                              │ │
│  │  并行执行：                                                  │ │
│  │                                                              │ │
│  │  ┌──────────────────┐    ┌──────────────────────────────┐  │ │
│  │  │  Schema 语义搜索  │    │   Skill 匹配                  │  │ │
│  │  │                  │    │                              │  │ │
│  │  │ 向量检索：        │    │ 搜索已安装 Skill：            │  │ │
│  │  │ "流失用户"        │    │ ✅ maxc.insight.churn        │  │ │
│  │  │ "留存"           │    │ ✅ maxc.data.query            │  │ │
│  │  │ "用户行为"        │    │ ❌ marketing.push（未安装）   │  │ │
│  │  │                  │    │                              │  │ │
│  │  │ 命中表：          │    │ → 标记 Step 3 需要外部集成   │  │ │
│  │  │ dws_user_        │    └──────────────────────────────┘  │ │
│  │  │  retention_1d   │                                        │ │
│  │  │ dwd_user_event  │    ┌──────────────────────────────┐  │ │
│  │  │  _detail        │    │  权限预检查                   │  │ │
│  │  └──────────────────┘    │                              │  │ │
│  │                          │ ✅ dws_user_retention_1d 有权 │  │ │
│  │                          │ ✅ dwd_user_event_detail 有权 │  │ │
│  │                          │ ⚠️  phone 字段需脱敏          │  │ │
│  │                          └──────────────────────────────┘  │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                           │                                        │
│                           ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Step 3：AgentAPI — 计划生成（Plan Generation）              │ │
│  │                                                              │ │
│  │  LLM 基于前两步信息，生成结构化计划：                        │ │
│  │                                                              │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │  Plan DSL（内部表示）                                 │  │ │
│  │  │                                                      │  │ │
│  │  │  steps:                                              │  │ │
│  │  │    - id: clarify_churn_definition                    │  │ │
│  │  │      type: nl2sql                                    │  │ │
│  │  │      description: "定义流失：30天无登录且无购买"     │  │ │
│  │  │      sql: |                                          │  │ │
│  │  │        SELECT user_id                                │  │ │
│  │  │        FROM dws_user_retention_1d                    │  │ │
│  │  │        WHERE dt BETWEEN '2025-12-01'                 │  │ │
│  │  │          AND '2025-12-31'                            │  │ │
│  │  │          AND login_days = 0                          │  │ │
│  │  │          AND order_cnt = 0                           │  │ │
│  │  │      estimated_cost_cu: 8.5                          │  │ │
│  │  │      estimated_rows: 230000                          │  │ │
│  │  │      risk: low                                       │  │ │
│  │  │      auto_approve: true                              │  │ │
│  │  │                                                      │  │ │
│  │  │    - id: enrich_user_profile                         │  │ │
│  │  │      type: skill                                     │  │ │
│  │  │      skill_id: maxc.data.query                       │  │ │
│  │  │      description: "补充用户画像（年龄/城市/消费层级）"│  │ │
│  │  │      depends_on: [clarify_churn_definition]          │  │ │
│  │  │      estimated_cost_cu: 3.2                          │  │ │
│  │  │      risk: low                                       │  │ │
│  │  │      auto_approve: true                              │  │ │
│  │  │                                                      │  │ │
│  │  │    - id: push_to_marketing                           │  │ │
│  │  │      type: external_skill                            │  │ │
│  │  │      skill_id: marketing.push_users                  │  │ │
│  │  │      description: "推送至营销系统（约23万用户）"     │  │ │
│  │  │      depends_on: [enrich_user_profile]               │  │ │
│  │  │      risk: high                                      │  │ │
│  │  │      auto_approve: false   ← 需要人工审批            │  │ │
│  │  │      reason: "写操作+外部系统+影响用户数>10万"       │  │ │
│  │  │      missing_skill: true   ← marketing.push 未安装  │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                           │                                        │
│                           ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Step 4：AgentAPI — 费用与风险汇总（Cost & Risk Analysis）   │ │
│  │                                                              │ │
│  │  total_cost_cu:    11.7                                      │ │
│  │  total_steps:      3                                         │ │
│  │  auto_steps:       2                                         │ │
│  │  approval_steps:   1                                         │ │
│  │  missing_skills:   ["marketing.push_users"]                  │ │
│  │  sensitive_fields: ["phone"]  → 自动脱敏处理                │ │
│  │  overall_risk:     medium                                    │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                           │                                        │
│                           ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Step 5：CLI 渲染输出                                        │ │
│  └──────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────┘
```

---

## 三、CLI 侧渲染：两种输出模式

### 3.1 人类可读模式（默认）

```
$ maxc agent plan "找出上月流失用户，圈选后推送到营销系统"

🤖 MaxCompute Agent 执行计划
══════════════════════════════════════════════════════════════

目标：找出上月流失用户，圈选后推送到营销系统
项目：retail_dw
流失定义：30天内无登录且无购买行为（基于 dws_user_retention_1d）

──────────────────────────────────────────────────────────────
STEP 1  定义并查询流失用户                              ✅ 自动执行
──────────────────────────────────────────────────────────────
类型：SQL 查询
表：  dws_user_retention_1d
SQL：
  SELECT user_id, last_login_dt, total_orders_30d
  FROM dws_user_retention_1d
  WHERE dt BETWEEN '2025-12-01' AND '2025-12-31'
    AND login_days = 0
    AND order_cnt = 0

预估费用：   8.5 CU
预估结果行： ~230,000 用户
风险等级：  🟢 低（只读，有分区过滤）
⚠️  注意：phone 字段将自动脱敏

──────────────────────────────────────────────────────────────
STEP 2  补充用户画像                                    ✅ 自动执行
──────────────────────────────────────────────────────────────
类型：Skill 调用
Skill：maxc.data.query
描述：关联 ads_user_profile，补充年龄/城市/消费层级字段
依赖：STEP 1 结果

预估费用：  3.2 CU
风险等级：  🟢 低

──────────────────────────────────────────────────────────────
STEP 3  推送至营销系统                                  🔴 需要审批
──────────────────────────────────────────────────────────────
类型：外部 Skill 调用
Skill：marketing.push_users
描述：将约 23 万流失用户推送至营销系统触发召回活动
依赖：STEP 2 结果

风险等级：  🔴 高
审批原因：
  • 写操作，影响外部系统
  • 影响用户数量 > 10 万
  • marketing.push_users Skill 未安装

⚠️  缺失 Skill，执行前请先安装：
    $ maxc skill install marketing.push_users

══════════════════════════════════════════════════════════════
总览
  总费用预估：  11.7 CU
  总步骤数：    3 步
  自动执行：    2 步
  需要审批：    1 步（STEP 3）
  缺失组件：    marketing.push_users
  风险等级：    🔴 高
```

---

### 2.4 maxc CLI 配置体系（Agent Context）
```bash
# ~/.maxc/config.yaml  —— 全局配置
default_project: prod_datawarehouse
default_format: json
default_region: cn-hangzhou

agent:
  nl_model: qwen-max          # 自然语言转 SQL 使用的模型
  auto_approve_cost_cu: 10    # 低于 10 CU 自动执行
  safety_mode: strict         # strict | balanced | permissive
  audit_log: ~/.maxc/audit.log

auth:
  type: oidc                  # oidc | ak_sk | ecs_role
  oidc_endpoint: https://...

# .maxc  —— 项目级配置（类似 .claude .gemini）<sub index="3" url="https://www.infoq.com/articles/agentic-terminal-cli-agents/" title="How Your Terminal Comes Alive with CLI Agents - InfoQ" snippet="**Listing 1: User prompt snippet**To understand how this instruction transforms into action, we will dissect the agent&rsquo;s workflow into its component stages. We begin with Intent Capture, where the agent grounds itself in the project's specific context, before moving to Planning Styles to contrast how different models architecture their reasoning. Subsequent sections will detail the Tool Execution loops that perform the actual work and the critical Safety Guardrails that prevent autonomous accidents. Finally, we will look at how the results are rendered back to the user, illustrating that beneath the varying brand names, most agentic tools share a common architectural DNA.### Stage 1: Intent Capture and Context FormationTo ensure a high-quality prompt for the LLM, the agent first gathers all necessary information before planning or execution. This approach involves several steps: linking the task to the current working directory, managing session state, and saving per-project configurations in dotfolders (e.g., ./.gemini and ./.claude). This approach eliminates the need to repeatedly use flags for recurring tasks.Additionally, instructions are sourced implicitly from various locations. Here are some of the primary signals the CLI agent sources from apart from the user&rsquo;s prompt:**Folder-specific Context Files**These are markdown files that encapsulate facts about how your repo is built and tested, plus your conventions for docs and scripts. They essentially act as onboarding docs for your agent. As an example, the file for Gemini CLI is called Gemini.md. Claude Code tool also uses a similar convention."></sub>
# 放在数仓项目根目录，Agent 自动加载

project_context: |
  这是零售业务数仓项目
  - 事实表前缀: dwd_
  - 汇总表前缀: dws_
  - 应用层前缀: ads_
  - 主分区字段: dt (格式 yyyy-MM-dd)
  - GMV 口径: 支付金额，不含退款
  - 核心业务表: dws_sale_province_1d, dws_user_behavior_1d

allowed_operations:
  - SELECT
  # DML/DDL 需要审批

cost_threshold_cu: 50         # 超过提示确认
sensitive_columns:
  - user_phone
  - id_card_no
  - bank_account
```

> 实施说明：真实 MaxCompute backend 的成本控制可以采用 `task_cost_cpu` / `task_cost_memory` / `estimated_input_size_bytes`，不必强行依赖 `cost_cu` 单一口径。

---

## 三、MaxCompute Skill 体系
### 3.1 Skill 的核心设计理念
```plain
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│   Skill ≠ API 调用                                                 │
│   Skill = 有语义、有状态、可组合的「能力胶囊」                    │
│                                                                    │
│   一个 Skill 封装了：                                              │
│   ├── What：我能做什么（语义描述）                                 │
│   ├── How：怎么执行（可以是 CLI/API/脚本的组合）                  │
│   ├── When：什么情况下调用我（触发条件）                          │
│   ├── Guard：安全边界（权限/费用/影响范围）                       │
│   └── Output：产出什么（结构化结果契约）                          │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 3.2 Skill 分层体系
```plain
┌─────────────────────────────────────────────────────────────────────┐
│                   MaxCompute Skill 三层体系                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Layer 3：业务 Skill（用户自定义）                                  │
│   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐              │
│   │ 用户留存分析  │ │ 营销人群圈选 │ │ 供应链预测   │  ...         │
│   │ (by 业务团队) │ │ (by 数据团队)│ │ (by 算法团队)│              │
│   └──────┬───────┘ └──────┬───────┘ └──────┬───────┘              │
│          │                │                 │                       │
│   Layer 2：领域 Skill（MaxC 官方提供）                               │
│   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐              │
│   │  data.query  │ │  data.etl    │ │ data.quality │              │
│   │  data.explore│ │  data.lineage│ │ data.profile │              │
│   └──────┬───────┘ └──────┬───────┘ └──────┬───────┘              │
│          │                │                 │                       │
│   Layer 1：原子 Skill（CLI 命令封装）                                │
│   ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐         │
│   │execute │ │submit  │ │get_    │ │describe│ │sample  │  ...     │
│   │_sql    │ │_job    │ │schema  │ │_table  │ │_data   │         │
│   └────────┘ └────────┘ └────────┘ └────────┘ └────────┘         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.3 Skill 定义规范
```yaml
# ~/.maxc/skills/data.query.yaml
# Layer 2 领域 Skill：数据查询分析

skill:
  id: maxc.data.query
  name: MaxCompute 数据查询
  version: "1.0.0"
  category: analytics

  # 语义描述（LLM 理解用）
  description: |
    查询 MaxCompute 数据仓库中的数据，支持自然语言和 SQL 两种方式。
    适用场景：数据查询、报表生成、指标计算、数据探查。
    不适用：数据写入、表结构变更、权限管理。

  # 示例（Few-shot，帮助 LLM 判断何时调用）
  examples:
    - "上个月各省份的销售额是多少"
    - "找出 GMV 超过 100 万的用户"
    - "统计今天的 DAU"
    - "SELECT * FROM ads_user_profile WHERE age > 30"

  # 输入契约
  input:
    schema:
      query:
        type: string
        description: "SQL 语句或自然语言问题"
        required: true
      project:
        type: string
        description: "MaxCompute 项目名"
        default: "${default_project}"
      max_rows:
        type: integer
        default: 1000
        max: 100000
      async:
        type: boolean
        default: false
        description: "预计执行时间 > 30s 时建议设为 true"

  # 输出契约
  output:
    schema:
      status:
        type: enum
        values: [success, failure, pending]
      rows:
        type: array
      job_id:
        type: string
        description: "异步模式下返回"
      insight:
        type: string
        description: "AI 生成的数据解读"

  # 安全边界（Guard）
  # 说明：
  # - 若平台存在统一成本抽象，可以继续保留 cost_cu。
  # - 对接真实 MaxCompute 时，建议优先使用 task_cost_cpu / task_cost_memory。
  guards:
    allow_operations: [SELECT]
    deny_tables_pattern: ["tmp_*", "raw_*"]
    max_task_cost_cpu: 500
    max_task_cost_memory: 2048
    require_approval_if:
      - "estimated_rows > 1000000"
      - "task_cost_cpu > 200"
      - "task_cost_memory > 1024"

  # 执行实现（底层调用 CLI）
  implementation:
    type: cli
    commands:
      # 步骤1：如果是自然语言，先转SQL
      - condition: "is_natural_language(input.query)"
        run: |
          maxc query @natural "{{input.query}}" \
            --project {{input.project}} \
            --max-rows {{input.max_rows}} \
            --json
      # 步骤2：直接执行SQL
      - condition: "is_sql(input.query)"
        run: |
          maxc query "{{input.query}}" \
            --project {{input.project}} \
            --max-rows {{input.max_rows}} \
            --json

  # 与其他 Skill 的组合关系
  composes_with:
    - maxc.data.explore    # 查询前可先探查数据
    - maxc.data.quality    # 查询后可做质量检测
    - maxc.meta.search     # 找不到表时自动搜索

  # 注册信息
  registry:
    published: true
    tags: [data, analytics, sql, nlp]
    author: maxcompute-team
```

---

### 3.4 官方 Skill 目录（MaxC Skill Catalog）
```plain
maxc-skill-catalog/
│
├── data/                          # 数据操作类
│   ├── data.query                 # 数据查询（支持 NL + SQL）
│   ├── data.explore               # 数据探查（schema+sample+profile）
│   ├── data.etl                   # ETL 数据处理
│   ├── data.export                # 数据导出
│   └── data.import                # 数据导入
│
├── meta/                          # 元数据类
│   ├── meta.search                # 语义搜索表和字段
│   ├── meta.lineage               # 血缘分析
│   ├── meta.impact                # 影响分析（改这张表会影响什么）
│   └── meta.document              # 自动生成数据字典
│
├── quality/                       # 数据质量类
│   ├── quality.profile            # 数据质量剖析
│   ├── quality.check              # 规则检测
│   ├── quality.anomaly            # 异常检测
│   └── quality.repair             # 数据修复建议
│
├── job/                           # 任务管理类
│   ├── job.diagnose               # 任务失败诊断
│   ├── job.optimize               # SQL 优化建议
│   ├── job.monitor                # 任务监控告警
│   └── job.replay                 # 任务重跑
│
├── insight/                       # 智能分析类（高阶）
│   ├── insight.trend              # 趋势分析
│   ├── insight.attribution        # 归因分析
│   ├── insight.anomaly            # 指标异动分析
│   └── insight.forecast           # 预测分析
│
└── compose/                       # 组合 Skill（示例）
    ├── compose.daily_report       # 日报生成（query+insight+export）
    ├── compose.data_migration     # 数据迁移（import+etl+quality）
    └── compose.incident_response  # 数据故障响应（diagnose+repair+notify）
```

---

### 3.5 Skill 执行引擎
```plain
┌────────────────────────────────────────────────────────────────────┐
│                     Skill Execution Engine                          │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│   Agent 调用 Skill 的完整流程：                                    │
│                                                                    │
│   ┌─────────┐                                                      │
│   │  Agent  │                                                      │
│   │ (LLM)   │                                                      │
│   └────┬────┘                                                      │
│        │ 1. 意图识别                                               │
│        ▼                                                           │
│   ┌─────────────────────┐                                         │
│   │   Skill Selector    │ ← 向量检索 Skill 语义描述               │
│   │   (Router)          │   返回 Top-K 候选 Skill                 │
│   └────────┬────────────┘                                         │
│            │ 2. Skill 匹配                                         │
│            ▼                                                       │
│   ┌─────────────────────┐                                         │
│   │   Guard Checker     │ ← 检查权限/费用/安全边界                │
│   │   (Safety Layer)    │   不通过 → 返回拒绝原因                 │
│   └────────┬────────────┘                                         │
│            │ 3. 安全检查通过                                       │
│            ▼                                                       │
│   ┌─────────────────────┐                                         │
│   │   Input Builder     │ ← 从 Agent Context 提取参数             │
│   │                     │   自动填充默认值                         │
│   └────────┬────────────┘                                         │
│            │ 4. 参数构建完成                                       │
│            ▼                                                       │
│   ┌─────────────────────┐                                         │
│   │   CLI Executor      │ ← 翻译为 maxc CLI 命令执行              │
│   │                     │   支持串行/并行/条件执行                 │
│   └────────┬────────────┘                                         │
│            │ 5. 执行中                                             │
│            ▼                                                       │
│   ┌─────────────────────┐                                         │
│   │  Stream Handler     │ ← 实时转发 NDJSON 事件流                │
│   │                     │   Agent 可感知中间状态                   │
│   └────────┬────────────┘                                         │
│            │ 6. 执行完成                                           │
│            ▼                                                       │
│   ┌─────────────────────┐                                         │
│   │  Output Normalizer  │ ← 按输出契约格式化结果                  │
│   │                     │   注入 agent_hints 字段                 │
│   └────────┬────────────┘                                         │
│            │ 7. 结构化结果返回 Agent                              │
│            ▼                                                       │
│   ┌─────────────────────┐                                         │
│   │  Audit Logger       │ ← 记录完整调用链路                      │
│   │                     │   skill/input/output/cost/user          │
│   └─────────────────────┘                                         │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 3.6 Skill 组合执行示例
```yaml
# compose/daily_report.yaml
# 组合 Skill：每日数据报告自动生成

skill:
  id: maxc.compose.daily_report
  name: 每日数据报告生成
  description: |
    自动查询核心业务指标，生成每日数据报告并推送。
    包含：指标计算 → 异动检测 → 洞察生成 → 报告输出。

  steps:
    # Step 1：并行查询多个指标
    - id: fetch_metrics
      parallel: true
      skills:
        - skill: maxc.data.query
          input:
            query: "SELECT SUM(gmv) AS gmv FROM dws_sale_1d WHERE dt='{{yesterday}}'"
          output_as: gmv_result

        - skill: maxc.data.query
          input:
            query: "SELECT COUNT(DISTINCT user_id) AS dau FROM dws_user_active_1d WHERE dt='{{yesterday}}'"
          output_as: dau_result

        - skill: maxc.data.query
          input:
            query: "SELECT COUNT(*) AS new_users FROM dws_user_new_1d WHERE dt='{{yesterday}}'"
          output_as: new_user_result

    # Step 2：异动检测
    - id: anomaly_check
      skill: maxc.quality.anomaly
      input:
        metrics:
          - name: gmv
            value: "{{gmv_result.rows[0].gmv}}"
          - name: dau
            value: "{{dau_result.rows[0].dau}}"
        baseline_days: 7
      output_as: anomaly_result

    # Step 3：生成洞察（有异动才执行）
    - id: generate_insight
      condition: "anomaly_result.has_anomaly == true"
      skill: maxc.insight.attribution
      input:
        anomaly: "{{anomaly_result}}"
        drill_down_dimensions: ["province", "category", "channel"]
      output_as: insight_result

    # Step 4：汇总输出
    - id: build_report
      type: template
      template: |
        ## 📊 {{yesterday}} 日报
        
        ### 核心指标
        | 指标 | 数值 | 环比 |
        |------|------|------|
        | GMV  | {{gmv_result.rows[0].gmv}} | {{anomaly_result.gmv_change_pct}}% |
        | DAU  | {{dau_result.rows[0].dau}} | {{anomaly_result.dau_change_pct}}% |
        
        ### 异动分析
        {{insight_result.summary}}
      output_as: report_content

  output:
    report: "{{report_content}}"
    anomalies: "{{anomaly_result.items}}"
    insights: "{{insight_result.summary}}"
```

---

### 3.7 Skill Registry（Skill 注册中心）
```plain
┌────────────────────────────────────────────────────────────────────┐
│                   MaxCompute Skill Registry                         │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  功能定位：Skill 的「应用市场」                                    │
│  类比：npm registry / Docker Hub / VS Code Extension Market       │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │  Registry API                                                 │ │
│  │                                                              │ │
│  │  # 搜索 Skill                                               │ │
│  │  $ maxc skill search "数据质量"                             │ │
│  │  > maxc.quality.profile   数据质量剖析     ⭐ 4.8  2.3k用  │ │
│  │  > maxc.quality.check     规则检测          ⭐ 4.6  1.8k用  │ │
│  │  > maxc.quality.anomaly   异常检测          ⭐ 4.9  3.1k用  │ │
│  │                                                              │ │
│  │  # 安装 Skill                                               │ │
│  │  $ maxc skill install maxc.quality.anomaly                  │ │
│  │                                                              │ │
│  │  # 查看 Skill 详情                                          │ │
│  │  $ maxc skill info maxc.quality.anomaly                     │ │
│  │                                                              │ │
│  │  # 发布自定义 Skill                                         │ │
│  │  $ maxc skill publish ./my_skill.yaml --scope private       │ │
│  │                                                              │ │
│  │  # 列出已安装 Skill                                         │ │
│  │  $ maxc skill list --installed                              │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│  Skill 可见性分级：                                                │
│  ├── public    → 全网可用（官方 + 社区贡献）                      │
│  ├── org       → 企业内部共享                                     │
│  └── private   → 个人/项目私有                                    │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## 四、CLI × Skill 协同工作机制
### 4.1 两者关系图
```plain
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│         CLI  是  Skill  的「执行底座」                             │
│         Skill 是  CLI  的「语义封装」                              │
│                                                                    │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │                      Agent (LLM)                         │    │
│   │   "帮我分析上周用户流失原因，并生成报告发到钉钉"         │    │
│   └──────────────────────────┬──────────────────────────────┘    │
│                               │ 意图理解                          │
│                               ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │                   Skill Layer                            │    │
│   │  ┌─────────────────┐    ┌─────────────────┐            │    │
│   │  │maxc.insight.     │    │maxc.compose.    │            │    │
│   │  │churn_analysis    │───►│report_and_notify│            │    │
│   │  └────────┬─────────┘    └────────┬────────┘            │    │
│   │           │                       │                      │    │
│   └───────────┼───────────────────────┼──────────────────────┘   │
│               │ 翻译为 CLI 命令        │                           │
│               ▼                       ▼                           │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │                     CLI Layer                            │    │
│   │                                                         │    │
│   │  maxc query @natural "上周流失用户特征"  --json         │    │
│   │  maxc agent analyze --skill insight.attribution         │    │
│   │  maxc data export --format markdown > report.md         │    │
│   │  maxc notify --channel dingtalk --file report.md        │    │
│   └──────────────────────────┬──────────────────────────────┘    │
│                               │                                   │
│                               ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐    │
│   │              MaxCompute Core（SQL/Job/Meta）              │    │
│   └─────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────┘
```

### 4.2 典型端到端执行链路
```bash
# === 场景：Agent 自主完成「用户流失归因分析」===

# Agent 内部执行链路（全程无需人工介入）：

# Step 1：理解数据环境
$ maxc agent context --json
# 返回：当前项目、可用表、业务上下文、权限范围

# Step 2：搜索相关表
$ maxc meta search "用户流失 留存" --json
# 返回：[dws_user_retention_1d, dwd_user_event_detail, ...]

# Step 3：探查数据
$ maxc data explore dws_user_retention_1d \
    --partition "dt=2026-01-01" \
    --json
# 返回：schema、数据量、字段分布、空值率

# Step 4：执行分析查询（Skill 调用）
$ maxc agent skill maxc.insight.churn_analysis \
    --input '{"period":"last_7d","dimensions":["age_group","city_tier"]}' \
    --stream
# 流式输出分析过程和结果

# Step 5：生成报告
$ maxc agent skill maxc.compose.daily_report \
    --input '{"type":"churn","date":"2026-01-08"}' \
    --output report.md

# Step 6：结果回传 Agent
# 结构化 JSON 结果 → Agent 做最终解读和决策

# === 全程退出码监控 ===
# Agent 可根据退出码决定是否重试/上报/告警
```

---

## 五、与 Agent 框架的集成方式
### 5.1 CLI 模式集成（推荐，框架无关）
```python
# 任何 Agent 框架都可以通过 subprocess 调用 CLI
# 这是 CLI 设计的核心价值：框架无关性

import subprocess, json
from typing import Any

class MaxCCLITool:
    """
    通用 MaxC CLI 工具包装
    可接入任何 Agent 框架：LangChain / AutoGen / CrewAI / 自研
    """
    
    def execute_query(self, sql: str, project: str = None) -> dict[str, Any]:
        cmd = ["maxc", "query", sql, "--json", "--format", "json"]
        if project:
            cmd += ["--project", project]
            
        result = subprocess.run(cmd, capture_output=True, text=True)
        output = json.loads(result.stdout)
        
        # 退出码处理
        if result.returncode == 0:
            return output
        elif result.returncode == 2:
            raise PermissionError(output["error"]["message"])
        elif result.returncode == 4:
            raise ValueError(f"SQL错误: {output['error']['message']} "
                           f"建议: {output['error']['suggestion']}")
        else:
            raise RuntimeError(output["error"]["message"])
    
    def call_skill(self, skill_id: str, input_data: dict,
                   stream: bool = False) -> dict[str, Any]:
        cmd = [
            "maxc", "agent", "skill", skill_id,
            "--input", json.dumps(input_data),
            "--json"
        ]
        if stream:
            cmd.append("--stream")
            # 返回事件流
            return self._stream_events(cmd)
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        return json.loads(result.stdout)
    
    def _stream_events(self, cmd):
        with subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True) as proc:
            for line in proc.stdout:
                yield json.loads(line.strip())

    def nl_query(self, question: str) -> dict[str, Any]:
        """自然语言查询"""
        cmd = ["maxc", "query", "@natural", question, "--json"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return json.loads(result.stdout)

    def get_context(self) -> dict[str, Any]:
        """获取当前 MaxC 环境上下文（给 LLM 做 System Prompt 用）"""
        result = subprocess.run(
            ["maxc", "agent", "context", "--json"],
            capture_output=True, text=True
        )
        return json.loads(result.stdout)
```

### 5.2 各框架接入示例
```python
# ── LangChain 接入 ──────────────────────────────────────────────────
from langchain.tools import tool

maxc = MaxCCLITool()

@tool
def query_maxcompute(sql_or_question: str) -> str:
    """
    查询 MaxCompute 数据仓库。
    支持 SQL 或自然语言（以 @natural: 开头）。
    返回结构化查询结果。
    """
    if sql_or_question.startswith("@natural:"):
        result = maxc.nl_query(sql_or_question[9:])
    else:
        result = maxc.execute_query(sql_or_question)
    return json.dumps(result, ensure_ascii=False)
    
@tool  
def call_maxc_skill(skill_id: str, input_json: str) -> str:
    """
    调用 MaxCompute Skill 执行复杂数据任务。
    skill_id 示例: maxc.insight.churn_analysis, maxc.quality.anomaly
    input_json: JSON 格式的输入参数
    """
    result = maxc.call_skill(skill_id, json.loads(input_json))
    return json.dumps(result, ensure_ascii=False)

# LangChain Agent 使用
from langchain.agents import create_react_agent
tools = [query_maxcompute, call_maxc_skill]
agent = create_react_agent(llm, tools, prompt)


# ── AutoGen 接入 ────────────────────────────────────────────────────
import autogen

maxc_tool_config = {
    "query_maxcompute": {
        "description": "查询 MaxCompute 数据，支持 SQL 和自然语言",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL 或自然语言问题"}
            },
            "required": ["query"]
        }
    },
    "call_maxc_skill": {
        "description": "调用 MaxCompute Skill 执行复杂分析任务",
        "parameters": {
            "type": "object", 
            "properties": {
                "skill_id": {"type": "string"},
                "input": {"type": "object"}
            },
            "required": ["skill_id", "input"]
        }
    }
}

data_agent = autogen.AssistantAgent(
    name="MaxCDataAgent",
    system_message="""你是 MaxCompute 数据分析专家。
    使用 query_maxcompute 查询数据，使用 call_maxc_skill 执行复杂分析。
    """,
    llm_config={"functions": list(maxc_tool_config.values())}
)


# ── CrewAI 接入 ─────────────────────────────────────────────────────
from crewai import Agent, Tool

maxc_query_tool = Tool(
    name="MaxCompute Query",
    description="查询 MaxCompute 数仓，支持 SQL 和自然语言",
    func=lambda q: json.dumps(maxc.execute_query(q), ensure_ascii=False)
)

maxc_skill_tool = Tool(
    name="MaxCompute Skill",
    description="调用 MaxC 高阶分析 Skill，如异动分析、归因分析、质量检测",
    func=lambda x: json.dumps(
        maxc.call_skill(x["skill_id"], x["input"]), 
        ensure_ascii=False
    )
)

data_analyst = Agent(
    role="数据分析师",
    goal="使用 MaxCompute 完成数据分析任务",
    tools=[maxc_query_tool, maxc_skill_tool],
    verbose=True
)


# ── 自研 Agent 框架（最轻量）──────────────────────────────────────
# 仅需 subprocess + json，零依赖
class SimpleMaxCAgent:
    def __init__(self, llm_client):
        self.llm = llm_client
        self.maxc = MaxCCLITool()
        self.context = self.maxc.get_context()
    
    def run(self, goal: str):
        # 构建 System Prompt（注入 MaxC 上下文）
        system = f"""
        你是 MaxCompute 数据助手。当前环境：
        {json.dumps(self.context, ensure_ascii=False, indent=2)}
        
        可用工具：
        - query(sql_or_nl): 查询数据
        - skill(id, input): 调用 Skill
        - plan(goal): 输出执行计划
        """
        # ReAct 循环
        messages = [{"role": "user", "content": goal}]
        while True:
            response = self.llm.chat(system=system, messages=messages)
            action = self._parse_action(response)
            if action["type"] == "finish":
                return action["result"]
            observation = self._execute_action(action)
            messages.append({"role": "tool", "content": str(observation)})
```

---

## 六、Human-in-the-Loop 设计

> 路线图状态：这是目标交互模型。当前仓库尚未实现 `approve / reject / modify` 闭环，现阶段先用只读命令、外部审批和审计日志替代。

```plain
┌────────────────────────────────────────────────────────────────────┐
│              Human-in-the-Loop 触发机制                             │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Agent 自主执行 ◄─────────────────────► 需要人工介入              │
│                                                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                   自动执行区（Green Zone）                    │  │
│  │  ✅ SELECT 查询，费用 < 10 CU                               │  │
│  │  ✅ 元数据读取                                               │  │
│  │  ✅ 数据采样（< 1000 行）                                   │  │
│  │  ✅ 已安装的 read-only Skill                                │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                   确认执行区（Yellow Zone）                   │  │
│  │  ⚠️  费用 10~50 CU，Agent 先展示计划，等待确认              │  │
│  │  ⚠️  涉及分区数 > 100                                       │  │
│  │  ⚠️  结果行数 > 100 万                                      │  │
│  │  ⚠️  首次访问新表                                           │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                   人工审批区（Red Zone）                      │  │
│  │  🔴 DDL 操作（CREATE/DROP/ALTER）                           │  │
│  │  🔴 DML 操作（INSERT/UPDATE/DELETE）                        │  │
│  │  🔴 费用 > 50 CU                                           │  │
│  │  🔴 访问敏感数据分级表                                      │  │
│  │  🔴 跨项目操作                                              │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  确认交互示例：                                                    │
│                                                                    │
│  $ maxc agent run "重建用户画像宽表"                               │
│                                                                    │
│  🤖 Agent 执行计划：                                              │
│  ┌─────────────────────────────────────────────────┐             │
│  │ Step 1: SELECT 用户行为数据 (预估 45 CU) ⚠️     │             │
│  │ Step 2: INSERT INTO ads_user_profile    🔴      │             │
│  │ Step 3: 更新分区元数据                  🔴      │             │
│  │                                                  │             │
│  │ 总预估费用: 48 CU                                │             │
│  │ 影响行数:   约 2000 万行                         │             │
│  └─────────────────────────────────────────────────┘             │
│                                                                    │
│  需要您的审批才能继续执行 Step 2-3。                              │
│  [approve] [reject] [modify]                                      │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## 七、整体架构总览（CLI + Skill 双核）
```plain
┌────────────────────────────────────────────────────────────────────┐
│              MaxCompute Agent-First 架构（2026）                    │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│   ┌─────────────────────────────────────────────────────────────┐ │
│   │                      Agent Layer                             │ │
│   │  百炼 / Qwen / Claude / GPT / 自研 Agent / CI Bot           │ │
│   └────────────┬──────────────────────────┬─────────────────────┘ │
│                │                          │                        │
│         自然语言 / 目标                  结构化调用                │
│                │                          │                        │
│   ┌────────────▼──────────────────────────▼─────────────────────┐ │
│   │                    Skill Layer                               │ │
│   │                                                             │ │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │ │
│   │  │data.query│ │insight.  │ │quality.  │ │compose.  │ ...  │ │
│   │  │data.etl  │ │anomaly   │ │check     │ │daily_rpt │      │ │
│   │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘      │ │
│   │       │            │            │            │              │ │
│   │  ┌────▼────────────▼────────────▼────────────▼───────────┐ │ │
│   │  │              Skill Execution Engine                     │ │ │
│   │  │   Selector → Guard → Input Builder → Executor          │ │ │
│   │  └──────────────────────────┬────────────────────────────┘ │ │
│   └─────────────────────────────┼───────────────────────────────┘ │
│                                 │ 翻译为 CLI 指令                  │
│   ┌─────────────────────────────▼───────────────────────────────┐ │
│   │                      CLI Layer                               │ │
│   │                                                             │ │
│   │  maxc query │ maxc job │ maxc meta │ maxc data │ maxc agent │ │
│   │                                                             │ │
│   │  ├── Agent-Native 输出（JSON/NDJSON）                      │ │
│   │  ├── 结构化退出码                                           │ │
│   │  ├── 流式事件流                                             │ │
│   │  ├── 幂等设计                                               │ │
│   │  └── Unix 管道友好                                          │ │
│   └──────────────────────────┬──────────────────────────────────┘ │
│                               │                                    │
│   ┌───────────────────────────▼──────────────────────────────────┐ │
│   │              MaxCompute Core API                              │ │
│   │    SQL Engine / Job API / Meta API / Security API            │ │
│   └──────────────────────────────────────────────────────────────┘ │
│                                                                    │
│   横切关注点：                                                     │
│   ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────────┐ │
│   │  认证/   │ │ 审计日志 │ │Human-in- │ │  Skill Registry      │ │
│   │  RAM集成 │ │ 全链路   │ │the-Loop  │ │  (发现/安装/发布)    │ │
│   └──────────┘ └──────────┘ └──────────┘ └──────────────────────┘ │
└────────────────────────────────────────────────────────────────────┘
```

---

## 八、建设路线图（2026 版）

> 本章节是产品路线图，不是当前仓库的开发 checklist。当前实现优先级和能力边界，请以 [docs/implementation.md](./implementation.md) 为准。

```plain
┌────────────────────────────────────────────────────────────────────┐
│                  MaxC CLI + Skill Roadmap 2026                     │
├─────────────┬──────────────────────────────────────────────────────┤
│   阶段       │  交付物                                               │
├─────────────┼──────────────────────────────────────────────────────┤
│             │  ✦ maxc CLI v1 发布                                  │
│  Q1 2026    │    - 核心命令：query / job / meta                    │
│  「立基」    │    - Agent-Native JSON 输出                          │
│             │    - 结构化退出码规范                                 │
│             │    - NDJSON 流式输出                                  │
│             │    - .maxc 项目配置文件                              │
│             │                                                       │
│             │  ✦ Layer 1 原子 Skill（10个）                        │
│             │    - execute_sql / submit_job / get_schema           │
│             │    - sample_data / search_meta / get_lineage         │
├─────────────┼──────────────────────────────────────────────────────┤
│             │  ✦ maxc CLI v2                                       │
│  Q2 2026    │    - @natural 自然语言查询                           │
│  「增强」    │    - maxc agent plan / run / context                │
│             │    - Human-in-the-Loop 确认机制                      │
│             │    - OAuth2 / OIDC 认证                              │
│             │                                                       │
│             │  ✦ Layer 2 领域 Skill（20个）                        │
│             │    - data.query / data.etl / data.explore            │
│             │    - quality.profile / quality.anomaly               │
│             │    - job.diagnose / job.optimize                     │
│             │                                                       │
│             │  ✦ Skill Registry v1（内部）                         │
│             │    - 安装 / 查询 / 发布                              │
├─────────────┼──────────────────────────────────────────────────────┤
│             │  ✦ Layer 3 Skill 开放（用户自定义）                  │
│  Q3 2026    │    - Skill 编写 SDK                                  │
│  「开放」    │    - Skill 测试框架                                  │
│             │    - 私有 Registry 支持                              │
│             │                                                       │
│             │  ✦ 组合 Skill（Compose Skill）                       │
│             │    - compose.daily_report                            │
│             │    - compose.data_migration                         │
│             │    - compose.incident_response                      │
│             │                                                       │
│             │  ✦ 主流框架官方集成                                   │
│             │    - LangChain MaxC Toolkit（官方发布）              │
│             │    - AutoGen MaxC Agent（官方发布）                  │
│             │    - 百炼工具集成（官方认证）                        │
├─────────────┼──────────────────────────────────────────────────────┤
│             │  ✦ Skill Registry 公开上线                           │
│  Q4 2026    │    - 社区贡献机制                                    │
│  「生态」    │    - Skill 评分 / 下载量统计                        │
│             │    - ISV Skill 认证体系                              │
│             │                                                       │
│             │  ✦ insight 系列高阶 Skill                            │
│             │    - insight.trend / insight.attribution             │
│             │    - insight.forecast / insight.anomaly              │
│             │                                                       │
│             │  ✦ CLI 企业级增强                                    │
│             │    - 多 Project 上下文切换                           │
│             │    - 团队共享配置（.maxc 团队版）                   │
│             │    - CI/CD 官方集成（GitHub Actions / 流水线）      │
└─────────────┴──────────────────────────────────────────────────────┘

```

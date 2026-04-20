# Send to Claude / Claude Code：maxc-cli 第一轮产品化改进任务

你将修改仓库：

- repo: `/root/.copaw/workspaces/default/maxc-cli`

你的任务不是横向增加很多新命令，而是把已有的 Agent-first CLI 能力打磨成一致、可执行、可恢复、低歧义的产品级体验。

## 一、目标

将 `maxc-cli` 从“已有 Agent-first 骨架”升级到“可稳定接入 Agent 工作流的产品级 CLI”。

重点不是继续堆功能，而是把这些现有能力做扎实：
- 统一 Envelope 输出
- `agent_hints`
- 默认只读 + `--force`
- `agent context`
- semantic metadata
- job lifecycle
- cache/context 复用

并重点补齐这些短板：
- `next_actions` 可执行性不够强
- command / command_id / action_id 一致性不够严格
- 缺少 markdown / brief 输出
- 错误增强不够完整
- 安全护栏不可见、不可解释
- semantic metadata 尚未融入主工作流
- capability discoverability 还偏文档化

---

## 二、必须完成（Phase 1）

### 1. 结构化 `agent_hints.actions`
扩展当前 `AgentHints`，在 `next_actions / warnings / insights` 之外新增结构化动作对象，供 Agent 程序直接消费。

建议：

```python
@dataclass
class SuggestedAction:
    id: str
    title: str
    command: str
    executable: bool = True
    placeholders: dict[str, str] = field(default_factory=dict)
    args_schema: dict[str, Any] = field(default_factory=dict)
```

要求：
- 保留现有 `next_actions` 以兼容旧行为
- 新增 `actions`
- 新增 `action_ids`
- `action_ids`、`actions[].id`、`command_id` 命名保持一致

### 2. 尽可能返回 fully-bound next actions
例如：

不要只返回：
```json
"meta describe <table_name> --json"
```

而是在上下文足够时返回：
```json
"meta describe california_schools.schools --json"
```

优先改造：
- `agent context`
- `meta search`
- `meta describe`
- `data sample`
- `query cost`
- `query explain`
- `job submit`
- `job wait`
- `meta semantic get`

### 3. 新增 `--format json|markdown|brief`
在当前 JSON + table/text 之外，新增：
- `--format markdown`
- `--format brief`

其中 `brief` 用于低 token 场景，只保留：
- 核心对象
- 关键指标
- 下一步动作

### 4. 为 query / job 增加显式 `safety` block
把“默认只读 + `--force`”从隐式行为变成显式返回。

建议结构：

```json
"safety": {
  "mode": "read_only",
  "force": false,
  "allowed_operations": ["SELECT"],
  "effective_hints": {
    "odps.sql.read.only": "true"
  },
  "policy_decision": "allowed"
}
```

若阻断则返回：

```json
"safety": {
  "mode": "read_only",
  "force": false,
  "policy_decision": "blocked",
  "reason": "WRITE_OPERATION_REQUIRES_FORCE"
}
```

### 5. 错误增强：schema/table/column not found
优先补齐：
- `SCHEMA_NOT_FOUND`
- `TABLE_NOT_FOUND`
- `COLUMN_NOT_FOUND`
- `WRITE_OPERATION_REQUIRES_FORCE`

目标不是只报错，而是返回：
- `context`
- `did_you_mean`
- `available_columns` / 可用 schema / 相似表
- 可直接执行的建议动作

### 6. 更新文档
同步更新：
- `docs/ENVELOPE_SPEC.md`
- `README.md`
- `docs/implementation.md`
- `src/maxc_cli/skills/SKILL.md`

---

## 三、如时间允许（Phase 2）

### 1. semantic metadata 融入主流程
- `meta describe` 自动带 semantic summary
- `meta search.matches[]` 增加 `has_semantic`
- 如时间允许，新增：
  - `meta semantic suggest <table> --json`
  - `meta semantic validate <table> --json`

### 2. `agent capabilities --json`
提供结构化 discoverability，而不只是文档。

建议输出主要能力、支持格式、followups、async/safety/semantic 标签。

### 3. job block 统一
不要只返回裸 `job_id`，建议统一为：

```json
"job": {
  "id": "...",
  "state": "pending|running|succeeded|failed",
  "resumable": true
}
```

---

## 四、改动前后预期对比

### 1. `meta search schools --json`

#### Before
- `next_actions` 仍带 `<table_name>` 占位符
- 缺少结构化 `actions`
- 命中项没有 `has_semantic`

#### After
- `next_actions` 尽量返回 fully-bound command
- `agent_hints.actions[]` 可直接程序消费
- 命中项增加 `has_semantic`
- cache 信息更结构化

### 2. `query cost "SELECT 1" --json`

#### Before
- 有 `analysis`
- 无 `safety`
- `action_ids` 与命令语义还不够统一

#### After
- `data` 内逐步拆成 `facts / analysis`
- 增加 `safety`
- `actions` / `action_ids` / `command_id` 统一

### 3. `job submit "SELECT 1" --json`

#### Before
- 返回裸 `job_id`
- follow-up 主要靠 next_actions 文本

#### After
- 返回统一 `job` block
- 有 `safety`
- 有结构化 `actions`
- 能明确 resumable / 后续动作

### 4. `data sample ...` 错误场景

#### Before
- `NOT_FOUND`
- 只有 message + suggestion

#### After
- 细分错误码，如 `SCHEMA_NOT_FOUND`
- 返回 `context`
- 返回更具体的 follow-up actions
- 能直接指导恢复

### 5. `meta describe --format brief`

#### Before
- 无 brief 输出

#### After
- 输出精简摘要：对象、关键指标、下一步动作
- 适合低 token 协作与日志沉淀

---

## 五、代码改动地图（优先阅读顺序）

建议先读：
1. `src/maxc_cli/models.py`
2. `src/maxc_cli/output.py`
3. `src/maxc_cli/cli.py`
4. `src/maxc_cli/app.py`
5. `src/maxc_cli/backend/query.py`
6. `src/maxc_cli/config.py`
7. `src/maxc_cli/masking.py`
8. `docs/ENVELOPE_SPEC.md`
9. `src/maxc_cli/skills/SKILL.md`

### 核心 touch points

#### 协议 / 数据模型
- `src/maxc_cli/models.py`
  - `class AgentHints`
  - `class Envelope`
  - 新增 `SuggestedAction`

#### 输出层
- `src/maxc_cli/output.py`
  - 现有：`emit_json` / `emit_ndjson` / `render_table`
  - 新增：`render_markdown` / `render_brief`

#### 命令入口与错误增强
- `src/maxc_cli/cli.py`
  - 参数定义
  - `--json` / 新增 `--format`
  - `_build_error_schema_context(...)`

#### 业务与 hints 组装
- `src/maxc_cli/app.py`
  - 搜 `AgentHints(`
  - 搜 `next_actions=`
  - 搜 `agent_hints=`
  - 搜各命令：`meta search` / `meta describe` / `query cost` / `job submit` / `semantic get`

#### 安全策略关键路径
- `src/maxc_cli/backend/query.py`
  - `_parse_sql_with_hints(...)`
  - `_READ_ONLY_HINTS = {"odps.sql.read.only": "true"}`

#### 配置 / masking
- `src/maxc_cli/config.py`
  - `allowed_operations`
  - `sensitive_columns`
  - `masking_enabled`
- `src/maxc_cli/masking.py`
  - `SENSITIVE_PATTERNS`
  - `_classify_column(...)`

---

## 六、建议的 commit 切分

### Commit 1
`feat(envelope): add structured agent actions and safety block`

### Commit 2
`feat(output): add markdown and brief output formats`

### Commit 3
`feat(errors): enrich schema/table/column not found diagnostics`

### Commit 4
`feat(semantic): surface semantic summary in search and describe`

### Commit 5
`feat(agent): add capabilities endpoint and docs`

### Commit 6
`test(cli): add golden outputs for json/markdown/brief and error flows`

---

## 七、测试要求

### Golden JSON tests
- `agent context --json`
- `meta search schools --json`
- `meta describe california_schools.schools --json`
- `query cost "SELECT 1" --json`
- `job submit "SELECT 1" --json`
- `meta semantic get app.user_profile --json`

### Error tests
- schema not found
- table not found
- column not found
- write operation without force

### Output tests
- markdown output
- brief output

### Consistency tests
校验这些字段关系：
- `command`
- `command_id`
- `agent_hints.action_ids`
- `agent_hints.actions[].id`
- `agent_hints.next_actions`

---

## 八、交付要求

请按下面顺序交付：

1. 先给设计说明
   - 新增字段
   - 兼容策略
   - 风险点
2. 再实现代码
3. 再补测试
4. 最后给变更总结
   - Before / After 示例
   - 兼容性说明
   - 后续演进建议

如果时间有限，优先做这四件事：
1. `actions`
2. `safety`
3. `markdown/brief`
4. `error diagnostics`

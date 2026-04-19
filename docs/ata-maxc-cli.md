# maxc-cli：给 AI Agent 调用的 MaxCompute CLI

> 2026-04-17 · 产品推广 · 适合发布在 ATA（内部技术平台）

AI Agent 真正接入 MaxCompute，缺的通常不是再写一层 MaxCompute 适配器，而是一个稳定、结构化、可约束的工具层。`maxc-cli` 的定位就是这个工具层：它不负责理解自然语言，而是负责认证、元数据发现、只读查询、任务跟踪和 Skill 集成，让 Claude Code、Cursor、Codex、Qwen 等 AI Agent 可以直接调用 `maxc` 完成数据任务。

## 为什么需要 maxc-cli

在真实的数据分析和排障场景里，用户和 AI Agent 面临的问题其实很类似：

- 不知道表名，先去搜文档、问同事、翻 DataWorks
- 不知道字段含义，先查 schema，再试 SQL
- 不知道分区格式，容易误跑全表
- IDE 和终端里没有顺手的 MaxCompute 入口
- AI Agent 即使会写 SQL，也缺少一套可直接调用的 MaxCompute 工具

`maxc-cli` 解决的不是“替代 Agent 推理”，而是把 Agent 真正需要的执行能力标准化：

- 统一的结构化 JSON 输出，便于 Agent 解析
- 面向 MaxCompute 的元数据和查询命令集合
- 认证、查询、异步任务、缓存、Skill 安装一条链打通
- 只读优先，适合先从分析、排障、辅助开发场景落地

## maxc-cli 能做什么

`maxc-cli` 当前已经覆盖了 AI Agent 使用 MaxCompute 的核心闭环：

### 1. 元数据发现

不知道从哪张表开始时，可以先让 Agent 调用：

```bash
maxc meta search "销售" --json
maxc meta describe schema.table --json
maxc meta partitions schema.table --json
maxc meta latest-partition schema.table --json
```

这一步解决的是“先理解数据，再写 SQL”，而不是让 Agent 盲猜表结构和分区。

### 2. 数据理解

在真正执行查询前，Agent 还可以先快速看样例和数据画像：

```bash
maxc data sample schema.table --json
maxc data profile schema.table --json
```

这比直接上来 `SELECT *` 更适合在 IDE 或 Agent 工作流里使用。

### 3. 查询、成本估算和执行计划

对于正式查询，可以先估算成本，再执行：

```bash
maxc query cost "SELECT * FROM schema.table WHERE ds='20260415'" --json
maxc query explain "SELECT * FROM schema.table WHERE ds='20260415'" --json
maxc query "SELECT * FROM schema.table WHERE ds='20260415'" --json
```

这条链路尤其适合大表、分区表和需要审慎控制扫描范围的场景。

### 4. 长查询与异步任务跟踪

查询耗时较长时，可以提交异步任务并继续跟踪：

```bash
maxc job submit "SELECT ..." --json
maxc job status <job_id> --json
maxc job wait <job_id> --json
maxc job result <job_id> --json
maxc job diagnose <job_id> --json
```

这让 Agent 不只是“发出一条 SQL”，而是能完整管理一次查询任务的生命周期。

### 5. Agent 集成

`maxc-cli` 不只是一个命令行工具，也提供了 Agent 可读的 Skill 和上下文能力：

```bash
maxc agent context --json
maxc agent skill --json
maxc agent install-skill codex --json
```

对 Agent 来说，这意味着它可以先自检环境，再读取 Skill，再按约定好的命令模式调用 `maxc`。

## 方式一：一键安装（推荐）

这是最适合 ATA 文档推广的主路径。用户只需要复制一条命令，即可完成 CLI 安装、认证配置和 Skill 安装。

### Akless 版本（阿里郎 / 集团弹内环境）

```bash
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap-ncs.sh | bash
```

适用场景：

- 集团弹内环境
- 使用 `ncs` 免 AK 认证
- 希望把 MaxCompute CLI 和 Agent Skill 一次性装好

脚本会交互式引导完成：

- 安装或升级 `ncs`
- 安装或升级 `maxc-cli`
- 配置认证
- 为 Claude Code、Cursor、Windsurf、Codex 或 Qwen 安装 Skill

### AK/SK 版本

```bash
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap.sh | bash
```

适用场景：

- 公共云环境
- 使用 AccessKey / SecretKey 认证
- 希望通过一条命令完成初始化

脚本会交互式引导完成：

- 安装或升级 `maxc-cli`
- 配置 AK/SK 认证
- 为 Claude Code、Cursor、Windsurf、Codex 或 Qwen 安装 Skill

安装完成后，建议先做一次就绪检查：

```bash
maxc auth whoami --json
maxc agent context --json
```

## 方式二：Aone 平台直接安装 SKILL

如果团队已经在 Aone 平台统一分发 Agent Skill，也可以直接走 Skill 安装路径。

SKILL：`maxcompute-cli-guidance`  
链接：<https://open.aone.alibaba-inc.com/console/platform/maxcompute-eco/skill/maxcompute-cli-guidance>

这种方式适合：

- 团队已经有统一的 Agent Skill 安装入口
- 希望由平台侧统一管理 Skill 的分发和升级
- 希望用户先装好 Skill，再在对应 Agent 工具里直接使用 `maxc-cli`

建议的落地顺序是：

1. 优先通过一键脚本安装 `maxc-cli`
2. 在 Aone 平台安装 `maxcompute-cli-guidance`
3. 按对应 Agent 平台要求重启或刷新 Skill
4. 使用 `maxc agent skill --json` 与 `maxc agent context --json` 检查安装结果

如果你希望直接在本地安装 Skill，而不是通过平台页面分发，也可以使用：

```bash
maxc agent install-skill claude-code --json
maxc agent install-skill cursor --json
maxc agent install-skill windsurf --json
maxc agent install-skill codex --json
maxc agent install-skill qwen --json
```

## 一个典型的 Agent 工作流

对 AI Agent 来说，`maxc-cli` 最重要的价值不是“执行一条 SQL”，而是给出一套可复用、可约束的标准流程。

一个典型的数据问答流程如下：

```bash
maxc meta search "销售" --json
maxc meta describe schema.table --json
maxc meta latest-partition schema.table --json
maxc query cost "SELECT col1, col2 FROM schema.table WHERE ds='20260415'" --json
maxc query "SELECT col1, col2 FROM schema.table WHERE ds='20260415'" --json
```

这套流程有几个关键点：

- 先发现表，再看 schema，不猜结构
- 先确认分区，再写查询，不猜日期格式
- 先估算成本，再跑正式查询，避免误扫大表
- SQL 中使用完整表名 `schema.table`

也就是说，Agent 不需要自己实现一套 MaxCompute 适配器，也不需要自己维护一套命令知识库，它只需要按 Skill 说明调用 `maxc` 即可。

## 适合推广的典型场景

### 1. IDE 内直接查数

用户不需要切到 DataWorks、文档站或 Wiki，就可以在 Claude Code、Cursor、Codex 等工具中直接让 Agent 调用 `maxc` 完成表搜索、schema 查看和 SQL 查询。

### 2. 数据分析辅助

分析师、产品、运营可以把“找表、看字段、查分区、写 SQL、拿结果”放到一个连续工作流里，不再在浏览器、文档和 IDE 之间来回切换。

### 3. 数据问题排查

当某张表数据异常、某个分区迟迟未出、某个查询长时间未返回时，Agent 可以基于 `meta`、`data`、`query`、`job` 这几组命令快速完成排查。

### 4. 团队统一 Agent 接入

对团队来说，Skill 可以统一分发，CLI 可以独立升级，用户不需要再为不同 Agent 单独写一套 MaxCompute 使用说明。

## 命令速查

| 场景 | 命令 |
|------|------|
| 查看当前身份 | `maxc auth whoami --json` |
| 查看 Agent 上下文 | `maxc agent context --json` |
| 查看 Skill 信息 | `maxc agent skill --json` |
| 搜索表 | `maxc meta search "<keyword>" --json` |
| 查看表结构 | `maxc meta describe schema.table --json` |
| 查看分区 | `maxc meta partitions schema.table --json` |
| 查看最新分区 | `maxc meta latest-partition schema.table --json` |
| 数据采样 | `maxc data sample schema.table --json` |
| 数据画像 | `maxc data profile schema.table --json` |
| 成本估算 | `maxc query cost "SELECT ..." --json` |
| 执行查询 | `maxc query "SELECT ..." --json` |
| 查询执行计划 | `maxc query explain "SELECT ..." --json` |
| 安装 Skill 到本地 Agent | `maxc agent install-skill <platform> --json` |

## 使用建议

为了让 AI Agent 更稳定地使用 `maxc-cli`，建议在 ATA 文档里明确以下约定：

- 所有命令默认带 `--json`
- 分区表先查分区，再写 SQL
- SQL 中使用 schema-qualified 表名
- 大表先 `query cost`，再执行正式查询
- 对只读分析场景优先推广，不把 DDL/DML 当成第一阶段目标

## 总结

如果希望 AI Agent 真正可控地接入 MaxCompute，重点不是让 Agent 再去实现一套 pyodps 适配逻辑，而是给它一套稳定、清晰、结构化的工具层。`maxc-cli` 正在承担这个角色：安装路径统一，认证路径明确，命令结构稳定，Skill 可以直接分发到主流 Agent 平台。

对个人用户，最推荐的入口是一键安装脚本。对团队推广，最适合的方式是在 ATA 发布统一说明，并配合 Aone 平台分发 `maxcompute-cli-guidance` Skill。这样既能降低首次接入成本，也能把后续的升级和使用方式统一起来。

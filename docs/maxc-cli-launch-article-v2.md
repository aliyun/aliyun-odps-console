# maxc-cli：给 AI Agent 用的 MaxCompute CLI

> 一句话概述：maxc-cli 是一套给 AI Agent 使用的 MaxCompute CLI，让 Agent 在 IDE 或终端里完成找表、看 schema、查分区、估成本和查询，把数据探索收敛成高效工作链路。

> 不是 Agent，而是给 Agent 调用的结构化工具层。

![maxc-cli 封面](maxc-cli-cover.png)

当 AI Agent 开始进入开发、分析和运维流程之后，数据库和数据仓库会很快成为它最常需要连接的外部系统之一。

但在 MaxCompute 这类场景里，Agent 真正缺的并不是"写 SQL 的能力"，而是一层稳定、结构化、可约束的执行接口。否则，它只能在文档、控制台、DataWorks、脚本和零散适配器之间来回切换，最终把一个本来应该连续完成的任务拆成很多割裂步骤。

`maxc-cli` 想解决的就是这件事。

它不是一个新的 Agent 产品，也不是一个内建聊天系统。它更像是 MaxCompute 面向 AI Agent 的工具层：Agent 负责理解问题，`maxc-cli` 负责执行数据相关动作，返回结构化结果，方便 Agent 继续决策。

## 为什么 MaxCompute 需要一套给 Agent 用的 CLI

很多人以为，Agent 接入数据仓库，核心问题是"能不能写出 SQL"。

但真实使用里，更耗时间的往往不是 SQL 本身，而是 SQL 之前和之后的一整段流程。

比如一个看起来很普通的需求：

> "帮我查一下最近一周某主题的数据情况。"

落到真实执行，通常会变成：

1. 先找表，不知道表名
2. 再看 schema，不知道字段名
3. 再查分区，不知道日期格式
4. 再估成本，不知道会不会扫大表
5. 最后才是执行查询和拿回结果

如果这些步骤都要靠人工去不同系统里切换完成，那么 Agent 即使能写 SQL，也很难真正提升效率。而 `maxc-cli` 把这些步骤收敛成了统一的命令入口——从 `maxc meta search` 到 `maxc query`，Agent 不需要自己实现一套 MaxCompute 适配器，调用 `maxc` 即可完成整条链路。

![Agent 接入数据仓库的真正痛点](maxc-cli-pain-point.png)

## 为什么是 CLI，而不是 odpscmd、PyODPS 或专门写一层 Agent 适配器

MaxCompute 生态里已经有 odpscmd 和 PyODPS，为什么还需要 `maxc-cli`？

简单说，**odpscmd 是给人用的，PyODPS 是给 Python 程序用的，`maxc-cli` 是给 AI Agent 用的**。它们解决的问题不在同一个层面：

**odpscmd** 的交互模式和输出格式都是为人类设计的——表格对齐、分页提示、交互式确认。Agent 很难从这些输出中稳定地解析结构化信息，也无法处理交互式提示。更关键的是，odpscmd 没有成本预估、`agent_hints` 这类帮助 Agent 做决策的能力。

**PyODPS** 是一个功能完整的 Python SDK，能力上没问题，但它要求 Agent 先生成一段 Python 脚本，再执行脚本，再从输出中提取结果。对于"找表→看字段→估成本→跑查询"这类连续动作，每一步都要写代码、跑代码、读输出，链路太长。而且不同 Agent 平台（Claude Code、Cursor、Codex）对 Python 运行环境的支持参差不齐。

**`maxc-cli` 的设计选择**是：一条命令做一件事，`--json` 输出统一的结构化结果，Agent 直接在 shell 层调用。这带来几个具体的好处：

**跨平台零依赖。** CLI 是所有 Agent 平台都能调用的最大公约数。不需要 Python 环境，不需要特定 SDK 版本，不需要额外的适配层。Claude Code、Cursor、Windsurf、Codex、Qwen——只要能执行 shell 命令，就能用 `maxc-cli`。

**一条命令完成一个动作。** `maxc meta search "销售" --json` 直接返回搜索结果，不需要写 `from odps import ODPS; o = ODPS(...); for t in o.list_tables(): ...`。对 Agent 来说，调用成本从"生成+执行一段代码"降低到"执行一条命令"。

**结构化输出为 Agent 决策而设计。** 每条命令返回统一的 JSON Envelope，包含 `status`、`data`、`metadata`、`error` 和 `agent_hints`。其中 `agent_hints` 会给出建议的下一步动作和 warnings，这是 odpscmd 和 PyODPS 都没有的。

**内置安全约束。** 查询链路默认注入只读约束，成本估算前置于执行。Agent 不会因为一个错误的 SQL 意外扫描全表或执行 DDL。

## 当前已经支持的核心能力

### 1. 元数据发现

在陌生项目里，第一步通常不是写 SQL，而是先理解数据对象。

```bash
maxc meta search "销售" --json
maxc meta describe schema.table --json
maxc meta partitions schema.table --json
maxc meta latest-partition schema.table --json
maxc meta freshness schema.table --json
```

这组命令让 Agent 能快速完成"先找表、看字段、确认分区"这一前置流程。

### 2. 数据理解

很多时候 schema 还不够，Agent 还需要先看样例和分布：

```bash
maxc data sample schema.table --json
maxc data profile schema.table --json
```

这让"看一眼数据长什么样"也能进入标准工作流。

> 坦率地说，当前 `data sample` 和 `data profile` 的实现还比较基础——本质上是 Agent 自己拼 SQL 也能做到的事。我们把它们先收进来，是为了让工作链路完整。但更长远的方向是在 MaxCompute 侧构建真正的语义层，让 Agent 能直接拿到字段含义、业务口径、数据血缘这些更高阶的上下文，而不只是裸的 schema 和采样数据。这部分正在建设中。

### 3. 查询、成本估算和执行计划

正式查询前，可以先估成本，再决定是否执行：

```bash
maxc query cost "SELECT * FROM schema.table WHERE ds='20260415'" --json
maxc query explain "SELECT * FROM schema.table WHERE ds='20260415'" --json
maxc query "SELECT * FROM schema.table WHERE ds='20260415'" --json
```

对 MaxCompute 这类按量计费的场景来说，成本前置能显著降低误扫大表的风险。

### 4. 长查询任务跟踪

对于耗时任务，还可以走异步任务链路：

```bash
maxc job submit "SELECT ..." --json
maxc job status <job_id> --json
maxc job wait <job_id> --json
maxc job result <job_id> --json
maxc job diagnose <job_id> --json
```

Agent 不只是"发出一条查询"，而是能完整跟踪一次任务从提交到拿结果的生命周期。

### 5. Agent 集成

为了让主流 Agent 工具直接接入，`maxc-cli` 还提供了上下文和 Skill 安装能力：

```bash
maxc agent context --json
maxc agent skill --json
maxc agent install-skill codex --json
```

当前本地 Skill 安装支持：Claude Code、Cursor、Windsurf、Codex、Qwen。

![maxc-cli 核心能力全景](maxc-cli-capabilities.png)

## 一个更接近真实使用的例子

假设你在 IDE 里对 Agent 说：

> "帮我看看 `california_schools` 里有哪些表，再看一下 `frpm` 的字段和最新分区，最后查一小段数据。"

一个合理的命令链路通常会是这样：

```bash
maxc meta list-tables --schema california_schools --json
maxc meta describe california_schools.frpm --json
maxc meta latest-partition california_schools.frpm --json
maxc query cost "SELECT * FROM california_schools.frpm WHERE ds='20260415'" --json
maxc query "SELECT * FROM california_schools.frpm WHERE ds='20260415' LIMIT 20" --json
```

这里最关键的不是某一条命令，而是这条链路本身——先理解对象再执行查询，先确认分区再写 SQL，先估成本再正式执行，SQL 始终使用完整的 `schema.table` 名。Agent 按照这条路径走，犯错的空间就很小。

## 在 IDE 里，体验会发生什么变化

如果没有 `maxc-cli`，一次数据问题的处理路径往往是这样的：

1. 在 IDE 里提出问题
2. 切去浏览器
3. 打开 DataWorks 或控制台
4. 搜表、看字段、查分区
5. 写 SQL
6. 等结果
7. 再把结果带回当前对话

而有了 `maxc-cli` 之后，路径会更像这样：

1. 在 IDE 中直接提问
2. Agent 调用 `maxc meta ...`
3. Agent 调用 `maxc query cost ...`
4. Agent 调用 `maxc query ...`
5. 结构化结果直接回到当前上下文

少了几次切换，但体验差别很大——"理解数据"和"解决问题"终于发生在同一个上下文里了。

![IDE 体验对比：有无 maxc-cli](maxc-cli-ide-comparison.png)

## 安全边界：先把只读分析跑通

团队让 Agent 接数据库时，最大的顾虑通常不是"查不到"，而是"会不会乱写"。

`maxc-cli` 当前阶段优先支持元数据发现、只读查询、成本估算、任务跟踪和差异比较，查询链路默认注入只读约束，DDL/DML 不在第一阶段主线内。

这是有意为之的。在真实落地里，最先需要跑通、也最容易形成价值的场景，就是找表、看字段、查分区、跑只读分析、排查数据问题。先把这一层做稳，Agent 接入 MaxCompute 才有一个可信的起点。

## 安装路径要足够短

`maxc-cli` 当前提供两条主路径。

### 方式一：一键安装（推荐）

#### Akless 版本（阿里郎 / 集团弹内环境）

```bash
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap-ncs.sh | bash
```

脚本会交互式引导完成：

- 安装或升级 `ncs`
- 安装或升级 `maxc-cli`
- 配置认证
- 为目标 Agent 平台安装 Skill

#### AK/SK 版本

```bash
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap.sh | bash
```

脚本会交互式引导完成：

- 安装或升级 `maxc-cli`
- 配置 AK/SK 认证
- 为目标 Agent 平台安装 Skill

安装完成后，建议先执行：

```bash
maxc auth whoami --json
maxc agent context --json
```

### 方式二：平台直接分发 Skill

如果团队已经通过 Aone 平台统一分发 Skill，也可以直接安装：

`maxcompute-cli-guidance`

链接预留：

```text
https://open.aone.alibaba-inc.com/console/platform/maxcompute-eco/skill/maxcompute-cli-guidance
```

这更适合团队统一推广和持续升级。

## 适合从哪些场景开始落地

从当前能力边界看，`maxc-cli` 最适合先落在以下几类场景：

### 1. 数据探索

新同学入职、接手陌生项目、或者需要快速摸清一个 schema 下有什么表、字段含义是什么、分区粒度怎样。以前需要开 DataWorks 一个个点，现在在 IDE 里让 Agent 帮你跑一遍 `maxc meta search` + `maxc meta describe` 就够了。

### 2. 只读分析

报表数字核对、指标口径验证、样本抽查、分区数据确认——这些高频动作都是"写一条 SQL、看一眼结果"的模式。`maxc-cli` 的成本估算 + 只读查询链路天然适合这类场景，Agent 可以连续完成"估成本→确认安全→执行查询→返回结果"而不需要人工介入。

### 3. 数据问题排查

"数据为什么没来""结果为什么为空""分区为什么延迟"——这类问题的排查路径通常是看分区、看 freshness、抽样看数据内容。`maxc meta freshness` + `maxc data sample` 让 Agent 能自己走完这条排查链路，把结论直接带回对话。

### 4. Agent 统一接入

团队里不同同学可能各自维护着 PyODPS 脚本、odpscmd alias、甚至手写的 curl 调用。`maxc-cli` + Skill 提供了一套标准化入口，装一次就能在 Claude Code、Cursor、Codex 等不同平台上复用，避免重复造轮子。

## 总结

AI Agent 真正接入 MaxCompute，关键在于先提供一套足够稳定、足够短路径、足够结构化的工具层。

`maxc-cli` 做的正是这件事。它不替代 Agent，但它让 Agent 真正"有工具可用"；它不试图包办一切，但它先把最刚需的能力整理成了一条从安装到查询到 Skill 分发的清晰链路。

![让 Agent 真正有工具可用](maxc-cli-summary.png)

如果你希望在 IDE 里，让 Claude Code、Cursor、Codex、Qwen 这类 AI Agent 更自然地接入 MaxCompute，那么 `maxc-cli` 会是一个很合适的起点。

## 视频 Demo

> 视频链接：`<VIDEO_DEMO_LINK>`

以下是 Demo 的录制脚本，使用 QoderWork 演示，建议控制在 3 分钟左右。

### 场景设定

你是一个刚接手 `california_schools` 项目的数据开发，需要快速了解这个项目里有什么数据、数据长什么样，最终跑一条查询拿到结果。全程在 QoderWork 里用自然语言完成，不切换任何其他工具。

### 录制流程

**第一幕：破冰提问（~30s）**

打开 QoderWork，直接输入：

> "我刚接手 california_schools 这个项目，帮我看看里面有哪些表？"

等待 Agent 调用 `maxc meta search` 或 `maxc meta list-tables`，返回表列表。画面重点：Agent 自动选择了正确的命令，结果以结构化形式回到对话。

**第二幕：深入了解一张表（~40s）**

继续追问：

> "frpm 这张表是干什么的？帮我看看字段和最新分区。"

等待 Agent 连续调用 `maxc meta describe` 和 `maxc meta latest-partition`，返回表结构和分区信息。画面重点：Agent 自动串联了两个命令，没有人工干预。

**第三幕：成本估算 + 执行查询（~50s）**

继续说：

> "帮我查最新分区的前 20 条数据，先估一下成本。"

等待 Agent 先调用 `maxc query cost`，确认成本可接受后，自动调用 `maxc query` 执行查询。画面重点：Agent 主动做了成本预估这一步，而不是直接执行——这是 `maxc-cli` 的安全链路在起作用。

**第四幕：基于结果追问（~40s）**

查询结果返回后，继续提问：

> "这些学校里，哪些 county 的记录数最多？帮我按 county 做个分组统计。"

等待 Agent 根据前面已经了解的表结构，自己写出分组查询 SQL 并调用 `maxc query`。画面重点：Agent 利用了前几步积累的上下文（表名、字段名、分区格式），没有重新搜表或看 schema。

**第五幕：收尾总结（~20s）**

最后说：

> "帮我总结一下刚才查到的情况。"

Agent 用自然语言输出一段数据摘要。画面重点：从"什么都不知道"到"拿到结论"，全程在一个对话窗口里完成。

### 录制建议

- 屏幕录制分辨率建议 1920x1080，字体放大到清晰可读
- 每一幕之间可以稍作停顿，让观众看清命令和返回结果
- 如果某一步 Agent 的响应时间较长，可以在后期做加速处理
- 建议标题：**"从零了解一张表到跑出结果：maxc-cli + QoderWork 完整演示"**

## 附：快速开始

```bash
# 一键安装（公共云）
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap.sh | bash

# 一键安装（集团弹内 / Akless）
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap-ncs.sh | bash

# 检查身份
maxc auth whoami --json

# 查看 Agent 上下文
maxc agent context --json

# 搜索表
maxc meta search "school" --schema california_schools --json

# 看表结构
maxc meta describe california_schools.frpm --json

# 查分区
maxc meta latest-partition california_schools.frpm --json

# 先估成本再查询
maxc query cost "SELECT * FROM california_schools.frpm WHERE ds='20260415'" --json
maxc query "SELECT * FROM california_schools.frpm WHERE ds='20260415' LIMIT 20" --json
```

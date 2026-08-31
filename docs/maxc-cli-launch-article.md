# maxc-cli：给 AI Agent 用的 MaxCompute CLI

> 历史发布草稿。当前安装、认证和 Skill 名称以
> [安装指南](./install-guide.md) 为准：公共云首选 `aliyun maxc` + OAuth，
> Skill 名为 `alibabacloud-maxcompute-cli`。下文旧截图和安装片段仅保留为
> 发布材料，不应作为操作手册。

> 给 Agent 调用的 MaxCompute 结构化工具层。

【配图建议 1：文章封面图】  
建议放一张“IDE / Agent 对话窗口 + 终端执行 maxc 命令 + MaxCompute 数据查询结果”的组合视觉图，突出“在 IDE 中直接调用 MaxCompute”的使用方式。

AI Agent 正在迅速成为新的工作入口，但一旦它要真正连接数据库和数据仓库，问题马上就出现了。

对 MaxCompute 场景来说，AI Agent 具备写 SQL 的能力，还需要一层稳定、结构化、可约束的执行接口。缺少统一接口时，Agent 会在文档、浏览器、DataWorks、脚本和零散适配器之间切换，增加调用成本和不确定性。

`maxc-cli` 想解决的就是这个问题。

它的定位包括：

- 给 AI Agent 调用的 MaxCompute CLI
- 给 IDE、Bot、脚本和工作流复用的结构化工具层
- 给 MaxCompute 数据探索、只读分析和问题排查准备的统一入口

## 为什么 MaxCompute 需要一个给 Agent 用的 CLI？

今天很多数据任务的阻塞点覆盖 SQL 前后的完整流程。

比如一个很常见的问题：

> “帮我查一下最近一周某业务主题下的数据情况。”

看起来像是在写一条 SQL，实际上往往要经历这些步骤：

1. 先找表，不知道叫啥
2. 再看 schema，不知道字段名
3. 再查分区，不知道日期格式
4. 再估成本，不知道会不会扫全表
5. 最后才是执行查询，拿回结构化结果

如果这些动作只能靠人工在不同系统之间切换，Agent 的价值就很难真正发挥出来。

而如果这些动作都有统一的 CLI 能力：

- `meta search`
- `meta describe`
- `meta partitions`
- `meta latest-partition`
- `query cost`
- `query`
- `job wait / result / diagnose`

那么外部 Agent 就可以按实时命令契约调用 MaxCompute CLI，并把精力放在目标
理解、结果校验和下一步编排上。

## 为什么选择 CLI 作为 Agent 工具接口？

原因其实很简单：CLI 是目前最容易被 Agent 消化、复用和组合的一层接口。

它至少有三个明显优势。

### 1. 更接近真实工作流

对 Agent 来说，真实数据任务需要一组连续动作：

- 认证
- 看当前环境
- 找表
- 看字段
- 看分区
- 估成本
- 跑查询
- 跟踪异步任务

CLI 适合承载这种连续工作流和单次工具调用。

### 2. 更适合结构化输出

`maxc-cli` 的命令可以统一输出 JSON Envelope，包含：

- `status`
- `data`
- `metadata`
- `error`
- `agent_hints`

Agent 可以拿到稳定、可解析的结果对象。

### 3. 更适合推广和落地

从团队接入角度看，CLI 也更容易标准化：

- 可以一键安装
- 可以通过 Skill 分发到不同 Agent 平台
- 可以通过版本升级持续增强
- 可以在终端、IDE、脚本和自动化工作流里复用

这比为每个 Agent、每个工作流单独写一套 MaxCompute 适配逻辑现实得多。

【配图建议 2：能力定位图】  
建议放一张“用户 / 外部 Agent / maxc-cli / MaxCompute”的四层结构图。  
示意：用户提出问题 -> Agent 理解任务 -> `maxc-cli` 执行元数据与查询命令 -> MaxCompute 返回结果。

## maxc-cli 是什么

`maxc-cli` 是一个面向 MaxCompute 的 Agent-first CLI。

它不负责理解自然语言，不负责替你做完整规划，也不试图取代 IDE 里的 Agent。本质上，它只做三件事：

- 把 MaxCompute 常见数据任务做成一组稳定命令
- 把结果组织成适合 Agent 消费的结构化输出
- 把安装、认证和 Skill 集成路径尽量压缩到最短

换句话说：

- 推理由外部 Agent 负责
- 执行由 `maxc-cli` 负责
- 约束、错误恢复和下一步提示，也由 `maxc-cli` 负责

## 当前已经支持的核心能力

### 1. 元数据发现

在一个陌生项目里，Agent 通常先理解有哪些数据对象，再写 SQL。

`maxc-cli` 提供了一组围绕元数据发现的命令：

```bash
maxc meta search "销售" --json
maxc meta describe schema.table --json
maxc meta partitions schema.table --json
maxc meta latest-partition schema.table --json
maxc meta freshness schema.table --json
```

Agent 可以先找表、看字段、确认分区，再进入查询阶段，避免盲写 SQL。

### 2. 数据理解

对分析任务来说，schema 还不够，很多时候还需要先看一眼样例和分布：

```bash
maxc data sample schema.table --json
maxc data profile schema.table --json
```

这类命令很适合在真正执行复杂 SQL 之前，帮助 Agent 快速理解字段内容和数据状态。

### 3. 查询、成本估算和执行计划

正式查询前，先做成本评估，是 MaxCompute 场景里非常重要的一步：

```bash
maxc query cost "SELECT * FROM schema.table WHERE ds='20260415'" --json
maxc query explain "SELECT * FROM schema.table WHERE ds='20260415'" --json
maxc query "SELECT * FROM schema.table WHERE ds='20260415'" --json
```

这让 Agent 可以先收敛扫描范围，再执行正式查询，降低误扫大表的风险。

### 4. 长查询任务跟踪

部分查询需要较长时间才能返回。对于耗时任务，`maxc-cli` 提供了完整的任务链路：

```bash
maxc job submit "SELECT ..." --json
maxc job status <job_id> --json
maxc job wait <job_id> --json
maxc job result <job_id> --json
maxc job diagnose <job_id> --json
```

这样，Agent 可以发出 SQL、持续跟踪任务状态，并在失败时获取下一步动作。

### 5. Agent 集成

为了让主流 Agent 工具真正能接上这套能力，`maxc-cli` 也提供了 Agent 侧上下文和 Skill 安装能力：

```bash
maxc agent context --json
maxc agent manifest --json
maxc agent doctor --online --json
maxc agent skill --json
maxc agent skill install codex --json
```

目前本地 Skill 安装支持：

- Claude Code
- Cursor
- Windsurf
- Codex
- Qwen

## 一个真实可落地的工作流长什么样？

假设你在 IDE 里对 Agent 说：

> “帮我看看 `california_schools` 里有哪些表，再看看 `frpm` 的字段和最新分区，最后查一下某个分区下的数据。”

一个合理的流程通常会是这样：

```bash
maxc meta list-tables --schema california_schools --json
maxc meta describe california_schools.frpm --json
maxc meta latest-partition california_schools.frpm --json
maxc query cost "SELECT * FROM california_schools.frpm WHERE ds='20260415'" --json
maxc query "SELECT * FROM california_schools.frpm WHERE ds='20260415' LIMIT 20" --json
```

它有几个非常关键的特征：

- 先理解对象，再执行查询
- 先确认分区，再拼 SQL
- 先估成本，再正式执行
- SQL 使用完整表名 `schema.table`

这也是 `maxc-cli` 最适合 Agent 使用的地方：它把“做对一件事”所需的步骤拆成了清晰、稳定、可组合的命令。

【配图建议 3：终端命令截图】  
建议放一张真实终端截图，内容包含：
- `maxc meta describe california_schools.frpm --json`
- `maxc meta latest-partition california_schools.frpm --json`
- `maxc query cost ... --json`

## 输出供 Agent 稳定解析的结果对象

传统命令行工具对人类友好，对 Agent 未必友好。它们往往返回大段表格、杂糅日志和不稳定格式，Agent 很难可靠消费。

`maxc-cli` 这套设计更强调结构化结果。以 `--json` 输出为例，核心结构包括：

- `status`：成功还是失败
- `data`：规范化后的查询、元数据或任务结果
- `metadata`：上下文信息，如项目、job_id、耗时等
- `error`：结构化错误码、提示和恢复建议
- `agent_hints`：下一步推荐动作、warning 和 insight

这件事看起来很“工程”，但对 Agent 来说非常关键。

因为这意味着：

- 出错时不需要只看一段报错字符串
- 可以直接读取 `error` 和 `agent_hints`
- 可以根据 `next_actions` 决定下一步该查什么
- 可以根据 `warnings` 判断缓存、分区、成本等风险

## 安全边界：先把只读分析做好

很多团队在考虑让 Agent 接数据库时，主要顾虑是未经确认的写操作。

`maxc-cli` 当前阶段非常明确地把主线放在只读场景上：

- 重点支持元数据发现、只读查询、成本估算、任务跟踪和差异比较
- 查询链路默认注入 MaxCompute 只读约束
- 第一阶段不以 DDL/DML 为主线

这背后的思路很直接：

- 先把高频、低风险的分析链路跑通
- 先让 Agent 真正能安全地“看数据、理解数据、验证数据”
- 再讨论更复杂的执行型能力

对多数团队来说，这也是最现实的落地顺序。

## 安装路径要足够短

一项工具能不能推广起来，很大程度上取决于第一次安装是否足够顺。

`maxc-cli` 当前提供两条安装路径。

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

#### 公共云 OAuth 版本

```bash
aliyun version                 # Alibaba Cloud CLI >= 3.3.19
aliyun maxc auth login --oauth --json
```

已有 profile 或运行时凭证时先验证当前身份，不要覆盖认证。只有运行环境明确
要求时才使用 STS、环境变量、外部凭证进程或直接 AK/SK。

安装完成后，可以先做一次检查：

```bash
aliyun maxc agent context --json
aliyun maxc agent manifest --json
aliyun maxc agent doctor --online --json
```

### 方式二：平台直接分发 Skill

如果团队已经在 Aone 平台统一分发 Skill，也可以直接安装：

`alibabacloud-maxcompute-cli`

链接预留：

```text
https://code.alibaba-inc.com/cloud-skills-computing-platform/alibabacloud-maxcompute-cli
```

这种方式更适合团队统一推广和持续升级。

【配图建议 4：安装流程图或安装界面截图】  
建议放两张图二选一：
- 一键安装脚本的交互式终端截图
- Aone Skill 页面截图

## 在 IDE 里，体验会发生什么变化？

如果没有 `maxc-cli`，很多数据任务大致是这样完成的：

1. 在 IDE 里想到问题
2. 切到浏览器
3. 打开 DataWorks 或别的控制台
4. 搜表、看字段、查分区
5. 写 SQL
6. 等结果
7. 再把结果带回 IDE 或 Agent 对话

而有了 `maxc-cli` 之后，路径会变成：

1. 在 IDE 中直接提问
2. Agent 调用 `maxc meta ...`
3. Agent 调用 `maxc query cost ...`
4. Agent 调用 `maxc query ...`
5. 结构化结果直接回到当前上下文

这看上去只是少了几次切换，但对真实工作流的影响非常大。

因为一旦“数据理解”和“问题求解”在同一个上下文里发生，Agent 的效率和连贯性都会明显提升。

【配图建议 5：IDE 使用截图】  
建议放一张 Claude Code / Cursor / Codex 中自然语言提问后，Agent 调用 `maxc` 命令的截图。

## 适合从哪些场景开始落地？

从当前能力边界看，`maxc-cli` 最适合优先落地在下面几类场景：

### 1. 数据探索

适合陌生项目、陌生表、陌生字段的快速理解。

### 2. 只读分析

适合报表核对、指标验证、样本抽查、分区确认这类高频任务。

### 3. 数据问题排查

适合排查“数据为什么没来”“为什么结果为空”“为什么某个分区延迟”这类问题。

### 4. Agent 统一接入

适合团队把 MaxCompute 的常见分析动作统一抽象成一套 Skill + CLI 入口，减少各 Agent 和小团队的重复脚本。

## 总结

AI Agent 真正接入 MaxCompute，关键不在于让 Agent 再自己写一套适配器，而在于提供一套足够稳定、足够短路径、足够结构化的工具层。

`maxc-cli` 想做的就是这个工具层。

它不取代 Agent，但它让 Agent 真正“有工具可用”；它不做一切，但它把最常见、最刚需、最容易落地的数据动作先做成了一条完整链路：

- 安装
- 认证
- 找表
- 看 schema
- 查分区
- 估成本
- 跑查询
- 跟踪任务
- 集成 Skill

如果你希望在 IDE 里，让 Claude Code、Cursor、Codex、Qwen 这类 AI Agent 更自然地接入 MaxCompute，那么 `maxc-cli` 会是一个很合适的起点。

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

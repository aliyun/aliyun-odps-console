# maxc-cli Go 化与 Aliyun CLI 插件接入设计

- 日期：2026-04-28
- 状态：Draft（待评审）
- 仓库：
  - 当前 Python：`/Users/dingxin/pythonProject/maxc-cli`
  - 目标插件位：`/Users/dingxin/MaxQuery/aliyun-cli-plugins`
  - 依赖 Go SDK：`/Users/dingxin/GolandProjects/odps-sdk-go`（`github.com/aliyun/aliyun-odps-go-sdk`）

## 1. 背景与目标

`maxc-cli` 当前是一个 ~12K 行的 Python CLI，定位为 AI agent（Claude Code / Codex / 自研 agent）调用 MaxCompute 的结构化工具层：

- 输出统一 envelope（含 `agent_hints` / `warnings` / `insights`），便于 agent 决策
- 覆盖数据面（SQL 执行、sample、profile）、语义面（catalog 检索、search-tables/columns）、作业面（wait/diagnose）、认证面
- 本地 SQLite 缓存元数据加速 agent 多轮调用

存在两个结构性问题：
1. Python 二进制分发与多平台支持成本高
2. 与阿里云生态的 `aliyun` CLI 体系隔离，用户需要维护两套凭证/配置

目标：**用 Go 重写为 `aliyun-cli` 插件**，借助 aliyun-cli profile/runtime 体系统一认证，借助 Go 静态二进制简化分发，同时保留 envelope/agent_hints 这一差异化能力。

## 2. 关键决策（已与 Owner 确认）

| 项 | 决策 |
|---|---|
| 与现有 `plugin-maxcompute` 关系 | 新建独立插件，不覆盖、不合并 |
| 首版范围 | 全量等价移植 Python 版能力 |
| 认证体系 | 完全嫁接 aliyun-cli profile（AK/STS/RamRole/CloudSSO 等），不再保留独立 AK 文件 |
| 默认输出 | envelope（与 Python 一致），可通过 `--output aliyun-json/aliyun-text` 切回 aliyun-cli 标准输出 |
| CLI 表面 | 子命令名 + 参数名与 Python 版 1:1 对齐 |
| 命名 | 插件目录 `plugin-maxc`，二进制 `aliyun-cli-maxc`，顶层命令 `maxc`，用法 `aliyun maxc <subcommand>` |
| envelope/hints 抽独立 module | 否 |
| skills 资源交付 | `embed.FS` 内嵌到二进制 |

## 3. 总体架构

### 3.1 目录布局

```
plugin-maxc/
├── manifest.json            # name=aliyun-cli-maxc, cmdNames, version, platforms
├── main/main.go             # 入口：注册 maxc 顶层命令到 cobra root
├── go.mod                   # require runtime + odps-sdk-go (开发期 replace 本地)
├── commands/                # 每个子命令一个文件，仅做参数解析+调用 app
│   ├── query.go
│   ├── wait_job.go
│   ├── list_tables.go
│   └── ...                  # 与 Python cli.py 1:1
├── internal/
│   ├── app/                 # 业务编排层（对应 Python app.py，按命令族拆 ≤400 行/文件）
│   │   ├── query.go
│   │   ├── job.go
│   │   ├── meta.go
│   │   └── ...
│   ├── backend/             # ODPS 调用层（对应 Python backend/）
│   │   ├── odps.go          # 工厂：从 aliyun-cli profile → odps.Odps
│   │   ├── query.go
│   │   ├── job.go
│   │   ├── meta.go
│   │   └── data.go
│   ├── envelope/            # Envelope + AgentHints 编排（核心差异化）
│   │   ├── envelope.go
│   │   ├── hints.go         # 集中所有 next_actions/warnings/insights 规则
│   │   └── output.go        # 与 runtime/output 桥接
│   ├── cache/               # 元数据/语义缓存（modernc.org/sqlite，纯 Go 无 cgo）
│   ├── store/               # JobStore（本地 job 跟踪）
│   ├── config/              # MaxC 专属配置（endpoint/region/cache 路径）
│   ├── auth/                # runtime credentials → odps account 适配
│   ├── errors/              # 错误码 + suggestion（对应 Python exceptions.py）
│   └── skills/              # //go:embed 嵌入 prompt 资源
└── tests/
    ├── mock/                # FakeOdps（对齐 Python 的 FakeODPS）
    └── integration/         # 真实 backend，CI 跳过
```

### 3.2 分层边界

- **commands/** 只做 cobra flag 绑定和调用 `app`；不写业务逻辑。命名/风格参考现有 plugin-maxcompute 便于对方 reviewer 接受。
- **app/** 编排：调用 backend、组装 envelope、生成 hints。Python 当前 4000 行 `app.py` 是反例；Go 版按命令族拆分，每文件 ≤400 行。
- **backend/** 仅与 odps-sdk-go 打交道，对外暴露 `Backend` interface，方便 `FakeBackend` mock。
- **envelope/hints.go** 是单一真理源：错误码→suggestion 映射、查询特征→next_actions 模板、warnings 检测器集中实现。Python 版规则散落各处是技术债，移植时一并清理。

### 3.3 与 aliyun-cli-runtime 的接合点（依赖最小化）

- `runtime/aly`：取 profile + credentials
- `runtime/output`：默认走 envelope 自有 writer，`--output aliyun-json/aliyun-text` 时切回 runtime 标准 writer
- `runtime/config`：读 `~/.aliyun/config.json`
- `runtime/executor`：cobra 命令注册（按现有插件惯例）

### 3.4 MaxC 专属配置

`~/.aliyun/maxc/config.yaml`：仅存非认证类设置（默认 endpoint/region、cache 路径、skills 模式开关），**不再存任何 AK**。

## 4. 主要风险与缓解

| # | 风险 | 缓解 |
|---|---|---|
| 1 | aliyun-cli profile 拿到的 STS token 是否被 ODPS endpoint 接受 | Phase 0 PoC 必跑通；如不兼容，在 odps-sdk-go 增加 STS account 适配 |
| 2 | odps-sdk-go 能力 gap（profile_table 分位数、diagnose_job fuxi 详情、cost estimate input bytes 等） | SDK 由本方维护，开发期 `go.mod replace` 指向本地 fork，能力补完后 PR 入库 |
| 3 | go-sqlite3 cgo 跨平台分发麻烦 | 使用 `modernc.org/sqlite`（纯 Go） |
| 4 | skills 资源交付 | `//go:embed skills/*` 内嵌二进制 |
| 5 | agent_hints 规则散落 4000 行 app.py，移植易漏 | Phase 3 单独立项，grep + 规则表 + 同输入对比 |
| 6 | Python 版持续迭代造成追赶 | Phase 1 完成时 Python 版 feature freeze，仅接 critical bugfix |

## 5. 落地计划

### Phase 0 — 可行性 PoC（1~2 周）

只验证两件事：风险 1（STS 透传）和风险 2（SDK gap）真实可解。

- 在 `aliyun-cli-plugins` 下建 `plugin-maxc/`，最小骨架（main.go + 一个 `query` 命令）
- 用 `aliyun-cli-runtime/aly` 取 profile 的 credentials，构造 odps-sdk-go 的 account（AK/STS 两种都跑通）
- 跑通 `aliyun maxc query --sql "select 1"`
- 用 STS profile 跑一次确认 endpoint 接受
- 调 `list_tables` / `describe_table` / `get_instance`，输出 SDK gap 清单

**Exit criteria**：PoC demo + SDK gap 清单 + ~50 行 auth bridge 范例代码。

### Phase 1 — 框架与基础设施（2~3 周）

为后续命令移植打地基，不实现具体业务：

- 完整目录搭建（按 §3.1）
- `internal/envelope`：Envelope 结构 + AgentHints + Output writer
- `internal/auth`：profile → odps account 工厂
- `internal/backend`：抽 `Backend` interface + `OdpsBackend` 骨架 + `FakeBackend`
- `internal/errors`：错误码 + suggestion 体系
- `internal/cache`：基于 modernc.org/sqlite 的 KV/元数据缓存骨架，schema 与 Python 对齐
- `internal/config`：MaxC 专属配置加载
- 单测框架 + CI（lint / vet / test）
- odps-sdk-go 仓建 `feature/maxc-cli-deps` 分支，用 `go.mod replace` 对接

**Exit criteria**：能跑 `aliyun maxc whoami` 输出标准 envelope；CI 全绿。

### Phase 2 — 命令族移植（6~8 周）

按从底层到上层、从无依赖到有依赖排序。每个命令族落地标准：Python 测试用例 1:1 翻译为 Go 测试（用 FakeBackend）。

| 顺序 | 命令族 | 关键命令 | 周数 |
|---|---|---|---|
| 1 | auth | `whoami`, `can-i`, `login`（薄包装，引导用户使用 `aliyun configure`，不再独立写 AK 文件）| 0.5w |
| 2 | meta-基础 | `list-tables`, `describe-table`, `list-projects` | 1w |
| 3 | query | `query`, `submit-query`, `explain`, `estimate-cost` | 1.5w |
| 4 | job | `get-job`, `wait-job`, `fetch-result`, `cancel-job`, `list-jobs`, `diagnose-job` | 1.5w |
| 5 | data | `sample`, `profile` | 1w |
| 6 | meta-高级 | `search-tables`, `search-columns`, `lineage` 占位 | 1w |
| 7 | 其他 | `audit`, `masking`, `diff`, `store` 相关 | 1.5w |

每跨完一族做一次 release candidate，发到内网二进制源给真实 agent 试跑。

### Phase 3 — agent_hints 规则迁移与体验对齐（2 周）

- grep Python 所有 hints 触发点，整理成规则表
- `internal/envelope/hints.go` 集中实现，挂到 envelope builder
- Python vs Go 同输入对比，确保 next_actions/warnings/insights 一致或更优
- Python 版进入 feature freeze

### Phase 4 — 发版与生态接入（1~2 周）

- 完善 `manifest.json`：cmdNames 全列、platforms 6 平台齐全
- 跨平台交叉编译脚本
- 与 aliyun-cli plugin index 联调注册
- README + Python 老用户迁移指南
- Python 仓 README 加 deprecation notice

**总周期估算**：12~16 周（3~4 个月）；双人并行可压至 8~10 周。

## 6. 前置条件清单

| 前置条件 | 状态 | 备注 |
|---|---|---|
| odps-sdk-go push & PR 权限 | ✅ 已具备 | Owner 已确认 |
| aliyun-cli-plugins 仓库 push & 注册插件权限 | ⚠️ 待确认 | 是否需对方 owner review |
| 插件二进制分发渠道（内网 / 公网 index） | ⚠️ 待确认 | 影响发版形态 |
| Python 版本 freeze 时间点 | ⚠️ 待对齐 | 建议 Phase 1 完成时启动 |
| MaxCompute 测试 project（含 STS RAM Role）| ⚠️ 需准备 | PoC 必需 |
| CI 资源（GitHub Actions / 内部 Jenkins）| ⚠️ 待确认 | 跨平台编译用 |

## 7. 验收标准

- 全部 Python 子命令在 Go 插件下有同名、同参、同 envelope schema 的实现
- Python 测试集 ≥95% 覆盖率翻译为 Go 测试，全部通过
- agent_hints 在覆盖的命令上输出与 Python 版语义一致或更优（人工抽样对比 ≥30 个 case）
- 6 平台二进制全部能正常 `aliyun maxc whoami`
- aliyun-cli plugin index 注册完成，可通过 `aliyun plugin install maxc` 安装

## 8. 后续（不在本 spec 范围）

- Python 版退役时间表
- agent_hints 规则进一步打磨（基于 Go 版上线后的真实 agent 反馈）
- 是否要把 envelope 协议形成跨产品规范（暂不考虑）

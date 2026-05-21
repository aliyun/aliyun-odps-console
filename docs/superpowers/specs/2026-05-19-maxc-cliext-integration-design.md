# maxc-cli → aliyun-cli cliext 集成设计

**日期**: 2026-05-19
**状态**: 设计已批准，待写实施 plan
**相关**: `2026-04-28-go-port-aliyun-cli-plugin-design.md`（Go 重写方案，已搁置）

## 背景

`maxc-cli` 是 Python 实现的 MaxCompute CLI 工具层，输出结构化 JSON envelope 供 AI agent 调用。当前作为独立 Python 包发布；为了让用户在 `aliyun` 命令下统一调用，决定把它以 PyInstaller onedir 二进制形式集成进 `aliyun/aliyun-cli`，参照 `cliext/cms2`、`cliext/saectl` 的"in-tree launcher + 外部托管二进制"模式。

不走 Go 重写路线（见 `2026-04-28-go-port-aliyun-cli-plugin-design.md`，工作量 ≥3 个月）。本方案保留 Python 实现，只在 aliyun-cli 内补一层 Go launcher 做发现/下载/exec。

## 关键决策

| 维度 | 决策 |
|---|---|
| 落地方式 | 上游 PR 到 `aliyun/aliyun-cli`，加 `cliext/maxc/` |
| 分发渠道 | 公共 OSS（仿 cms2 的 `o11y-addon-hangzhou-public` 模式），bucket 名待定 |
| 命令名 | `aliyun maxc ...` |
| 凭证桥接 | cliext 优先读 aliyun profile 注入 `ALIBABA_CLOUD_*` env，profile 缺失时静默 fallback 到 maxc 自身配置链 |
| Skills 资源 | 沿用现有 `maxc.spec` 的 `collect_data_files('maxc_cli')`，onedir 内完整包含 |
| 版本/更新策略 | TTL 自动更新（cms2 同款，路径 A）；不引入 pinned/双轨方案 |
| CI 平台 | **开放项**，进入实施 plan 前必须先 PoC 决策（见 § 6） |

## § 1 — 总体架构

三个相对独立的产物，靠 OSS 上的目录约定串起来：

```
┌───────────────────────────────────────────────────────────────┐
│  maxc-cli 仓库（本仓库）                                       │
│  ├─ scripts/build_release.sh   ← 新增：6 平台 PyInstaller       │
│  ├─ maxc.spec                  ← 已有                          │
│  └─ scripts/upload_to_oss.sh   ← 新增：推到 OSS                │
│                  │                                             │
│                  ▼                                             │
│  OSS bucket: <bucket>.oss-cn-hangzhou.aliyuncs.com（待定）     │
│  ├─ versions/latest                  ← 文本，唯一可信版本号     │
│  └─ {version}/{os}-{arch}/                                     │
│      ├─ maxc.tar.gz                  ← onedir 打包             │
│      └─ maxc.tar.gz.sha256                                     │
│                  │                                             │
│                  ▼                                             │
│  aliyun-cli 仓库（上游 PR）                                    │
│  └─ cliext/maxc/                     ← 新增 Go 包              │
│      ├─ main.go      NewMaxcCommand() *cli.Command            │
│      ├─ maxc.go      Context.Run / 下载 / 凭证注入 / exec      │
│      └─ *_test.go                                             │
└───────────────────────────────────────────────────────────────┘
```

**调用链**：用户 `aliyun maxc query --sql "..."` → aliyun root cobra 路由到 `NewMaxcCommand()` → `Context.Run`：① 找/下载 `~/.aliyun/maxc/maxc[.exe]` ② 把当前 aliyun profile 抽出 AK/SK/STS/Region 注入 env ③ `exec.Command("~/.aliyun/maxc/maxc", "query", "--sql", "...")` 透传 stdin/stdout/stderr 与退出码。Python 端无任何感知。

**边界划分**（每层单一职责，可独立替换）：

- **打包层**（maxc-cli 仓库）：只关心 Python → 二进制。CI 产物落到固定 OSS 路径，不感知 aliyun-cli。
- **分发层**（OSS bucket + 目录约定）：唯一一份契约 `versions/latest` + `{version}/{os}-{arch}/maxc.tar.gz`。任何一边改这个目录约定都需要双方对齐。
- **接入层**（cliext/maxc/）：单一职责——找 binary、注 env、exec。绝不实现业务逻辑；maxc 升级也不需要它改代码（路径 A 选型的核心理由）。

## § 2 — cliext/maxc/ Go 包结构

参考 `cliext/cms2/` 的代码组织，文件 1:1 对齐方便 reviewer：

```
cliext/maxc/
├── main.go            # NewMaxcCommand() — 入口工厂
├── maxc.go            # Context + Run + 下载/解压/exec 主流程
├── credentials.go     # 从 aliyun profile 抽 AK/STS → env（cms2 没单独抽）
├── main_test.go
├── maxc_test.go
└── credentials_test.go
```

### 关键差异（vs `cliext/cms2/cms2.go`）

| 方面 | cms2 | maxc |
|---|---|---|
| 安装产物 | 单文件 `aliyuncms2[.exe]` | onedir 目录 `~/.aliyun/maxc/`（含 `maxc` + `_internal/`） |
| 下载 | 直接 GET 二进制 | GET tarball → 解压（`archive/tar` + `compress/gzip`） |
| 校验 | size/exec 检查 | tarball 旁的 `.sha256` 文件做摘要校验，不通过则不写入磁盘 |
| 安装位置 | `~/.aliyun/aliyuncms2` | `~/.aliyun/maxc/maxc[.exe]`（onedir 整体放一个子目录） |
| 凭证注入 | `PrepareEnv` 简单透传 env | 调 `config.LoadProfile` 主动取 STS/AK/Region，注入 `ALIBABA_CLOUD_*` |
| Env 覆盖 | `ALIBABA_CLOUD_CMS2_EXEC_PATH` | `ALIBABA_CLOUD_MAXC_EXEC_PATH` |
| 版本检查 | TTL 86400s | 同左（直接复用 cms2 范式） |

### `Context.Run` 主流程（伪码）

```go
func (c *Context) Run(args []string) error {
    c.InitBasicInfo()                  // 解析 ~/.aliyun/maxc/maxc 路径、平台
    c.CheckOsTypeAndArch()             // 6 平台白名单
    if !c.osSupport { return ... }

    if err := c.EnsureInstalledAndUpdated(); err != nil {
        if !c.installed { return err } // 没装过就硬错
        // 已装但更新检查失败：仅 stderr warning，继续用旧版（cms2 同行为）
    }

    if err := c.InjectAliyunCredentials(args); err != nil {
        return err                     // profile 解析失败直接退出
    }

    childArgs := c.RemoveFlagsForMainCli(args)  // 抹掉 --profile 等 aliyun 自己的全局 flag
    return c.Execute(childArgs)        // exec.Command + 自定义 ExitError 透传退出码
}
```

### 凭证注入逻辑（`credentials.go`）

对应"优先 aliyun profile，fallback maxc 自身"的语义：

1. 读 `--profile <name>`（无则 default），调 `config.LoadProfile`。
2. 按 profile mode 取 credentials：
   - `AK` → 注入 `ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET`
   - `StsToken` / `RamRoleArn` / `EcsRamRole` / `CloudSSO` → 走 profile 的 `GetCredential()` 取临时凭证 → 注入 `..._ID/SECRET/SECURITY_TOKEN`
3. 注入 `MAXCOMPUTE_REGION`（`profile.RegionId`）。
4. **不注入** `MAXCOMPUTE_PROJECT` 与 `MAXCOMPUTE_ENDPOINT` — aliyun profile 没这两个概念，留给 maxc 自己的配置层处理（`CLAUDE.md` 已经定义了 env > config 的优先级，不破坏既有契约）。
5. 失败处理：profile 不存在 / 没配置时**不报错**，让 env 保持空，maxc 自己沿 `~/.maxc/config.yaml` 链路找配置——这就是"fallback maxc 自身"的语义。

### 直接照抄 cms2 已踩过的坑

- `IsHelp()` 处理 `--help` ↔ `help` 互转（`cliext/cms2/main.go` 里的 if 块）
- `KeepArgs: true` + `EnableUnknownFlag: true` 让 aliyun root cobra 不要解析 maxc 的子命令参数
- 子进程退出码用自定义 `ExitError` 透传，不在 `Run` 里直接 `os.Exit`（保留 defer）

## § 3 — OSS 目录契约 & 打包流水线

这是连接两个仓库的唯一接口，必须先钉死才能动 cliext 代码。

### OSS 目录约定

bucket 名以 `<bucket>` 占位，待 § 6 前置验证后定下：

```
<bucket>/
├── versions/latest                       # 单行文本，如 "0.2.5"，cliext 拉这个判断是否需更新
└── {version}/
    ├── linux-amd64/maxc.tar.gz
    ├── linux-amd64/maxc.tar.gz.sha256    # 单行 hex 摘要
    ├── linux-arm64/maxc.tar.gz
    ├── linux-arm64/maxc.tar.gz.sha256
    ├── darwin-amd64/...
    ├── darwin-arm64/...
    ├── windows-amd64/...
    └── windows-arm64/...
```

### tarball 内部结构

解压后直接 cp 到 `~/.aliyun/maxc/`：

```
maxc.tar.gz
└── maxc/                       # 顶层目录名固定为 "maxc"
    ├── maxc                    # PyInstaller 入口（windows 是 maxc.exe）
    ├── _internal/              # PyInstaller onedir 内部依赖
    │   ├── base_library.zip
    │   ├── maxc_cli/skills/    # 已通过 collect_data_files 包含
    │   └── ...
    └── ...
```

cliext 端解压逻辑：拉到 tar.gz → 校验 sha256 → 解压到临时目录（产生 `/tmp/xxx/maxc/`）→ 把这个 `maxc/` 子目录原子 rename 到 `~/.aliyun/maxc/`（先把旧目录 rename 成 `maxc.old.<ts>`，新目录 rename 上来，再异步删旧的——避免半截状态）。**最终二进制路径固定为 `~/.aliyun/maxc/maxc[.exe]`**——这是 § 2 表格里"安装位置"那行的唯一含义，cliext 全程只认这个路径。

### maxc-cli 仓库新增脚本

```
scripts/
├── build_release.sh         # 调 pyinstaller，打 tar.gz，算 sha256
├── upload_to_oss.sh         # ossutil cp 到 <bucket>/{version}/{platform}/
└── publish_latest.sh        # 最后一步：把 versions/latest 改成新版本号
```

### 6 平台构建策略

PyInstaller 不能交叉编译，6 平台必须 6 个真实 runner：`{linux, darwin, windows} × {amd64, arm64}`。具体平台来源（GitHub Actions / Aone CI / 混合）作为 § 6 前置验证产出。

发版顺序严格为：**先全部 6 平台的 tarball 上 OSS（含 sha256）→ 最后才更新 `versions/latest`**。这样 cliext 不会拉到一个 latest 指向但平台缺位的版本。

### 首版规模（YAGNI 砍掉的）

- 不做 manifest.json（cms2 也没用，单纯 GET tarball + sha256 已经够）
- 不做签名（aliyun-cli plugin 体系也没强签名，先对齐）
- 不做增量更新（onedir ~50MB，整包重下可接受）
- 不做镜像/CDN（一开始单 region OSS，跑通了再加）

## § 4 — 用户体验 & 错误处理

### 首次调用体验

```
$ aliyun maxc query --sql "select 1"
maxc: not installed locally, downloading from <bucket> (this may take ~30s)...
maxc: downloaded 0.2.5 (52MB), installed to ~/.aliyun/maxc/
{... maxc 的正常 envelope 输出 ...}
```

下载进度提示走 stderr，stdout 保持 maxc envelope 干净，pipeline 友好。

### 已安装 + 24h TTL 触发更新检查

cms2 同款：检查不阻塞业务调用，失败仅 stderr warning。

### 错误路径

每条错误码语义独立、不混在一起：

| 场景 | exit code | 行为 |
|---|---|---|
| 平台不支持 | cliext 自身退出 1 | `your os/arch X-Y is not supported` |
| 首次下载失败 + 本地无 binary | cliext 退出 1 | 打印重试命令 + `ALIBABA_CLOUD_MAXC_EXEC_PATH` 手动指向的提示 |
| 已装但更新检查失败 | 继续用旧版 | stderr warning，子进程退出码透传 |
| sha256 校验失败 | cliext 退出 1 | 不写磁盘，提示可能是 OSS 镜像中间态，建议稍后重试 |
| 子进程非 0 退出 | 透传子进程 exit code | 不再额外包装错误信息 |
| profile 不存在 | 不报错 | 留 env 空，maxc 自己沿配置链找 |

### Env 控制开关（最小集合）

- `ALIBABA_CLOUD_MAXC_EXEC_PATH` — 指向自定义 maxc 可执行路径（开发/离线/调试用，跳过下载）
- `ALIBABA_CLOUD_MAXC_NO_UPDATE_CHECK=1` — 跳过 TTL 更新检查（CI 环境/离线场景）
- `ALIBABA_CLOUD_MAXC_DOWNLOAD_BASE_URL` — 覆盖 OSS base URL（内部测试镜像）。覆盖的是 bucket 根 URL，整个 § 3 目录约定（`versions/latest` + `{version}/{platform}/maxc.tar.gz`）相对它生效，不允许只改其中一段。

## § 5 — 测试策略

### cliext/maxc/ Go 端测试

参考 `cliext/cms2/cms2_test.go` 已经把 `httpGetFunc` / `execCommandFunc` / `runtimeGOOSFunc` 都做成了 `var xxFunc = ...` 形式可替换的注入点，照抄即可：

- `main_test.go`：cobra 路由 + IsHelp 处理 + KeepArgs 行为
- `maxc_test.go`：用 mock 的 `httpGetFunc` / `execCommandFunc` / `runtimeGOOSFunc` 注入点，覆盖：
  - 下载成功 / 下载 404 / sha256 不匹配 / tarball 损坏
  - 已装最新版（短路）/ 已装但需更新（拉新版）/ 检查失败但已装（warn 后继续）
  - exit code 透传
- `credentials_test.go`：mock `config.LoadProfile`，覆盖 AK / StsToken / RamRoleArn / 空 profile / RamRoleArn 取临时凭证失败 5 个分支，断言注入了哪些 env

### maxc-cli 仓库端

- `tests/test_pyinstaller_bundle.py`（新增）：把现有 `bin-smoke` 的 3 条断言扩展成 pytest 用例：
  - `dist/maxc/maxc --version` 输出符合 SemVer
  - `dist/maxc/maxc --help` 退出码 0 + stdout 含 `Usage:`
  - skill 资源包含且可读
  - PyInstaller bundle 内不含 `.pyc` 之外的源码（防意外泄露 docstring 里的内部信息）
- 跨平台 smoke：每个 runner 跑完构建后立刻跑 `./dist/maxc/maxc --format json agent skill`，输出 JSON 解析成功即过

### 端到端集成测试（手动 + runbook）

- 在干净 macOS / Linux / Windows 各一台机器：装 aliyun-cli、跑 `aliyun configure`、跑 `aliyun maxc query --sql "select 1"`，期望首次下载并出结果
- 验证 STS 透传：用 RamRoleArn profile 跑同一条命令，确认 maxc 端实际拿到的是临时 AK/SECURITY_TOKEN（用 `maxc whoami` 子命令的输出对账）

不写 e2e 自动化的原因：需要真实 MaxCompute project + AK，测试矩阵 × 凭证类型组合太多，不适合放 CI。手动 runbook 在每次 cliext PR 前跑一次。

## § 6 — 前置验证待办

实施 plan 第一阶段必须先吃掉这些，spec 主体不被它们卡住：

1. **CI 平台选型** — Aone CI 是否提供 macOS / Windows / arm64 runner；`cli-hub` 是否能托管非 plugin 形态的 tarball 并暴露稳定 URL 给 cliext 拉取。三选一：
   - 全 Aone CI（如果 runner 矩阵齐全 + cli-hub 可用）
   - 混合：GH Actions 做 6 平台 matrix，Aone 做最终 publish
   - 全 GH Actions（确定性最高、维护最简）
2. **OSS bucket 创建** — bucket 名 / region / 公共读权限 / 跨域（cliext 需要 GET）
3. **修复 `.aoneci/cli-publish.yaml` 引用了不存在的 `make dist`** — 当前 Makefile 没这个 target，独立 bug，先修掉
4. **Aliyun-cli 上游沟通** — 在 `aliyun/aliyun-cli` 提 issue 探讨"加 cliext/maxc/"的接受度，确认 reviewer / 命名（`maxc` vs `maxcompute2`）/ release 节奏
5. **PyInstaller arm64 验证** — 当前 `maxc.spec` 在 darwin-arm64 上是否已验证（`dist/` 现存只有 wheel，没看到 onedir 验证记录）
6. **凭证流程 PoC** — 写个 50 行 Go 小程序：`config.LoadProfile` → 注入 env → 调一次 maxc 容器跑通 `select 1`，证明 STS 这条链通

## 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| Aone CI 不支持非 linux/amd64 runner | 阻塞全内部 CI 方案 | § 6.1 PoC 后切混合 / 全 GH Actions |
| OSS bucket 公共读策略合规审批慢 | 阻塞首发 | 提早走审批；同时探查 cli-hub 替代 |
| aliyun/aliyun-cli 不接受新 cliext | 整个上游 PR 路径作废 | § 6.4 提前提 issue 探口风；备选改走 `cli/plugin/` 体系（已有索引机制，不需改 aliyun-cli 主仓） |
| PyInstaller onedir 在 windows-arm64 不稳定 | 该平台首发缺位 | 退化方案：windows-arm64 首发暂缺，等 PyInstaller 升级；用户用 `ALIBABA_CLOUD_MAXC_EXEC_PATH` 自管 |
| maxc agent_hints 在 envelope JSON 输出里依赖动态 import | onedir 需把所有 hint 子模块手工加 hidden imports | 构建时跑全命令集合 smoke 暴露 import 错误（已有 bin-smoke 部分覆盖） |

## 不在范围内

- maxc-cli 自身的功能演进 / API 变更
- aliyun-cli 主体（非 cliext）的任何改动
- maxc 的 Go 重写（见 `2026-04-28-go-port-aliyun-cli-plugin-design.md`）
- aliyun-cli plugin 索引体系（`cli/plugin/`）的接入——本方案选 cliext 一条道
- 国际化 / 中文错误信息——首版只提供英文，沿用 cms2 风格

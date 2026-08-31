# Changelog

## [0.5.1] — 2026-08-31

### Fixes

- 修复多个进程首次初始化同一状态目录时的创建竞态；并发创建不再误报
  `FileExistsError`，且仍通过 descriptor-relative、`O_NOFOLLOW` 和所有权检查
  保持原有安全边界。
- 隔离只读 SQL 门禁的 CLI 回归测试，不再依赖开发机已有认证配置，确保干净的
  Linux、Windows 和 Python 版本矩阵得到一致结果。

## [0.5.0] — 2026-08-31

### Breaking Changes

- 自动发现的工作区配置不再允许定义 `auth`；凭据配置必须位于用户级
  `~/.maxc/config.yaml`，或由用户通过 `--config` 显式选择。
- `data upload` 不再隐式创建缺失分区；必须显式传入
  `--create-partition`。
- 远程查询不再接受会造成重复提交歧义的自动重试参数；异步恢复统一使用
  `job_id`、`job wait` 和作用域绑定的 cursor。
- `agent_hints.next_actions` 只包含无需补参、无需确认且允许 Agent 自动执行的
  命令；完整候选及其 effect/confirmation 契约以 `actions[]` 为准。

### Features

- 新增由实时 parser 生成的 `agent manifest`，覆盖全部命令的参数、网络要求、
  凭据要求、副作用和输出形状。
- OAuth 登录、刷新和一次性 continuation 采用可恢复的结构化 Envelope；
  `agent context` 保持纯本地，`agent doctor --online` 才执行在线验证。
- `aliyun maxc` 可向本地 context 传递不含凭据的 profile 就绪提示；离线检查
  无需解析或注入 AK/SK/STS，也不会把“已配置”误写成“已认证”。
- Job 失败状态、等待流和结果读取统一返回可恢复的类型化错误；query/job cursor
  绑定 SQL、project、job 和本地 session，拒绝跨作用域复用。
- 读取已有 remote cursor 不再重新解析当前 MCQA v2 配置；配置在提交后发生变化时，
  仍按 cursor 中已绑定的 job/session 取回原结果，而提交专用参数会被明确拒绝。
- 查询超过同步等待预算时会保留请求的行数上限，并返回可直接执行的
  `job result --max-rows ...` 动作，续跑不会重新提交 SQL 或丢失分页意图。
- `query --output` 遇到异步 pending 时不再把控制 Envelope 写成结果文件；
  CLI 保留路径、格式与 overwrite 意图，并由 `job result --output` 在完成后
  原子发布真实结果。
- Upload 在创建 Tunnel session 前完成整文件校验，并从同一份 owner-private
  快照重放到 Tunnel；源文件在校验后被原地改写也不会改变远端提交内容。view
  下载恢复命令保留 project/schema/User-Agent 上下文。
- Upload 失败会区分 commit 未尝试与 commit 结果未知：前者明确无可见行并等待
  server-side session 过期，后者禁止盲目重传，避免重复 append/overwrite。
- 本地 metadata FTS 索引可重建且可在无 FTS5 的 SQLite 上降级运行。

### Security and Reliability

- 配置、OAuth、Job store、cache 和 audit 使用 owner-only、原子替换及跨进程锁；
  POSIX 使用 descriptor-relative I/O，Windows 使用受验证的路径 fallback。
- 外部凭据 helper 以 argv + `shell=false` 执行，拒绝工作区隐式激活和 shell
  pipeline；审计日志递归清除凭据、token 和 SQL 原文。
- SQL 执行采用 SELECT-only 正向允许列表，未知操作 fail closed；缺失分区创建、
  overwrite、cancel 和本地写入在 manifest/actions 中显式建模。
- `cache status`、`cache build-status` 和 semantic 读取使用不建库、不迁移、
  不改权限的零写快照；遇到活动 WAL 或无法证明一致性的并发变化时返回可恢复
  失败，不会静默读取陈旧数据。POSIX 读取直接绑定已验证文件描述符，避免路径
  被短暂替换后恢复所造成的 TOCTOU；Windows 路径由文件和父目录句柄锁定。
- Markdown/brief 只把可执行、允许 Agent 且无需确认的 action 展示为下一步，
  并在 `aliyun maxc` 分发中统一恢复命令的入口名称。
- 遗留 `session_override.yaml` 仅在用户明确执行配置写命令时迁移；context、
  doctor 和 session show 等读取不会创建或改写本地状态。配置、迁移 marker 和
  源文件删除按耐久顺序执行，解析或写入失败不会吞掉唯一一份旧配置。
- 实时 manifest 逐命令声明审计写入、输出目录预检和旧配置清理等隐式副作用；
  `agent context`、`agent manifest` 与离线 doctor 的确认零审计分支保持零写。
- 安装器增加目录穿越、链接、并发安装、校验和与原子切换防护；Python 最低版本
  统一为 3.9。
- PyInstaller 只打包 MaxC 实际使用的 PyODPS 核心路径，排除 pandas、NumPy、
  notebook、测试框架等可选生态，降低本地启动与发布包体积的不确定性。
- macOS release tar 在归档时剥离 AppleDouble、provenance、quarantine 等构建机
  扩展属性，避免把本机执行策略元数据传播给公共下载包。

### Tests and CI

- 增加全命令 manifest/runtime、OAuth 并发、状态文件耐久性、跨平台 fallback、
  输出格式、打包元数据及 AI-native 回归测试。
- 发布前门禁覆盖 Python 3.9/3.12、Windows 单元测试、wheel 安装和五平台
  PyInstaller smoke。

## [0.4.2] — 2026-06-02

### Features

- `job submit --dry-run`：预估 SQL 成本而不实际提交异步 job
- `data upload --dry-run`：校验表结构和 CSV 文件（列映射、行数、文件大小）而不上传数据

## [0.4.1] — 2026-06-02

### Features

- 统一设置 UserAgent，所有 ODPS 请求携带 `maxc-cli/{version}` 前缀，便于服务端区分请求来源

## [0.4.0] — 2026-06-01

### Breaking Changes

- `--help` 输出格式全面改为 aliyun CLI 风格，脚本如果 parse help 文本可能需要适配
- Agent skill 安装目录从 `maxcompute-cli-guidance` / `use-maxc-cli` 统一为 `maxc-cli`（旧目录自动清理）
- Claude Code skill 安装路径从 `~/.claude/plugins/maxc-cli` 改为 `~/.claude/skills/maxc-cli`

### Features

- **aliyun CLI 风格帮助输出**：版本头置顶、Commands/Arguments/Flags 三段式、`--long,-short` 格式、去掉 Sample/footer
- **`MAXC_CLI_NAME` 环境变量**：设置后控制 help/version/SKILL 模板中的程序名（例如 `MAXC_CLI_NAME="aliyun maxc"`），支持作为 aliyun CLI 子命令嵌入

### Bug Fixes

- `--help` / `-h` 现在正确路由到用户指定的子命令，而非总显示顶层帮助
- `--version` 不再重复输出版本头
- `session set/unset/show` 正确处理 `--config` 参数
- `query` 不再向 PyODPS `run_sql` 传递 `unique_identifier_id=None`

### Refactoring

- 所有平台 skill 安装目录统一为 `maxc-cli`
- `{{cli}}` 模板渲染由 `MAXC_CLI_NAME` 环境变量直接驱动，移除中间 key 映射层

### Tests & CI

- 新增 E2E subprocess 黑盒测试覆盖 `-h/--help/--version` 路由
- CI 流水线在 unit gate 后运行 e2e 测试
- 移除无效断言，替换为真实检查

### Docs

- 新增 agent-driven install guide（OSS 托管分发）

---

## [0.3.2] — 2026-05-26

### Bug Fixes

- `ExternalAuthConfig` import 路径修正
- `query`: 将 `odps.instance.priority` hint 传入 `run_sql` kwarg

---

## [0.3.1] — 2026-05-25

### Features

- **`agent skill` 六动词子命令**：`install`, `update`, `uninstall`, `list`, `diff`, `path` — 取代旧 `agent install-skill`
- **平台注册表** (`agent_platforms.py`)：支持 claude-code, cursor, windsurf, codex, qwen, qoder, qoderwork
- **全局 flag 位置无关**：`--json`, `--format`, `--config` 等可放在子命令后面
- **aliyun CLI 帮助格式初版**：Section 重命名、compact synopsis、Sample epilog、color subcommands
- **auth 自动重定向**：未认证时裸 `maxc` 或任意子命令自动跳转 `auth login`
- **auth 识别 external/ncs 认证**
- **ErrorPayload 增加 exit_code + context 字段**

### Bug Fixes

- `envelope failure` exit code 现在反映原始异常
- pre-release quality pass：修复 4 个 P0 阻塞 + 若干小修
- `meta --schema` 在 describe/latest/freshness/partitions 上生效
- `query` 检测 alias 时无 SQL 输入给出清晰错误
- `data`: JSON-safe Decimal/bytes、auto-create partition、reject views
- `backend`: pyodps fallback CSV reader 时恢复行数据
- `job`: OdpsNoSuchObject 正确 re-raise

### Refactoring

- 移除旧 `agent install-skill` 代码路径
- SKILL.md 使用 intent→command quick map + on-demand reference

### Tests & CI

- ruff + pytest `--cov-fail-under=70` 门禁
- SKILL 文档引用真实 CLI verb 校验

---

## [0.2.5] — 2026-05-19

### Features

- PyInstaller onedir 二进制构建流水线
- SKILL 新增 SQL generation references

### Refactoring

- 移除 `session_override` 优先级层，session set 直接写全局 config
- 简化 session_show/session_unset 逻辑
- `--format json` on errors 修正（agent surface 精简）

### Bug Fixes

- `pyodps>=0.12.0` 版本检查，不兼容时早期报错
- 遗留 `session_override.yaml` 自动迁移到全局 config

---

## [0.2.4] — 2026-05-09

### Features

- **`data upload`**：CSV/TSV 批量导入已有表（Tunnel API）
- **`data download`**：表/分区导出为 CSV/TSV
- `csv_parse_value` / `csv_format_value` / `csv_supported_type` 辅助函数

### Bug Fixes

- `data upload`: TableTunnel shim 构造、partition key 校验、extra header 列警告

---

## [0.2.3] — 2026-05-07

### Bug Fixes

- Python 3.9 兼容性修复
- External auth 健壮性改进

---

## [0.2.2] — 2026-04-27

### Bug Fixes

- Python 3.9 兼容性修复（release bump only）

---

## [0.2.1] — 2026-04-22

### Features

- **Catalog API 集成**：`auth login` 交互式项目选择器
- **External credential provider**：支持外部进程提供凭证
- **全局 `--format`**：markdown / brief / csv / ndjson
- **Semantic metadata**：`meta semantic set/get/list-missing`
- **Agent install-skill**：一键注册 SKILL 到各平台
- **NCS auth flow**
- **Client-side read-only**：DDL/DML 客户端拦截 + `--force` 旁路
- **`query --wait`**：替代旧 `--async/--timeout`，auto-promote 逻辑
- **`job wait --timeout`** / **`job list --limit`** / **`job result --cursor`**
- `auth can-i`：单表权限探测

### Bug Fixes

- 24+ 修复（error envelope、permission denied、session override、idempotency key、NCS token caching 等）

### Initial Release (0.1.x → 0.2.0)

- MVP：query, job, meta, auth, session, data sample, cache
- 模块化 backend 架构
- JSON envelope 输出协议
- SKILL.md agent guidance 体系

# Changelog

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

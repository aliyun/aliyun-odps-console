# Changelog

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

## [0.3.3] — 2026-05-26

Internal release, never tagged. Changes folded into 0.4.0.

## [0.3.2] — 2026-05-26

Previous stable release.

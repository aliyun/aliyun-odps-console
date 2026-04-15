# maxc-cli

`maxc-cli` 是一个面向外部 Agent 的 MaxCompute 工具层。它不是 Agent 本身，而是给 Codex、Claude Code、Cursor 或自研 Agent 调用的结构化 CLI。

当前工作树以真实 MaxCompute 为目标。连接信息可以来自环境变量，也可以通过 `maxc auth login` 持久化到本地配置文件。缺少认证时，CLI 会返回结构化引导信息，不再回退到运行时 mock catalog。

## 文档入口

- `docs/design.md`
  产品定位、命令体系和 skill/source 布局
- `docs/implementation.md`
  当前代码的真实行为和输出契约
- `docs/product-positioning.md`
  为什么当前应先把 `maxc-cli` 做成工具层
- `docs/roadmap.md`
  当前路线图和发布后续项
- `src/maxc_cli/skills/`
  SKILL.md 及 references（随 pip 包安装，也是唯一源）

## 当前能力

- 统一的 Agent-Native JSON envelope
- `auth / session / query / job / meta / data / diff / cache / agent`
- `auth login`、`auth whoami`、`auth can-i`
- `session set/show/unset`
- `query cost`、`query explain`、分页 `--page-size` / `--cursor`
- `meta search-columns`、richer `meta describe`、`meta latest-partition`、`meta freshness`
- `meta lineage` 对真实 backend 明确返回 `supported=false` 占位契约
- `data sample --partition --columns --rows`
- `data profile --partition`
- `diff schema`、`diff partition`、`diff data`
- SQLite 本地缓存、结构化审计日志、语义元数据缓存
- `agent context / skill / commands / install-skill`

## 安装

仓库内开发安装：

```bash
python -m pip install -e .
```

发布后安装：

```bash
python -m pip install maxc-cli
```

当前打包元数据支持 Python `3.6` 到 `3.12`。

基础依赖已经包含：

- `pyodps`
- `PyYAML`
- Python 3.6 下的 `dataclasses` backport

按需依赖：

- `pandas`
  某些包含 TIMESTAMP-like 类型的结果集读取路径可能需要它，但它不是安装 `maxc-cli` 的直接前置依赖

## Agent Skill 注册

安装 maxc-cli 后，将 SKILL 注册到 Agent 平台：

```bash
maxc agent install-skill              # Claude Code（默认）
maxc agent install-skill cursor       # Cursor
maxc agent install-skill windsurf     # Windsurf
maxc agent install-skill codex        # OpenAI Codex
```

SKILL 文件随 pip 包安装（`src/maxc_cli/skills/`），由 `install-skill` 拷贝到各平台目录。升级后重跑即可同步：

```bash
pip install --upgrade maxc-cli
maxc agent install-skill
```

## 登录与 Bootstrap

建议的最短接入路径：

```bash
maxc auth whoami --json
maxc auth login --from-env --json
maxc auth whoami --json
maxc cache build --json
maxc meta list-tables --json
```

如果当前 shell 已经有 MaxCompute 环境变量，可以直接持久化：

```bash
maxc auth login --from-env --json
```

显式传参登录：

```bash
maxc auth login \
  --access-id "<access_key_id>" \
  --secret-access-key "<access_key_secret>" \
  --project "<project>" \
  --endpoint "http://service.<region>.maxcompute.aliyun.com/api" \
  --region "<region>" \
  --json
```

`auth whoami --json` 的重点字段在 `data.identity`：

- `authenticated`
- `configured`
- `validation_status`
- `identity_source`
- `project`

如果 `authenticated=false`，继续查看 `data.auth_options` 获取推荐登录动作。

默认配置发现顺序：

```text
~/.maxc/config.yaml
./.maxc/config.yaml
./.maxc.yaml
./.maxc
```

项目和 schema 的会话覆盖保存在：

```text
~/.maxc/session_override.yaml
```

## 快速运行

```bash
maxc auth whoami --json
maxc agent context --json
maxc session show --json
maxc cache build --json
maxc meta list-tables --json
maxc meta search-columns "id" --json
maxc meta describe your_table --json
maxc meta latest-partition your_table --json
maxc meta freshness your_table --json
maxc meta lineage your_table --json
maxc data sample your_table --partition ds=2026-03-20 --columns id,ds --rows 5 --json
maxc data profile your_table --partition ds=2026-03-20 --json
maxc query "SELECT 1 AS one" --json
maxc query cost "SELECT 1 AS one" --json
maxc query explain "SELECT 1 AS one" --json
maxc job submit "SELECT 1 AS one" --json
maxc job wait job_xxx --stream
maxc diff schema left_table right_table --json
maxc diff partition left_table right_table --json
maxc diff data left_table right_table --keys id --columns value_col --rows 100 --json
maxc cache status --json
maxc cache build-status --build-id build_xxx --json
maxc agent skill --json
maxc agent commands --json
```

## `cache build --json` 行为

- `stdout` 只输出单个最终 JSON envelope
- `stderr` 持续输出进度文本，避免慢构建期间完全静默
- `--async --json` 会立即返回 `build_id`，随后用 `cache build-status --build-id <id> --json` 轮询

## JSON 契约

所有 `--json` 命令都返回 envelope：

- `version`
- `command`
- `command_id`
- `status`
- `data`
- `metadata`
- `error`
- `agent_hints`

常用规范化 `data` 结构：

- `auth whoami` -> `data.identity`
- `auth can-i` -> `data.authorization`
- `query` / `job wait` / `job result` -> `data.result` 和 `data.pagination`
- `query cost` / `query explain` -> `data.analysis`
- `meta describe` -> `data.table`
- `meta search` / `meta search-columns` -> `data.search.matches`
- `data sample` -> `data.sample`
- `data profile` -> `data.profile`
- `agent context` -> `data.context`

## 当前限制

- `meta lineage` 还没有接真实血缘 API；真实 backend 会明确返回 `supported=false`、`coverage=unsupported`
- `auth can-i` 当前只支持表级 `SELECT` 预检
- `auth login` 会把 AccessKey 明文写入本地 YAML；CLI 会尽量把文件权限收敛到 `0600`
- 环境变量优先于配置文件；`session_override.yaml` 对 project/schema 的优先级高于两者
- `meta list-tables` 是 cache-backed；冷启动时需要先执行 `cache build`
- `diff data` 当前是 keyed snapshot compare：每侧最多读取 `--rows` 行，不是全表 exhaustive diff
- 真实 backend 目前不提供统一 CU 口径成本，因此 `--cost-check` 在真实 backend 上不可用
- 真实 backend 的 `query explain / query cost` 当前基于 `execute_sql_cost` 和结构化 query outline，不是完整优化器执行计划树
- `--cursor` 当前是 CLI 侧 offset token，不是 MaxCompute 服务端游标

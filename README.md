# maxc-cli

`maxc-cli` 是一个面向外部 Agent 的 MaxCompute 工具层。它不是 Agent 本身，而是给 Claude Code、Codex、Cursor 或自研 Agent 调用的结构化 CLI。

当前版本优先连接真实 MaxCompute。连接信息可以来自环境变量，也可以通过 `maxc auth login` 持久化到本地配置文件；只有在 `backend.type=auto` 且连接信息不完整时，CLI 才会回退到本地 mock catalog。

## 文档入口

- `docs/design.md`
  产品定位、命令体系和配置设计
- `docs/implementation.md`
  当前已实现能力、鉴权方式和真实 MaxCompute 映射
- `docs/product-positioning.md`
  为什么当前应先把 `maxc cli` 做成工具层
- `docs/roadmap.md`
  当前路线图和后续优先级

## 当前能力

- 统一的 Agent-Native JSON envelope
- `query / job / meta / data / auth / diff / agent context / cache`
- `auth login` 持久化 AccessKey 登录配置
- `auth whoami`、`auth can-i`
- `query cost`、`query explain`、`--page-size` / `--cursor`
- `meta search-columns`、richer `meta describe`、`meta latest-partition`、`meta freshness`
- `meta lineage` 对真实 backend 明确返回 `supported=false` 占位契约
- `diff schema`、`diff partition`、`diff data`
- `data sample --partition --columns --rows`
- `data profile --partition`
- richer `job status / wait / diagnose`
- `.maxc/skills/*.md` 形式的外部 Agent 技能文档
- SQLite 本地缓存与结构化审计日志

## 安装

仓库内开发安装：

```bash
python -m pip install -e .
```

发布后安装：

```bash
python -m pip install maxc-cli
```

基础依赖已经包含：

- `pyodps`
- `pandas`
- `PyYAML`

## 登录与鉴权

`maxc` 现在支持 `auth login`。它会把 AccessKey 配置写入 `~/.maxc/config.yaml`，或写入 `--config` 指定的文件。环境变量依然优先于配置文件。

如果当前 shell 已经有 MaxCompute 环境变量，直接持久化：

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

登录后自检：

```bash
maxc auth whoami --json
maxc auth can-i --table your_table --operation SELECT --json
```

默认配置发现顺序：

```text
~/.maxc/config.yaml
./.maxc/config.yaml
./.maxc.yaml
./.maxc
```

## 快速运行

```bash
maxc auth whoami --json
maxc agent context --json
maxc meta list-tables --json
maxc meta search-columns "id" --json
maxc meta latest-partition your_table --json
maxc meta freshness your_table --json
maxc meta lineage your_table --json
maxc data sample your_table --partition ds=2026-03-20 --columns id,ds --rows 5 --json
maxc data profile your_table --partition ds=2026-03-20 --json
maxc query "SELECT 1 AS one" --json
maxc query cost "SELECT 1 AS one" --json
maxc query explain "SELECT 1 AS one" --json
maxc diff schema left_table right_table --json
maxc diff partition left_table right_table --json
maxc diff data left_table right_table --keys id --columns value_col --rows 100 --json
maxc cache status --json
maxc job diagnose job_xxx --json
```

异步任务示例：

```bash
maxc job submit "SELECT 1 AS one" --json
maxc job wait job_xxx --stream
maxc query "SELECT 1 AS one UNION ALL SELECT 2 AS one" --page-size 1 --json
```

## 当前限制

- `meta.lineage` 还没有接真实血缘 API；真实 backend 会明确返回 `supported=false`、`coverage=unsupported`
- `auth whoami` 当前输出的是 access_id 脱敏值，不能直接还原 RAM 用户显示名
- `auth can-i` 当前只支持表级 `SELECT` 预检
- `auth login` 会把 AccessKey 明文写入本地 YAML；CLI 会尽量把文件权限收敛到 `0600`
- 环境变量优先于配置文件；如果 shell 中已有 MaxCompute 变量，它们会覆盖 `auth login` 保存的值
- `diff data` 当前是 keyed snapshot compare：每侧最多读取 `--rows` 行，不是全表 exhaustive diff
- 真实 backend 目前不提供统一 CU 口径成本，因此 `--cost-check` 只在 mock backend 下可用
- 真实 backend 的 `query explain / query cost` 当前基于 `execute_sql_cost` 和结构化 query outline，不是完整优化器执行计划树
- `--cursor` 当前是 CLI 侧 offset token，不是 MaxCompute 服务端游标
- `agent` 组当前只暴露 `agent context`

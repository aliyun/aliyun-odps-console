# maxc-cli

这是一个按 `docs/design.md` 落地的 Q1 MVP 骨架，不试图一次性实现整份 2026 愿景，而是先把能跑的 CLI、统一输出协议、本地状态存储和最小 Skill 链路搭起来。

当前版本会优先读取真实 MaxCompute 环境变量；只有在缺少连接信息时才回退到本地 mock catalog。

## 文档入口

- `docs/design.md`
  产品愿景、目标接口和 2026 路线图
- `docs/implementation.md`
  当前开发基线、真实 MaxCompute 映射和已知缺口
- `docs/product-positioning.md`
  为什么当前应先把 `maxc cli` 做成基础执行层，以及最有价值的杀手场景
- `docs/base-api-roadmap.md`
  不依赖 `maxc agent` 的基础 API 优先级和实施顺序

## 这版做了什么

- 统一的 Agent-Native JSON 输出 envelope
- `query / job / meta / data / agent context / agent skill / skill list|info`
- `query cost`、`query explain`、`--page-size` / `--cursor` 分页
- `meta search-columns` 和 richer `meta describe`
- 本地 `.maxc/` 目录配置、技能清单和运行时状态
- `auto` backend：优先接真实 MaxCompute，缺失环境变量时自动回退 mock
- NDJSON 形式的 `job wait --stream`
- 结构化退出码

## 设计上做的收敛

- 用 `.maxc/config.yaml` 取代文档里的单文件 `.maxc`，避免和未来的 `.maxc/skills`、`.maxc/state` 冲突。
- 严格按路线图拆分范围：Q1 先做 `query / job / meta` 和 Layer 1/最小 Layer 2 Skill 骨架，`@natural`、`agent plan/run`、Registry 安装发布留到后续。
- CLI 和后端能力之间引入 `backend` 抽象，避免把真实 API 调用逻辑和命令解析耦合死。
- 真实 backend 直接读取环境变量：`ALIBABA_CLOUD_ACCESS_KEY_ID`、`ALIBABA_CLOUD_ACCESS_KEY_SECRET`、`MAXCOMPUTE_PROJECT`、`MAXCOMPUTE_ENDPOINT`。

## 快速运行

直接在仓库根目录执行：

```bash
PYTHONPATH=src python -m maxc_cli query "SELECT 1 AS one" --json
PYTHONPATH=src python -m maxc_cli query cost "SELECT 1 AS one" --json
PYTHONPATH=src python -m maxc_cli query explain "SELECT 1 AS one" --json
PYTHONPATH=src python -m maxc_cli meta list-tables --json
PYTHONPATH=src python -m maxc_cli meta search-columns "id" --json
PYTHONPATH=src python -m maxc_cli agent context --json
PYTHONPATH=src python -m maxc_cli agent skill maxc.data.query \
  --input '{"query":"SELECT 1 AS one"}' \
  --json
```

异步任务示例：

```bash
PYTHONPATH=src python -m maxc_cli job submit "SELECT 1 AS one" --json
PYTHONPATH=src python -m maxc_cli job wait job_xxx --stream
PYTHONPATH=src python -m maxc_cli query "SELECT 1 AS one UNION ALL SELECT 2 AS one" \
  --page-size 1 \
  --json
```

## 当前限制

- `meta.lineage` 还没有接真实血缘 API，先返回空数组占位。
- `@natural`、`agent plan`、`agent run` 只保留命令位，尚未实现。
- 真实 backend 目前不提供 CU 口径成本，因此 `--cost-check` 只在 mock backend 下可用。
- 真实 backend 的 `query explain` / `query cost` 当前基于 `execute_sql_cost` 和结构化 query outline，不是完整的优化器执行计划树。
- `--cursor` 当前是 offset token，不是 MaxCompute 服务端游标。
- Skill 先支持内置执行器，不做远端 Registry 安装/发布。

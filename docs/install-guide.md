# maxc-cli 安装指南

`maxc-cli` 是 MaxCompute 的结构化 CLI 工具，专为 AI Agent（Claude Code、Cursor、Qwen Code 等）调用设计。

---

## 方式一：一键安装（推荐）

### Akless 版本（集团弹内）

```bash
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap-ncs.sh | bash
```

### AK/SK 版本（公共云）

```bash
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap.sh | bash
```

脚本会交互式引导完成：安装 maxc-cli、配置认证、安装 AI Agent Skill。

---

## 方式二：AgentHub 安装 SKILL

直接在 AgentHub 页面点击安装：

[https://open.aone.alibaba-inc.com/console/platform/maxcompute-eco/skill/maxcompute-cli-guidance](https://open.aone.alibaba-inc.com/console/platform/maxcompute-eco/skill/maxcompute-cli-guidance)

前提：已安装 maxc-cli（`pip install maxc-cli`）并完成认证。

---

## 命令一览

| 家族 | 命令 | 说明 |
|------|------|------|
| **query** | `query [run]` | 执行 SQL 查询 |
| | `query cost` | 估算 SQL 费用 |
| | `query explain` | 查看执行计划 |
| **job** | `job submit` | 提交异步任务 |
| | `job status` | 查看任务状态 |
| | `job wait` | 等待任务完成 |
| | `job result` | 获取任务结果 |
| | `job cancel` | 取消任务 |
| | `job diagnose` | 诊断任务问题 |
| | `job list` | 列出任务 |
| **meta** | `meta list-tables` | 列出表 |
| | `meta describe` | 查看表结构 |
| | `meta search` | 搜索表 |
| | `meta search-columns` | 搜索列 |
| | `meta partitions` | 查看分区列表 |
| | `meta latest-partition` | 最新分区 |
| | `meta freshness` | 数据新鲜度 |
| | `meta list-projects` | 列出项目 |
| | `meta list-schemas` | 列出 Schema |
| **data** | `data sample` | 数据采样 |
| | `data profile` | 数据画像 |
| **auth** | `auth login` | AK/SK 认证 |
| | `auth login-external` | 外部凭证认证（ncs 等） |
| | `auth whoami` | 查看当前身份 |
| | `auth can-i` | 检查权限 |
| **session** | `session set` | 切换项目/Schema |
| | `session show` | 查看当前 session |
| | `session unset` | 清除 session |
| **diff** | `diff schema` | 表结构对比 |
| | `diff partition` | 分区对比 |
| | `diff data` | 数据对比 |
| **cache** | `cache build` | 构建元数据缓存 |
| | `cache status` | 缓存状态 |
| | `cache clear` | 清除缓存 |
| **agent** | `agent context` | Agent 上下文信息 |
| | `agent skill` | 查看 Skill 信息 |
| | `agent install-skill` | 安装 Skill 到 Agent 平台 |

所有命令支持 `--json` 输出结构化 JSON 响应。

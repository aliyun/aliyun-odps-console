# maxc-cli 安装指南

`maxc-cli` 是 MaxCompute 的结构化 CLI 工具，专为 AI Agent 调用设计。
公共云 Skill 名为 `alibabacloud-maxcompute-cli`，首选命令入口是
`aliyun maxc`。

---

## 公共云：Alibaba Cloud CLI（推荐）

`aliyun maxc` 需要 Alibaba Cloud CLI 3.3.19 或更高版本：

```bash
aliyun version
```

版本不足时，在用户确认后按现有安装方式更新。支持自升级的非 Homebrew
Alibaba Cloud CLI 3.3.5+ 可运行 `aliyun upgrade`；更早版本、Homebrew 安装
或缺失 CLI 按官方安装方式处理。

使用 MaxCompute 自带的 OAuth 登录流程。它是公共云交互式场景的首选，
避免把长期 AK/SK 放进命令行或聊天记录：

```bash
aliyun maxc auth login --oauth --json
aliyun maxc agent doctor --online --json
```

OAuth 需要当前账号/组织已安装并分配官方 `official-cli` OAuth 应用；未满足时
按返回的结构化错误联系 RAM 管理员，不要直接降级为在聊天中传递 AK/SK。

OAuth 使用 Authorization Code + PKCE，并在 CLI 所在主机的 `127.0.0.1`
监听回调。`--no-browser` 只会打印登录 URL、避免自动打开浏览器，仍需要同一
loopback 回调；它不是 device-code/headless flow。CLI 运行在 SSH 主机时，
先配置端口转发，或使用该主机上的浏览器完成授权。

如果已经配置了可用的 Alibaba Cloud CLI OAuth profile、STS 或运行时凭证，
先运行 `aliyun maxc auth whoami --json`；不要为了安装 Skill 覆盖现有身份。

---

## 独立 Python 入口

仅在明确需要 PyPI 发行版或 `aliyun maxc` 不可用时使用。要求 Python 3.9
或更高版本：

```bash
python3 --version
python3 -m pip install --upgrade maxc-cli
maxc auth login --oauth --json
maxc agent doctor --online --json
```

CI 或托管运行时可以使用已经注入的环境变量、STS 或
`auth login-external`。只有用户或运行环境明确要求时才使用直接 AK/SK。
外部 credential helper 只能来自可信用户级配置或用户显式选择的 `--config`；
自动发现的 workspace 配置不得定义 `auth`。helper 以 executable + argv
执行，不经过 shell，因此不支持管道、重定向或命令替换。

集团弹内使用既有短期凭证流程时，可继续使用 NCS 安装脚本：

```bash
curl -fsSL https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli/bootstrap-ncs.sh | bash
```

---

## 安装 Agent Skill

从公共 Skill 仓库安装 `alibabacloud-maxcompute-cli`，或者使用当前 CLI
包内的同源 Skill：

```bash
# 公共云入口
aliyun maxc agent skill install codex --invocation aliyun-maxc --json

# 独立入口
maxc agent skill install codex --invocation maxc --json
```

平台列表及默认安装目录以 `agent skill install --help` 为准。安装目录名
统一为 `alibabacloud-maxcompute-cli`。
更新时不传 `--invocation` 会保留各安装目录记录的入口；即使使用
`agent skill update --all`，也不会把不同平台统一改成当前调用入口。

## 验证

Agent 会话先生成并复用一个 32 位小写十六进制 session ID：

```bash
UA="AlibabaCloud-Agent-Skills/alibabacloud-maxcompute-cli/<session-id>"
aliyun maxc agent context --json
aliyun maxc agent manifest --json
aliyun maxc agent doctor --online --user-agent "$UA" --json
```

- `agent context` 仅检查本地版本和配置，不访问网络。
- `agent manifest` 从当前运行版本的 parser 生成命令、参数和副作用清单。
- `agent doctor --online` 才是远端认证与可达性的就绪门禁。

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
| | `data upload` | 上传 CSV/TSV；支持 `--dry-run` 预检；仅显式 `--create-partition` 创建缺失分区 |
| | `data download` | 下载 CSV/TSV；默认拒绝覆盖已有文件，显式 `--overwrite` 才替换 |
| **auth** | `auth login --oauth` | OAuth 认证（公共云交互式首选） |
| | `auth login-external` | 外部凭证认证（ncs 等） |
| | `auth whoami` | 查看当前身份 |
| | `auth can-i` | 检查权限 |
| **session** | `session set` | 切换项目/Schema |
| | `session show` | 查看当前 session |
| | `session unset` | 清除 session |
| **cache** | `cache build` | 构建元数据缓存 |
| | `cache build-status` | 查看异步构建状态 |
| | `cache status` | 缓存状态 |
| | `cache clear` | 清除缓存 |
| **agent** | `agent context` | 本地上下文信息（不访问网络） |
| | `agent doctor [--online]` | 本地/在线就绪检查 |
| | `agent manifest` | 实时命令契约与副作用清单 |
| | `agent skill` | 查看 Skill 信息 |
| | `agent skill install/update/uninstall/list/diff/path` | 管理 Agent 平台中的 Skill |

普通命令支持 `--json` 输出 Envelope v2.0。CSV/NDJSON 行流和
`job wait --stream` 生命周期流不为每条记录封装 Envelope。

远程查询不接受自动 retry flags。收到 `metadata.job_id` 后，应先用 `job status`
或 `job diagnose` 检查原任务，再决定是否重新提交。普通上传不会创建缺失分区；
显式 `--create-partition` 是元数据副作用，后续失败时可能留下空分区。

# maxc-cli：用 AI Agent 的方式查询 MaxCompute

> 不是 Agent，而是给 Agent 调用的结构化工具层。

## 痛点

作为数据开发/分析师，你是不是经常这样：

1. **记不住表名** — 去 DataWorks 翻文档、问同事、搜 Wiki
2. **记不住字段** — `SELECT *` 然后等 5 分钟跑出来看字段名
3. **记不住分区** — 跑了一个全表扫描，被运维找到
4. **每次都要认证** — 配置 AK/SK、endpoint、project，折腾半小时
5. **IDE 里无法直接查数据** — 切换到浏览器 → DataWorks → 写 SQL → 等结果 → 复制回来

## 解决方案

`maxc-cli` 是为 AI Agent 设计的 MaxCompute CLI 工具，让你在 IDE 里直接用自然语言查数据。

### 核心特性

- **结构化输出** — 所有命令返回 JSON，Agent 可直接解析
- **Agent 优先** — 内置 SKILL 支持，Claude Code、Cursor、Qwen Code 等开箱即用
- **安全** — 弹内支持 ncs 免 AK 方案，无需管理 AccessKey
- **元数据缓存** — 本地缓存表结构，Agent 自动发现表和字段
- **成本可控** — 查询前自动估算成本，超阈值自动拦截

## 一键安装

### 弹内环境

```bash
curl -fsSL <oss-url>/bootstrap-ncs.sh | bash
```

3 分钟完成 ncs + maxc-cli 安装 + 认证 + Skill 安装。

### 公共云环境

```bash
curl -fsSL <oss-url>/bootstrap.sh | bash
```

输入 AK/SK 即可开始使用。

## 使用示例

安装完成后，在你的 IDE 中直接对话：

### 查找表

> "帮我找一下跟用户行为相关的表"

```
maxc meta search "user behavior" --json
```

### 查看表结构

> "california_schools.frpm 这张表的字段有哪些？"

```
maxc meta describe california_schools.frpm --json
```

### 执行查询

> "查一下 frpm 表里 Charter 学校有多少条记录"

```
maxc query "SELECT COUNT(*) FROM california_schools.frpm WHERE Charter = 'Y' AND ds = '20260415'" --json
```

### 查看分区

> "这张表有哪些可用分区？"

```
maxc meta partitions california_schools.frpm --json
```

## 支持的 AI Agent 平台

| 平台 | 支持状态 |
|------|----------|
| Claude Code | ✅ |
| Cursor | ✅ |
| Qwen Code | ✅ |
| Windsurf | ✅ |
| Codex | ✅ |

安装对应 SKILL 后，直接用自然语言对话即可查询 MaxCompute 数据。

## 为什么选择 maxc-cli？

| | DataWorks | maxc-cli + Agent |
|---|---|---|
| **入口** | 浏览器 | IDE / 终端 |
| **交互** | 点菜单、填表单 | 自然语言对话 |
| **查询等待** | 页面等待，切出去就没了 | 后台执行，结果返回可复用 |
| **表发现** | 手动搜索 | "帮我找 xxx 相关的表" |
| **认证** | 每次打开要登录 | 一次配置，持久有效 |
| **安全** | 需管理 AK/SK | 弹内 ncs 免 AK |

## 快速开始

```bash
# 安装
curl -fsSL <oss-url>/bootstrap.sh | bash   # 公共云
curl -fsSL <oss-url>/bootstrap-ncs.sh | bash  # 弹内

# 验证
maxc auth whoami --json

# 查询
maxc query "SELECT 1" --json

# 在 IDE 中安装 Skill
maxc agent skill install cursor --json
```

## 更多信息

- 📖 [安装文档](./docs/install-guide.md)
- 📋 [AGENTS.md](./AGENTS.md)
- 🐛 问题反馈：提交 Issue

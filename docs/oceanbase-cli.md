# OceanBase seekdb-cli：专为 AI Agent 设计的数据库接口

> 2026-03-20 · 产品特性 · [所属专题：OceanBase seekdb 技术解析与开发、运维实践 24 篇博文](https://open.oceanbase.com/blog/topics/23956633456)

OpenClaw 作者 Peter Steinberger 在 2026 年 2 月与 Lex Fridman 的访谈中提到，相比 MCP，CLI 或许是 AI 连接外部世界更直接的方式。

CLI 的天然优势是可组合性和更低的试错成本，可以大幅减少 Agent 的 token 消耗。数据库作为 AI Agent 最常需要操作的外部系统之一，正是这一理念的绝佳应用场景。

为此，我们专门设计了 seekdb-cli：它不仅仅可以执行 SQL，还提供 schema 查看、数据画像、表间关系推断等命令，让 [AI](https://www.oceanbase.com/solution/ai) 在动手写 SQL 之前就能充分理解[数据库](https://www.oceanbase.com/topic/techwiki-database)结构。文末准备了完整的实战场景和命令速查表，帮你快速上手。

## 为什么是 CLI 而不是 MCP？

为什么 CLI 会更高效？有两个核心理由：

- **可组合性（Composability）**：CLI 支持通过 jq 等工具灵活过滤和组合输出，甚至可以写成脚本做进一步计算，只把 AI 真正需要的信息返回给模型。
- **试错成本（Cost of Trial-and-Error）**：MCP 每个操作都是一次独立的工具调用，AI 出错后只能靠多轮调用来补救；CLI 可以在一次调用中返回错误和修复线索，大幅减少 Agent 的试错步骤和 token 消耗。

举个例子：AI Agent 要「查最近一周下单金额 Top 5 的用户姓名」，中间写错了一次列名。

- **MCP 方式**：`list_tables` 拿全量 schema → 写错 SQL → `execute_query` 报错 → `describe_table` 补列名 → 再查另一张表结构 → 终于写对。**5 次调用，约 2000 tokens** 灌进上下文。
- **CLI 方式**：写错 SQL → CLI 报错并自动附上正确列名 → 一次改对，`| jq '.rows'` 只留数据。**2 次调用，约 200 tokens**。

同一个任务，上下文消耗从 2000 降到 200，步骤从 5 降到 2。在实际的多轮 Agent 工作流中，这类子任务会连续出现十几次——MCP 方式几轮就撑满上下文窗口；CLI 方式同一窗口内能完成的任务量多出一个数量级。

AI 概念大师 Andrej Karpathy 的观点更进一步：未来的产品都应该设计一套 For AI 的 Portal，包含三个要素：**稳定的 CLI + 清晰的权限体系 + AI 可读的文档**。

CLI 的 `--help` 就是完美的工具说明书（Prompt）。

落到具体场景，数据库是 AI Agent 最常需要操作的外部系统之一。但现有的数据库 CLI 工具（比如 `mysql` 命令行）能做的只是执行 SQL、返回一张 ASCII 表格。对 AI 来说，这远远不够：它不知道库里有哪些表、每个字段是什么含义、数据大致分布如何、哪些列之间存在关联——而这些信息恰恰是 AI 写出正确 SQL 的前提。

OceanBase seekdb 作为一款 AI 原生数据库，也针对上述场景设计了 OceanBase seekdb-cli：除了执行 SQL，它还提供 schema 查看、数据画像、表间关系推断等命令，让 AI 在动手写 SQL 之前就能充分理解数据库结构。

> **OceanBase seekdb 是什么**
>
> OceanBase seekdb 是一个支持向量搜索、全文检索、混合搜索和数据库内 AI 推理的 AI 原生数据库。如果你熟悉 MySQL，你会发现 OceanBase seekdb 的 SQL 用法几乎完全一致，同时它还额外提供了向量和 AI 能力。
>
> OceanBase seekdb-cli 同样适用于 OceanBase 的 MySQL 模式和 MySQL 数据库。

## OceanBase seekdb-cli：一个为 AI Agent 设计的数据库命令行客户端

### 为什么需要 OceanBase seekdb-cli

OceanBase seekdb-cli 做了一系列专门面向 AI Agent 的设计：

### 核心特性详解

#### 1. JSON 默认输出 — 让 AI 读得懂

所有命令默认输出结构化 JSON，AI Agent 可以直接解析。以列出数据库中所有表为例：

```bash
seekdb schema tables
```

```json
{
  "ok": true,
  "data": [
    {"name": "orders", "columns": 4, "rows": 3}
  ]
}
```

当然，也可以用 `--format table` 切换到表格视图：

```bash
seekdb --format table schema tables
```

#### 2. 安全护栏 — 让 AI 不闯祸

让 AI 操作数据库，最大的顾虑就是安全。OceanBase seekdb-cli 内置了四重防护：

**行数保护**：没有 `LIMIT` 的查询，如果结果超过 100 行，会被自动拦截：

```json
{"ok": false, "error": {"code": "LIMIT_REQUIRED", "message": "Query returns more than 100 rows. Please add LIMIT to your SQL."}}
```

**写保护**：写操作默认禁止，必须显式加 `--write` 才能执行。即使加了 `--write`，没有 `WHERE` 的 `DELETE`/`UPDATE` 以及 `DROP`/`TRUNCATE` 也会被拦截：

```bash
# 读操作，直接执行
seekdb sql "SELECT * FROM orders LIMIT 10"

# 写操作，必须加 --write
seekdb sql --write "INSERT INTO orders (user_id, amount, status, phone) VALUES (4, 199.00, 'paid', '13700000001')"

# 危险操作，即使加了 --write 也会被拦截
seekdb sql --write "DELETE FROM orders"  # ← 没有 WHERE，被拦截！
```

**敏感字段脱敏**：列名匹配 `phone`、`email`、`password`、`id_card` 等模式时，值会被自动脱敏。查询 `orders` 表时，`phone` 字段会自动处理：

```bash
seekdb sql "SELECT * FROM orders LIMIT 3"
```

```json
{"ok": true, "columns": ["id", "user_id", "amount", "status", "phone"], "rows": [
  {"id": 1, "user_id": 1, "amount": "99.00", "status": "paid", "phone": "138****5678"},
  {"id": 2, "user_id": 2, "amount": "256.50", "status": "paid", "phone": "139****4321"},
  {"id": 3, "user_id": 1, "amount": "38.00", "status": "pending", "phone": "136****1234"}
], "time_ms": 9}
```

**操作日志**：所有命令记录到 `~/.seekdb/sql-history.jsonl`，SQL 中的敏感字面量会被自动脱敏。

#### 3. 错误自纠正 — 让 AI 自己修 Bug

这是前文"试错成本"差距的关键来源。传统工具（包括 MCP）在报错时只返回错误信息本身，AI 要修复就得额外调一次"查看表结构"——每多一次调用，就多一轮 LLM 推理、多消耗一段上下文。

OceanBase seekdb-cli 的设计原则是：**错误响应必须自带修复线索，让 Agent 下一步就能改对，而不是再绕一圈。**

具体来说，当 SQL 报错时，CLI 会自动附带修复所需的上下文：

**列名写错？** 返回该表的全部列名：

```bash
seekdb sql "SELECT price FROM orders LIMIT 5"
```

```json
{"ok": false, "error": {"code": "SQL_ERROR", "message": "execute sql failed 1054 Unknown column 'price' in 'field list'"}, "schema": {"table": "orders", "columns": ["id", "user_id", "amount", "status", "phone"], "indexes": ["PRIMARY(id)"]}}
```

**表名写错？** 返回所有可用的表名：

```bash
seekdb sql "SELECT * FROM ordes LIMIT 5"
```

```json
{"ok": false, "error": {"code": "SQL_ERROR", "message": "execute sql failed 1146 Table 'test.ordes' doesn't exist"}, "schema": {"tables": ["orders"]}}
```

AI Agent 看到这样的错误，能够立刻自我纠正，根据返回的 schema 信息重写 SQL，而不需要额外调用一次"查看表结构"的命令。这大大减少了 AI 完成任务的步骤数。

#### 4. ai-guide — CLI 的自我说明书

运行 `seekdb ai-guide`，CLI 会输出一份完整的 JSON 格式使用指南，包括所有命令、参数、推荐工作流和安全规则。AI Agent 只需执行一次这个命令，就能"学会"整个工具的用法：

```bash
seekdb ai-guide
```

这正是 Karpathy 说的 "AI 可读的文档"— 不是给人看的 README，而是给 AI 解析的结构化指南。

## 在 OpenClaw 中使用 OceanBase seekdb-cli 的实战场景

OpenClaw 是目前最流行的开源 AI Agent 框架，为了验证 OceanBase seekdb-cli 在 AI Agent 中的价值，下面展示几个在 OpenClaw 中使用 OceanBase seekdb-cli 的真实场景。你只需要用自然语言告诉 OpenClaw 你想做什么，它会自动调用 OceanBase seekdb-cli 来完成。

### 安装与连接

安装只需一行：

```bash
pip install seekdb-cli
```

安装后即可使用 `seekdb` 命令。不配置任何连接信息时，默认使用 `~/.seekdb/seekdb.db` 嵌入式数据库——装完就能用，零配置（嵌入式模式目前仅支持 Linux 或 macOS 15 及以上，Windows 暂不支持）。

需要连接远程数据库或指定其他数据库路径时，可以通过全局配置文件或命令行参数设置：

```bash
mkdir -p ~/.seekdb

# 连接远程 seekdb / OceanBase / MySQL
echo 'SEEKDB_DSN="seekdb://user:password@host:port/database"' > ~/.seekdb/config.env

# 或使用嵌入式模式（指定本地数据库路径）
echo 'SEEKDB_DSN="embedded:/path/to/mydata"' > ~/.seekdb/config.env
```

也支持 `--dsn` 命令行参数、`SEEKDB_DSN` 环境变量、项目根目录 `.env` 文件等方式，优先级依次递减。

### 验证连接

```bash
seekdb status
```

输出：

```json
{
  "ok": true,
  "data": {
    "cli_version": "0.1.0",
    "mode": "embedded",
    "server_version": "...",
    "database": "test",
    "connected": true
  }
}
```

### 快速体验

建一张表，插几条数据，查一下：

```bash
seekdb sql --write "CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, user_id INT, amount DECIMAL(10,2), status VARCHAR(20), phone VARCHAR(20))"
seekdb sql --write "INSERT INTO orders (user_id, amount, status, phone) VALUES (1, 99.00, 'paid', '13812345678'), (2, 256.50, 'paid', '13987654321'), (1, 38.00, 'pending', '13612341234')"
seekdb sql "SELECT * FROM orders LIMIT 10"
```

输出默认是 JSON，AI 可以直接解析。加上 `--format table` 就是人类友好的表格视图：

```bash
seekdb --format table sql "SELECT * FROM orders LIMIT 10"
```

下面展示几个在 OpenClaw 中使用 OceanBase seekdb-cli 的真实场景。你只需要用自然语言告诉 OpenClaw 你想做什么，它会自动调用 OceanBase seekdb-cli 来完成。

### 准备测试数据

在 Linux 或 macOS 15 及以上系统，OceanBase seekdb-cli 默认使用 `~/.seekdb/seekdb.db` 作为嵌入式数据库，无需任何配置，直接写入数据即可：

```bash
# 验证连接（自动使用默认数据库 ~/.seekdb/seekdb.db）
seekdb status
```

如果你想使用其他路径的数据库目录，可以通过全局配置指定：

```bash
mkdir -p ~/.seekdb
echo 'SEEKDB_DSN="embedded:/path/to/your/data/directory"' > ~/.seekdb/config.env
```

下面的示例直接使用默认数据库。

**建表：用户、商品、订单三张表**

```bash
# 用户表
seekdb sql --write "
CREATE TABLE users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(50),
  email VARCHAR(100),
  phone VARCHAR(20),
  city VARCHAR(20),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)"

# 商品表
seekdb sql --write "
CREATE TABLE products (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100),
  category VARCHAR(50),
  price DECIMAL(10,2),
  stock INT
)"

# 订单表（关联用户和商品）
seekdb sql --write "
CREATE TABLE orders (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT,
  product_id INT,
  quantity INT,
  amount DECIMAL(10,2),
  status VARCHAR(20),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)"
```

**插入测试数据**

```bash
# 用户数据（10 个用户，来自不同城市）
seekdb sql --write "
INSERT INTO users (name, email, phone, city) VALUES
  ('张三', 'zhang@example.com', '13812345678', '北京'),
  ('李四', 'li@example.com', '13987654321', '上海'),
  ('王五', 'wang@example.com', '13600000001', '广州'),
  ('赵六', 'zhao@example.com', '13700000002', '深圳'),
  ('陈七', 'chen@example.com', '13800000003', '北京'),
  ('周八', 'zhou@example.com', '13900000004', '上海'),
  ('吴九', 'wu@example.com', '13600000005', '成都'),
  ('郑十', 'zheng@example.com', '13700000006', '杭州'),
  ('冯十一', 'feng@example.com', '13800000007', '北京'),
  ('褚十二', 'chu@example.com', '13900000008', '上海')"

# 商品数据（不同品类）
seekdb sql --write "
INSERT INTO products (name, category, price, stock) VALUES
  ('无线蓝牙耳机', '数码', 299.00, 500),
  ('机械键盘', '数码', 599.00, 200),
  ('人体工学椅', '家具', 1299.00, 80),
  ('咖啡豆（500g）', '食品', 89.00, 1000),
  ('运动水壶', '户外', 59.00, 800),
  ('笔记本支架', '数码', 199.00, 300),
  ('降噪耳机', '数码', 999.00, 150),
  ('瑜伽垫', '户外', 149.00, 400)"

# 订单数据（近 30 天，模拟真实分布）
seekdb sql --write "
INSERT INTO orders (user_id, product_id, quantity, amount, status, created_at) VALUES
  (1, 2, 1, 599.00, 'paid', DATE_SUB(NOW(), INTERVAL 1 DAY)),
  (1, 7, 1, 999.00, 'paid', DATE_SUB(NOW(), INTERVAL 3 DAY)),
  (2, 1, 2, 598.00, 'paid', DATE_SUB(NOW(), INTERVAL 2 DAY)),
  (2, 3, 1, 1299.00, 'paid', DATE_SUB(NOW(), INTERVAL 5 DAY)),
  (3, 4, 3, 267.00, 'paid', DATE_SUB(NOW(), INTERVAL 1 DAY)),
  (3, 5, 2, 118.00, 'pending', DATE_SUB(NOW(), INTERVAL 6 DAY)),
  (4, 6, 1, 199.00, 'paid', DATE_SUB(NOW(), INTERVAL 4 DAY)),
  (4, 2, 1, 599.00, 'paid', DATE_SUB(NOW(), INTERVAL 2 DAY)),
  (5, 1, 1, 299.00, 'paid', DATE_SUB(NOW(), INTERVAL 3 DAY)),
  (5, 8, 2, 298.00, 'refund', DATE_SUB(NOW(), INTERVAL 7 DAY)),
  (6, 7, 1, 999.00, 'paid', DATE_SUB(NOW(), INTERVAL 1 DAY)),
  (7, 3, 1, 1299.00, 'paid', DATE_SUB(NOW(), INTERVAL 4 DAY)),
  (8, 4, 5, 445.00, 'paid', DATE_SUB(NOW(), INTERVAL 2 DAY)),
  (9, 2, 2, 1198.00, 'paid', DATE_SUB(NOW(), INTERVAL 6 DAY)),
  (10, 1, 1, 299.00, 'pending', DATE_SUB(NOW(), INTERVAL 1 DAY)),
  (1, 5, 3, 177.00, 'paid', DATE_SUB(NOW(), INTERVAL 15 DAY)),
  (2, 8, 1, 149.00, 'paid', DATE_SUB(NOW(), INTERVAL 20 DAY)),
  (3, 6, 2, 398.00, 'paid', DATE_SUB(NOW(), INTERVAL 25 DAY))"
```

**准备[向量](https://www.oceanbase.com/solution/ai)文档数据（场景三会用到）**

```bash
# 准备文档文件
cat > docs.jsonl << 'EOF'
{"id": "doc1", "text": "seekdb 部署指南：支持 Docker 一键部署，也可以通过二进制包手动安装。推荐使用 Docker Compose 部署生产环境。"}
{"id": "doc2", "text": "seekdb 向量索引原理：使用 HNSW 算法构建向量索引，支持余弦相似度、欧氏距离等多种距离度量。"}
{"id": "doc3", "text": "seekdb 混合搜索：将语义向量搜索和 BM25 全文检索的结果通过 RRF 算法融合，在召回率和精度上优于单一搜索方式。"}
{"id": "doc4", "text": "seekdb 性能调优：合理设置 HNSW 的 ef_construction 参数可以在索引速度和搜索精度之间取得平衡。"}
{"id": "doc5", "text": "seekdb 数据备份：支持逻辑备份和物理备份两种方式，生产环境建议每天全量备份并配合 binlog 实现 PITR。"}
EOF

# 创建向量集合
seekdb collection create product_docs --dimension 384 --distance cosine

# 导入文档（seekdb 自动调用内置嵌入模型生成向量）
seekdb add product_docs --file docs.jsonl
```

数据准备完毕。接下来在 OpenClaw 中安装 OceanBase seekdb-cli 技能，让 OpenClaw 能按技能描述使用 OceanBase seekdb-cli。

### OceanBase seekdb-cli 的 Agent Skill

除了直接在终端使用，OceanBase seekdb-cli 更主要地是配合 AI Agent 使用。OceanBase seekdb-cli 提供了配套的 Agent Skill（AI 技能包）。安装 OceanBase seekdb-cli 技能后，OpenClaw 能自动感知到 OceanBase seekdb-cli 的存在和用法，不需要你提前教它。

1. **安装 OceanBase seekdb-agent-skills python 安装包**

```bash
pip install seekdb-agent-skills
```

2. **运行 OceanBase seekdb-agent-skills 交互式安装器**

```bash
seekdb-agent-skills
```

交互式安装器会引导你选择 AI 工具（OpenClaw、Cursor、Claude Code 等）和要安装的技能。选择你使用的 AI 工具，然后选择 OceanBase seekdb-cli 技能，安装器会自动将技能文件部署到对应工具的技能目录中。

3. 选择 OpenClaw

4. 选择 seekdb-cli

安装完成后，当你用自然语言向 AI Agent 提出数据库相关的需求时（比如「帮我看看数据库里有什么表」），Agent 会自动加载 OceanBase seekdb-cli 技能，调用相应的命令来完成任务。

打开 OpenClaw，直接用自然语言和它对话，下面进入实战场景。

### 场景一：探索一个陌生数据库

你说：「使用 seekdb-cli 帮我看看 OceanBase seekdb 数据库里有什么表，分析一下数据分布，表之间有什么关联」

OpenClaw 会执行：

```bash
# 第一步：看有哪些表
seekdb schema tables
```

```json
{"ok": true, "data": [
  {"name": "users", "columns": 6, "rows": 10},
  {"name": "products", "columns": 5, "rows": 8},
  {"name": "orders", "columns": 7, "rows": 18}
]}
```

```bash
# 第二步：看 orders 表的详细结构
seekdb schema describe orders
```

```json
{"ok": true, "data": {"table": "orders", "columns": [
  {"name": "id", "type": "INT", "nullable": false, "key": "PRI"},
  {"name": "user_id", "type": "INT", "nullable": true},
  {"name": "product_id", "type": "INT", "nullable": true},
  {"name": "quantity", "type": "INT", "nullable": true},
  {"name": "amount", "type": "DECIMAL(10,2)","nullable": true},
  {"name": "status", "type": "VARCHAR(20)", "nullable": true},
  {"name": "created_at", "type": "DATETIME", "nullable": true}
]}}
```

```bash
# 第三步：分析订单数据的分布画像
seekdb table profile orders
```

```json
{"ok": true, "data": {"table": "orders", "row_count": 18,
  "columns": [
    {"name": "status", "null_rate": 0, "distinct": 3,
     "top_values": [{"value": "paid", "count": 14}, {"value": "pending", "count": 2}, {"value": "refund", "count": 1}]},
    {"name": "amount", "null_rate": 0, "min": 118.00, "max": 1299.00, "avg": 513.17}
  ]
}}
```

```bash
# 第四步：推断表之间的关联关系
seekdb relations infer
```

```json
{"ok": true, "data": {"relations": [
  {"from": "orders.user_id", "to": "users.id", "confidence": 0.95},
  {"from": "orders.product_id", "to": "products.id", "confidence": 0.92}
]}}
```

OpenClaw 解析后会给你一份清晰的数据库概览，包括表结构、数据分布和表间关联。

### 场景二：用自然语言查数据

你说：「查一下最近一周下单金额最高的 5 个用户，帮我列出他们的名字和城市」

OpenClaw 会执行（分两步）：

```bash
# 第一步：先查 Top 5 用户 ID 和金额
seekdb sql "
SELECT user_id, SUM(amount) AS total
FROM orders
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
  AND status = 'paid'
GROUP BY user_id
ORDER BY total DESC
LIMIT 5"
```

```json
{"ok": true, "columns": ["user_id", "total"], "rows": [
  {"user_id": 2, "total": "1897.00"},
  {"user_id": 9, "total": "1198.00"},
  {"user_id": 7, "total": "1299.00"},
  {"user_id": 6, "total": "999.00"},
  {"user_id": 1, "total": "598.00"}
], "time_ms": 12}
```

```bash
# 第二步：关联 users 表获取姓名和城市
seekdb sql "
SELECT u.name, u.city, SUM(o.amount) AS total
FROM orders o JOIN users u ON o.user_id = u.id
WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
  AND o.status = 'paid'
GROUP BY u.id, u.name, u.city
ORDER BY total DESC
LIMIT 5"
```

```json
{
  "ok": true,"columns": ["name", "city", "total"],"rows": [
    {"name": "李四", "city": "上海", "total": "1897.00"},
    {"name": "张三", "city": "北京", "total": "1598.00"},
    {"name": "吴九", "city": "成都", "total": "1299.00"},
    {"name": "冯十一", "city": "北京", "total": "1198.00"},
    {"name": "周八", "city": "上海", "total": "999.00"}
  ],
  "affected": 0,
  "time_ms": 0
}
```

如果 SQL 写错了（比如 `user_id` 写成了 `uid`），OceanBase seekdb-cli 会返回带 schema 的错误信息，OpenClaw 会自动修正并重试—整个过程你甚至不会感知到。

### 场景三：搜索向量集合

你说：「在 product_docs 集合里搜索关于'部署'的内容」

OpenClaw 会执行：

```bash
# 第一步：确认集合状态
seekdb collection info product_docs
```

```json
{"ok": true, "data": {"name": "product_docs", "count": 5, "dimension": 384, "distance": "cosine"}}
```

```bash
# 第二步：语义搜索
seekdb query product_docs --text "部署指南" --limit 5
```

```json
{
  "ok": true,"data": {"results": [
    {
      "id": "doc1",
      "score": 0.6445,
      "document": "seekdb 部署指南：支持 Docker 一键部署...",
      "metadata": {}
    },
    {
      "id": "doc2",
      "score": 0.3827,
      "document": "seekdb 向量索引原理：使用 HNSW 算法构建向量索引...",
      "metadata": {}
    },
    {
      "id": "doc5",
      "score": 0.3103,
      "document": "seekdb 数据备份：支持逻辑备份和物理备份两种方式...",
      "metadata": {}
    },
    {
      "id": "doc3",
      "score": 0.2157,
      "document": "seekdb 混合搜索：将语义向量搜索和 BM25 全文检索的结果...",
      "metadata": {}
    },
    {
      "id": "doc4",
      "score": 0.2089,
      "document": "seekdb 性能调优：合理设置 HNSW 的 ef_construction 参数...",
      "metadata": {}
    }
  ],
  "count": 5
  },
  "time_ms": 4259
}
```

`--mode hybrid` 把语义向量搜索和 BM25 全文检索的结果融合，召回率比单一方式更高。

### 场景四：通过管道组合

OceanBase seekdb-cli 天然支持 Unix 管道，可以和其他工具自由组合：

```bash
# 用 jq 从查询结果中筛选出上海用户
seekdb sql "SELECT * FROM users LIMIT 100" | jq '.rows[] | select(.city == "上海") | {name, email}'

# 统计各城市的付款订单总金额，输出 CSV 格式
seekdb sql "SELECT u.city, SUM(o.amount) as gmv FROM orders o JOIN users u ON o.user_id=u.id WHERE o.status='paid' GROUP BY u.city ORDER BY gmv DESC LIMIT 10" \
  | jq -r '["city","gmv"], (.rows[] | [.city, .gmv]) | @csv'

# 将数据导入集合
cat docs.jsonl | seekdb add product_docs --stdin
```

这就是 Peter Steinberger 说的可组合性—每个 CLI 只做一件事，通过管道灵活拼接，远比一个"大而全"的 API 更灵活。

## 完整命令速查

| 命令 | 说明 |
|------|------|
| `seekdb status` | 查看连接状态 |
| `seekdb schema tables` | 列出所有表 |
| `seekdb schema describe <table>` | 查看表结构 |
| `seekdb table profile <table>` | 数据画像 |
| `seekdb relations infer` | 推断表间关系 |
| `seekdb sql "<sql>"` | 执行 SQL（只读） |
| `seekdb sql --write "<sql>"` | 执行 SQL（写入） |
| `seekdb ai-guide` | 输出 AI 使用指南 |
| `seekdb collection create <name>` | 创建向量集合 |
| `seekdb collection info <name>` | 查看集合信息 |
| `seekdb add <name> --file <file>` | 导入数据到集合 |
| `seekdb query <name> --text "<query>"` | 语义搜索 |

## 总结

回到文章开头的问题：AI 连接世界的最佳接口是什么？

OceanBase seekdb-cli 用实践给出了回答：

- **JSON 默认输出**，让 AI 不用猜格式
- **安全护栏内置**，让 AI 不会误操作
- **错误自带修复线索**，让 AI 能自我纠正
- **ai-guide 自描述**，让 AI 一条命令学会全部用法
- **无状态调用 + 管道组合**，让 AI 像搭乐高一样编排工作流

这不只是一个数据库 CLI 工具，更是一个面向 AI Agent 时代的数据库交互范式。

安装试试吧：

```bash
pip install seekdb-cli
```

项目地址：[OceanBase seekdb-cli](https://github.com/oceanbase/seekdb-ecology-plugins/tree/main/seekdb-cli)


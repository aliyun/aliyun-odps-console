# 语义包试用版使用指南

maxc 管理远端语义包的 Draft、Published 版本，以及差异审阅、写入和发布。dbt 语义转换由 dbt-maxcompute 提供。

## 安装与连接

```shell
python -m pip install 'maxc-cli==0.9.5b1'
maxc --version
```

如果需要从 dbt 导出语义定义，建议 Python 3.12，使用配套版本：

```shell
python -m pip install 'dbt-core==1.12.6' 'dbt-maxcompute[semantic]==1.12.1b1' 'maxc-cli==0.9.5b1'
```

试用版通过独立的 Python 入口 maxc 使用。`aliyun maxc` 的内置运行时需要等待对应分发版本更新。已有连接配置时先验证身份；尚未配置时使用 `maxc auth login --oauth --json` 登录。

```shell
maxc agent context --json
maxc agent manifest --json
maxc agent doctor --online --json
maxc auth whoami --json
```

在线检查的 data.ready 应为 true。Agent 调用云接口时需追加 Skill 规定的 `--user-agent`；完整规范见仓库 AGENTS.md。

省略 `--namespace` 时，从已认证身份解析主账号 namespace。RAM 主体仅在服务端返回明确 AccountId 或可识别复合身份时自动解析；无法确定则停止，使用独立核实的主账号 ID 显式传入 `--namespace`。项目 tenant ID 不作为账号 ID。

## 从 dbt 生成提案

先按[适配器中文指南](https://github.com/aliyun/dbt-maxcompute/blob/master/docs/semantic-export.zh-CN.md)创建目标包，配置项目并完成数据测试。在 dbt 项目目录执行：

```shell
dbt run-operation dbt_maxcompute.export_semantic_package \
  --args '{name: sales, timezone: UTC, join_type: LEFT}'
```

宏读取当前项目编译产物、实时表结构和当前 USER_DRAFT，将提案写入 `target/maxagent-semantic-proposal.json`。导出操作只生成本地文件。

用于转换的实时结构接口也可独立调用：

```shell
maxc meta describe orders --project my_project --schema default --metadata-only --json
```

`data.table` 包含完整 columns、partition_columns、project、schema_name 和 table_name。该模式不读取样本行、分区值，也不回退到缓存。

## 审阅、写入与发布

```shell
maxc semantic diff sales --file target/maxagent-semantic-proposal.json --json
```

审阅全部 before/after，确认指标过滤条件、聚合方式、时间定义和关联关系。使用本次 diff 返回的 planDigest 写入：

```shell
maxc semantic apply sales --file target/maxagent-semantic-proposal.json \
  --plan-digest '<reviewed_plan_digest>' --yes --json
maxc semantic get sales --json
```

完成业务查询验证后，使用回读的精确 specId 和 Draft revisionId 发布：

```shell
maxc semantic publish sales --expected-spec-id '<verified_spec_id>' \
  --expected-revision '<verified_draft_revision>' --yes --json
maxc semantic get sales --source PUBLISHED --revision '<published_revision>' --json
```

版本冲突时重新读取并审阅；不自动重放写入。`SEMANTIC_WRITE_UNCERTAIN` 表示需先回读核对远端结果。具体参数以 `maxc semantic <command> --help` 为准。

旧 PyODPS 返回裸 Catalog DNS 域名时，CLI 自动补 HTTPS；显式 HTTP(S) 端点保持原值，非法 URL 会被拒绝。RAM 身份解析已有单元测试，本次云端验收使用主账号身份。

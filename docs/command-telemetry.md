# MaxC 命令用量遥测

受管渠道部署后台后默认开启。`maxc` 和 `aliyun maxc` 使用同一套命令 ID，按北京时间（UTC+8）统计调用开始日期。`query` 和 `query run` 归为 `query`，成本和执行计划分别记为 `query.cost` 和 `query.explain`。每个可识别的外部调用计一次，帮助、版本、解析错误和内部自动登录重入跳过。

## 用户开关

在用户全局文件 `~/.maxc/config.yaml` 设置 `telemetry.enabled: false` 可关闭两个入口的采集和上报。后台检测到停用后清理未发数据。宿主的持久开关单独作用于 `aliyun maxc`，宿主禁用环境变量只跳过继承它的相应调用。可参见[阿里云 CLI 遥测设置](https://github.com/aliyun/aliyun-cli/blob/9dfe9ba390eb48c0a3a89f865afc44aa8e48a0d8/docs/zh-CN/telemetry.md#如何管理遥测)。

## 受管部署

由用户级服务管理器或受管运行时保持一个 `maxc telemetry worker` 进程，向它和同一用户的 MaxC 命令会话注入相同的 `MAXC_TELEMETRY_RECEIVER`。选择一个空闲本机 UDP 端口，并生成随机的 32 位小写十六进制校验值。以下命令可用于受管试点。

```sh
export MAXC_TELEMETRY_RECEIVER="43820:$(openssl rand -hex 16)"
maxc telemetry worker --json
```

通过进程管理器将同一个变量传给 CLI 会话。接收进程绑定 `127.0.0.1`。前台尝试一次非阻塞投递后继续执行。本机状态默认位于用户的 `~/.maxc/state/telemetry`，`--state-dir` 可指定它的父目录。后台暂停或缺席的环境会形成统计覆盖缺口。

每轮上报把同一日期的计数汇成批次，然后通过已有 ODPS 签名连接调用 `PUT /projects/{project}/logs`。默认读取用户层配置和启动环境。后台的 `--config` 可指向受管的连接文件，停用设置始终取用户全局配置。OAuth 使用已有的有效 STS。暂时没有连接或接口失败时保留待发批次，在后续周期以相同批次 ID 补传。统计日期允许回补到过去七个自然日。每轮最多三批，默认间隔为 30 分钟加最多 5 分钟的抖动。后台退出时输出累计计数及最后一次 HTTP 请求的状态和回执 ID。

## 生成日报

从有权限的 ODPS Worker 日志数据源导出 MaxC 对应完整的 `SDK LOG` 记录。服务端日志有 `UserAgent` 和 `ProjectName`，可用 `maxc-cli/` 前缀及试点范围筛选。报表输入支持逐行的原始 JSON 载荷，或每行一个含 `"SDK LOG"` 字符串字段的 JSON 对象。

```sh
python3 -m maxc_cli.telemetry_report < maxc-sdk-logs.jsonl > maxc-command-daily.csv
```

CSV 列为 `stat_date`、`command`、`invocation_count`。相同批次 ID 与内容只汇总一次，出现内容冲突的批次从结果中排除。工具在 stderr 输出接收、重复、冲突和拒绝批次数。每次重建日期分区时提供这些日期的全部批次，包含已入仓的迟到记录。这样重跑和补传仍得到一致的结果。

日报表头显示「已上报调用量」、北京时间和覆盖渠道，流水线以读回的 `SDK LOG` 作为数据源。HTTP 200 用于确认客户端批次投递。

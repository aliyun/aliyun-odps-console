# MaxC 命令用量遥测

普通安装默认开启。`maxc` 和 `aliyun maxc` 使用同一套命令 ID，按北京时间（UTC+8）统计调用开始日期。`query` 和 `query run` 归为 `query`，成本和执行计划分别记为 `query.cost` 和 `query.explain`。每个可识别的外部调用计一次，帮助、版本、解析错误和内部自动登录重入跳过。

## 用户开关

在用户全局文件 `~/.maxc/config.yaml` 设置 `telemetry.enabled: false` 可关闭两个入口的采集和上报。命令运行或上传进程检测到停用时清理未发数据。宿主的持久开关单独作用于 `aliyun maxc`，宿主禁用环境变量只跳过继承它的相应调用。 [阿里云 CLI 遥测设置](https://github.com/aliyun/aliyun-cli/blob/9dfe9ba390eb48c0a3a89f865afc44aa8e48a0d8/docs/zh-CN/telemetry.md#如何管理遥测)

## 本地记录与上传

两个入口在 `~/.maxc/state/telemetry` 按入口及调用日期追加标准命令 ID。首次记录后满 30 分钟，下一次命令会触发一次独立的后台上传，此后每 30 分钟至多触发一次。上传进程在一轮尝试后退出，前台无需等网络。空闲期间文件保留在本地，等下一次调用按原日期补传。

每轮最多尝试三个批次，按日期汇总命令计数，通过已有 ODPS 签名连接调用 `PUT /projects/{project}/logs`。连接取自用户全局配置和启动环境。接口成功后清理该批次，暂时没有连接或请求失败时保留原批次 ID。单个追加文件上限 256 KiB，本地上报和日报均支持回补过去七个自然日。

## 生成日报

从有权限的 ODPS Worker 日志数据源导出 MaxC 对应完整的 `SDK LOG` 记录。服务端日志有 `UserAgent` 和 `ProjectName`，可用 `maxc-cli/` 前缀及试点范围筛选。报表输入支持逐行的原始 JSON 载荷，或每行一个含 `"SDK LOG"` 字符串字段的 JSON 对象。

```sh
python3 -m maxc_cli.telemetry_report < maxc-sdk-logs.jsonl > maxc-command-daily.csv
```

CSV 列为 `stat_date`、`command`、`invocation_count`。相同批次 ID 与内容只汇总一次，出现内容冲突的批次从结果中排除。工具在 stderr 输出接收、重复、冲突和拒绝批次数。每次重建日期分区时提供这些日期的全部批次，包含已入仓的迟到记录。这样重跑和补传仍得到一致的结果。

日报表头显示「已上报调用量」、北京时间和覆盖渠道，流水线以读回的 `SDK LOG` 作为数据源。HTTP 200 用于确认客户端批次投递。

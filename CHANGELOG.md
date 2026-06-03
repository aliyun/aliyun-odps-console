# 0.57.0-public [2026-06-03]

Synced from odps-internal-console (0.57.0 ~ 0.57.8).

### Features
- **Credentials 重构**: 使用 `ICredentials`/`Credentials` 统一认证方式，替代原有的 accessId/accessKey/stsToken 分散管理
- **交互式查询回退 (Interactive Auto Fallback)**: 支持 `set interactive_auto_fallback=true/false` 和 `set fallback_quotaname=<name>`，MaxQA 查询失败时自动回退到指定 quota
- **UseQuota 命令增强**: 支持 `--fallback_quotaname` 和 `--interactive_auto_fallback` 参数；使用 `MaxQAConnInfo` 获取连接信息
- **InteractiveQueryCommand 增强**: 新增 `--interactive-mode` 命令行参数，支持在命令行启动时指定交互模式；MaxQA session 自动初始化
- **Tunnel upload 支持 JSON 类型**: `RecordConverter` 新增 JSON 类型的序列化/反序列化支持
- **Tunnel upload record 复用优化**: `TunnelUploadSession` 使用 `newRecord` 复用 Record 对象以减少 GC 压力
- **Desc extended 支持 ViewExpandedText**: 扩展表描述信息
- **Desc extended reserved 字段改为黑名单模式**: 从白名单 (只显示指定字段) 改为黑名单 (显示除排除字段外的所有字段)，展示更完整的表元信息
- **SetProject 属性值去引号**: 自动去除属性值两端的双引号
- **ODPS 连接设置 JobInsightHost**: 自动设置 MaxCompute 作业洞察页面地址
- **UseProject 缓存优化**: 使用 `AllowStaleMetadataRead` 缓存 project/tenant 信息，减少网络请求

### Bug Fixes
- **MERGE 命令正则修复**: 添加 `Pattern.DOTALL` 标志，修复多行 MERGE SQL 无法匹配的问题
- **BlockUploader 上传速度计算修复**: 使用 `double` 替代 `long` 进行除法运算，避免首秒内除零导致速度显示为 0
- **WaitCommand 修复**: 仅在 instance 未终止时设置 mcqaV2 标志，避免已终止实例的异常
- **UseProject quota 清理逻辑**: 命令行指定的 quota 不会在 UseProject 时被清除

### Enhancements
- **MaxQA Session 初始化**: 新增 `SessionUtils.initMaxQASession()` 方法，统一 MaxQA 连接初始化逻辑
- **UseSchemaCommand**: HELP_TAGS 增加 "schemas" 别名
- **UnSetCommand**: HELP_TAGS 可见性改为 public
- **ODPSConsoleConstants**: 新增 `INTERACTIVE_AUTO_FALLBACK`, `FALLBACK_QUOTANAME`, `ODPS_SCHEMA_MODEL_ENABLED` 常量
- **SparkJobUtils**: 移除 log4j 日志抑制代码，使用原生日志行为
- **InteractiveQueryCommand logview 逻辑优化**: 改进 fallback/offline 模式下 logview 的追加逻辑
- **odpscmd 脚本**: 调整 Java 版本检测位置

### Dependency Updates
- 升级 `odps-sdk` 从 `0.56.0-public` 到 `0.57.2-public`
- 升级 `commons-codec` 从 `1.9` 到 `1.13`
- 升级 `commons-compress` (xflow) 从 `1.2` 到 `1.28.0`

# 0.56.0-public [2026-01-30]
### Features
- Added `set region` command to support setting Region ID
- Added new configuration options: `region_id`, `signature_v4_corporation`, `quota_name`, and `LABEL` for configuring Region ID, signature corporation, quota name, and test environment label in the configuration file
- Support temporary quotas (quota names starting with `temp_`) with automatic 1-day cache expiration
- Added `odps.console.http.submit.headers` configuration option to support custom HTTP request headers
- Support subquery LogView generation in interactive query mode

### Enhancements
- In MCQA V2 mode, support asynchronous retrieval of Summary information with a maximum wait time of 30 seconds
- Improved LogView generation logic with support for LogView version number control
- Quota caching mechanism optimization with expiration time checking, default cache duration of 1 day
- Enhanced `use quota` command to support quota switching in MCQA V2 mode, automatically setting related Session variables and HTTP request headers

### Dependency Updates
- Upgraded odps-sdk from `0.53.0-public` to `0.56.0-public`
- Upgraded `credentials-java` from `0.3.12` to `1.0.2`
- Added `arrow-memory-netty` dependency (with exclusions for netty-buffer and netty-common)
- Upgraded `mockito-core` from `1.10.8` to `4.11.0`
- Upgraded `junit` from `4.11` to `4.13.2`, added JUnit 5 support (junit-jupiter-api, junit-jupiter-engine, junit-jupiter-params, junit-vintage-engine)

### Build & Test
- Added Maven profile to support JDK 21 compilation and testing
- Optimized Maven surefire plugin configuration with parallel test support (20 threads, 3 forks)
- Added necessary JVM parameters in JDK 21 mode to support Arrow memory access
- Enhanced startup script `odpscmd` to automatically add necessary JVM parameters for Arrow memory access in JDK 9+ environments

### Other Changes
- Updated copyright year to 2026
- `ExternalCredentialsProvider` implements `getProviderName()` and `close()` methods
- Removed `sts_token` configuration option, now handled internally by the SDK

# 0.48.0-public [2024-07-22]
### Features
- The odps-sdk version has been upgraded from `0.47.0-public` to `0.48.6-public`. For the enhancements and fixes included, please refer to [odps-sdk change log](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.48.x/CHANGELOG.md)
- `desc extended` command adds display of column masking (data desensitization) information
### Fixes
- MCQA mode can more accurately identify fallback behavior and avoid repeated display of logview


# 0.47.1-public [2024-05-23]
### Features
- Add new config params `network_read_timeout` and `network_connect_timeout` to configure network timeout

### Fixes
- Removed tunnel upsert `ow` parameter as it doesn't actually work


# 0.47.0-public [2024-04-10]

### General
- **SDK Version Upgrade**: Updated to `0.47.0-public`.

### Features
- **HTTP Command**: Introduced a new `http` command allowing users to quickly initiate HTTP requests with their current identity.
- **Keep Session Variables**: A new `--keep-session-variables` startup argument has been added. This prevents clearing of user-set flags with the `use [project]` command, maintaining the set flags such as `set a=b` across project switches.

### Enhancements
- **New Data Types in Read Command**: The `read` command now supports `TIMESTAMP_NTZ` and `JSON` data types.

- **Tunnel Command Upgrades**:
- `tunnel upload/download` commands have been enhanced with a `-qn` flag to specify Tunnel QuotaName.
- `tunnel upload` now accepts a `-dfp` flag to set the format for uploading DATETIME type text.

- **HistoryCommand with Grep**: The `history` now features `grep` query functionality for enhanced search capabilities.

- **Project Deletion Syntax**: To align with consistent console syntax, `drop project` is now supported alongside `delete project`.

- **Setproject Command Optimization**: The command has been optimized to support longer value strings, enabling settings for complex types such as JSON.


# 0.43.0
- add vfs -create commmand to create external volume
- add vfs command to public package

# 0.42.0
- add show external tables
- add show views/ materialized views

# 0.40.9
- update odps-sdk version to 0.40.9
- support PMC Task

# 0.40.8
- update odps-sdk version to 0.40.8

# 0.40.0
- fix timestamp value in instance tunnel mode
- add show version command

# 0.39.0
- add lifecycle state to table's extended desc
- add freeze/restore command
- add cold storage status to table's extended desc

# 0.36.0
- support DLF database as external project

# 0.35.2
- support sts account

# 0.35.1
- fix incorrect nano value when downloading a timestamp

# 0.34.2
- add volume 2 commands
- add cupid commands
- support download views

# 0.34.1
- dship command supports overwriting
- support external project

# 0.34.0
- support SQL function

# 0.33.0
- whoami command outputs source ip and vpc id
- handle INT signal properly
- bugfix: multi line input not working on windows

# 0.32.1
- fix unexpected exit when pressing ctrl + c

# 0.32.0
- fix bug in export table resource with partition
- support application authentication
- add welcome message, including project SQL timezone
- add an option to tunnel command to show elapsed time of local/network IO
- session query now supports instance priority
- allow white spaces around column data when upload
- setproject just set additional props instead of all props. (server supported >= s30)

# 0.31.2
- setproject supports multiple input properties
- whoami command now prints SourceIp and VpcId

# 0.31.1
- support PAI instance priority
- make tunnel command a wrapper of dship command

# 0.31.0
- support prefix search in show tables command
- remove fastjson
- tunnel support csv file

# 0.30.6
- wait command supports ODPS hooks
- add download policy to copy command
- support alink xflow

# 0.30.5
- wait command could trigger odps hooks

# 0.30.4
- update fastjson to avoid vulnerabilities

# 0.30.3
- support passing pai priority by command line

# 0.30.2
- add export table/project command

# 0.30.0
- remove datahub/streamjob/topoligy command
- copyright change to 2018
- support java 10+
- add sql session command
- add extended labels

# 0.29.x
- remove antlr
- refine to support async auth query

# 0.28.0
- add auth version when show securityconfiguration
- add create offline model
- fix windows console space in classpath error
- pai support instance priority
- suport java 9
- support volume lifecycle
- add top instance command


# 0.27.4
- remove antlr

# 0.27.x 
- read table support odps.sql.timezone flag
- support tunnel download instance://project/instanceId path ...
- select query flush result from instance tunnel

# 0.26.x 
- mk ArchiveCommand and MergeCommand public
- tunnel support download selected columns using -ci or -cn option
- tunnel support basic hive type
- support script mode (-s)
- read table support other partition type other than string
- public pai command

# 0.25.x
- add odpscmd_admin
- rm html command
- auto prompt to update clt version
- using a state machine to refactor InstanceRunner.waitForCompletion
- change copyright to 2016
- add existences check option for drop function
- support display the table schema with new OdpsType
- support print odps 2.0 sql warnings
- clear basic dependency
- add a new method findUserClassMethods to odps-sdk-commons, cannot run openmr in older service 
- print quota usage infos in long time running instance
- tunnel upload support auto create partition, using -acp arg
- add list xflows command
- split jars into lib and odpslib

# 0.24.0
- simplify sync instance process
- support desc external table
- refactor instance progress output
- optimize latency of waiting for instance completion
- tunnel support multi and mix unicode delimiter
- copy task support tunnel endpoint router

# 0.23.1
- support ~ to present user home in tunnel and resource command

# 0.23.0 
- tunnel download: add -e(-exponential) option to make double in exponential format;
- command support automatic complete; 
- fix the abort-as-a-flash problem on Windows;
- add -cost option to estimate task costs in 'jar' and 'pai' command 
- support desc xflow instance
- 新增onlinemode的create/update/delete/desc/show/updateabtest功能

# 0.22.2
- fallback to use scanner in windows. jline is not reliable on special key such as arrow keys in windows.

# 0.22.0
- support new security command: show role grant user <username>; show principals <rolename>; show package <packagename>;
- add history command;
- trim values parsed from conf;
- add command: show offlinemodels; read offlinemodel; desc offlinemodel;
- hide runtime args;
- better latency in waiting for instance;
- Change Instance`s 'isSuccessful' check condition: an instance is 'isSuccessful' when and only when all its tasks`s state is successful;

# 0.21.2
- xlib: boost xlibclient version to 0.1.10;
- delete xlib model: Percentile,appendID,appendColumns,Declustering，Transform，FillMissingValues，Normalize，Standardize，WeightedSample，RandomSample，Histogram;

# 0.21.1
- update sdk version to 0.21.3

# 0.21.0
- tunnel command support unicode delimiter
- Post Instance Retry 优化

# 0.20.3
- fs -put directory 行为修改, 不再目标 volume 额外创建同名目录
- list offline models
- add xlab command support

# 0.20.2
- tunnel endpoint 可设置

# 0.20.0
- desc table 添加 TableID
- add spark command
- fix openmr command text invalid
- fix -j mode desc table cross project failed
- update commons-io version

# 0.19.0
- wait command 适配pai命令
- 新增 help 命令
- 运行命令中 Ctrl-C 不再直接退出
- 修正 add volumearchive 不支持 -f 参数的问题
- 新增 lib 目录冲突检测
- 新增 unset | unalias 命令
- 使用 AntlrObject.splitCommands 替换 SqlLinesParser.lineParser 进行多条命令的切分
- 新增 matrix 操作
- 新增 datahub 命令
- desc table 增加 max Label
- 新增 update Function 和 desc Function 命令
- job priority 插件更新

# 0.18.0

- resource add support volume archive
- tunnel command change default charset to ignore
- improve schema mismatch err message when uploading

# 0.17.3

- fix -- 不被认为 comment 的问题

# 0.17.2

- fix domain bug

# 0.17.1

- fluent merge in console
- dship perfomance
- cost sql command
- backupinstance progress
- desc instance status
- PS Command update
- PAI 支持 R script

# 0.17.0

- PAI command 支持 temp file
- create function 不支持 对外模式
- list instance 不支持 --start 和 --end --limit
- 增加 desc project
- 增加 Wait 命令
- 合并 dship
- 增加 list 命令，支持 public 风格
- 增加 kill 命令的 -sync 选项 [issue #12](http://gitlab.alibaba-inc.com/odps/odps-internal-console/issues/12)

# 0.16.3

- pai 支持 xflow adapter
- logview 可以设置不显示

# 0.16.0

- 修正 label 显示不正确的问题 [issue #6](http://gitlab.alibaba-inc.com/odps/odps-internal-console/issues/6)
- PAI 命令支持多行
- 增加 runpl 命令，效果等同于 -X

# 0.15.1

- 修正 post instance 失败重试时的时区错误 [bug #ODPS-26819](http://k3.alibaba-inc.com/issue/ODPS-26819)
- 修正 desc table partition 不支持双引号的问题 [SDK bug #ODPS-26787](http://k3.alibaba-inc.com/issue/5753833)

# 0.15.0

- 使用新 sdk 重写了代码，并同步更新了 hook 的实现方式
- 新增 get resource 命令
- 新增 create/drop/list event

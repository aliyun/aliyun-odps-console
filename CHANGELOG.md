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

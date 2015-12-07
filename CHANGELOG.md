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

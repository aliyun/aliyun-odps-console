# 更新日志

## 0.47.0-public [2024-04-10]

### 概览
- **SDK 版本升级**：更新至 `0.47.0-public`。

### 新功能
- **HTTP 命令**：新增 `http` 命令，让用户可以以当前用户身份快速发起HTTP请求。

- **保持会话变量**：新增 `--keep-session-variables` 启动参数。开启后，`use [project]` 命令将不会清除用户已设置的标志，比如 `set a=b`，在不同项目间切换时将保持这些设置。

### 增强功能
- **读取命令支持新类型**：`read` 命令现在支持 `TIMESTAMP_NTZ` 和 `JSON` 数据类型。

- **Tunnel 命令升级**：
    - `tunnel upload/download` 命令新增 `-qn` 标志，用于指定 Tunnel QuotaName。
    - `tunnel upload` 命令新增 `-dfp` 标志，用于设置上传 DATETIME 类型文本的格式。

- **HistoryCommand 支持 Grep**：`HistoryCommand` 现增加了 grep 查询功能，提高搜索能力。

- **项目删除语法**：为了与控制台一致的语法对齐，现在支持使用 `drop project`，与 `delete project` 并行使用。

- **Setproject 命令优化**：该命令已优化，可以支持更长的值字符串，使其能设置复杂类型的值，例如 JSON 类型。
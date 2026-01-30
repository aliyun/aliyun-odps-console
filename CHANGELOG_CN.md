# 更新日志
# 0.56.0-public [2026-01-30]
### 新功能
- 新增 `set region` 命令，支持设置 Region ID
- 新增配置项 `region_id`、`signature_v4_corporation`、`quota_name` 和 `LABEL`，支持在配置文件中配置 Region ID、签名主体、配额名称和测试环境标签
- 支持临时配额（以 `temp_` 开头的配额名称），自动设置 1 天缓存过期时间
- 新增 `odps.console.http.submit.headers` 配置项，支持设置 HTTP 提交时的自定义请求头
- 支持交互式查询模式下的子查询 LogView 生成

### 增强功能
- MaxQA 模式下，支持异步获取 Summary 信息，最多等待 30 秒
- 改进 LogView 生成逻辑，支持通过 LogView 版本号控制生成方式
- 配额缓存机制优化，支持过期时间检查，默认缓存 1 天
- `use quota` 命令增强，支持 MaxQA 模式下的配额切换，自动设置相关 Session 变量和 HTTP 请求头

### 依赖更新
- odps-sdk 版本从 `0.53.0-public` 升级至 `0.56.0-public`
- 升级 `credentials-java` 从 `0.3.12` 至 `1.0.2`
- 新增 `arrow-memory-netty` 依赖（排除了 netty-buffer 和 netty-common）
- 升级 `mockito-core` 从 `1.10.8` 至 `4.11.0`
- 升级 `junit` 从 `4.11` 至 `4.12`，新增 JUnit 5 支持（junit-jupiter-api、junit-jupiter-engine、junit-jupiter-params、junit-vintage-engine）

### 构建与测试
- 新增 Maven profile 支持 JDK 21 编译和测试
- 优化 Maven surefire 插件配置，支持并行测试（20 个线程，3 个 fork）
- JDK 21 模式下添加必要的 JVM 参数以支持 Arrow 内存访问

### 其他变更
- 更新版权年份至 2026
- 移除 `sts_token` 配置项，改由 SDK 内部处理

# 0.48.0-public [2024-07-22]
### 新功能
- odps-sdk 版本从 `0.47.0-public` 升级至 `0.48.6-public`, 包含的增强和修复参阅 [odps-sdk 变更日志](https://github.com/aliyun/aliyun-odps-java-sdk/blob/release/0.48.x/CHANGELOG_CN.md)
- `desc extended` 命令增加了对数据脱敏信息的展示
### 修复
- MCQA 模式能够更准确的识别 fallback 行为，避免对 logview 的重复展示 


# 0.47.1-public [2024-05-23]
### 新功能
- 新增配置 `network_read_timeout` 和 `network_connect_timeout` 用来控制网络超时时间
### 修复
- 移除了 tunnel upsert `ow` 参数，因为它并不实际起作用

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
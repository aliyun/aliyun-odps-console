# MaxC CLI 待改进点清单 (Agent 可读版)

> 本文档面向 AI Agent，以结构化方式描述 maxc-cli 的待改进点，便于 Agent 理解和执行改进任务。

---

## 文档元信息

| 字段 | 值 |
|------|-----|
| 项目 | maxc-cli |
| 版本 | 开发阶段 (MVP) |
| 最后更新 | 2026-03-24 |
| 文档类型 | 改进任务清单 |

---

## 改进点概览

```yaml
summary:
  total_improvements: 12
  by_priority:
    P0: 4
    P1: 4
    P2: 3
    P3: 1
  by_category:
    command_interface: 3
    output_format: 3
    authentication: 3
    documentation: 2
    performance: 1
```

---

## 详细改进点

### 1. query cost/explain 命令风格统一 [P0]

**问题描述**
当前 `query` 命令使用 `--mode` flag 来区分 run/cost/explain 模式，与其他命令的子命令风格不一致。

**当前行为**
```bash
maxc query "SELECT..." --mode cost
maxc query "SELECT..." --mode explain
```

**期望行为**
```bash
maxc query cost "SELECT..."
maxc query explain "SELECT..."
maxc query run "SELECT..."   # 默认，可省略 run
maxc query "SELECT..."       # 等价于 query run
```

**影响范围**
- 文件: `src/maxc_cli/cli.py`
- 函数: `_resolve_query_mode()`, `_handle_query()`
- 文档: `docs/improvement-suggestions.md`

**实现提示**
- 修改 `_resolve_query_mode()` 函数，支持从 `sql_parts` 第一个参数解析子命令
- 保持向后兼容：`--mode` 仍可用但标记为 deprecated
- 更新 help 文本和描述

**验收标准**
- [ ] `maxc query cost "SELECT 1"` 正常工作
- [ ] `maxc query explain "SELECT 1"` 正常工作
- [ ] `maxc query "SELECT 1"` 默认执行 run 模式
- [ ] `--mode` 参数仍可用但输出 deprecation 警告
- [ ] 所有现有测试通过

---

### 2. agent_hints.next_actions 返回可执行命令 [P0]

**问题描述**
JSON 输出的 `agent_hints.next_actions` 字段包含的命令格式与实际可执行命令不一致。

**当前输出**
```json
{
  "agent_hints": {
    "next_actions": ["query.explain", "meta.list-tables"]
  }
}
```

**问题**
- `query.explain` 不是有效命令（应为 `query --mode explain` 或 `query explain`）
- `meta.list-tables` 不是有效命令（应为 `meta list-tables`）

**期望输出**
```json
{
  "agent_hints": {
    "next_actions": [
      "maxc query explain \"SELECT * FROM t\" --json",
      "maxc meta list-tables --json"
    ]
  }
}
```

**影响范围**
- 文件: `src/maxc_cli/models.py`
- 函数: `_format_next_action()`, `_render_agent_hints()`
- 相关: `src/maxc_cli/app.py` 中的 AgentHints 构造

**实现提示**
- `_format_next_action()` 函数已存在，需要确保所有 action 类型都返回完整命令
- 需要传入当前上下文（如 SQL、table_name 等）来生成完整命令
- 确保命令格式与 CLI 实际接受的格式一致

**验收标准**
- [ ] 所有 `next_actions` 返回的命令都可以直接复制执行
- [ ] 命令包含必要的 `--json` 参数
- [ ] 命令中的变量（如 SQL、table_name）正确填充
- [ ] 所有 action 类型都有对应的格式化逻辑

---

### 3. 错误提示语言统一 [P1]

**问题描述**
错误消息中英文混合，影响用户体验和国际化。

**当前行为**
```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "ODPS-0130131: Table not found - table dingxin.nonexistent_table cannot be resolved",
    "suggestion": "请先执行 maxc meta list-tables 或 maxc meta search 确认对象是否存在。"
  }
}
```

**期望行为**
```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Table not found - table 'nonexistent_table' cannot be resolved",
    "suggestion": "Run 'maxc meta list-tables' or 'maxc meta search' to verify the object exists."
  }
}
```

**影响范围**
- 文件: `src/maxc_cli/exceptions.py`
- 文件: `src/maxc_cli/helpers.py` (translate_odps_error)
- 文件: `src/maxc_cli/app.py` (所有错误消息)

**实现提示**
- 统一使用英文错误消息
- 保留原始 ODPS 错误代码，但提供清晰的英文描述
- 建议消息也统一为英文

**验收标准**
- [ ] 所有错误消息的 `message` 字段为英文
- [ ] 所有错误消息的 `suggestion` 字段为英文
- [ ] 保留原始错误代码（如 ODPS-0130131）
- [ ] 测试用例更新为英文消息

---

### 4. JSON 输出 data 字段结构统一 [P1]

**问题描述**
不同命令的 `data` 结构差异大，Agent 需要针对每个命令编写特定解析逻辑。

**当前结构差异**
| 命令 | data 结构 |
|------|-----------|
| `auth whoami` | 扁平对象 `{authenticated, backend, ...}` |
| `meta list-tables` | 嵌套 `{tables: [...]}` |
| `query` | 嵌套 `{rows, schema, total_rows, ...}` |

**期望统一结构**
```json
// auth whoami
{
  "data": {
    "identity": {
      "authenticated": true,
      "backend": "odps",
      "auth_type": "access_key",
      "principal": "ALIYUN$xxx",
      "project": "dingxin"
    }
  }
}

// meta list-tables
{
  "data": {
    "tables": [...],
    "pagination": {
      "total": 142,
      "has_more": false
    }
  }
}

// query
{
  "data": {
    "result": {
      "rows": [...],
      "schema": [...],
      "row_count": 100
    },
    "pagination": {
      "has_more": true,
      "next_cursor": "abc123"
    }
  }
}
```

**影响范围**
- 文件: `src/maxc_cli/models.py`
- 函数: `_normalize_data()`, `_already_normalized()`
- 文件: `src/maxc_cli/app.py` (所有 Envelope 构造)

**实现提示**
- `_normalize_data()` 函数已存在，用于规范化数据结构
- 需要确保所有命令都使用语义化的嵌套结构
- 保持向后兼容，避免破坏现有客户端

**验收标准**
- [ ] 所有命令的 data 结构遵循统一规范
- [ ] 列表数据包含 `pagination` 字段
- [ ] 身份相关数据包含在 `identity` 字段
- [ ] 查询结果包含在 `result` 字段
- [ ] 测试用例验证规范化后的结构

---

### 5. cache build 行为增强 [P2]

**问题描述**
`cache build` 命令缺乏详细的执行反馈和预览功能。

**当前行为**
- 构建缓存时输出简单的进度信息
- 缺乏详细的统计信息
- 不支持 `--dry-run` 预览

**期望行为**
```bash
# 详细输出
maxc cache build --json
```
```json
{
  "data": {
    "action": "build",
    "scope": "all_tables",
    "tables_scanned": 142,
    "cache_entries_created": 142,
    "cache_entries_updated": 10,
    "elapsed_ms": 5234,
    "cache_location": "~/.maxc/cache/metadata.db"
  }
}
```

```bash
# 预览模式
maxc cache build --dry-run --json
```
```json
{
  "data": {
    "tables_to_scan": 142,
    "estimated_time_seconds": 5,
    "cache_entries_to_create": 142
  }
}
```

**影响范围**
- 文件: `src/maxc_cli/cli.py`
- 文件: `src/maxc_cli/app.py`
- 函数: `cache_build()`, `_build_cache_sync()`

**实现提示**
- 添加 `--dry-run` 参数支持
- 在 JSON 输出中包含详细的统计信息
- 计算预估时间（基于表数量和历史数据）

**验收标准**
- [ ] `--dry-run` 参数正常工作，不实际修改缓存
- [ ] JSON 输出包含完整的统计信息
- [ ] 预估时间基于合理的启发式算法
- [ ] 缓存位置信息正确显示

---

### 6. meta list-tables 数据来源透明化 [P2]

**问题描述**
`meta list-tables` 返回的数据来源不明确（缓存 vs 实时查询）。

**期望行为**
在 `metadata` 中说明数据来源：
```json
{
  "data": {
    "tables": [...]
  },
  "metadata": {
    "source": "cache",
    "cache_age_seconds": 3600,
    "cache_stale": false,
    "project": "dingxin",
    "refresh_command": "maxc cache build"
  }
}
```

**影响范围**
- 文件: `src/maxc_cli/app.py`
- 函数: `meta_list_tables()`
- 相关: `src/maxc_cli/cache.py`

**实现提示**
- 使用 `_cache_metadata()` 辅助函数（已存在）
- 确保 `source` 字段正确标识数据来源
- 添加缓存年龄和过期状态

**验收标准**
- [ ] `metadata.source` 正确标识 "cache" 或 "live"
- [ ] 缓存模式下包含 `cache_age_seconds` 和 `cache_stale`
- [ ] 实时模式下包含 `query_time_ms`
- [ ] 包含 `refresh_command` 提示

---

### 7. 鉴权方式扩展：完善 STS Token 支持 [P0]

**问题描述**
STS Token 鉴权已部分实现，但需要完善配置和文档。

**当前状态**
- `auth login` 支持 `--security-token` 参数
- `AuthConfig` 支持 `security_token` 字段
- 但缺乏完整的 STS 专用流程

**期望行为**
```bash
# STS Token 登录
maxc auth login \
  --access-id "<临时 AK>" \
  --secret-access-key "<临时 Secret>" \
  --security-token "<STS Token>" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --json
```

```json
{
  "data": {
    "identity": {
      "authenticated": true,
      "backend": "odps",
      "auth_type": "sts_token",
      "principal_display": "ALIYUN$xxx",
      "project": "my_project",
      "token_expires_at": "2026-03-23T15:00:00Z"
    }
  }
}
```

**影响范围**
- 文件: `src/maxc_cli/auth_providers.py`
- 文件: `src/maxc_cli/config.py`
- 文件: `src/maxc_cli/app.py`

**实现提示**
- 完善 `AuthProvider` 抽象接口
- 实现 `StsTokenAuthProvider` 类
- 在 `auth whoami` 中显示 token 过期时间

**验收标准**
- [ ] STS Token 登录流程完整可用
- [ ] `auth whoami` 显示 `token_expires_at`
- [ ] 过期前发出警告
- [ ] 支持通过环境变量配置 STS Token

---

### 8. 鉴权方式扩展：完善 NCS (无 AK) 支持 [P0]

**问题描述**
NCS (无 AK) 鉴权已部分实现，但需要完善交互式引导和验证。

**当前状态**
- `auth login-ncs` 命令已存在
- 支持 `--interactive` 模式
- 但缺乏完整的账号列表获取和验证

**期望行为**
```bash
# 列出可用账号
maxc auth login-ncs --list-accounts --account-type user

# 交互式登录
maxc auth login-ncs --interactive

# 快速登录
maxc auth login-ncs \
  --account-type user \
  --employee-id xxx \
  --project my_project \
  --endpoint http://service-corp.odps.aliyun-inc.com/api
```

**影响范围**
- 文件: `src/maxc_cli/auth_providers.py`
- 文件: `src/maxc_cli/app.py`
- 函数: `auth_login_ncs()`

**实现提示**
- 完善 `list_ncs_accounts()` 函数
- 确保 ncs CLI 命令正确执行
- 添加错误处理和引导

**验收标准**
- [ ] `--list-accounts` 正确列出可用账号
- [ ] `--interactive` 模式引导完整
- [ ] ncs 凭证正确写入配置
- [ ] 验证失败时提供清晰的错误信息

---

### 9. 鉴权架构：抽象 AuthProvider 接口 [P1]

**问题描述**
当前鉴权逻辑分散，缺乏统一的 Provider 抽象。

**期望架构**
```python
class AuthProvider(ABC):
    @property
    @abstractmethod
    def auth_type(self) -> str: ...

    @abstractmethod
    def load(self, config: MaxCConfig) -> AuthCredential: ...

    @abstractmethod
    def refresh(self, credential: AuthCredential) -> AuthCredential: ...

class AccessKeyAuthProvider(AuthProvider): ...
class StsTokenAuthProvider(AuthProvider): ...
class NcsAuthProvider(AuthProvider): ...
```

**影响范围**
- 文件: `src/maxc_cli/auth_providers.py`
- 文件: `src/maxc_cli/backend/odps.py`
- 新增: `AuthProviderRegistry`

**实现提示**
- 创建 `AuthProvider` 抽象基类
- 实现具体的 Provider 类
- 使用注册模式管理 Provider

**验收标准**
- [ ] AuthProvider 抽象接口定义完整
- [ ] 所有鉴权方式都有对应的 Provider 实现
- [ ] Provider 注册机制正常工作
- [ ] 易于扩展新的鉴权方式

---

### 10. auth whoami 引导机制增强 [P2]

**问题描述**
当用户未配置凭证时，`auth whoami` 应该提供清晰的引导。

**期望行为**
```json
{
  "data": {
    "authenticated": false,
    "auth_options": [
      {
        "type": "access_key",
        "description": "Use Access Key authentication",
        "command": "maxc auth login --from-env"
      },
      {
        "type": "ncs",
        "description": "No-AK authentication (internal)",
        "command": "maxc auth login-ncs --interactive",
        "requirements": ["ncs CLI"]
      },
      {
        "type": "sts",
        "description": "Use STS Token authentication",
        "command": "maxc auth login --sts-token"
      }
    ]
  }
}
```

**影响范围**
- 文件: `src/maxc_cli/app.py`
- 函数: `_unauthenticated_whoami_envelope()`, `build_auth_options()`

**实现提示**
- 完善 `build_auth_options()` 函数
- 检测可用的鉴权方式
- 生成对应的命令示例

**验收标准**
- [ ] 未认证时返回 `authenticated: false`
- [ ] 包含所有可用的鉴权选项
- [ ] 每个选项包含描述和命令
- [ ] 检测 ncs CLI 是否安装

---

### 11. 命令参数位置规范化 [P3]

**问题描述**
SQL 和 flags 的参数顺序不明确，用户容易混淆。

**当前行为**
```bash
maxc query "SELECT * FROM t" --mode cost --json
maxc query --mode cost "SELECT * FROM t" --json  # 这个也行？
maxc query "SELECT * FROM t" --json --mode cost  # 这个呢？
```

**期望行为**
如果采用改进点 1 的子命令方案：
```bash
maxc query cost "SELECT * FROM t" --json
maxc query explain "SELECT * FROM t" --json
maxc query run "SELECT * FROM t" --page-size 100 --json
```

**影响范围**
- 文件: `src/maxc_cli/cli.py`
- 函数: `build_parser()`, `_resolve_query_mode()`

**实现提示**
- 明确 SQL 作为位置参数放在最后
- 在 `--help` 中明确说明参数顺序
- 考虑添加参数验证

**验收标准**
- [ ] 参数顺序在 help 中明确说明
- [ ] SQL 参数始终作为最后一个位置参数
- [ ] 参数顺序不一致时给出警告

---

### 12. --brief 模式支持 [P2]

**问题描述**
某些场景下 Agent 只需要知道命令是否成功，不需要完整输出。

**期望行为**
```bash
maxc auth can-i --table my_table --operation SELECT --brief
```
输出：
```
ALLOWED
```

或者通过退出码：
```bash
maxc auth can-i --table my_table --operation SELECT
echo $?  # 0 = allowed, 1 = denied
```

**影响范围**
- 文件: `src/maxc_cli/cli.py`
- 函数: `_emit_envelope()`, `_render_brief()`

**实现提示**
- `_render_brief()` 函数已存在，需要完善
- 支持更多命令的 brief 输出
- 确保退出码正确

**验收标准**
- [ ] `--brief` 参数在支持的命令中可用
- [ ] `auth can-i --brief` 输出 ALLOWED/DENIED
- [ ] 退出码正确反映结果状态
- [ ] 其他命令的 brief 输出简洁明了

---

## 实施路线图

### 第一阶段 (P0)
1. 改进点 1: query cost/explain 命令风格统一
2. 改进点 2: agent_hints.next_actions 返回可执行命令
3. 改进点 7: 完善 STS Token 支持
4. 改进点 8: 完善 NCS 支持

### 第二阶段 (P1)
5. 改进点 3: 错误提示语言统一
6. 改进点 4: JSON 输出 data 字段结构统一
7. 改进点 9: 抽象 AuthProvider 接口
8. 改进点 10: auth whoami 引导机制增强

### 第三阶段 (P2/P3)
9. 改进点 5: cache build 行为增强
10. 改进点 6: meta list-tables 数据来源透明化
11. 改进点 12: --brief 模式支持
12. 改进点 11: 命令参数位置规范化

---

## 文件变更清单

| 文件 | 变更类型 | 相关改进点 |
|------|----------|------------|
| `src/maxc_cli/cli.py` | 修改 | 1, 5, 11, 12 |
| `src/maxc_cli/app.py` | 修改 | 2, 3, 4, 6, 8, 10 |
| `src/maxc_cli/models.py` | 修改 | 2, 4 |
| `src/maxc_cli/config.py` | 修改 | 7, 8, 9 |
| `src/maxc_cli/auth_providers.py` | 修改 | 7, 8, 9 |
| `src/maxc_cli/exceptions.py` | 修改 | 3 |
| `src/maxc_cli/helpers.py` | 修改 | 3 |
| `tests/test_cli_mock.py` | 修改 | 所有 |

---

## 测试策略

### 单元测试
- 每个改进点都需要对应的单元测试
- 使用 `pytest` 框架
- Mock 外部依赖（ODPS, ncs CLI）

### 集成测试
- 使用真实 MaxCompute 后端验证
- 测试文件: `tests/test_integration_real.py`

### 测试命令
```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/test_cli_mock.py::test_auth_login -v

# 运行集成测试
pytest tests/test_integration_real.py -v
```

---

## 附录

### 相关文档
- `docs/design.md` - 产品设计文档
- `docs/implementation.md` - 实现细节
- `docs/roadmap.md` - 路线图
- `CLAUDE.md` - 开发指南

### 代码规范
- 使用 Python 3.9+ 类型注解
- 遵循 PEP 8 风格指南
- 所有函数需要 docstring
- 复杂逻辑需要注释

### 提交规范
```
<type>: <subject>

<body>

<footer>
```

类型:
- `feat`: 新功能
- `fix`: 修复
- `refactor`: 重构
- `docs`: 文档
- `test`: 测试

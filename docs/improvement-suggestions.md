# MaxC CLI 改进建议

> 基于对 maxc-cli 的实际验证和 SKILL 编写过程中的发现，本文档整理了产品行为层面的改进建议。

## 1. `query cost/explain` 命令风格不一致

### 问题描述

当前用法是 `maxc query "SELECT..." --mode cost`，但与其他命令风格不一致：

| 命令 | 风格 |
|------|------|
| `meta list-tables` | 子命令 |
| `auth whoami` | 子命令 |
| `job status` | 子命令 |
| `query --mode cost` | **flag** ← 不一致 |

### 影响

- 用户直觉上会尝试 `maxc query cost "SELECT..."`，导致命令执行失败
- Agent 编写 SKILL 时需要额外记忆这个特例
- JSON 输出的 `command: "query.cost"` 与可执行命令不对应

### 建议方案

改为子命令风格：

```bash
# 当前（反直觉）
maxc query "SELECT * FROM t" --mode cost

# 建议改为
maxc query cost "SELECT * FROM t"
maxc query explain "SELECT * FROM t"
maxc query run "SELECT * FROM t"   # 默认 run 可省略
maxc query "SELECT * FROM t"       # 等价于 query run
```

### 实现建议

- `query` 作为命令组，支持 `cost`、`explain`、`run` 子命令
- `query` 直接跟 SQL 时，默认执行 `run`
- 保持向后兼容：`--mode cost` 仍可使用，但标记为 deprecated

---

## 2. `agent_hints.next_actions` 返回值不可执行

### 问题描述

JSON 返回的建议命令和实际可执行命令不一致：

```json
{
  "command": "query.cost",
  "agent_hints": {
    "next_actions": ["query.explain", "query"]
  }
}
```

```json
{
  "command": "auth.whoami",
  "agent_hints": {
    "next_actions": ["auth.can-i", "meta.list-tables"]
  }
}
```

问题点：
- `query.explain` 不是有效命令（应该是 `query --mode explain`）
- `meta.list-tables` 不是有效命令（应该是 `meta list-tables`）

### 影响

- Agent 无法直接执行 `next_actions` 中的值
- 需要额外的映射逻辑来转换命令格式
- 这个字段对 Agent 的实用性大打折扣

### 建议方案

**方案 A**：返回完整的可执行命令

```json
{
  "agent_hints": {
    "next_actions": [
      "maxc query \"SELECT * FROM t\" --mode explain",
      "maxc meta list-tables --json"
    ]
  }
}
```

**方案 B**：返回正确的命令片段（推荐）

```json
{
  "agent_hints": {
    "next_actions": [
      {"command": "query", "args": ["--mode", "explain"], "description": "Explain query plan"},
      {"command": "meta list-tables", "args": [], "description": "List available tables"}
    ]
  }
}
```

**方案 C**：保持简单字符串，但使用正确格式

```json
{
  "agent_hints": {
    "next_actions": ["query --mode explain", "meta list-tables"]
  }
}
```

---

## 3. 错误提示语言混合

### 问题描述

当前错误消息中英文混合：

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "ODPS-0130131: Table not found - table dingxin.nonexistent_table cannot be resolved",
    "suggestion": "请先执行 maxc meta list-tables 或 maxc meta search 确认对象是否存在。"
  }
}
```

### 影响

- 用户体验不一致
- 国际化困难
- Agent 需要处理多语言提示

### 建议方案

**方案 A**：统一为英文

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Table not found - table 'nonexistent_table' cannot be resolved",
    "suggestion": "Run 'maxc meta list-tables' or 'maxc meta search' to verify the object exists."
  }
}
```

**方案 B**：支持 `--locale` 参数

```bash
maxc query "SELECT..." --locale zh-CN --json
```

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "表不存在 - 表 'nonexistent_table' 无法解析",
    "suggestion": "请先执行 'maxc meta list-tables' 或 'maxc meta search' 确认对象是否存在。"
  }
}
```

---

## 4. JSON 输出的 `data` 字段结构不统一

### 问题描述

不同命令的 `data` 结构差异很大：

| 命令 | data 结构 |
|------|-----------|
| `auth whoami` | 扁平对象 `{authenticated, backend, ...}` |
| `meta list-tables` | 嵌套 `{tables: [...]}` |
| `query` | 嵌套 `{rows, schema, total_rows, ...}` |

### 影响

- Agent 解析时需要针对每个命令编写特定逻辑
- 文档维护困难
- 新命令设计时缺乏统一参考

### 建议方案

统一为语义化的嵌套结构：

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
    "tables": [
      {"name": "table1", "type": "TABLE", ...},
      {"name": "table2", "type": "VIRTUAL_VIEW", ...}
    ],
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

---

## 5. `cache build` 行为不明确

### 问题描述

`cache build` 的行为缺乏明确说明：
- 构建什么内容的缓存？
- 全量构建还是增量？
- 会扫描所有表吗？
- 耗时多久？

### 建议

**方案 A**：在 JSON 输出中提供详细信息

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

**方案 B**：支持 `--dry-run` 预览

```bash
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

---

## 6. `meta list-tables` 数据来源不透明

### 问题描述

`meta list-tables` 返回的数据来源不明确：
- 是实时查询 MaxCompute API？
- 还是读取本地缓存？
- 缓存是否过期？

### 建议

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

或者：

```json
{
  "metadata": {
    "source": "live",
    "query_time_ms": 1234,
    "project": "dingxin"
  }
}
```

---

## 7. 命令参数位置有歧义

### 问题描述

SQL 和 flags 混在一起，参数顺序不明确：

```bash
maxc query "SELECT * FROM t" --mode cost --json
maxc query --mode cost "SELECT * FROM t" --json  # 这个也行？
maxc query "SELECT * FROM t" --json --mode cost  # 这个呢？
```

### 建议

如果采用方案 1 的子命令改造，SQL 作为位置参数放在最后：

```bash
maxc query cost "SELECT * FROM t" --json
maxc query explain "SELECT * FROM t" --json
maxc query run "SELECT * FROM t" --page-size 100 --json
```

如果不改造，建议在文档中明确说明参数顺序，并在 `--help` 中体现。

---

## 8. 缺少 `--quiet` 或 `--brief` 模式

### 问题描述

某些场景下，Agent 只需要知道命令是否成功，不需要完整输出：

```bash
maxc auth can-i --table my_table --operation SELECT --json
```

当前输出包含完整的 JSON 结构，但 Agent 只需要 "yes/no"。

### 建议

支持 `--brief` 模式：

```bash
maxc auth can-i --table my_table --operation SELECT --brief
```

输出：

```
ALLOWED
```

或者通过退出码表示（当前可能已支持）：

```bash
maxc auth can-i --table my_table --operation SELECT
echo $?  # 0 = allowed, 1 = denied
```

---

## 优先级总结

| 优先级 | 改进项 | 理由 | 预计工作量 |
|--------|--------|------|------------|
| **P0** | `query cost/explain` 改为子命令 | 影响核心用法，现在改成本低 | 中 |
| **P0** | `next_actions` 返回可执行命令 | 否则这个字段对 Agent 无用 | 小 |
| **P1** | 错误提示语言统一 | 提升专业感，便于国际化 | 小 |
| **P1** | `data` 字段结构统一 | 降低 Agent 解析复杂度 | 大 |
| **P2** | `cache build` 行为说明 | 用户和 Agent 都需要知道 | 小 |
| **P2** | `meta list-tables` 数据来源说明 | 帮助判断是否需要刷新 | 小 |
| **P3** | 命令参数位置规范 | 如果采用子命令方案则自然解决 | - |
| **P3** | `--brief` 模式 | 优化特定场景，非必需 | 小 |

---

## 9. 鉴权方式扩展

### 当前状态

当前 maxc-cli 仅支持 Access Key 鉴权：

```python
# helpers.py:198
"auth_type": "access_key",  # 硬编码

# odps.py:56-63
self.client = ODPS(
    access_id=self.settings["access_id"],
    secret_access_key=self.settings["secret_access_key"],
    ...
)
```

### 问题

| 问题 | 影响 |
|------|------|
| `auth_type` 硬编码 | 无法支持其他鉴权类型 |
| ODPS 客户端初始化只接受 access_id/secret | 无法使用 RAM Role、STS、无 AK 等 |
| 没有抽象的 AuthProvider 接口 | 每增加一种鉴权方式需改多处代码 |
| 缺少引导机制 | 用户不知道如何使用无 AK 鉴权 |

### 需要支持的鉴权方式

| 鉴权方式 | 使用场景 | 优先级 |
|----------|----------|--------|
| **Access Key** | 当前支持，个人/服务账号 | ✓ 已支持 |
| **STS Token** | 临时凭证，安全性高 | P0 需新增 |
| **无 AK（ncs）** | 阿里内部，免密钥 | P0 需新增 |
| **RAM Role** | ECS 实例角色 | P1 未来扩展 |

---

### 9.1 无 AK 鉴权方案（基于 ncs CLI）

#### 概述

无 AK 鉴权依赖阿里内部的 `ncs` CLI 工具，通过 `ncs create credential` 获取临时凭证。

#### 核心流程

```
┌─────────────────────────────────────────────────────────────┐
│                    无 AK 鉴权流程                            │
├─────────────────────────────────────────────────────────────┤
│  1. 检测 ncs 是否安装                                        │
│     └─ 未安装 → 引导安装                                      │
│                                                              │
│  2. 选择 Endpoint（区域）                                     │
│     └─ 国内弹内 / 新加坡弹内 / 德国弹内 / 美国蚂蚁 / 越南蚂蚁   │
│                                                              │
│  3. 选择项目空间                                              │
│                                                              │
│  4. 选择账号类型                                              │
│     ├─ 个人账号（employee）                                   │
│     ├─ 公共账号（department）                                 │
│     └─ 应用账号（app）                                        │
│                                                              │
│  5. ncs 生成临时凭证                                          │
│     └─ 写入 ~/.maxc/config.yaml 或 ODPS 配置文件              │
└─────────────────────────────────────────────────────────────┘
```

#### ncs 关键命令

```bash
# 列出可用账号
ncs list authorizations odpsuser -o custom-columns=BUC_USER_ID:.extension.bucUserId,BUC_USER_TYPE:.extension.bucUserType,BUC_ACCOUNT_NAME:.extension.bucDomainAccount

ncs list authorizations odpsaccount --scenario app -o custom-columns=accountName:.extension.accountName

ncs list authorizations odpsapp -o custom-columns=AppName:.extension.appName

# 创建凭证（生成临时 AK）
ncs create credential odpsuser --employee-id <id> -o template -t odpscmd
ncs create credential odpsaccount --account-name <name> -o template -t odpscmd
ncs create credential odpsapp --app-name <name> -o template -t odpscmd
```

#### 配置文件格式

无 AK 配置需要写入 ODPS 配置文件 `~/.odps_config.ini` 或 `~/.maxc/config.yaml`：

**odps_config.ini 格式**：

```ini
project_name=<project>
account_provider=external
processCommand=ncs create credential odpsuser --employee-id <id> -o template -t odpscmd
processCommandTimeout=20
end_point=<endpoint>
```

**maxc config.yaml 格式**（建议新增）：

```yaml
auth:
  provider: ncs
  ncs:
    account_type: user  # user | account | app
    employee_id: xxx
    # 或
    account_name: xxx
    # 或
    app_name: xxx
  project: my_project
  endpoint: http://service-corp.odps.aliyun-inc.com/api
```

#### maxc 新增命令

建议新增 `auth login-ncs` 子命令：

```bash
# 交互式引导（推荐）
maxc auth login-ncs --interactive

# 快速登录（指定参数）
maxc auth login-ncs \
  --account-type user \
  --employee-id xxx \
  --project my_project \
  --endpoint http://service-corp.odps.aliyun-inc.com/api \
  --json

# 列出可用账号
maxc auth login-ncs --list-accounts
```

#### 检测与引导机制

当 `auth whoami` 检测到未配置凭证时，建议输出引导信息：

```json
{
  "data": {
    "authenticated": false,
    "auth_options": [
      {
        "type": "access_key",
        "description": "使用 Access Key 鉴权",
        "command": "maxc auth login --from-env"
      },
      {
        "type": "ncs",
        "description": "无 AK 鉴权（阿里内部）",
        "command": "maxc auth login-ncs --interactive",
        "requirements": ["ncs CLI"]
      },
      {
        "type": "sts",
        "description": "使用 STS Token 鉴权",
        "command": "maxc auth login --sts-token"
      }
    ]
  }
}
```

---

### 9.2 STS Token 鉴权方案

#### 概述

STS（Security Token Service）提供临时访问凭证，具有自动过期时间，安全性更高。

#### 使用场景

- 临时授权访问
- 跨账号访问
- 前端/移动端应用
- CI/CD 流水线

#### 配置方式

**环境变量方式**：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="<临时 AK>"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="<临时 Secret>"
export ALIBABA_CLOUD_SECURITY_TOKEN="<STS Token>"
export MAXCOMPUTE_PROJECT="<project>"
export MAXCOMPUTE_ENDPOINT="<endpoint>"
```

**命令行方式**：

```bash
maxc auth login \
  --access-id "<临时 AK>" \
  --secret-access-key "<临时 Secret>" \
  --security-token "<STS Token>" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --json
```

**配置文件方式**：

```yaml
# ~/.maxc/config.yaml
auth:
  access_id: <临时 AK>
  secret_access_key: <临时 Secret>
  security_token: <STS Token>
  project: my_project
  endpoint: http://service.cn-shanghai.maxcompute.aliyun.com/api
```

#### JSON 输出扩展

```json
{
  "data": {
    "authenticated": true,
    "backend": "odps",
    "auth_type": "sts_token",  // 新增类型
    "identity_source": "environment",
    "principal_display": "ALIYUN$xxx",
    "project": "my_project",
    "token_expires_at": "2026-03-23T15:00:00Z"  // STS 过期时间
  }
}
```

---

### 9.3 可扩展鉴权架构设计

#### AuthProvider 抽象接口

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime

@dataclass
class AuthCredential:
    """统一的凭证结构"""
    auth_type: str  # access_key | sts_token | ncs | ram_role
    access_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    security_token: Optional[str] = None
    expires_at: Optional[datetime] = None

    # ncs 专用字段
    ncs_account_type: Optional[str] = None  # user | account | app
    ncs_employee_id: Optional[str] = None
    ncs_account_name: Optional[str] = None
    ncs_app_name: Optional[str] = None

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at

    def to_odps_kwargs(self) -> dict[str, Any]:
        """转换为 pyodps ODPS() 构造参数"""
        kwargs = {
            "access_id": self.access_id,
            "secret_access_key": self.secret_access_key,
        }
        if self.security_token:
            kwargs["security_token"] = self.security_token
        return kwargs


class AuthProvider(ABC):
    """鉴权提供者抽象基类"""

    @property
    @abstractmethod
    def auth_type(self) -> str:
        """鉴权类型标识"""
        pass

    @abstractmethod
    def load(self, config: "MaxCConfig") -> AuthCredential:
        """从配置加载凭证"""
        pass

    @abstractmethod
    def refresh(self, credential: AuthCredential) -> AuthCredential:
        """刷新凭证（用于临时凭证续期）"""
        pass

    def is_available(self, config: "MaxCConfig") -> bool:
        """检查该鉴权方式是否可用"""
        return True


class AccessKeyAuthProvider(AuthProvider):
    """Access Key 鉴权"""

    @property
    def auth_type(self) -> str:
        return "access_key"

    def load(self, config: "MaxCConfig") -> AuthCredential:
        settings, sources = resolve_odps_settings(config)
        return AuthCredential(
            auth_type="access_key",
            access_id=settings.get("access_id"),
            secret_access_key=settings.get("secret_access_key"),
        )

    def refresh(self, credential: AuthCredential) -> AuthCredential:
        # Access Key 不过期，直接返回
        return credential


class StsTokenAuthProvider(AuthProvider):
    """STS Token 鉴权"""

    @property
    def auth_type(self) -> str:
        return "sts_token"

    def load(self, config: "MaxCConfig") -> AuthCredential:
        return AuthCredential(
            auth_type="sts_token",
            access_id=config.auth.access_id,
            secret_access_key=config.auth.secret_access_key,
            security_token=config.auth.security_token,
            expires_at=config.auth.token_expires_at,
        )

    def refresh(self, credential: AuthCredential) -> AuthCredential:
        # STS Token 需要重新获取，这里可以调用外部服务
        raise NotImplementedError("STS Token refresh requires external service")


class NcsAuthProvider(AuthProvider):
    """无 AK 鉴权（基于 ncs CLI）"""

    @property
    def auth_type(self) -> str:
        return "ncs"

    def is_available(self, config: "MaxCConfig") -> bool:
        # 检查 ncs 是否安装
        import shutil
        return shutil.which("ncs") is not None

    def load(self, config: "MaxCConfig") -> AuthCredential:
        # 调用 ncs 获取临时凭证
        import subprocess
        import json

        ncs_config = config.auth.ncs
        cmd = self._build_ncs_command(ncs_config)

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise AuthError(f"ncs credential creation failed: {result.stderr}")

        # 解析 ncs 输出，提取临时 AK
        credential = self._parse_ncs_output(result.stdout)
        return credential

    def _build_ncs_command(self, ncs_config) -> str:
        if ncs_config.account_type == "user":
            return f"ncs create credential odpsuser --employee-id {ncs_config.employee_id} -o template -t odpscmd"
        elif ncs_config.account_type == "account":
            return f"ncs create credential odpsaccount --account-name {ncs_config.account_name} -o template -t odpscmd"
        elif ncs_config.account_type == "app":
            return f"ncs create credential odpsapp --app-name {ncs_config.app_name} -o template -t odpscmd"
        raise ValueError(f"Unknown ncs account type: {ncs_config.account_type}")

    def refresh(self, credential: AuthCredential) -> AuthCredential:
        # ncs 凭证过期时重新获取
        return self.load(self.config)
```

#### AuthProvider 注册机制

```python
class AuthProviderRegistry:
    """鉴权提供者注册表"""

    _providers: dict[str, type[AuthProvider]] = {}

    @classmethod
    def register(cls, provider_class: type[AuthProvider]) -> None:
        cls._providers[provider_class().auth_type] = provider_class

    @classmethod
    def get(cls, auth_type: str) -> Optional[AuthProvider]:
        provider_class = cls._providers.get(auth_type)
        return provider_class() if provider_class else None

    @classmethod
    def list_available(cls, config: "MaxCConfig") -> list[AuthProvider]:
        """列出所有可用的鉴权方式"""
        return [
            cls.get(auth_type)
            for auth_type in cls._providers
            if cls.get(auth_type).is_available(config)
        ]


# 注册内置提供者
AuthProviderRegistry.register(AccessKeyAuthProvider)
AuthProviderRegistry.register(StsTokenAuthProvider)
AuthProviderRegistry.register(NcsAuthProvider)
```

#### OdpsBackend 改造

```python
class OdpsBackend:
    def __init__(self, config: MaxCConfig) -> None:
        self.config = config

        # 根据 auth_type 选择对应的 Provider
        auth_type = config.auth.provider or "access_key"
        provider = AuthProviderRegistry.get(auth_type)

        if provider is None:
            raise ValidationError(f"Unsupported auth type: {auth_type}")

        if not provider.is_available(config):
            raise ValidationError(
                f"Auth provider '{auth_type}' is not available. "
                f"Available providers: {[p.auth_type for p in AuthProviderRegistry.list_available(config)]}"
            )

        self.auth_provider = provider
        self.credential = provider.load(config)

        # 检查凭证是否过期
        if self.credential.is_expired():
            self.credential = provider.refresh(self.credential)

        # 创建 ODPS 客户端
        self.client = ODPS(
            project=config.default_project,
            endpoint=config.auth.endpoint,
            **self.credential.to_odps_kwargs()
        )
```

---

### 优先级更新

| 优先级 | 改进项 | 理由 | 预计工作量 |
|--------|--------|------|------------|
| **P0** | STS Token 鉴权 | 安全性需求高，外部用户常用 | 中 |
| **P0** | 无 AK 鉴权（ncs） | 阿里内部核心需求 | 大 |
| **P1** | AuthProvider 抽象接口 | 为未来扩展打基础 | 中 |
| **P1** | `auth login-ncs` 命令 | 提升用户体验 | 中 |
| **P2** | `auth whoami` 引导机制 | 帮助用户快速上手 | 小 |

---

## 附录：当前命令验证结果

基于 `maxc --help` 和实际执行验证的命令列表：

### 命令组

| 命令组 | 子命令 |
|--------|--------|
| `query` | 直接执行 SQL，支持 `--mode {run,cost,explain}` |
| `job` | `submit`, `status`, `wait`, `diagnose`, `result`, `cancel`, `list` |
| `meta` | `list-tables`, `list-projects`, `list-schemas`, `describe`, `search`, `search-columns`, `partitions`, `latest-partition`, `freshness`, `lineage` |
| `data` | `sample`, `profile` |
| `auth` | `login`, `whoami`, `can-i` |
| `diff` | `schema`, `partition`, `data` |
| `agent` | `context` |
| `cache` | `build`, `status`, `clear`, `save-semantic`, `get-semantic` |

### 错误码（已验证）

| 错误码 | 含义 |
|--------|------|
| `NOT_FOUND` | 表或对象不存在 |

### 待补充的错误码

| 错误码 | 含义 | 触发场景 |
|--------|------|----------|
| `VALIDATION_ERROR` | 参数校验失败 | 待验证 |
| `PERMISSION_DENIED` | 权限不足 | 待验证 |
| `SQL_ERROR` | SQL 语法错误 | 待验证 |
| `COST_LIMIT_EXCEEDED` | 超出成本限制 | 待验证 |
| `FEATURE_UNAVAILABLE` | 功能不可用 | 待验证 |
| `BACKEND_CONNECTION_ERROR` | 后端连接错误 | 待验证 |

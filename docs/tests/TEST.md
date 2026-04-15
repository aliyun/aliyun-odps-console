# maxc-cli 测试规划

> 当前 3376 行测试代码，10 个测试文件。本文档规划测试分层、标记策略和盲区补充。

## 1. 现有测试清单

| 文件 | 行数 | 类型 | 依赖 |
|------|------|------|------|
| `conftest.py` | 14 | 共享 fixture | — |
| `test_compat.py` | 36 | Unit | — |
| `test_cache.py` | 120 | Unit | SQLite |
| `test_cli_mock.py` | 1350 | Unit (mock) | FakeODPS |
| `test_agent_hints_and_cli.py` | 315 | Unit | FakeODPS |
| `test_query_auto_promote.py` | 228 | Unit | FakeODPS |
| `test_job_improvements.py` | 323 | Unit | FakeODPS |
| `test_e2e_smoke.py` | 110 | E2E (fake) | FakeODPS |
| `test_integration.py` | 124 | Integration | 真实 ODPS |
| `test_integration_real.py` | 756 | Integration | 真实 ODPS |

## 2. 测试分层

### 2.1 标记体系

```python
import pytest

@pytest.mark.unit          # 无外部依赖，mock ODPS
def test_envelope_construction():
    ...

@pytest.mark.integration   # 需要真实 ODPS 连接
def test_real_query():
    ...

@pytest.mark.e2e           # 子进程调用 maxc CLI
def test_cli_invocation():
    ...
```

### 2.2 pytest.ini 配置

```ini
[pytest]
markers =
    unit: Unit tests (no external dependencies)
    integration: Integration tests (require real ODPS connection)
    e2e: End-to-end tests (subprocess CLI invocation)
addopts = -m "not integration and not e2e"
```

默认只跑 unit，integration/e2e 需显式指定：
```bash
pytest                              # unit only
pytest -m integration               # integration only
pytest -m "unit or integration"     # both
pytest -m e2e                       # e2e only
pytest                              # CI 默认跑 unit
```

## 3. 现有测试标记映射

| 文件 | 建议标记 |
|------|---------|
| `test_compat.py` | `@pytest.mark.unit` |
| `test_cache.py` | `@pytest.mark.unit` |
| `test_cli_mock.py` | `@pytest.mark.unit` |
| `test_agent_hints_and_cli.py` | `@pytest.mark.unit` |
| `test_query_auto_promote.py` | `@pytest.mark.unit` |
| `test_job_improvements.py` | `@pytest.mark.unit` |
| `test_e2e_smoke.py` | `@pytest.mark.e2e` |
| `test_integration.py` | `@pytest.mark.integration` |
| `test_integration_real.py` | `@pytest.mark.integration` |

## 4. 盲区测试（需补充）

### 4.1 `maxc agent skill` 命令

```python
# test_agent_skill_command.py
@pytest.mark.unit
class TestAgentSkillCommand:
    def test_agent_skill_returns_path(self, fake_odps, tmp_path):
        """maxc agent skill --json 应返回 SKILL.md 路径"""
        ...

    def test_agent_skill_yaml_front_matter(self, fake_odps, tmp_path):
        """SKILL.md 应包含合法 YAML front matter"""
        ...

    def test_agent_skill_file_exists(self, fake_odps, tmp_path):
        """返回的路径应指向一个真实存在的文件"""
        ...
```


```python
# test_agent_context_enhanced.py
@pytest.mark.unit
class TestAgentContextEnhanced:
    def test_context_has_version(self, ...):
        """data 中应包含 version 字段"""
        ...

    def test_context_has_python_version(self, ...):
        """data 中应包含 python_version 字段"""
        ...

    def test_context_has_auth_status(self, ...):
        """data 中应包含 auth_status 字段"""
        ...

    def test_context_has_backend_reachable(self, ...):
        """data 中应包含 backend_reachable 字段"""
        ...

    def test_context_has_capabilities(self, ...):
        """data 中应包含 capabilities 字典"""
        ...

    def test_context_no_backend_auth_status(self, ...):
        """无 backend 时 auth_status 应为 not_configured"""
        ...
```

### 4.4 `--mode` 废弃警告

```python
# test_query_mode_deprecation.py
@pytest.mark.unit
class TestQueryModeDeprecation:
    def test_mode_flag_hidden_from_help(self, ...):
        """--mode 不应出现在 --help 输出中"""
        ...

    def test_mode_flag_still_works(self, ...):
        """--mode 仍能正常工作（向后兼容）"""
        ...

    def test_mode_flag_emits_deprecation(self, ...):
        """--mode 应触发 DeprecationWarning"""
        ...
```

### 4.5 ErrorPayload.recovery_steps

```python
# test_error_recovery_steps.py
@pytest.mark.unit
class TestErrorRecoverySteps:
    def test_permission_denied_has_recovery(self, ...):
        """PERMISSION_DENIED 错误应有 recovery_steps"""
        ...

    def test_backend_connection_error_has_recovery(self, ...):
        """BACKEND_CONNECTION_ERROR 错误应有 recovery_steps"""
        ...

    def test_unknown_error_empty_recovery(self, ...):
        """未知错误 code 的 recovery_steps 应为空列表"""
        ...

    def test_recovery_steps_in_envelope(self, ...):
        """error payload to_dict() 应包含 recovery_steps"""
        ...
```

### 4.6 后端 docstring 一致性

```python
# test_backend_docstrings.py
@pytest.mark.unit
class TestBackendDocstrings:
    """确保所有 backend 公开方法都有 Args/Returns/Raises docstring。"""

    @pytest.mark.parametrize("mixin_path", [
        "maxc_cli.backend.query.QueryMixin",
        "maxc_cli.backend.job.JobMixin",
        "maxc_cli.backend.meta.MetaMixin",
        "maxc_cli.backend.data.DataMixin",
        "maxc_cli.backend.auth.AuthMixin",
    ])
    def test_public_methods_have_docstrings(self, mixin_path):
        """所有非下划线开头的方法应有 docstring"""
        import importlib
        module_path, class_name = mixin_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        for name in dir(cls):
            if name.startswith("_"):
                continue
            method = getattr(cls, name)
            if callable(method) and hasattr(method, "__doc__"):
                assert method.__doc__, f"{mixin_path}.{name} missing docstring"
```

## 5. 执行策略

### 5.1 CI Pipeline

```yaml
# .gitlab-ci.yml (建议)
test:unit:
  script: pytest -m unit
  coverage: true

test:integration:
  script: pytest -m integration
  when: manual  # 需手动触发，依赖真实 ODPS
  only:
    - main
```

### 5.2 本地开发

```bash
# 快速验证 (unit only)
pytest -m unit -x -q

# 完整验证
pytest -m "unit or integration"

# 单文件
pytest tests/test_agent_hints_and_cli.py -v
```

## 6. 实施优先级

| 优先级 | 任务 | 估时 |
|--------|------|------|
| P0 | 给现有 10 个测试文件添加 mark | 30 min |
| P0 | 添加 pytest.ini | 5 min |
| P1 | 补充 agent skill/commands 测试 | 1 hr |
| P1 | 补充 agent context 增强测试 | 45 min |
| P1 | 补充 ErrorPayload.recovery_steps 测试 | 30 min |
| P2 | 补充 --mode deprecation 测试 | 30 min |
| P2 | 补充 backend docstring 一致性测试 | 30 min |

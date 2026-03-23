# maxc-cli 测试指南

## 测试类型

### 1. Mock 测试（默认）

使用 mock backend，不需要 MaxCompute 凭证，适合本地开发和 CI：

```bash
cd /Users/dingxin/pythonProject/maxc-cli
PYTHONPATH=src python -m pytest tests/test_cli_mock.py -v
```

所有 mock 测试都会设置 `MAXC_ALLOW_MOCK=1` 环境变量。

### 2. 集成测试（需要真实 MaxCompute）

需要配置 MaxCompute 凭证才能运行：

```bash
# 设置环境变量
export MAXCOMPUTE_PROJECT=your_project
export MAXCOMPUTE_ENDPOINT=http://service.cn-hangzhou.maxcompute.aliyun.com/api
export MAXCOMPUTE_ACCESS_ID=your_access_id
export MAXCOMPUTE_ACCESS_KEY=your_access_key

# 运行集成测试
PYTHONPATH=src python -m pytest tests/test_integration.py -v
```

或者使用 legacy ODPS_* 变量名：

```bash
export ODPS_PROJECT=your_project
export ODPS_ENDPOINT=...
export ODPS_ACCESS_ID=...
export ODPS_ACCESS_KEY=...
```

如果没有设置凭证，集成测试会自动跳过。

## 运行所有测试

```bash
# 只运行 mock 测试（快，无需凭证）
PYTHONPATH=src python -m pytest tests/test_cli_mock.py -v

# 只运行集成测试（需要凭证，否则跳过）
PYTHONPATH=src python -m pytest tests/test_integration.py -v

# 运行所有测试
PYTHONPATH=src python -m pytest tests/ -v
```

## 测试覆盖

当前测试覆盖：

- ✅ CLI 命令解析
- ✅ Mock backend 行为
- ✅ JSON 输出格式
- ✅ 错误处理
- ✅ 分页 cursor 机制
- ✅ 缓存管理
- ✅ 元数据操作
- ✅ 查询执行
- ✅ 差异对比
- ✅ 认证检查

集成测试额外验证：

- ✅ 真实 MaxCompute 连接
- ✅ 实际 SQL 执行
- ✅ 真实表元数据读取
- ✅ 性能回归检测（如 `agent context` 不应调用 `list_tables`）

## 添加新测试

### Mock 测试示例

```python
def test_your_feature_with_mock_backend(tmp_path: Path) -> None:
    config_path = write_mock_files(tmp_path)
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        ["your", "command", "--json"],
    )
    assert code == 0
    assert payload["status"] == "success"
    # ... 更多断言
```

### 集成测试示例

```python
@pytest.mark.skipif(not has_real_credentials(), reason="Requires credentials")
def test_your_feature_with_real_backend(real_config: Path) -> None:
    stdout = StringIO()
    code = run(["--config", str(real_config), "your", "command", "--json"], 
               stdout=stdout, stderr=StringIO())
    assert code == 0
    result = json.loads(stdout.getvalue())
    # ... 验证真实后端行为
```

# maxc-cli 测试指南

## 测试类型

### 1. FakeODPS 测试（默认）

这些测试通过 monkeypatch `odps.ODPS` 为 `FakeODPS`，验证认证、CLI 契约和错误处理，不需要真实 MaxCompute 凭证：

```bash
cd /Users/dingxin/pythonProject/maxc-cli
PYTHONPATH=src python -m pytest tests/test_cli_mock.py -v
```

它们不是运行时 mock backend，也不依赖 `MAXC_ALLOW_MOCK=1`。

### 2. 真实集成测试（需要 MaxCompute 凭证）

需要配置真实环境变量：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID=your_access_id
export ALIBABA_CLOUD_ACCESS_KEY_SECRET=your_access_key
export MAXCOMPUTE_PROJECT=your_project
export MAXCOMPUTE_ENDPOINT=http://service.cn-hangzhou.maxcompute.aliyun.com/api
```

可选别名仍然支持，例如：

```bash
export ODPS_ACCESS_ID=your_access_id
export ODPS_ACCESS_KEY=your_access_key
export ODPS_PROJECT=your_project
export ODPS_ENDPOINT=http://service.cn-hangzhou.maxcompute.aliyun.com/api
```

运行真实集成测试：

```bash
PYTHONPATH=src python -m pytest tests/test_integration_real.py -v
```

如果没有设置凭证，集成测试会自动跳过。

可选优化：

```bash
# 指定一个已知存在的测试表，避免依赖 meta list-tables + cache build
export MAXC_INTEGRATION_TABLE=your_table

# 如需覆盖冷缓存路径，显式允许集成测试触发 cache build
export MAXC_INTEGRATION_ALLOW_CACHE_BUILD=1
```

真实集成测试会在临时目录里使用独立的 `state_dir` 和 `cache_dir`。默认不会偷偷触发整项目 `cache build`；只有显式设置 `MAXC_INTEGRATION_ALLOW_CACHE_BUILD=1` 时，才会覆盖冷缓存路径。

## 运行建议

```bash
# 快速本地回归
PYTHONPATH=src python -m pytest tests/test_agent_hints_and_cli.py tests/test_cli_mock.py -v

# 真实环境回归
PYTHONPATH=src python -m pytest tests/test_integration_real.py -v

# 全量
PYTHONPATH=src python -m pytest tests/ -v
```

## 覆盖重点

- CLI 命令解析
- 规范化 JSON envelope
- 认证 bootstrap 与 whoami 语义
- query / job / meta / data / diff / cache 主路径
- `cache build --json` 的单 envelope + `stderr` 进度行为
- 真实 MaxCompute 只读操作和 session/cache 集成

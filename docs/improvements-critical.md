# MaxC CLI 关键改进清单

> 本文档记录了对 maxc-cli 的深度审查中发现的关键问题和改进建议,重点关注性能、可靠性和用户体验问题。

## 文档元信息

| 字段 | 值 |
|------|-----|
| 项目 | maxc-cli |
| 版本 | 0.1.0 (MVP) |
| 审查日期 | 2026-03-24 |
| 审查类型 | 代码质量与性能审查 |

---

## 问题概览

```yaml
summary:
  total_issues: 15
  critical: 5
  high: 4
  medium: 4
  low: 2
  
by_category:
  timeout_handling: 4
  error_handling: 3
  performance: 3
  api_design: 2
  observability: 2
  documentation: 1
```

---

##🔴严重问题 (Critical)

### 1. 查询执行无超时控制

**问题描述**

ODPS 实例的 `wait_for_success()` 调用**完全没有超时参数**,可能导致命令永久阻塞。这是用户报告"命令运行一分钟没反应"的根本原因。

**影响范围**
- 文件: `src/maxc_cli/backend/query.py:62`
- 文件: `src/maxc_cli/backend/job.py:30`
- 影响所有同步查询和作业等待操作

**当前代码**
```python
# query.py:62
instance = self.client.execute_sql(sql, project=project)
instance.wait_for_success()  #❌无超时,可能永久阻塞

# job.py:30
instance.wait_for_success()  #❌无超时
```

**问题表现**
- 用户执行查询后,CLI 一直等待无响应
- 无法判断命令是否还在执行还是已卡死
- 用户只能强制终止进程 (Ctrl+C)

**解决方案**

为所有 `wait_for_success()` 调用添加超时参数:

```python
# query.py - 添加 timeout 参数
def execute_query(
    self,
    sql: str,
    *,
    project: str,
    max_rows: int,
    dry_run: bool,
    offset: int = 0,
    timeout: int | None = None,  # ✅ 新增
) -> QueryResult:
    # ...
    instance = self.client.execute_sql(sql, project=project)
    instance.wait_for_success(timeout=timeout or 300)  # ✅ 默认 5 分钟
```

```python
# job.py - 添加 timeout 参数
def wait_job(
    self, 
    job_id: str, 
    *, 
    project: str | None = None,
    timeout: int | None = None  # ✅ 新增
) -> JobInfo:
    instance = self._get_instance(job_id, project=project)
    try:
        instance.wait_for_success(timeout=timeout or 300)  # ✅ 默认 5 分钟
    except Exception:
        pass
    return self._instance_to_job_info(instance, project=project or self.project)
```

**验收标准**
- [ ] 所有 `wait_for_success()` 调用都有超时保护
- [ ] 超时后抛出明确的错误信息
- [ ] 用户可以通过 `--timeout` 参数自定义超时时间
- [ ] 默认超时时间合理 (建议 5-10 分钟)

---

### 2. CLI 的 --timeout 参数定义了但未使用

**问题描述**

`cli.py:52` 定义了 `--timeout` 参数,但在 `app.py` 的查询方法中**从未使用这个参数**,导致用户无法控制超时。

**影响范围**
- 文件: `src/maxc_cli/cli.py:52` (定义)
- 文件: `src/maxc_cli/app.py` (未使用)

**当前代码**
```python
# cli.py:52
query_parser.add_argument("--timeout", type=int)  # ✅ 定义了参数

# 但在 app.py 的 query() 方法中完全没有引用这个参数  #❌未使用
```

**解决方案**

在 CLI 层传递 timeout 参数到应用层:

```python
# cli.py - _handle_query 函数
def _handle_query(args: argparse.Namespace) -> None:
    # ...
    envelope = app.query(
        command=command,
        sql=sql,
        project=args.project,
        max_rows=max_rows,
        cursor=cursor,
        dry_run=args.dry_run,
        async_mode=args.async_mode,
        cost_check=args.cost_check,
        idempotency_key=args.idempotency_key,
        retry_on=retry_on,
        max_retries=max_retries,
        timeout=args.timeout,  # ✅ 新增:传递 timeout 参数
    )
```

```python
# app.py - query() 方法签名
def query(
    self,
    *,
    command: str,
    sql: str,
    project: str | None = None,
    max_rows: int = 100,
    cursor: str | None = None,
    dry_run: bool = False,
    async_mode: bool = False,
    cost_check: float | None = None,
    idempotency_key: str | None = None,
    retry_on: list[str] | None = None,
    max_retries: int = 0,
    timeout: int | None = None,  # ✅ 新增
) -> Envelope:
```

**验收标准**
- [ ] `--timeout` 参数可以在 CLI 中使用
- [ ] timeout 值正确传递到后端执行层
- [ ] 超时后返回明确的错误信息

---

### 3. 缺少查询进度反馈机制

**问题描述**

对于长时间运行的查询,用户完全不知道当前状态,只能盲目等待。

**影响范围**
- 所有同步查询操作
- 影响用户体验和 Agent 的可观测性

**问题表现**
- 执行查询后无任何反馈
- 无法判断是正在执行、排队等待还是已失败
- Agent 无法向用户报告进度

**解决方案**

添加 NDJSON 流式进度输出:

```python
# app.py - query() 方法
def query(self, *, ..., stream: bool = False, ...) -> Envelope:
    if stream:
        # 输出初始状态
        emit_ndjson({
            "type": "progress",
            "status": "submitted",
            "job_id": instance.id,
            "timestamp": now_utc_iso()
        })
        
        # 轮询进度
        start_time = monotonic()
        while not instance.is_finished():
            elapsed = int((monotonic() - start_time) * 1000)
            emit_ndjson({
                "type": "progress",
                "status": "running",
                "elapsed_ms": elapsed,
                "job_id": instance.id
            })
            time.sleep(3)  # 每 3 秒输出一次
        
        instance.wait_for_success(timeout=timeout)
```

CLI 添加 `--stream` 参数:
```python
query_parser.add_argument("--stream", action="store_true", 
                         help="Stream progress updates")
```

**验收标准**
- [ ] `--stream` 模式下定期输出进度
- [ ] 输出包含已用时间、作业 ID 等信息
- [ ] 完成后输出最终结果

---

### 4. SQLite 连接超时过短可能导致锁竞争

**问题描述**

`cache.py:102` 的 SQLite 连接超时只有 5 秒,在高并发场景可能导致锁竞争失败。

**影响范围**
- 文件: `src/maxc_cli/cache.py:102`
- 所有缓存操作

**当前代码**
```python
# cache.py:102
conn = sqlite3.connect(str(self.db_path), timeout=5.0)  # 只有 5 秒
```

**问题场景**
- 多个命令同时访问缓存
- 缓存构建期间其他操作被阻塞
- 5 秒后抛出 `OperationalError: database is locked`

**解决方案**

增加超时时间或使用 WAL 模式:

```python
# cache.py:102
conn = sqlite3.connect(str(self.db_path), timeout=30.0)  # ✅ 增加到 30 秒

# 或在初始化时启用 WAL 模式
def _init_db(self) -> None:
    with self._connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")  # ✅ 允许多个读取器
        conn.executescript("""...""")
```

**验收标准**
- [ ] SQLite 连接超时增加到 30 秒
- [ ] 启用 WAL 模式提高并发性能
- [ ] 高并发场景下无锁竞争错误

---

### 5. 作业等待采用阻塞模式而非轮询

**问题描述**

`job.py:30` 的 `wait_job()` 使用阻塞式的 `wait_for_success()`,而不是轮询检查状态,无法提供中间状态反馈。

**影响范围**
- 文件: `src/maxc_cli/backend/job.py:26-31`
- `maxc job wait` 命令

**当前代码**
```python
# job.py:26-31
def wait_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
    instance = self._get_instance(job_id, project=project)
    try:
        instance.wait_for_success()  #❌阻塞等待
    except Exception:
        pass
    return self._instance_to_job_info(instance, project=project or self.project)
```

**解决方案**

改为轮询模式,定期检查状态:

```python
def wait_job(
    self, 
    job_id: str, 
    *, 
    project: str | None = None,
    poll_interval: int = 3,
    timeout: int | None = None
) -> JobInfo:
    instance = self._get_instance(job_id, project=project)
    
    start_time = monotonic()
    while True:
        # 检查超时
        if timeout and (monotonic() - start_time) > timeout:
            raise TimeoutError(
                f"Job {job_id} did not complete within {timeout} seconds"
            )
        
        # 检查状态
        try:
            instance.reload(blocking=False)  # 非阻塞刷新
        except Exception:
            pass
        
        status = str(getattr(instance, "status", "")).split(".")[-1]
        if status != "RUNNING":
            break
        
        # 等待后重试
        time.sleep(poll_interval)
    
    return self._instance_to_job_info(instance, project=project or self.project)
```

**验收标准**
- [ ] `wait_job()` 使用轮询而非阻塞
- [ ] 支持自定义轮询间隔
- [ ] 支持超时控制
- [ ] 可以获取中间状态

---

##高优先级问题 (High)

### 6. 错误分类不准确导致重试逻辑失效

**问题描述**

`app.py:2078-2091` 的重试逻辑只重试特定错误码,但 `classify_failure_reason()` 的分类可能不准确。

**影响范围**
- 文件: `src/maxc_cli/app.py:2078-2091`
- 文件: `src/maxc_cli/helpers.py` (classify_failure_reason)

**当前代码**
```python
# app.py:2078-2091
except MaxCError as exc:
    attempts += 1
    can_retry = (
        attempts <= max_retries
        and exc.recoverable
        and exc.error_code in retry_on  #❌依赖准确的 error_code
    )
    if not can_retry:
        raise
```

**问题**
- ODPS 错误码种类繁多,映射不完整
- 某些可重试错误被错误分类为不可重试
- 用户配置的 `--retry-on` 可能不生效

**解决方案**

改进错误分类逻辑,基于错误消息模式匹配:

```python
# helpers.py - classify_failure_reason()
def classify_failure_reason(failure_reason: str | None) -> dict[str, Any]:
    if not failure_reason:
        return {
            "category": "unknown",
            "retryable": None,
            "summary": "No failure reason available.",
        }
    
    lowered = failure_reason.lower()
    
    # 更精确的模式匹配
    if any(token in lowered for token in ("quota", "resource", "timeout", "timed out")):
        return {
            "category": "resource_exhausted",
            "retryable": True,  # ✅ 资源问题通常可重试
            "summary": "Resource or quota exceeded; retrying may help.",
        }
    
    if any(token in lowered for token in ("syntax", "parse error", "invalid")):
        return {
            "category": "sql_syntax_error",
            "retryable": False,  # ✅ 语法错误不可重试
            "summary": "SQL syntax error; check query and retry.",
        }
    
    if any(token in lowered for token in ("permission", "denied", "unauthorized")):
        return {
            "category": "permission_denied",
            "retryable": False,  # ✅ 权限问题不可重试
            "summary": "Permission denied; check credentials and retry.",
        }
    
    # 网络/连接问题 - 可重试
    if any(token in lowered for token in ("connection", "network", "socket")):
        return {
            "category": "network_error",
            "retryable": True,
            "summary": "Network or connection error; retry recommended.",
        }
    
    return {
        "category": "unknown",
        "retryable": None,
        "summary": "Unclassified error; review logs and determine if retry is appropriate.",
    }
```

**验收标准**
- [ ] 错误分类覆盖常见错误模式
- [ ] 可重试错误正确标记为 `retryable: true`
- [ ] 不可重试错误正确标记为 `retryable: false`
- [ ] 用户可通过 `--retry-on` 指定错误码

---

### 7. 缓存构建缺少进度追踪和取消机制

**问题描述**

`cli.py:750-800` 的缓存构建过程虽然有超时保护,但缺少进度追踪和取消机制。

**影响范围**
- 文件: `src/maxc_cli/cli.py:750-800`
- `maxc cache build` 命令

**当前代码**
```python
# cli.py:773-779
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    future = executor.submit(_do_fetch)
    try:
        return future.result(timeout=30)  # ✅ 30秒超时
    except concurrent.futures.TimeoutError:
        return table_name, f"{table_name}: timeout after 30s"
```

**问题**
- 用户不知道缓存构建进度
- 无法取消正在进行的构建
- 失败后无法恢复

**解决方案**

添加进度追踪和状态记录:

```python
# cli.py - _build_cache_sync()
def _build_cache_sync(app: MaxCApp, project: str, tables: list[str]) -> None:
    build_id = f"build_{int(monotonic())}"
    app.cache.start_build(project, build_id, len(tables))
    
    processed = 0
    failed = 0
    
    for table_name in tables:
        try:
            # 调用带超时的获取
            result = _fetch_table_metadata_with_timeout(app, project, table_name)
            processed += 1
            
            # 更新进度
            app.cache.update_build_progress(project, build_id, processed, failed)
            
            # 输出进度 (每 3 秒)
            if processed % 10 == 0:
                print(f"Progress: {processed}/{len(tables)} tables processed")
                
        except Exception as exc:
            failed += 1
            print(f"Failed to fetch metadata for {table_name}: {exc}")
    
    # 完成构建
    app.cache.complete_build(project, build_id)
    print(f"Cache build completed: {processed} succeeded, {failed} failed")
```

**验收标准**
- [ ] 缓存构建记录开始时间
- [ ] 定期更新进度
- [ ] 输出进度信息 (每 N 个表)
- [ ] 记录完成状态和失败数量

---

### 8. 本地作业存储使用 JSON 文件而非数据库

**问题描述**

`store.py` 使用 JSON 文件存储作业状态,在大量作业时性能下降且不具备原子性。

**影响范围**
- 文件: `src/maxc_cli/store.py`
- 本地作业存储

**当前代码**
```python
# store.py
class JobStore:
    def __init__(self, state_dir: Path):
        self.jobs_file = state_dir / "jobs.json"
        self.jobs = self._load()
    
    def _load(self) -> dict:
        if self.jobs_file.exists():
            return json.loads(self.jobs_file.read_text())
        return {"jobs": {}, "idempotency": {}}
    
    def _save(self) -> None:
        self.jobs_file.write_text(json.dumps(self.jobs))
```

**问题**
- JSON 文件读写不具备原子性
- 大量作业时内存占用大
- 无法高效查询和过滤
- 并发写入可能丢失数据

**解决方案**

迁移到 SQLite 或使用原子写入:

```python
# store.py - 使用 SQLite
class JobStore:
    def __init__(self, state_dir: Path):
        self.db_path = state_dir / "jobs.db"
        self._init_db()
    
    def _init_db(self):
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    sql TEXT NOT NULL,
                    project TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
                
                CREATE TABLE IF NOT EXISTS idempotency (
                    key TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)
    
    def create_job(self, sql, project, result, idempotency_key):
        job_id = str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO jobs (job_id, sql, project, status, result_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (job_id, sql, project, "pending", json.dumps(result), now_utc_iso(), now_utc_iso())
            )
            if idempotency_key:
                conn.execute(
                    "INSERT INTO idempotency (key, job_id, created_at) VALUES (?, ?, ?)",
                    (idempotency_key, job_id, now_utc_iso())
                )
        return {"job_id": job_id, ...}
```

**验收标准**
- [ ] 作业存储使用 SQLite
- [ ] 支持高效查询和过滤
- [ ] 并发安全
- [ ] 向后兼容现有数据格式

---

##中优先级问题 (Medium)

### 9. 缺少查询执行时间的日志记录

**问题描述**

审计日志中缺少查询执行时间的记录,无法分析慢查询。

**影响范围**
- 文件: `src/maxc_cli/audit.py`
- 文件: `src/maxc_cli/app.py`

**当前日志格式**
```json
{
  "timestamp": "2026-03-24T10:00:00Z",
  "command": "query",
  "status": "success",
  "metadata": {
    "project": "my_project",
    "elapsed_ms": 1234
  }
}
```

**问题**
- 有 `elapsed_ms` 但没有分解为各个阶段
- 无法区分是 SQL 执行慢还是结果读取慢

**解决方案**

添加详细的时序记录:

```python
# app.py - query()
def query(self, *, sql: str, ...) -> Envelope:
    timings = {}
    
    # 提交阶段
    t_start = monotonic()
    instance = self.client.execute_sql(sql, project=project)
    timings["submit_ms"] = int((monotonic() - t_start) * 1000)
    
    # 等待阶段
    t_start = monotonic()
    instance.wait_for_success(timeout=timeout)
    timings["wait_ms"] = int((monotonic() - t_start) * 1000)
    
    # 读取阶段
    t_start = monotonic()
    result = self._instance_to_query_result(instance, ...)
    timings["read_ms"] = int((monotonic() - t_start) * 1000)
    
    # 总时间
    timings["total_ms"] = sum(timings.values())
    
    # 记录到审计日志
    self.log(command, "success", {
        **metadata,
        "timings": timings  # ✅ 新增
    })
```

**验收标准**
- [ ] 审计日志包含 `timings` 字段
- [ ] 分解为 submit/wait/read 三个阶段
- [ ] 可以分析慢查询的瓶颈

---

### 10. 错误消息中缺少可操作的调试信息

**问题描述**

错误消息缺少 logview URL 等调试信息,用户难以排查问题。

**影响范围**
- 所有错误处理路径
- 文件: `src/maxc_cli/exceptions.py`

**当前错误输出**
```json
{
  "error": {
    "code": "SQL_ERROR",
    "message": "ODPS-0130013: ..."
  }
}
```

**问题**
- 缺少 logview URL
- 缺少作业 ID
- 缺少建议的调试命令

**解决方案**

在错误元数据中包含调试信息:

```python
# app.py - 错误处理
except MaxCError as exc:
    envelope = Envelope(
        command=command,
        status="failure",
        error=exc.to_payload(),
        metadata={
            "project": target_project,
            "sql_executed": sql,
            "job_id": result.job_id if hasattr(result, "job_id") else None,
            "logview": result.logview if hasattr(result, "logview") else None,
            "debug_commands": [
                f"maxc job status {result.job_id}",
                f"maxc job diagnose {result.job_id}"
            ] if hasattr(result, "job_id") else []
        }
    )
```

**验收标准**
- [ ] 错误响应包含 job_id (如果有)
- [ ] 错误响应包含 logview URL (如果有)
- [ ] 错误响应包含调试命令建议

---

### 11. 缓存清理策略不明确

**问题描述**

缓存没有自动清理机制,可能无限增长。

**影响范围**
- 文件: `src/maxc_cli/cache.py`
- 缓存管理

**问题**
- 查询会话无限累积
- 旧元数据缓存占用空间
- 没有 TTL 策略

**解决方案**

添加缓存清理命令和自动清理策略:

```python
# cache.py
def cleanup_old_sessions(self, keep_hours: int = 24) -> int:
    """清理超过 keep_hours 的会话"""
    with self._connect() as conn:
        cursor = conn.execute(
            "DELETE FROM query_sessions WHERE datetime(created_at) < datetime('now', ?)",
            (f"-{keep_hours} hours",)
        )
        return cursor.rowcount

def cleanup_stale_metadata(self, stale_days: int = 7) -> int:
    """清理超过 stale_days 的元数据"""
    with self._connect() as conn:
        cursor = conn.execute(
            "DELETE FROM table_metadata WHERE datetime(updated_at) < datetime('now', ?)",
            (f"-{stale_days} days",)
        )
        return cursor.rowcount

# cli.py - 添加清理命令
cache_clean = meta_subparsers.add_parser("clean", help="Clean cache")
cache_clean.add_argument("--sessions", action="store_true", help="Clean old sessions")
cache_clean.add_argument("--metadata", action="store_true", help="Clean stale metadata")
cache_clean.add_argument("--days", type=int, default=7, help="Keep days for metadata")
cache_clean.add_argument("--hours", type=int, default=24, help="Keep hours for sessions")
```

**验收标准**
- [ ] `maxc cache clean` 命令可用
- [ ] 支持清理会话和元数据
- [ ] 可配置保留时间
- [ ] 自动清理策略 (每次构建时清理)

---

### 12. 缺少命令执行的总体超时控制

**问题描述**

即使用户设置了 `--timeout`,整个命令执行链路没有总体超时控制。

**影响范围**
- 所有 CLI 命令
- 文件: `src/maxc_cli/cli.py`

**问题场景**
- 用户设置 `--timeout 60`
- 但实际执行了 5 分钟才超时
- 因为超时只作用于 `wait_for_success()`,不包括其他阶段

**解决方案**

在 CLI 层添加总体超时控制:

```python
# cli.py - _handle_query()
def _handle_query(args: argparse.Namespace) -> None:
    timeout = args.timeout
    
    if timeout:
        # 使用信号或线程实现总体超时
        import signal
        
        def timeout_handler(signum, frame):
            print(f"Error: Command timed out after {timeout} seconds", file=sys.stderr)
            sys.exit(1)
        
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        
        try:
            envelope = app.query(...)
        finally:
            signal.alarm(0)  # 取消闹钟
    else:
        envelope = app.query(...)
```

或使用线程池:

```python
import concurrent.futures

def _handle_query(args: argparse.Namespace) -> None:
    timeout = args.timeout
    
    if timeout:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(app.query, ...)
            try:
                envelope = future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                print(f"Error: Command timed out after {timeout} seconds", file=sys.stderr)
                sys.exit(1)
    else:
        envelope = app.query(...)
```

**验收标准**
- [ ] `--timeout` 控制整个命令执行时间
- [ ] 超时后快速退出
- [ ] 错误信息明确

---

##低优先级问题 (Low)

### 13. 缺少性能基准测试

**问题描述**

项目缺少性能基准测试,无法评估改进效果。

**影响范围**
- 测试基础设施
- 性能监控

**解决方案**

添加基准测试套件:

```python
# tests/test_performance.py
import pytest
import time

@pytest.mark.benchmark
def test_query_execution_time(benchmark):
    """基准测试: 简单查询执行时间"""
    def run_query():
        app = MaxCApp(...)
        return app.query(sql="SELECT 1", project="test")
    
    result = benchmark(run_query)
    assert result.elapsed_ms < 1000  # 应该在 1 秒内完成

@pytest.mark.benchmark
def test_cache_lookup_time(benchmark):
    """基准测试: 缓存查找时间"""
    def lookup_cache():
        cache = LocalCache(...)
        return cache.get_cached_table("project", "table")
    
    duration = benchmark(lookup_cache)
    assert duration < 0.01  # 应该在 10ms 内完成
```

**验收标准**
- [ ] 关键操作有基准测试
- [ ] CI 中运行基准测试
- [ ] 性能回归时告警

---

### 14. 文档中缺少故障排查指南

**问题描述**

用户遇到问题时缺少系统的故障排查指南。

**影响范围**
- 文档
- 用户体验

**解决方案**

添加故障排查文档:

```markdown
# docs/troubleshooting.md

## 命令执行超时

### 症状
- 命令运行后一直等待无响应
- 超过预期时间仍未完成

### 排查步骤
1. 检查作业状态: `maxc job status <job_id>`
2. 查看诊断信息: `maxc job diagnose <job_id>`
3. 检查 logview: 访问响应中的 logview URL

### 解决方案
- 使用 `--timeout` 设置超时时间
- 检查 MaxCompute 集群状态
- 优化 SQL 查询

## 缓存锁竞争

### 症状
- `database is locked` 错误

### 解决方案
- 等待后重试
- 避免同时执行多个缓存构建命令
```

**验收标准**
- [ ] 常见问题都有文档
- [ ] 包含排查步骤
- [ ] 提供解决方案

---

### 15. 缺少对 Agent 的性能建议

**问题描述**

Agent 使用时缺少性能相关的最佳实践指导。

**影响范围**
- 文档
- SKILL 文档

**解决方案**

在 SKILL 文档中添加性能建议:

```markdown
# .maxc/skills/performance.md

## 查询性能优化

### 使用分页
对于大结果集,使用 `--page-size` 避免一次性加载:
```bash
maxc query "SELECT * FROM large_table" --page-size 1000
```

### 使用异步模式
对于长时间运行的查询,使用异步模式:
```bash
maxc job submit "SELECT ..." --json
# 获取 job_id 后轮询状态
maxc job wait <job_id> --stream
```

### 设置合理的超时时间
```bash
maxc query "SELECT ..." --timeout 300  # 5分钟超时
```

### 利用缓存
元数据查询使用缓存:
```bash
maxc meta list-tables  # 使用缓存,快速返回
maxc cache build       # 定期刷新缓存
```
```

**验收标准**
- [ ] SKILL 文档包含性能建议
- [ ] Agent 可以使用最佳实践
- [ ] 示例代码可直接使用

---

## 实施优先级

### 第一阶段 (立即修复 - P0)

1. ✅ **为 `wait_for_success()` 添加超时参数** - 解决用户报告的卡死问题
2. ✅ **实现 `--timeout` 参数** - 让用户可以控制超时
3. ✅ **添加查询进度反馈** - 提升可观测性
4. ✅ **增加 SQLite 连接超时** - 避免锁竞争
5. ✅ **改进 `wait_job()` 为轮询模式** - 提供状态反馈

**预计工作量**: 2-3 天

### 第二阶段 (高优先级 - P1)

6. ✅ **改进错误分类逻辑** - 提升重试效果
7. ✅ **缓存构建进度追踪** - 提升可观测性
8. ✅ **本地作业存储迁移** - 提升性能和可靠性
9. ✅ **添加时序记录** - 便于性能分析

**预计工作量**: 3-4 天

### 第三阶段 (中低优先级 - P2/P3)

10. 增强错误调试信息
11. 缓存清理策略
12. 总体超时控制
13. 性能基准测试
14. 故障排查文档
15. Agent 性能建议

**预计工作量**: 4-5 天

---

## 总结

本次审查发现了 15 个关键问题,其中:
- **5 个严重问题**: 主要涉及超时控制和进度反馈
- **4 个高优先级问题**: 涉及错误处理、缓存和存储
- **4 个中优先级问题**: 涉及可观测性和维护性
- **2 个低优先级问题**: 涉及测试和文档

**最紧急的修复**:
1. 为所有 `wait_for_success()` 调用添加超时
2. 实现 `--timeout` 参数的传递和使用
3. 添加长任务的进度输出机制

这些改进将显著提升 maxc-cli 的可靠性和用户体验,特别是解决用户报告的"命令运行一分钟没反应"的问题。

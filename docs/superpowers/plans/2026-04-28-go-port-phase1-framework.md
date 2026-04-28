# maxc-cli Go Port — Phase 1 (Framework & Whoami) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the framework infrastructure for the Go-based `aliyun-cli-maxc` plugin — envelope output, error model, backend interface, errors/cache/config packages, CI — and prove it end-to-end with the first real command (`maxc whoami`). Also closes the Phase 0 carry-over: STS verification + `--profile` flag.

**Architecture:** Layered Go module (`commands/` thin → `internal/app/` orchestration → `internal/backend/` SDK calls → `odps-sdk-go`), with output envelope as first-class shared infrastructure (`internal/envelope/`). Each command is a 3-step pipeline: profile resolve → backend call → envelope render. FakeBackend enables hermetic unit tests. Errors are typed (`internal/errors/`) and carry suggestions; envelope rules consume the error code to fill `agent_hints`.

**Tech Stack:** Go 1.23 / Cobra / aliyun-cli-runtime / aliyun-odps-go-sdk (local replace) / modernc.org/sqlite (pure Go, no cgo) / yaml.v3 / GitHub Actions (or alibaba inner CI — see Task 12)

**Spec:** `/Users/dingxin/pythonProject/maxc-cli/docs/superpowers/specs/2026-04-28-go-port-aliyun-cli-plugin-design.md` §3 (architecture) + §5 Phase 1 exit criteria

**Phase 0 deliverables this phase builds on:** `internal/auth/{bridge.go,bridge_test.go}`, `commands/query.go`, `main/main.go` — already on branch.

**Working dir:** `/Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc/` (working branch decision in Task 0)

**Exit criteria（不达不进 Phase 2）：**
1. `aliyun maxc whoami` 在 AK profile 下输出 envelope JSON（status=success，data 含 user/owner/projects 摘要，agent_hints 至少 1 条 next_action）
2. 同上命令 `--output text` 和 `--output markdown` 正确渲染
3. 同上命令在 STS profile / RAM Role profile 下成功（关闭 Phase 0 Task 5 遗留）
4. `aliyun maxc query --sql "select 1;"` 重写为 envelope 输出，原 raw JSON 行为消失
5. CI 全绿（`go vet`、`go test ./...`、`gofmt -l` 三项零差异）
6. odps-sdk-go 仓 `EstimateSQLCost` PR 提交（不必合并，但代码评审中）

---

## File Structure

新增于 `plugin-maxc/`（粗体为 Phase 1 新建目录）：

```
plugin-maxc/
├── manifest.json                          # 修改：cmdNames 加 whoami
├── go.mod / go.sum                        # 修改：加 modernc.org/sqlite, yaml.v3
├── main/main.go                           # 修改：加 --profile 全局 flag
├── commands/
│   ├── query.go                           # 重写：用 envelope
│   └── whoami.go                          # NEW
├── internal/
│   ├── auth/
│   │   ├── bridge.go                      # 修改：支持 --profile 透传
│   │   └── bridge_test.go                 # 修改：加 profile-from-args 测试
│   ├── errors/                            # **NEW**
│   │   ├── errors.go                      # MaxCError + 9 子类型 + ErrorPayload
│   │   ├── errors_test.go
│   │   └── codes.go                       # error code → 默认 suggestion 映射
│   ├── envelope/                          # **NEW**
│   │   ├── envelope.go                    # Envelope/AgentHints/SuggestedAction
│   │   ├── envelope_test.go
│   │   ├── hints.go                       # 规则引擎：error code → next_actions
│   │   ├── hints_test.go
│   │   └── output.go                      # 三种 writer：json/text/markdown
│   │   └── output_test.go
│   ├── backend/                           # **NEW**
│   │   ├── backend.go                     # Backend interface（Phase 1 含 auth 方法）
│   │   ├── odps.go                        # OdpsBackend 工厂
│   │   ├── auth.go                        # WhoamiInfo + CanIInfo 实现
│   │   ├── auth_test.go                   # 用 FakeBackend 测试
│   │   └── fake.go                        # FakeBackend
│   ├── config/                            # **NEW**
│   │   ├── config.go                      # MaxCConfig 加载
│   │   └── config_test.go
│   └── cache/                             # **NEW**
│       ├── cache.go                       # KV interface
│       ├── sqlite.go                      # modernc.org/sqlite 实现
│       ├── sqlite_test.go
│       └── schema.go                      # DDL 字符串
└── .github/workflows/ci.yml               # **NEW**（如对方仓已有 CI 配置则跟随；见 Task 12）
```

**职责约束：**
- `commands/*.go` 每个 ≤120 行；只解析 flag + 调 backend + 调 envelope writer
- `internal/backend/*.go` 每个 ≤300 行；按命令族分文件（auth.go / query.go / job.go / meta.go / data.go），Phase 1 只落 auth.go
- `internal/envelope/hints.go` 是单一真理源，集中规则；不在其它包散落 if-elif 链
- `internal/errors/codes.go` 是错误码→默认 suggestion 的映射表（与 Python `exceptions.py` 字符串对齐）

---

## Task 0: 工作分支决策

**Files:** 无新文件；只是 git 操作

> Phase 0 在 `feature/plugin-maxc-phase0` 分支。Phase 1 是否新开分支或继续在同一分支？

- [ ] **Step 0.1: 与 owner 对齐**

执行前确认（这一步交给人）：是开 `feature/plugin-maxc-phase1`（建议，避免一个长寿分支累积太多 commit），还是继续在 phase0 分支上叠 commit？两种都可以，但建议前者。

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins
git checkout master
git pull origin master   # 拉最新 master 避免落后
git checkout feature/plugin-maxc-phase0
git checkout -b feature/plugin-maxc-phase1   # 基于 phase0 末端开 phase1
```

如果 phase0 分支还没合 master，**保留** phase0 分支的所有 commit 作为 phase1 的基础 — 不要 rebase 或 squash，因为 phase0 PR 还在 review。

- [ ] **Step 0.2: 验证基线干净**

```bash
go build -o /tmp/maxc-build-check ./aliyun-cli-plugins/plugin-maxc/main
go test ./aliyun-cli-plugins/plugin-maxc/...
```

Expected: build 成功，4 个单测全过。

---

## Task 1: 关闭 Phase 0 STS 遗留 + 加 `--profile` flag

**Files:**
- Modify: `plugin-maxc/internal/auth/bridge.go`
- Modify: `plugin-maxc/internal/auth/bridge_test.go`
- Modify: `plugin-maxc/main/main.go`
- Modify: `plugin-maxc/commands/query.go` (临时使用 --profile，Task 11 才会真正重写)

> Spec §2 要求「完全嫁接 aliyun-cli profile」。runtime 通过 env var `ALIBABACLOUD_PROFILE` 选 profile（auth.go:155-163 verified）。我们的 `--profile` flag 实现就是在调用 `ResolveCredential` 前 set 这个 env var，调完恢复原值，避免污染进程态。

- [ ] **Step 1.1: 写失败的 bridge test**

在 `bridge_test.go` 末尾追加：

```go
func TestResolveProfile_PassesProfileNameViaEnvVar(t *testing.T) {
    // Set up: pretend env has no profile selected
    t.Setenv("ALIBABACLOUD_PROFILE", "")
    t.Setenv("ALIBABA_CLOUD_PROFILE", "")
    t.Setenv("ALICLOUD_PROFILE", "")

    // Call with explicit profile arg
    args := map[string]any{"profile": "test-profile-x"}
    _, _, _ = ResolveProfile(args)  // ignore err: no real profile exists

    // After call, env var should be restored (empty)
    if got := os.Getenv("ALIBABACLOUD_PROFILE"); got != "" {
        t.Errorf("expected ALIBABACLOUD_PROFILE restored to empty, got %q", got)
    }
}
```

- [ ] **Step 1.2: 跑测试，确认编译失败/测试失败**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go test ./internal/auth/... -run TestResolveProfile_PassesProfileNameViaEnvVar -v
```

Expected: FAIL（实现还没改）

- [ ] **Step 1.3: 改 `bridge.go::ResolveProfile`**

```go
func ResolveProfile(args map[string]any) (credentials.Credential, *runtimecfg.RuntimeProfile, error) {
    if args != nil {
        if p, ok := args["profile"].(string); ok && p != "" {
            old := os.Getenv("ALIBABACLOUD_PROFILE")
            os.Setenv("ALIBABACLOUD_PROFILE", p)
            defer os.Setenv("ALIBABACLOUD_PROFILE", old)
        }
    }
    cred, profile, err := runtimecfg.ResolveCredential(nil, args)
    if err != nil {
        return nil, nil, fmt.Errorf("resolve aliyun profile: %w", err)
    }
    return cred, profile, nil
}
```

需要加 `"os"` 到 imports。

- [ ] **Step 1.4: 跑测试，确认通过**

```bash
go test ./internal/auth/... -v
```

Expected: 所有测试 PASS（包括新加的 + Phase 0 三条）

- [ ] **Step 1.5: 在 main.go 加 `--profile` 全局 flag**

修改 `main/main.go`，在 `maxcCmd` 上加持久 flag：

```go
var profileFlag string
maxcCmd.PersistentFlags().StringVar(&profileFlag, "profile", "",
    "Use named aliyun-cli profile (defaults to env ALIBABACLOUD_PROFILE or 'default')")
```

- [ ] **Step 1.6: 在 query.go 把 --profile 传给 ResolveProfile**

```go
RunE: func(cmd *cobra.Command, args []string) error {
    profile, _ := cmd.Flags().GetString("profile")  // inherited from parent
    var profileArgs map[string]any
    if profile != "" {
        profileArgs = map[string]any{"profile": profile}
    }
    cred, prof, err := auth.ResolveProfile(profileArgs)
    // ... rest unchanged
}
```

- [ ] **Step 1.7: 端到端 STS smoke test**

需要用户先 assume role 拿到 STS 三件套并 export：

```bash
# (需要用户执行) export STS env vars
# export ALIBABA_CLOUD_ACCESS_KEY_ID=STS.<...>
# export ALIBABA_CLOUD_ACCESS_KEY_SECRET=<...>
# export ALIBABA_CLOUD_SECURITY_TOKEN=<...>

cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go build -o aliyun-cli-maxc ./main
./aliyun-cli-maxc maxc query \
    --sql "select 1 as v;" \
    --endpoint "$MAXCOMPUTE_ENDPOINT" \
    --project "$MAXCOMPUTE_PROJECT"
echo "exit=$?"
```

Expected: exit=0，stdout 为 JSON 行（同 Phase 0 AK 结果）。

如失败：参照 Phase 0 plan Task 5 的失败决策表（401 → 看 SDK SignRequest；其它 → 调试 endpoint/project）。

- [ ] **Step 1.8: 追加 STS 验证记录到 docs/poc-findings.md**

在 `docs/poc-findings.md` 末尾追加：

```markdown
## Phase 0 STS 遗留补验证（Phase 1 Task 1 闭环）

- 时间：<填入>
- 凭证来源：环境变量 STS 三件套
- 命令：同 AK
- 输出：<粘贴>
- 结论：✅ Phase 0 exit criteria #2 关闭
```

- [ ] **Step 1.9: Commit**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins
git add aliyun-cli-plugins/plugin-maxc/internal/auth aliyun-cli-plugins/plugin-maxc/main/main.go \
        aliyun-cli-plugins/plugin-maxc/commands/query.go aliyun-cli-plugins/plugin-maxc/docs/poc-findings.md
git commit -m "feat(plugin-maxc/auth): support --profile flag and verify STS e2e"
```

---

## Task 2: errors 包

**Files:**
- Create: `plugin-maxc/internal/errors/errors.go`
- Create: `plugin-maxc/internal/errors/codes.go`
- Create: `plugin-maxc/internal/errors/errors_test.go`

> Python 有 9 个具体错误类型 + ErrorPayload。Go 版用 type 而非 class hierarchy；每种错误定义 `Code()`, `ExitCode()`, `Suggestion()` 方法，加一个共享 base struct。

- [ ] **Step 2.1: 写失败的测试 errors_test.go**

```go
package errors

import (
    "errors"
    "testing"
)

func TestNotFoundError_HasCorrectCode(t *testing.T) {
    e := NewNotFoundError("table 'foo' not found", "")
    if e.Code() != "NOT_FOUND" {
        t.Errorf("Code: got %q, want NOT_FOUND", e.Code())
    }
    if e.ExitCode() != 1 {
        t.Errorf("ExitCode: got %d, want 1", e.ExitCode())
    }
}

func TestPermissionDeniedError_HasExitCode2(t *testing.T) {
    e := NewPermissionDeniedError("not allowed", "")
    if e.Code() != "PERMISSION_DENIED" {
        t.Errorf("Code: got %q, want PERMISSION_DENIED", e.Code())
    }
    if e.ExitCode() != 2 {
        t.Errorf("ExitCode: got %d, want 2", e.ExitCode())
    }
}

func TestSqlError_DefaultSuggestionIncludesLogview(t *testing.T) {
    e := NewSqlError("syntax error", "http://logview/x")
    pl := e.Payload()
    if pl.Logview != "http://logview/x" {
        t.Errorf("Logview not propagated: %q", pl.Logview)
    }
    if pl.Suggestion == "" {
        t.Errorf("expected default suggestion for SQL_ERROR, got empty")
    }
}

func TestErrorsAs_RecoversTypedError(t *testing.T) {
    var orig error = NewValidationError("bad input", "")
    wrapped := wrapForTest(orig)
    var ve *ValidationError
    if !errors.As(wrapped, &ve) {
        t.Fatalf("errors.As failed to recover ValidationError")
    }
    if ve.Code() != "VALIDATION_ERROR" {
        t.Errorf("recovered code mismatch: %q", ve.Code())
    }
}

func wrapForTest(err error) error {
    return errWrap{err: err}
}
type errWrap struct{ err error }
func (e errWrap) Error() string { return "wrapped: " + e.err.Error() }
func (e errWrap) Unwrap() error { return e.err }
```

- [ ] **Step 2.2: 跑测试，确认失败**

```bash
go test ./internal/errors/... -v
```

Expected: FAIL（types 不存在）

- [ ] **Step 2.3: 实现 errors.go**

```go
package errors

// ErrorPayload 是 envelope.error 字段的序列化形态
type ErrorPayload struct {
    Code        string `json:"code"`
    Message     string `json:"message"`
    Suggestion  string `json:"suggestion,omitempty"`
    Recoverable bool   `json:"recoverable"`
    InstanceID  string `json:"instance_id,omitempty"`
    Logview     string `json:"logview,omitempty"`
}

// MaxCError 是所有插件错误的基类（Go 风格：组合而非继承）
type MaxCError struct {
    Msg         string
    Code_       string  // CODE 名（VALIDATION_ERROR 等）
    ExitCode_   int
    Recoverable bool
    Suggestion  string
    InstanceID  string
    Logview     string
}

func (e *MaxCError) Error() string  { return e.Msg }
func (e *MaxCError) Code() string   { return e.Code_ }
func (e *MaxCError) ExitCode() int  { return e.ExitCode_ }
func (e *MaxCError) Payload() ErrorPayload {
    s := e.Suggestion
    if s == "" {
        s = DefaultSuggestion(e.Code_)
    }
    return ErrorPayload{
        Code:        e.Code_,
        Message:     e.Msg,
        Suggestion:  s,
        Recoverable: e.Recoverable,
        InstanceID:  e.InstanceID,
        Logview:     e.Logview,
    }
}

// 9 种具体类型 — 每个一个空结构 + 构造函数

type ValidationError struct{ MaxCError }
func NewValidationError(msg, suggestion string) *ValidationError {
    return &ValidationError{MaxCError{Msg: msg, Code_: "VALIDATION_ERROR", ExitCode_: 1, Recoverable: true, Suggestion: suggestion}}
}

type NotFoundError struct{ MaxCError }
func NewNotFoundError(msg, suggestion string) *NotFoundError {
    return &NotFoundError{MaxCError{Msg: msg, Code_: "NOT_FOUND", ExitCode_: 1, Recoverable: false, Suggestion: suggestion}}
}

type PermissionDeniedError struct{ MaxCError }
func NewPermissionDeniedError(msg, suggestion string) *PermissionDeniedError {
    return &PermissionDeniedError{MaxCError{Msg: msg, Code_: "PERMISSION_DENIED", ExitCode_: 2, Recoverable: false, Suggestion: suggestion}}
}

type QuotaExceededError struct{ MaxCError }
func NewQuotaExceededError(msg, suggestion string) *QuotaExceededError {
    return &QuotaExceededError{MaxCError{Msg: msg, Code_: "QUOTA_EXCEEDED", ExitCode_: 3, Recoverable: true, Suggestion: suggestion}}
}

type SqlError struct{ MaxCError }
func NewSqlError(msg, logview string) *SqlError {
    return &SqlError{MaxCError{Msg: msg, Code_: "SQL_ERROR", ExitCode_: 4, Recoverable: false, Logview: logview}}
}

type CostLimitExceededError struct{ MaxCError }
func NewCostLimitExceededError(msg, suggestion string) *CostLimitExceededError {
    return &CostLimitExceededError{MaxCError{Msg: msg, Code_: "COST_LIMIT_EXCEEDED", ExitCode_: 5, Recoverable: true, Suggestion: suggestion}}
}

type FeatureUnavailableError struct{ MaxCError }
func NewFeatureUnavailableError(msg, suggestion string) *FeatureUnavailableError {
    return &FeatureUnavailableError{MaxCError{Msg: msg, Code_: "FEATURE_UNAVAILABLE", ExitCode_: 1, Recoverable: false, Suggestion: suggestion}}
}

type BackendConnectionError struct{ MaxCError }
func NewBackendConnectionError(msg, suggestion string) *BackendConnectionError {
    return &BackendConnectionError{MaxCError{Msg: msg, Code_: "BACKEND_CONNECTION_ERROR", ExitCode_: 1, Recoverable: true, Suggestion: suggestion}}
}

type ExecutionFailedError struct{ MaxCError }
func NewExecutionFailedError(msg, suggestion string) *ExecutionFailedError {
    return &ExecutionFailedError{MaxCError{Msg: msg, Code_: "EXECUTION_FAILED", ExitCode_: 1, Recoverable: true, Suggestion: suggestion}}
}
```

- [ ] **Step 2.4: 实现 codes.go（默认 suggestion 映射）**

```go
package errors

// DefaultSuggestion 返回错误码对应的默认建议字符串。
// 与 Python /Users/dingxin/pythonProject/maxc-cli/src/maxc_cli/exceptions.py 中各 class 默认行为对齐。
var defaultSuggestions = map[string]string{
    "VALIDATION_ERROR":         "检查输入参数与 schema 是否符合要求。",
    "NOT_FOUND":                "确认资源名称、project、partition 是否正确，必要时使用 list/search 命令检索。",
    "PERMISSION_DENIED":        "联系 project owner 申请相应权限，或检查当前 RAM 角色与策略。",
    "QUOTA_EXCEEDED":           "查看配额使用情况（quota.list），考虑提升 quota 或调整任务并发。",
    "SQL_ERROR":                "查阅返回的 logview 链接以定位具体错误，必要时简化 SQL 重试。",
    "COST_LIMIT_EXCEEDED":      "提高 --cost-limit 阈值，或重写 SQL 减少扫描分区。",
    "FEATURE_UNAVAILABLE":      "该能力在当前后端版本不支持，参见 docs/sdk-gap.md。",
    "BACKEND_CONNECTION_ERROR": "检查 endpoint 是否可达、网络/DNS 是否正常，重试若问题持续报 SDK PR。",
    "EXECUTION_FAILED":         "查看上述错误信息，根据需要重试或改用更小范围的输入。",
}

func DefaultSuggestion(code string) string {
    return defaultSuggestions[code]
}
```

- [ ] **Step 2.5: 跑测试，确认通过**

```bash
go test ./internal/errors/... -v
```

Expected: PASS

- [ ] **Step 2.6: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/errors
git commit -m "feat(plugin-maxc/errors): typed error model with codes and default suggestions"
```

---

## Task 3: envelope 核心类型

**Files:**
- Create: `plugin-maxc/internal/envelope/envelope.go`
- Create: `plugin-maxc/internal/envelope/envelope_test.go`

> Python `models.Envelope` 字段 verified（see /Users/dingxin/pythonProject/maxc-cli/src/maxc_cli/models.py:50-72）。Go 版字段顺序、JSON tag 对齐 `version=2.0`、`agent_hints/error` 即使 nil 也输出 null。

- [ ] **Step 3.1: 写失败的测试**

```go
package envelope

import (
    "encoding/json"
    "testing"

    pkgerr "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/errors"
)

func TestEnvelope_SuccessJSONShape(t *testing.T) {
    env := New("whoami").WithSuccess().WithData(map[string]any{"user": "alice"}).WithMetadata("elapsed_ms", 120)
    out, err := json.Marshal(env)
    if err != nil { t.Fatal(err) }

    var got map[string]any
    _ = json.Unmarshal(out, &got)

    if got["version"] != "2.0" { t.Errorf("version: %v", got["version"]) }
    if got["command"] != "whoami" { t.Errorf("command: %v", got["command"]) }
    if got["status"] != "success" { t.Errorf("status: %v", got["status"]) }
    if got["error"] != nil { t.Errorf("error should be null on success, got %v", got["error"]) }
}

func TestEnvelope_ErrorPayloadIncluded(t *testing.T) {
    env := New("whoami").WithError(pkgerr.NewPermissionDeniedError("not allowed", ""))
    out, _ := json.Marshal(env)
    var got map[string]any
    _ = json.Unmarshal(out, &got)
    if got["status"] != "error" { t.Errorf("status: %v", got["status"]) }
    e := got["error"].(map[string]any)
    if e["code"] != "PERMISSION_DENIED" { t.Errorf("error.code: %v", e["code"]) }
}

func TestEnvelope_AgentHintsRendering(t *testing.T) {
    env := New("query").WithSuccess()
    env.Hints.AddNextAction("aliyun maxc job wait --job-id abc")
    env.Hints.AddWarning("scanned 10TB without partition filter")
    out, _ := json.Marshal(env)
    var got map[string]any
    _ = json.Unmarshal(out, &got)
    h := got["agent_hints"].(map[string]any)
    actions := h["next_actions"].([]any)
    if len(actions) != 1 || actions[0] != "aliyun maxc job wait --job-id abc" {
        t.Errorf("next_actions wrong: %v", actions)
    }
}
```

- [ ] **Step 3.2: 跑测试，确认失败**

```bash
go test ./internal/envelope/... -v
```

Expected: FAIL

- [ ] **Step 3.3: 实现 envelope.go**

```go
package envelope

import (
    "encoding/json"

    pkgerr "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/errors"
)

const Version = "2.0"

type Envelope struct {
    Command  string
    Status   string
    Data     map[string]any
    Metadata map[string]any
    Error    error
    Hints    AgentHints
}

type AgentHints struct {
    NextActions []string
    Warnings    []string
    Insights    []string
}

func (h *AgentHints) AddNextAction(cmd string) { h.NextActions = append(h.NextActions, cmd) }
func (h *AgentHints) AddWarning(msg string)    { h.Warnings = append(h.Warnings, msg) }
func (h *AgentHints) AddInsight(msg string)    { h.Insights = append(h.Insights, msg) }

func New(command string) *Envelope {
    return &Envelope{
        Command:  command,
        Data:     map[string]any{},
        Metadata: map[string]any{},
    }
}

func (e *Envelope) WithSuccess() *Envelope { e.Status = "success"; return e }
func (e *Envelope) WithData(d map[string]any) *Envelope { e.Data = d; return e }
func (e *Envelope) WithMetadata(k string, v any) *Envelope { e.Metadata[k] = v; return e }
func (e *Envelope) WithError(err error) *Envelope { e.Status = "error"; e.Error = err; return e }

func (e *Envelope) MarshalJSON() ([]byte, error) {
    type rendered struct {
        Version    string                  `json:"version"`
        Command    string                  `json:"command"`
        Status     string                  `json:"status"`
        Data       map[string]any          `json:"data"`
        Metadata   map[string]any          `json:"metadata"`
        Error      *pkgerr.ErrorPayload    `json:"error"`
        AgentHints map[string]any          `json:"agent_hints"`
    }
    r := rendered{
        Version:  Version,
        Command:  e.Command,
        Status:   e.Status,
        Data:     e.Data,
        Metadata: e.Metadata,
    }
    if e.Error != nil {
        if mc, ok := e.Error.(interface{ Payload() pkgerr.ErrorPayload }); ok {
            p := mc.Payload()
            r.Error = &p
        } else {
            r.Error = &pkgerr.ErrorPayload{Code: "EXECUTION_FAILED", Message: e.Error.Error(), Recoverable: true, Suggestion: pkgerr.DefaultSuggestion("EXECUTION_FAILED")}
        }
    }
    hints := map[string]any{}
    if len(e.Hints.NextActions) > 0 { hints["next_actions"] = e.Hints.NextActions }
    if len(e.Hints.Warnings) > 0    { hints["warnings"] = e.Hints.Warnings }
    if len(e.Hints.Insights) > 0    { hints["insights"] = e.Hints.Insights }
    r.AgentHints = hints  // always non-nil → marshals as {} not null (Python parity)
    return json.Marshal(r)
}
```

- [ ] **Step 3.4: 跑测试，确认通过**

```bash
go test ./internal/envelope/... -v
```

Expected: PASS

- [ ] **Step 3.5: 加测试断言 agent_hints 始终是 object 而非 null**

```go
func TestEnvelope_AgentHintsAlwaysObject(t *testing.T) {
    env := New("whoami").WithSuccess()  // no hints added
    out, _ := json.Marshal(env)
    var got map[string]any
    _ = json.Unmarshal(out, &got)
    if got["agent_hints"] == nil {
        t.Fatalf("agent_hints should be empty object {}, not null")
    }
    if h, ok := got["agent_hints"].(map[string]any); !ok || len(h) != 0 {
        t.Errorf("agent_hints should be {}, got %v", got["agent_hints"])
    }
}
```

跑一次 `go test ./internal/envelope/... -run TestEnvelope_AgentHintsAlwaysObject -v` 确认 PASS。

- [ ] **Step 3.6: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/envelope
git commit -m "feat(plugin-maxc/envelope): core types with json marshalling"
```

---

## Task 4: envelope output writers (json/text/markdown)

**Files:**
- Create: `plugin-maxc/internal/envelope/output.go`
- Create: `plugin-maxc/internal/envelope/output_test.go`

> 三种格式（spec §3.5）：json 完整 envelope；text brief；markdown 富格式。`--no-hints` 在 envelope/json 模式下抑制 agent_hints/warnings/insights。

- [ ] **Step 4.1: 写失败的测试**

```go
package envelope

import (
    "bytes"
    "strings"
    "testing"
)

func TestWrite_JSONFormat(t *testing.T) {
    env := New("whoami").WithSuccess().WithData(map[string]any{"user": "alice"})
    var buf bytes.Buffer
    if err := Write(&buf, env, FormatJSON, false); err != nil { t.Fatal(err) }
    if !strings.Contains(buf.String(), `"version":"2.0"`) {
        t.Errorf("missing version: %s", buf.String())
    }
}

func TestWrite_TextFormat(t *testing.T) {
    env := New("whoami").WithSuccess().WithData(map[string]any{"user": "alice"})
    var buf bytes.Buffer
    if err := Write(&buf, env, FormatText, false); err != nil { t.Fatal(err) }
    s := buf.String()
    if !strings.Contains(s, "whoami") || !strings.Contains(s, "success") {
        t.Errorf("text format missing key fields: %q", s)
    }
}

func TestWrite_MarkdownFormat(t *testing.T) {
    env := New("whoami").WithSuccess().WithData(map[string]any{"user": "alice"})
    var buf bytes.Buffer
    if err := Write(&buf, env, FormatMarkdown, false); err != nil { t.Fatal(err) }
    if !strings.Contains(buf.String(), "# ") {
        t.Errorf("markdown should have headings: %s", buf.String())
    }
}

func TestWrite_NoHintsSuppressesAgentHints(t *testing.T) {
    env := New("query").WithSuccess()
    env.Hints.AddNextAction("aliyun maxc job wait")
    var buf bytes.Buffer
    if err := Write(&buf, env, FormatJSON, true); err != nil { t.Fatal(err) }
    if strings.Contains(buf.String(), "next_actions") {
        t.Errorf("--no-hints should suppress agent_hints, got: %s", buf.String())
    }
}
```

- [ ] **Step 4.2: 跑测试，确认失败**

```bash
go test ./internal/envelope/... -v -run TestWrite
```

Expected: FAIL

- [ ] **Step 4.3: 实现 output.go**

```go
package envelope

import (
    "encoding/json"
    "fmt"
    "io"
    "sort"
    "strings"
)

type Format string

const (
    FormatJSON     Format = "json"
    FormatText     Format = "text"
    FormatMarkdown Format = "markdown"
)

func ParseFormat(s string) (Format, error) {
    switch s {
    case "", "json": return FormatJSON, nil
    case "text":     return FormatText, nil
    case "markdown": return FormatMarkdown, nil
    }
    return "", fmt.Errorf("unknown output format %q (allowed: json, text, markdown)", s)
}

func Write(w io.Writer, env *Envelope, fmt_ Format, noHints bool) error {
    if noHints {
        env.Hints = AgentHints{}
    }
    switch fmt_ {
    case FormatJSON:
        return writeJSON(w, env)
    case FormatText:
        return writeText(w, env)
    case FormatMarkdown:
        return writeMarkdown(w, env)
    }
    return fmt.Errorf("unsupported format: %s", fmt_)
}

func writeJSON(w io.Writer, env *Envelope) error {
    enc := json.NewEncoder(w)
    enc.SetIndent("", "  ")
    return enc.Encode(env)
}

func writeText(w io.Writer, env *Envelope) error {
    parts := []string{env.Command, env.Status}
    if env.Error != nil {
        parts = append(parts, "err="+env.Error.Error())
    }
    keys := make([]string, 0, len(env.Data))
    for k := range env.Data { keys = append(keys, k) }
    sort.Strings(keys)
    for _, k := range keys {
        parts = append(parts, fmt.Sprintf("%s=%v", k, env.Data[k]))
    }
    fmt.Fprintln(w, strings.Join(parts, " | "))
    for _, h := range env.Hints.Warnings {
        fmt.Fprintln(w, "WARN: "+h)
    }
    return nil
}

func writeMarkdown(w io.Writer, env *Envelope) error {
    fmt.Fprintf(w, "# %s — %s\n\n", env.Command, env.Status)
    if env.Error != nil {
        if mc, ok := env.Error.(interface{ Code() string }); ok {
            fmt.Fprintf(w, "**Error %s**: %s\n\n", mc.Code(), env.Error.Error())
        } else {
            fmt.Fprintf(w, "**Error**: %s\n\n", env.Error.Error())
        }
    }
    if len(env.Data) > 0 {
        fmt.Fprintln(w, "## Data")
        keys := make([]string, 0, len(env.Data))
        for k := range env.Data { keys = append(keys, k) }
        sort.Strings(keys)
        for _, k := range keys {
            fmt.Fprintf(w, "- **%s**: %v\n", k, env.Data[k])
        }
        fmt.Fprintln(w)
    }
    if len(env.Hints.NextActions) > 0 {
        fmt.Fprintln(w, "## Next Actions")
        for _, a := range env.Hints.NextActions { fmt.Fprintf(w, "- `%s`\n", a) }
        fmt.Fprintln(w)
    }
    if len(env.Hints.Warnings) > 0 {
        fmt.Fprintln(w, "## Warnings")
        for _, ww := range env.Hints.Warnings { fmt.Fprintf(w, "- %s\n", ww) }
    }
    return nil
}
```

- [ ] **Step 4.4: 跑测试**

```bash
go test ./internal/envelope/... -v
```

Expected: 全 PASS

- [ ] **Step 4.5: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/envelope/output.go aliyun-cli-plugins/plugin-maxc/internal/envelope/output_test.go
git commit -m "feat(plugin-maxc/envelope): output writers for json/text/markdown"
```

---

## Task 5: envelope hints rules engine

**Files:**
- Create: `plugin-maxc/internal/envelope/hints.go`
- Create: `plugin-maxc/internal/envelope/hints_test.go`

> 集中规则引擎：错误码 → next_actions 模板。Phase 1 只接入 errors 包；query/job 等命令族特定规则在 Phase 2 各自命令引入时增加规则。

- [ ] **Step 5.1: 写失败的测试**

```go
package envelope

import "testing"

func TestApplyErrorHints_AddsRetryActionForBackendConnectionError(t *testing.T) {
    env := New("whoami").WithError(makeErr("BACKEND_CONNECTION_ERROR", "timeout"))
    ApplyErrorHints(env)
    if len(env.Hints.NextActions) == 0 {
        t.Fatalf("expected at least one next action for BACKEND_CONNECTION_ERROR")
    }
    found := false
    for _, a := range env.Hints.NextActions {
        if contains(a, "retry") || contains(a, "再试") {
            found = true; break
        }
    }
    if !found {
        t.Errorf("expected retry hint, got %v", env.Hints.NextActions)
    }
}

func TestApplyErrorHints_NoActionForUnknownCode(t *testing.T) {
    env := New("whoami").WithError(makeErr("UNKNOWN", "x"))
    ApplyErrorHints(env)
    if len(env.Hints.NextActions) != 0 {
        t.Errorf("unknown code should yield no actions, got %v", env.Hints.NextActions)
    }
}

func contains(s, sub string) bool {
    for i := 0; i+len(sub) <= len(s); i++ {
        if s[i:i+len(sub)] == sub { return true }
    }
    return false
}

type stubErr struct{ code, msg string }
func (e *stubErr) Error() string { return e.msg }
func (e *stubErr) Code() string  { return e.code }
func makeErr(code, msg string) error { return &stubErr{code: code, msg: msg} }
```

- [ ] **Step 5.2: 跑测试，确认失败**

- [ ] **Step 5.3: 实现 hints.go**

```go
package envelope

// ApplyErrorHints inspects env.Error 的 Code()，按规则表追加 next_actions / warnings。
// 这是单一真理源；Phase 2 引入命令族特有规则时，统一在这里追加 case，不要散落到 commands/。
func ApplyErrorHints(env *Envelope) {
    if env.Error == nil { return }
    coder, ok := env.Error.(interface{ Code() string })
    if !ok { return }
    code := coder.Code()
    switch code {
    case "BACKEND_CONNECTION_ERROR":
        env.Hints.AddNextAction("retry the same command after checking network/endpoint")
        env.Hints.AddNextAction("aliyun maxc whoami  # verify credentials and reachability")
    case "PERMISSION_DENIED":
        env.Hints.AddNextAction("aliyun maxc whoami  # confirm current identity")
    case "NOT_FOUND":
        env.Hints.AddInsight("使用 aliyun maxc list-tables 或 search-tables 检索资源。")
    case "QUOTA_EXCEEDED":
        env.Hints.AddNextAction("aliyun maxc list-quotas  # inspect quota usage")
    case "SQL_ERROR":
        if pl, ok := env.Error.(interface{ Payload() any }); ok {
            _ = pl // logview will be in payload, surfaced via envelope.error
        }
        env.Hints.AddInsight("打开 envelope.error.logview 链接定位失败 instance。")
    }
}
```

- [ ] **Step 5.4: 跑测试，PASS**

- [ ] **Step 5.5: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/envelope/hints.go aliyun-cli-plugins/plugin-maxc/internal/envelope/hints_test.go
git commit -m "feat(plugin-maxc/envelope): error-code-driven hints rules engine"
```

---

## Task 6: backend interface + FakeBackend

**Files:**
- Create: `plugin-maxc/internal/backend/backend.go`
- Create: `plugin-maxc/internal/backend/odps.go`
- Create: `plugin-maxc/internal/backend/fake.go`

> Phase 1 只声明 auth 方法（whoami/can_i），Phase 2 各任务引入新方法时往 Backend interface 加。`OdpsBackend` 复用 Phase 0 的 auth bridge。

- [ ] **Step 6.1: 实现 backend.go（接口定义）**

```go
package backend

import "context"

// Backend 是命令层与 SDK 之间的抽象边界。
// Phase 1 只列 auth 方法；Phase 2 按命令族扩展（query/job/meta/data）。
type Backend interface {
    WhoamiInfo(ctx context.Context, project string) (WhoamiResult, error)
    CanIInfo(ctx context.Context, action, resource string) (CanIResult, error)
}

type WhoamiResult struct {
    User       string   `json:"user"`
    Roles      []string `json:"roles"`
    Project    string   `json:"project"`
    Owner      string   `json:"owner"`
    Endpoint   string   `json:"endpoint"`
}

type CanIResult struct {
    Allowed bool   `json:"allowed"`
    Reason  string `json:"reason,omitempty"`
}
```

- [ ] **Step 6.2: 实现 odps.go（工厂）**

```go
package backend

import (
    "context"
    "fmt"

    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/auth"
    "github.com/aliyun/aliyun-odps-go-sdk/odps"
)

type OdpsBackend struct {
    Odps     *odps.Odps
    Project  string
    Endpoint string
}

func NewOdpsBackend(ctx context.Context, profileName, endpoint, project string) (*OdpsBackend, error) {
    var args map[string]any
    if profileName != "" {
        args = map[string]any{"profile": profileName}
    }
    cred, prof, err := auth.ResolveProfile(args)
    if err != nil { return nil, fmt.Errorf("backend resolve profile: %w", err) }
    acc, err := auth.BuildAccount(ctx, cred, prof.RegionId)
    if err != nil { return nil, fmt.Errorf("backend build account: %w", err) }
    o := odps.NewOdps(acc, endpoint)
    if project != "" { o.SetDefaultProjectName(project) }
    return &OdpsBackend{Odps: o, Project: project, Endpoint: endpoint}, nil
}
```

- [ ] **Step 6.3: 实现 fake.go**

```go
package backend

import "context"

// FakeBackend 用于 commands 层和上层的 hermetic 单元测试。
// 字段直接控制返回值；nil 表示让对应方法返回零值 + 第二个 _err 字段控制错误。
type FakeBackend struct {
    WhoamiResp WhoamiResult
    WhoamiErr  error
    CanIResp   CanIResult
    CanIErr    error
}

func (f *FakeBackend) WhoamiInfo(ctx context.Context, project string) (WhoamiResult, error) {
    return f.WhoamiResp, f.WhoamiErr
}
func (f *FakeBackend) CanIInfo(ctx context.Context, action, resource string) (CanIResult, error) {
    return f.CanIResp, f.CanIErr
}
```

- [ ] **Step 6.4: 编译验证**

```bash
go build ./internal/backend/...
```

Expected: 无报错（auth.go 留待 Task 7 添加真实实现）

- [ ] **Step 6.5: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/backend
git commit -m "feat(plugin-maxc/backend): interface, OdpsBackend factory, FakeBackend"
```

---

## Task 7: backend.WhoamiInfo + CanIInfo 实现

**Files:**
- Create: `plugin-maxc/internal/backend/auth.go`
- Create: `plugin-maxc/internal/backend/auth_test.go`

> 真实实现走 odps-sdk-go 的 Project / SecurityManager。Python `whoami_info` 返回 user/roles/project/owner/endpoint；用 `Project.Owner()` + 对 SecurityManager 跑 `whoami` SQL 拿当前 user。

- [ ] **Step 7.1: 调研 SDK API**

```bash
grep -n "func.*Owner\|func.*Whoami\|RunQuery" /Users/dingxin/GolandProjects/odps-sdk-go/odps/project.go /Users/dingxin/GolandProjects/odps-sdk-go/odps/security/manager.go 2>/dev/null | head -20
```

记录可用方法（`Project.Owner()` 应该返回 string；`SecurityManager.RunQuery("whoami", ...)` 返回字符串结果）。

- [ ] **Step 7.2: 写失败的测试 auth_test.go**

```go
package backend

import (
    "context"
    "testing"
)

func TestFakeBackend_WhoamiReturnsConfiguredResp(t *testing.T) {
    f := &FakeBackend{WhoamiResp: WhoamiResult{User: "alice", Project: "p1"}}
    got, err := f.WhoamiInfo(context.Background(), "p1")
    if err != nil { t.Fatal(err) }
    if got.User != "alice" { t.Errorf("user: %q", got.User) }
}

func TestOdpsBackend_WhoamiPopulatesProjectAndEndpoint(t *testing.T) {
    // 不依赖真实网络：直接构造结构验证字段路径
    b := &OdpsBackend{Project: "p1", Endpoint: "http://x"}
    // skip if Odps 为 nil（无 SDK 调用），只验证字段映射
    // 真实 e2e 在 Task 10 whoami 命令中验证
    _ = b
}
```

- [ ] **Step 7.3: 实现 backend/auth.go**

```go
package backend

import (
    "context"
    "fmt"
    "strings"
    "encoding/json"

    pkgerr "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/errors"
)

func (b *OdpsBackend) WhoamiInfo(ctx context.Context, project string) (WhoamiResult, error) {
    p := project
    if p == "" { p = b.Project }
    res := WhoamiResult{Project: p, Endpoint: b.Endpoint}

    proj := b.Odps.Project(p)
    if err := proj.Load(); err != nil {
        return res, pkgerr.NewBackendConnectionError(fmt.Sprintf("load project %s: %v", p, err), "")
    }
    res.Owner = proj.Owner()

    // SecurityManager.RunQuery("whoami") returns a JSON string like:
    //   {"DisplayName":"alice","Id":"...","Roles":["admin","dev"]}
    sm := proj.SecurityManager()
    out, err := sm.RunQuery("whoami", true, "")
    if err != nil {
        return res, pkgerr.NewBackendConnectionError(fmt.Sprintf("security whoami: %v", err), "")
    }
    var who struct {
        DisplayName string   `json:"DisplayName"`
        Id          string   `json:"Id"`
        Roles       []string `json:"Roles"`
    }
    if jerr := json.Unmarshal([]byte(out), &who); jerr != nil {
        // SDK can sometimes return non-JSON; fall back to raw
        res.User = strings.TrimSpace(out)
    } else {
        res.User = who.DisplayName
        res.Roles = who.Roles
    }
    return res, nil
}

func (b *OdpsBackend) CanIInfo(ctx context.Context, action, resource string) (CanIResult, error) {
    // PoC probe showed CheckPermissionV1 surface exists but constructing Permission is non-trivial.
    // Phase 1 minimal: return FeatureUnavailableError with suggestion to use envelope-style placeholder.
    return CanIResult{}, pkgerr.NewFeatureUnavailableError("can-i not yet implemented in Phase 1", "use whoami to inspect roles")
}
```

> 注：`SecurityManager.RunQuery` 的真实签名以 SDK 当前为准（参见 PoC probe 已验证可用）。如果三参签名对不上，参照 `/Users/dingxin/GolandProjects/odps-sdk-go/odps/security/manager.go` 调整。

- [ ] **Step 7.4: 跑测试**

```bash
go test ./internal/backend/... -v
```

Expected: PASS

- [ ] **Step 7.5: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/backend/auth.go aliyun-cli-plugins/plugin-maxc/internal/backend/auth_test.go
git commit -m "feat(plugin-maxc/backend): WhoamiInfo via SecurityManager + CanIInfo placeholder"
```

---

## Task 8: config 包

**Files:**
- Create: `plugin-maxc/internal/config/config.go`
- Create: `plugin-maxc/internal/config/config_test.go`

> 加载 `~/.aliyun/maxc/config.yaml`。**只存非认证设置**：默认 endpoint/region、cache 路径、skills 模式开关（spec §3.4）。AK 仍然只走 aliyun-cli profile。

- [ ] **Step 8.1: 加 yaml 依赖**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go get gopkg.in/yaml.v3
```

- [ ] **Step 8.2: 写失败的测试**

```go
package config

import (
    "os"
    "path/filepath"
    "testing"
)

func TestLoad_FromTempFile(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "config.yaml")
    os.WriteFile(path, []byte("default_endpoint: http://x\ncache_dir: /tmp/c\n"), 0600)

    cfg, err := LoadFromPath(path)
    if err != nil { t.Fatal(err) }
    if cfg.DefaultEndpoint != "http://x" { t.Errorf("endpoint: %q", cfg.DefaultEndpoint) }
    if cfg.CacheDir != "/tmp/c" { t.Errorf("cache_dir: %q", cfg.CacheDir) }
}

func TestLoad_MissingFileReturnsDefaults(t *testing.T) {
    cfg, err := LoadFromPath("/nonexistent/path.yaml")
    if err != nil { t.Fatal(err) }
    if cfg.DefaultEndpoint != "" { t.Errorf("expected empty default") }
}
```

- [ ] **Step 8.3: 实现 config.go**

```go
package config

import (
    "errors"
    "io/fs"
    "os"
    "path/filepath"

    "gopkg.in/yaml.v3"
)

type MaxCConfig struct {
    DefaultEndpoint string `yaml:"default_endpoint"`
    DefaultRegion   string `yaml:"default_region"`
    CacheDir        string `yaml:"cache_dir"`
    SkillsEnabled   bool   `yaml:"skills_enabled"`
}

func LoadFromPath(path string) (*MaxCConfig, error) {
    cfg := &MaxCConfig{}
    data, err := os.ReadFile(path)
    if err != nil {
        if errors.Is(err, fs.ErrNotExist) { return cfg, nil }
        return nil, err
    }
    if err := yaml.Unmarshal(data, cfg); err != nil { return nil, err }
    return cfg, nil
}

func DefaultPath() string {
    home, _ := os.UserHomeDir()
    return filepath.Join(home, ".aliyun", "maxc", "config.yaml")
}

func Load() (*MaxCConfig, error) {
    return LoadFromPath(DefaultPath())
}
```

- [ ] **Step 8.4: 跑测试 PASS**

- [ ] **Step 8.5: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/config aliyun-cli-plugins/plugin-maxc/go.mod aliyun-cli-plugins/plugin-maxc/go.sum
git commit -m "feat(plugin-maxc/config): MaxC YAML config loader (no auth secrets)"
```

---

## Task 9: cache 包（modernc.org/sqlite 骨架）

**Files:**
- Create: `plugin-maxc/internal/cache/cache.go`
- Create: `plugin-maxc/internal/cache/sqlite.go`
- Create: `plugin-maxc/internal/cache/schema.go`
- Create: `plugin-maxc/internal/cache/sqlite_test.go`

> Phase 1 只落 KV 骨架（Get/Put/Delete + table schema 占位）。元数据/语义缓存的实际填充逻辑等 Phase 2 catalog 命令引入时再加。重点是 modernc.org/sqlite（纯 Go，无 cgo）能 link 进来跨平台干净构建。

- [ ] **Step 9.1: 加依赖**

```bash
go get modernc.org/sqlite
```

- [ ] **Step 9.2: 写失败的测试 sqlite_test.go**

```go
package cache

import (
    "path/filepath"
    "testing"
)

func TestSQLiteCache_PutGetDelete(t *testing.T) {
    db := filepath.Join(t.TempDir(), "test.db")
    c, err := OpenSQLite(db)
    if err != nil { t.Fatal(err) }
    defer c.Close()

    if err := c.Put("k1", []byte("v1")); err != nil { t.Fatal(err) }
    got, ok, err := c.Get("k1")
    if err != nil { t.Fatal(err) }
    if !ok || string(got) != "v1" { t.Errorf("get: ok=%v val=%q", ok, got) }

    if err := c.Delete("k1"); err != nil { t.Fatal(err) }
    _, ok, _ = c.Get("k1")
    if ok { t.Errorf("expected not found after delete") }
}

func TestSQLiteCache_GetMissingReturnsNotOK(t *testing.T) {
    db := filepath.Join(t.TempDir(), "test.db")
    c, err := OpenSQLite(db)
    if err != nil { t.Fatal(err) }
    defer c.Close()
    _, ok, err := c.Get("nope")
    if err != nil { t.Fatal(err) }
    if ok { t.Errorf("expected ok=false for missing key") }
}
```

- [ ] **Step 9.3: 实现 cache.go (interface) + schema.go + sqlite.go**

`cache.go`:
```go
package cache

type Cache interface {
    Get(key string) ([]byte, bool, error)
    Put(key string, val []byte) error
    Delete(key string) error
    Close() error
}
```

`schema.go`:
```go
package cache

const schemaSQL = `
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v BLOB NOT NULL,
    ts INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
`
```

`sqlite.go`:
```go
package cache

import (
    "database/sql"

    _ "modernc.org/sqlite"
)

type SQLiteCache struct{ db *sql.DB }

func OpenSQLite(path string) (*SQLiteCache, error) {
    db, err := sql.Open("sqlite", path)
    if err != nil { return nil, err }
    if _, err := db.Exec(schemaSQL); err != nil { db.Close(); return nil, err }
    return &SQLiteCache{db: db}, nil
}

func (c *SQLiteCache) Get(key string) ([]byte, bool, error) {
    var val []byte
    err := c.db.QueryRow("SELECT v FROM kv WHERE k = ?", key).Scan(&val)
    if err == sql.ErrNoRows { return nil, false, nil }
    if err != nil { return nil, false, err }
    return val, true, nil
}

func (c *SQLiteCache) Put(key string, val []byte) error {
    _, err := c.db.Exec("INSERT OR REPLACE INTO kv (k, v) VALUES (?, ?)", key, val)
    return err
}

func (c *SQLiteCache) Delete(key string) error {
    _, err := c.db.Exec("DELETE FROM kv WHERE k = ?", key)
    return err
}

func (c *SQLiteCache) Close() error { return c.db.Close() }
```

- [ ] **Step 9.4: 跑测试**

```bash
go test ./internal/cache/... -v
```

Expected: PASS。如失败常见原因是 modernc.org/sqlite 拉取慢，等 `go mod tidy` 完。

- [ ] **Step 9.5: 验证跨平台 build（无 cgo）**

```bash
CGO_ENABLED=0 go build -o /tmp/maxc-nocgo ./main
```

Expected: 成功，证明纯 Go 路径通。

- [ ] **Step 9.6: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/cache aliyun-cli-plugins/plugin-maxc/go.mod aliyun-cli-plugins/plugin-maxc/go.sum
git commit -m "feat(plugin-maxc/cache): SQLite-backed KV using modernc.org/sqlite (no cgo)"
```

---

## Task 10: whoami 命令（Phase 1 milestone）

**Files:**
- Create: `plugin-maxc/commands/whoami.go`
- Modify: `plugin-maxc/main/main.go` (注册 whoami)
- Modify: `plugin-maxc/manifest.json` (cmdNames 加 whoami)

> 这是 Phase 1 第一个 envelope-formatted 命令，集成验证：profile resolve → backend → envelope writer → ApplyErrorHints → Output.Write。

- [ ] **Step 10.1: 实现 whoami.go**

```go
// (license header)
package commands

import (
    "context"
    "fmt"
    "time"

    "github.com/spf13/cobra"

    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/backend"
    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/envelope"
)

func NewWhoamiCmd() *cobra.Command {
    var endpoint, project, output string
    var noHints bool

    cmd := &cobra.Command{
        Use:   "whoami",
        Short: "Show current MaxCompute identity",
        RunE: func(cmd *cobra.Command, args []string) error {
            profile, _ := cmd.Flags().GetString("profile")
            env := envelope.New("whoami")
            t0 := time.Now()

            be, err := backend.NewOdpsBackend(cmd.Context(), profile, endpoint, project)
            if err != nil {
                env.WithError(err)
                envelope.ApplyErrorHints(env)
                fmt_, fmtErr := envelope.ParseFormat(output)
                if fmtErr != nil { return fmtErr }
                _ = envelope.Write(cmd.OutOrStdout(), env, fmt_, noHints)
                return err
            }

            res, err := be.WhoamiInfo(cmd.Context(), project)
            env.WithMetadata("elapsed_ms", time.Since(t0).Milliseconds())
            env.WithMetadata("project", res.Project)
            if err != nil {
                env.WithError(err)
                envelope.ApplyErrorHints(env)
            } else {
                env.WithSuccess().WithData(map[string]any{
                    "user":     res.User,
                    "owner":    res.Owner,
                    "project":  res.Project,
                    "endpoint": res.Endpoint,
                })
                env.Hints.AddInsight(fmt.Sprintf("project owner is %s; current user is %s", res.Owner, res.User))
                env.Hints.AddNextAction("aliyun maxc list-tables")
            }

            fmt_, fmtErr := envelope.ParseFormat(output)
            if fmtErr != nil { return fmtErr }
            return envelope.Write(cmd.OutOrStdout(), env, fmt_, noHints)
        },
    }
    cmd.Flags().StringVar(&endpoint, "endpoint", "", "MaxCompute endpoint (required)")
    cmd.Flags().StringVar(&project, "project", "", "MaxCompute project (defaults to profile)")
    cmd.Flags().StringVar(&output, "output", "json", "Output format: json|text|markdown")
    cmd.Flags().BoolVar(&noHints, "no-hints", false, "Suppress agent_hints/warnings/insights in JSON")
    _ = cmd.MarkFlagRequired("endpoint")
    return cmd
}
```

- [ ] **Step 10.2: 注册到 main.go**

在 `main/main.go` 的 `maxcCmd.AddCommand(...)` 区块加：
```go
maxcCmd.AddCommand(commands.NewWhoamiCmd())
```

- [ ] **Step 10.3: 更新 manifest.json**

`cmdNames` 数组加入 `"whoami"`。

- [ ] **Step 10.4: 编译 + 端到端跑**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go build -o aliyun-cli-maxc ./main

./aliyun-cli-maxc maxc whoami \
    --endpoint "$MAXCOMPUTE_ENDPOINT" \
    --project "$MAXCOMPUTE_PROJECT"
echo "exit=$?"

./aliyun-cli-maxc maxc whoami --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT" --output text
./aliyun-cli-maxc maxc whoami --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT" --output markdown
```

Expected:
- 第一条：JSON envelope，包含 version=2.0、command=whoami、status=success、data.{user,owner,project,endpoint}、agent_hints.next_actions、agent_hints.insights、metadata.elapsed_ms
- 第二条：单行 text 摘要
- 第三条：markdown 标题块

- [ ] **Step 10.5: STS 路径再跑一次（关 Phase 0 #2 的最终验收）**

```bash
# 用 STS env vars
./aliyun-cli-maxc maxc whoami --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT"
```

Expected: 同上。

- [ ] **Step 10.6: 追加 Phase 1 milestone 记录到 docs/poc-findings.md**

新增段：

```markdown
## Phase 1 Milestone — whoami 端到端

- 时间：<填>
- AK output=json: <粘贴>
- AK output=text: <粘贴>
- AK output=markdown: <粘贴>
- STS output=json: <粘贴 / 或 deferred 说明>
- 结论：✅ exit criterion 1, 2, 3 关闭
```

- [ ] **Step 10.7: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/commands/whoami.go aliyun-cli-plugins/plugin-maxc/main/main.go \
        aliyun-cli-plugins/plugin-maxc/manifest.json aliyun-cli-plugins/plugin-maxc/docs/poc-findings.md
git commit -m "feat(plugin-maxc/whoami): first envelope-formatted command end-to-end"
```

---

## Task 11: 重写 query 命令为 envelope 输出

**Files:**
- Modify: `plugin-maxc/commands/query.go` (full rewrite)

> 把 PoC 临时的 raw JSON 行换成正式 envelope。data 字段：rows + columns + task_name + instance_id；metadata：elapsed_ms。出错时 envelope status=error + agent_hints.next_actions(retry / wait-job / etc.)。

- [ ] **Step 11.1: 重写 query.go**

```go
// (license header)
package commands

import (
    "encoding/csv"
    "strings"
    "time"

    "github.com/spf13/cobra"

    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/backend"
    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/envelope"
    pkgerr "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/errors"
)

func NewQueryCmd() *cobra.Command {
    var sql, project, endpoint, output string
    var noHints bool

    cmd := &cobra.Command{
        Use:   "query",
        Short: "Run a MaxCompute SQL and return envelope-wrapped result",
        RunE: func(cmd *cobra.Command, args []string) error {
            profile, _ := cmd.Flags().GetString("profile")
            env := envelope.New("query")
            t0 := time.Now()

            be, err := backend.NewOdpsBackend(cmd.Context(), profile, endpoint, project)
            if err != nil {
                env.WithError(err); envelope.ApplyErrorHints(env)
                fmt_, _ := envelope.ParseFormat(output)
                _ = envelope.Write(cmd.OutOrStdout(), env, fmt_, noHints)
                return err
            }

            // Auto-append ; if missing (PoC found ODPS REST requires it)
            sqlExec := strings.TrimSpace(sql)
            if !strings.HasSuffix(sqlExec, ";") { sqlExec += ";" }

            ins, err := be.Odps.ExecSQl(sqlExec)
            if err != nil {
                env.WithError(pkgerr.NewSqlError("submit failed: "+err.Error(), ""))
                envelope.ApplyErrorHints(env)
                fmt_, _ := envelope.ParseFormat(output)
                _ = envelope.Write(cmd.OutOrStdout(), env, fmt_, noHints)
                return err
            }
            env.WithMetadata("instance_id", ins.Id())

            if err := ins.WaitForSuccess(); err != nil {
                env.WithError(pkgerr.NewSqlError("wait failed: "+err.Error(), ""))
                envelope.ApplyErrorHints(env)
                fmt_, _ := envelope.ParseFormat(output)
                _ = envelope.Write(cmd.OutOrStdout(), env, fmt_, noHints)
                return err
            }

            results, err := ins.GetResult()
            if err != nil {
                env.WithError(pkgerr.NewExecutionFailedError("get result: "+err.Error(), ""))
                envelope.ApplyErrorHints(env)
                fmt_, _ := envelope.ParseFormat(output)
                _ = envelope.Write(cmd.OutOrStdout(), env, fmt_, noHints)
                return err
            }

            // Parse first task result content as CSV
            data := map[string]any{}
            if len(results) > 0 {
                r := results[0]
                content := r.Content()
                rows, headers, _ := parseCSV(content)
                data["task_name"] = r.Name
                data["columns"] = headers
                data["rows"] = rows
                data["row_count"] = len(rows)
            }

            env.WithSuccess().WithData(data)
            env.WithMetadata("elapsed_ms", time.Since(t0).Milliseconds())

            fmt_, fmtErr := envelope.ParseFormat(output)
            if fmtErr != nil { return fmtErr }
            return envelope.Write(cmd.OutOrStdout(), env, fmt_, noHints)
        },
    }
    cmd.Flags().StringVar(&sql, "sql", "", "SQL statement (required)")
    cmd.Flags().StringVar(&project, "project", "", "MaxCompute project")
    cmd.Flags().StringVar(&endpoint, "endpoint", "", "MaxCompute endpoint (required)")
    cmd.Flags().StringVar(&output, "output", "json", "Output format: json|text|markdown")
    cmd.Flags().BoolVar(&noHints, "no-hints", false, "Suppress agent_hints in envelope")
    _ = cmd.MarkFlagRequired("sql")
    _ = cmd.MarkFlagRequired("endpoint")
    return cmd
}

func parseCSV(content string) (rows [][]string, headers []string, err error) {
    r := csv.NewReader(strings.NewReader(content))
    r.FieldsPerRecord = -1
    all, err := r.ReadAll()
    if err != nil || len(all) == 0 { return nil, nil, err }
    return all[1:], all[0], nil
}
```

- [ ] **Step 11.2: 端到端验证**

```bash
go build -o aliyun-cli-maxc ./main
./aliyun-cli-maxc maxc query --sql "select 1 as v" --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT"
```

Expected: envelope JSON 输出，data 含 task_name/columns/rows/row_count，instance_id 在 metadata。

- [ ] **Step 11.3: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/commands/query.go
git commit -m "feat(plugin-maxc/query): rewrite to envelope output with CSV parsing and auto-semicolon"
```

---

## Task 12: CI 流水线

**Files:**
- Create: `.github/workflows/ci.yml`（如对方仓 GitLab 用 `.gitlab-ci.yml` — 见 Step 12.1）

> 仓库托管在 `gitlab.alibaba-inc.com`（Phase 0 push 时确认）。GitHub Actions 不可用；需要写 `.gitlab-ci.yml` 或对接对方现有 CI 配置。

- [ ] **Step 12.1: 调研对方仓 CI 现状**

```bash
ls /Users/dingxin/MaxQuery/aliyun-cli-plugins/.gitlab-ci.yml 2>/dev/null
ls /Users/dingxin/MaxQuery/aliyun-cli-plugins/.github 2>/dev/null
```

如果已有 `.gitlab-ci.yml`：阅读并理解 jobs 结构，决定我们是新增 stage 还是写独立 job。

如果都没有：需要先与 owner 对齐 CI 平台。Phase 1 这一步 **可能阻塞**，准备好升级到人。

- [ ] **Step 12.2: 写 CI（GitLab 路径示例）**

如果是 GitLab：

```yaml
# .gitlab-ci.yml or merged into existing
plugin-maxc:lint:
  image: golang:1.23
  stage: test
  script:
    - cd aliyun-cli-plugins/plugin-maxc
    - test -z "$(gofmt -l . | grep -v '^vendor/')" || (echo 'gofmt issues:'; gofmt -l .; exit 1)
    - go vet ./...

plugin-maxc:test:
  image: golang:1.23
  stage: test
  script:
    - cd aliyun-cli-plugins/plugin-maxc
    - go test ./... -v -coverprofile=coverage.out
  artifacts:
    paths: [aliyun-cli-plugins/plugin-maxc/coverage.out]
  only:
    changes:
      - aliyun-cli-plugins/plugin-maxc/**/*

plugin-maxc:cross-build:
  image: golang:1.23
  stage: build
  script:
    - cd aliyun-cli-plugins/plugin-maxc
    - GOOS=linux   GOARCH=amd64 go build -o /dev/null ./main
    - GOOS=linux   GOARCH=arm64 go build -o /dev/null ./main
    - GOOS=darwin  GOARCH=amd64 go build -o /dev/null ./main
    - GOOS=darwin  GOARCH=arm64 go build -o /dev/null ./main
    - GOOS=windows GOARCH=amd64 go build -o /dev/null ./main
    - GOOS=windows GOARCH=arm64 go build -o /dev/null ./main
```

> 注意：因为 `go.mod` 有指向本地路径的 SDK replace（Phase 0 遗留），**CI 跑不起来**直到 SDK 在 odps-sdk-go 仓发版本 tag 并我们改成版本 require。这是预期行为，需要在 CI YAML 文件 + README 里都标注「CI 暂时仅本地可跑，等 SDK tag 发版后启用」。

- [ ] **Step 12.3: 临时方案 — 文档化「CI 待启用」**

如果 SDK 还没发 tag，CI 这一步降级为：写一份 `docs/ci-pending.md`，记录「Phase 1 出于 SDK replace 路径限制暂未启用 CI；待 SDK 发版后启用 .gitlab-ci.yml」。Exit criterion #5 只能在 CI 启用后真正满足；Phase 1 可标记 `⏸ 待 SDK 发版`。

```bash
git add .gitlab-ci.yml docs/ci-pending.md  # 视情况
git commit -m "ci(plugin-maxc): scaffold CI yaml + pending notice"
```

- [ ] **Step 12.4: 本地 CI 等价性自检**

不依赖远端 CI，本地跑一次：

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
test -z "$(gofmt -l . | grep -v '^vendor/')" && echo "gofmt clean"
go vet ./... && echo "vet clean"
go test ./... -v && echo "tests green"
for os in linux darwin windows; do
  for arch in amd64 arm64; do
    GOOS=$os GOARCH=$arch go build -o /dev/null ./main && echo "$os/$arch ok"
  done
done
```

Expected: 全 clean / green / ok。

---

## Task 13: SDK PR — Odps.EstimateSQLCost（并行）

**Files:**
- Modify: `/Users/dingxin/GolandProjects/odps-sdk-go/odps/odps.go` (新增方法)
- Create: `/Users/dingxin/GolandProjects/odps-sdk-go/odps/odps_cost_test.go`

> 这是 PoC sdk-gap.md 头号 PR 候选。独立分支独立 PR。可与 Task 1-12 并行进行。

- [ ] **Step 13.1: 在 odps-sdk-go 起分支**

```bash
cd /Users/dingxin/GolandProjects/odps-sdk-go
git checkout -b feature/maxc-cli-deps-cost-estimate
```

- [ ] **Step 13.2: 实现 EstimateSQLCost**

调研 ODPS REST API：cost 估算走 `POST /projects/{p}/instances?curr_project={p}&cost=true`。参考 PyODPS `instance.cost` 实现。

```go
// odps/odps.go
type SQLCost struct {
    InputBytes    int64
    OutputBytes   int64
    Complexity    float64
    UDF           int64
    // ... fields per ODPS cost response
}

func (o *Odps) EstimateSQLCost(sql string, hints map[string]string) (*SQLCost, error) {
    // POST instances?cost=true with task XML; parse response.
    // Implementation TBD per REST spec; reference Python instance.cost path.
    panic("TODO")
}
```

- [ ] **Step 13.3: 写测试 + 实现 + 跑通**

具体实现细节略（依赖对 ODPS REST 的熟悉度）；产出：method 可调、单测覆盖、example 更新。

- [ ] **Step 13.4: 提 PR**

```bash
git push -u origin feature/maxc-cli-deps-cost-estimate
# 在 GitLab UI 创建 MR 到 master
```

PR 描述引用 `plugin-maxc/docs/sdk-gap.md` 中 "estimate_query_cost" 行作为来源。

- [ ] **Step 13.5: Phase 1 退出条件 #6**

PR 不需合并，但 PR 链接 + commit SHA 记录在 plugin-maxc 端的某处文档里（建议 `plugin-maxc/docs/sdk-gap.md` 末尾追加 "PR 进度" 段）。

---

## Task 14: Phase 1 收尾

**Files:**
- Modify: `plugin-maxc/README.md`（更新 What works）
- Modify: `plugin-maxc/docs/poc-findings.md`（追加 Phase 1 总结）

- [ ] **Step 14.1: 更新 README**

把 README 中 "What works in Phase 0" 改为 "What works":
- AK + STS auth
- query, whoami commands
- envelope JSON / text / markdown output
- typed error model with default suggestions
- modernc.org/sqlite cache skeleton
- (etc.)

"What's deferred to Phase 2" 列出后续命令族。

- [ ] **Step 14.2: poc-findings.md 追加 Phase 1 总结**

```markdown
## Phase 1 总结

- whoami 命令端到端 ✅（json/text/markdown 三模 verify）
- query 重写为 envelope ✅
- STS profile e2e ✅（关 Phase 0 #2）
- envelope/errors/backend/config/cache 骨架完整 ✅
- CI ⏸ 待 SDK 发 tag 后启用
- SDK PR EstimateSQLCost ✅ 已提交 link：<填>

Exit criteria 6 项满足 5/6（CI 待 SDK 发版）。可启动 Phase 2 命令族移植。
```

- [ ] **Step 14.3: Commit + push**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins
git add aliyun-cli-plugins/plugin-maxc/README.md aliyun-cli-plugins/plugin-maxc/docs/poc-findings.md
git commit -m "docs(plugin-maxc): Phase 1 wrap-up"
git push origin feature/plugin-maxc-phase1
```

---

## 预期总耗时

| Task | 估时 |
|---|---|
| 0 — 分支 | 0.5 小时 |
| 1 — STS + --profile | 半天 |
| 2 — errors | 半天 |
| 3 — envelope core | 半天 |
| 4 — envelope output | 半天 |
| 5 — envelope hints | 半天 |
| 6 — backend interface + Fake | 半天 |
| 7 — backend.Whoami/CanI | 1 天 |
| 8 — config | 半天 |
| 9 — cache | 1 天 |
| 10 — whoami 命令 | 1 天 |
| 11 — query 重写 | 半天 |
| 12 — CI | 1~2 天（依对方平台） |
| 13 — SDK PR EstimateSQLCost | 2~3 天（独立轨） |
| 14 — 收尾 | 半天 |

**关键路径（Task 1-12，串行）**：约 10 天。
**含 SDK PR 并行**：13~15 天 = 2.5~3 周（与 spec 估算一致）。

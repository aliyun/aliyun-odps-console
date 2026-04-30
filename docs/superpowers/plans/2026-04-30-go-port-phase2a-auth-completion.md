# maxc-cli Go Port — Phase 2a (Auth Family Completion) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close out the auth command family by porting `can-i` (permission preflight) and `login` (thin wrapper over `aliyun configure`) from Python to the Go plugin, fully envelope-formatted, with Python parity on payload fields.

**Architecture:** `can-i` follows the established whoami pattern (commands → backend interface → OdpsBackend). Permission probe degrades from Python's `execute_sql_cost` (cost API not in Go SDK) to a real `LIMIT 0` SELECT — explicit comment marks this as the divergence and how to switch back when `EstimateSQLCost` real impl lands. `login` is purely informational: emits an envelope whose `agent_hints.next_actions` point the user/agent at the concrete `aliyun configure` invocation for their case; no process spawning, no AK file writing.

**Tech Stack:** Go 1.23 · cobra · aliyun-odps-go-sdk (local replace) · existing `internal/{backend,envelope,errors,config}` packages

**Branch:** `feature/plugin-maxc-phase0` (continue per user's Phase-0/1 decision; rename happens at Phase 2 wrap)

**Repo paths:**
- Plugin code: `/Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc/`
- Python reference: `/Users/dingxin/pythonProject/maxc-cli/src/maxc_cli/backend/auth.py` + `app.py:auth_can_i,auth_login`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `internal/config/config.go` | Modify | Add `AllowedOperations []string` field with default `["SELECT"]` |
| `internal/config/config_test.go` | Modify | Test default + override of `allowed_operations` |
| `internal/backend/backend.go` | Modify | Change `CanIInfo` signature to `(ctx, tableName, operation, project string)`, expand `CanIResult` to Python parity fields |
| `internal/backend/odps.go` | Modify | Add `cfg` field + `SetConfig`/`allowedOps` helpers; replace `CanIInfo` placeholder with real impl (config gate + `LIMIT 0` probe) |
| `internal/backend/quote.go` | Create | Shared `quoteTableName` helper (Phase 2b query/meta will reuse) |
| `internal/backend/fake.go` | Modify | Update `FakeBackend.CanIInfo` to new signature |
| `internal/backend/auth_test.go` | Modify | Update existing 2 tests + add config-gate tests |
| `internal/backend/odps_canI_test.go` | Create | Integration-style test using real ODPS (skipped without env) |
| `commands/can_i.go` | Create | Cobra command for `aliyun maxc can-i` |
| `commands/login.go` | Create | Cobra command for `aliyun maxc login` (informational envelope) |
| `main/main.go` | Modify | Register both new commands |
| `manifest.json` | Modify | Add `"can-i"` and `"login"` to `cmdNames` |
| `README.md` | Modify | "What works" gains can-i + login |
| `docs/poc-findings.md` | Modify | Append Phase 2a entry (1-2 lines + commit SHA) |

**Why `commands/can_i.go` (not `cani.go`)**: matches Python CLI's `auth can-i` subcommand naming and reads cleaner.

---

## Task 0: Baseline verification

**Files:** none — sanity check only.

- [ ] **Step 0.1: Confirm branch + clean tree + build/test baseline**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins
git status -sb           # expect: clean, on feature/plugin-maxc-phase0
git log --oneline -3
cd aliyun-cli-plugins/plugin-maxc
go build ./... && go test ./...
```

Expected: clean tree, build OK, all tests PASS.

If anything fails: stop, surface to user, do not start Task 1.

---

## Task 1: Config — `AllowedOperations`

**Files:**
- Modify: `aliyun-cli-plugins/plugin-maxc/internal/config/config.go`
- Modify: `aliyun-cli-plugins/plugin-maxc/internal/config/config_test.go`

Python reference: `src/maxc_cli/config.py:402` — `allowed_operations` defaults to `["SELECT"]`, normalized to upper-case.

- [ ] **Step 1.1: Write failing test for default + uppercase normalization**

Add to `config_test.go`:

```go
func TestMaxCConfig_AllowedOperationsDefault(t *testing.T) {
    cfg, err := LoadFromPath("/nonexistent/path.yaml")
    if err != nil {
        t.Fatalf("missing file should not error: %v", err)
    }
    got := cfg.AllowedOperations
    want := []string{"SELECT"}
    if len(got) != 1 || got[0] != want[0] {
        t.Errorf("default AllowedOperations: got %v, want %v", got, want)
    }
}

func TestMaxCConfig_AllowedOperationsFromYAML(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "c.yaml")
    if err := os.WriteFile(path, []byte("allowed_operations:\n  - select\n  - describe\n"), 0o600); err != nil {
        t.Fatal(err)
    }
    cfg, err := LoadFromPath(path)
    if err != nil {
        t.Fatal(err)
    }
    want := []string{"SELECT", "DESCRIBE"}
    if !reflect.DeepEqual(cfg.AllowedOperations, want) {
        t.Errorf("AllowedOperations from yaml: got %v, want %v (uppercased)", cfg.AllowedOperations, want)
    }
}
```

Add imports `path/filepath`, `reflect` if missing.

- [ ] **Step 1.2: Run, confirm FAIL**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go test ./internal/config/ -run AllowedOperations -v
```

Expected: FAIL with "AllowedOperations undefined" or empty-default mismatch.

- [ ] **Step 1.3: Add `AllowedOperations` field + post-load default + uppercase**

```go
// MaxCConfig holds non-authentication settings for the maxc plugin.
type MaxCConfig struct {
    DefaultEndpoint   string   `yaml:"default_endpoint"`
    DefaultRegion     string   `yaml:"default_region"`
    CacheDir          string   `yaml:"cache_dir"`
    SkillsEnabled     bool     `yaml:"skills_enabled"`
    AllowedOperations []string `yaml:"allowed_operations"`
}
```

In `LoadFromPath`, after the YAML unmarshal block (success branch and missing-file branch both end up returning `cfg`), call a small helper:

```go
func (c *MaxCConfig) applyDefaults() {
    if len(c.AllowedOperations) == 0 {
        c.AllowedOperations = []string{"SELECT"}
        return
    }
    out := make([]string, 0, len(c.AllowedOperations))
    for _, op := range c.AllowedOperations {
        op = strings.TrimSpace(strings.ToUpper(op))
        if op != "" {
            out = append(out, op)
        }
    }
    c.AllowedOperations = out
}
```

Wire it into both return paths of `LoadFromPath`. Add `"strings"` import.

- [ ] **Step 1.4: Run, confirm PASS**

```bash
go test ./internal/config/ -v
```

Expected: all PASS (including pre-existing tests).

- [ ] **Step 1.5: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/config/
git commit -m "feat(plugin-maxc/config): add AllowedOperations with SELECT default"
```

---

## Task 2: Backend interface — expand `CanIResult` + change signature

**Files:**
- Modify: `aliyun-cli-plugins/plugin-maxc/internal/backend/backend.go`
- Modify: `aliyun-cli-plugins/plugin-maxc/internal/backend/fake.go`
- Modify: `aliyun-cli-plugins/plugin-maxc/internal/backend/auth_test.go`

> Why this task does both signature change and Fake update together: Go won't compile until callers match. Splitting would leave the tree red between commits.

- [ ] **Step 2.1: Update `Backend` interface + `CanIResult` struct**

Replace in `backend.go`:

```go
type Backend interface {
    WhoamiInfo(ctx context.Context, project string) (WhoamiResult, error)
    CanIInfo(ctx context.Context, tableName, operation, project string) (CanIResult, error)
}

// CanIResult is the result of a permission preflight check, mirroring the
// Python maxc_cli backend.auth.can_i_info payload one-for-one so that envelope
// consumers don't need to branch on language.
type CanIResult struct {
    ResourceType   string `json:"resource_type"`              // always "table" for now
    TableName      string `json:"table_name"`
    Project        string `json:"project"`
    Operation      string `json:"operation"`
    Allowed        bool   `json:"allowed"`
    CheckMode      string `json:"check_mode"`                 // see CheckMode* constants
    Reason         string `json:"reason,omitempty"`
    CheckErrorCode string `json:"check_error_code,omitempty"`
}

// CheckMode values describe how the permission decision was reached.
const (
    CheckModeConfigAllowedOperations = "config_allowed_operations"
    CheckModeCLIUnsupportedOperation = "cli_supported_operations"
    CheckModeODPSLimit0              = "odps_limit_0"
)
```

> Naming note: `CheckModeODPSLimit0` (not `…SQLCostLimit0`) — Python uses `odps_sql_cost_limit_0` because it goes through the cost API. We use a plain `LIMIT 0` SELECT (cost API is the placeholder). The Python envelope consumer should treat both as "real ODPS-level check" — document this in code comment.

- [ ] **Step 2.2: Update `FakeBackend.CanIInfo` signature**

In `fake.go`:

```go
func (f *FakeBackend) CanIInfo(ctx context.Context, tableName, operation, project string) (CanIResult, error) {
    return f.CanIResp, f.CanIErr
}
```

- [ ] **Step 2.3: Update existing `auth_test.go` tests for new signature**

```go
func TestFakeBackend_CanIReturnsConfiguredResponse(t *testing.T) {
    f := &FakeBackend{CanIResp: CanIResult{Allowed: true, TableName: "foo", Operation: "SELECT"}}
    got, err := f.CanIInfo(context.Background(), "foo", "SELECT", "p")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if !got.Allowed {
        t.Errorf("expected Allowed=true, got %+v", got)
    }
}

func TestFakeBackend_CanIReturnsConfiguredError(t *testing.T) {
    sentinel := errors.New("boom")
    f := &FakeBackend{CanIErr: sentinel}
    _, err := f.CanIInfo(context.Background(), "foo", "SELECT", "p")
    if !errors.Is(err, sentinel) {
        t.Errorf("expected sentinel propagated, got %v", err)
    }
}
```

- [ ] **Step 2.4: Build (will FAIL — OdpsBackend signature still old)**

```bash
go build ./internal/backend/...
```

Expected: FAIL — `OdpsBackend.CanIInfo` doesn't satisfy interface. This is intentional; Task 3 fixes it.

> Don't commit yet — tree red.

---

## Task 3: `OdpsBackend.CanIInfo` real implementation

**Files:**
- Modify: `aliyun-cli-plugins/plugin-maxc/internal/backend/odps.go`

Logic mirrors Python `auth.py:can_i_info` (49-150-ish):

1. Normalize operation to upper-case.
2. If operation NOT in `b.cfg.AllowedOperations` → `Allowed=false`, `CheckMode=ConfigAllowedOperations`, `CheckErrorCode=PERMISSION_DENIED`.
3. If operation is allowed but != "SELECT" → `Allowed=false`, `CheckMode=CLIUnsupportedOperation`, `CheckErrorCode=FEATURE_UNAVAILABLE`. (CLI only supports SELECT probe.)
4. For SELECT: build `SELECT * FROM <quoted_table> LIMIT 0;`, run via `b.Odps.ExecSQl` + `WaitForSuccess`. On success → `Allowed=true, CheckMode=ODPSLimit0`. On error → translate; if `BackendConnectionError` propagate up (not a permission verdict), otherwise `Allowed=false` with the error's code.

> Open question for execution: where does `b.cfg` come from? Look at `OdpsBackend` — Phase 1 doesn't pass `*MaxCConfig` in. **Discovery:** `NewOdpsBackend` only takes `(ctx, profile, endpoint, project)`. We need to inject config — either via a field set after construction (simpler) or by extending the constructor. Adding a setter `(b *OdpsBackend).SetConfig(*config.MaxCConfig)` is the smallest change; default-empty config still gives `AllowedOperations = ["SELECT"]` once `applyDefaults` is called by `LoadFromPath`. Use the setter approach.

- [ ] **Step 3.1: Add `cfg` field + setter + safe default (additive — keep existing `Project` field)**

The current struct (verified) is `{Odps *odps.Odps; Project string; Endpoint string}`. `WhoamiInfo` reads `b.Project` and `NewOdpsBackend` writes it; do **not** drop or rename it. Add only the new `cfg` field:

```go
type OdpsBackend struct {
    Odps     *odps.Odps
    Project  string
    Endpoint string
    cfg      *config.MaxCConfig // wired post-construction via SetConfig
}

// SetConfig wires plugin config into the backend so that command-family
// behaviour driven by config (e.g. AllowedOperations gating) is honoured.
// Safe to call with nil; future calls reading b.cfg should use b.allowedOps().
func (b *OdpsBackend) SetConfig(cfg *config.MaxCConfig) {
    b.cfg = cfg
}

func (b *OdpsBackend) allowedOps() []string {
    if b.cfg != nil && len(b.cfg.AllowedOperations) > 0 {
        return b.cfg.AllowedOperations
    }
    return []string{"SELECT"}
}
```

Add `"github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/config"` import.

> Verify no import cycle before relying on this: `go list -deps ./internal/config/... | grep plugin-maxc/internal/backend` must return empty (config must not pull in backend). Phase-1 design already assumed this; verifying anyway is cheap.

- [ ] **Step 3.2: Replace `CanIInfo` placeholder with real impl**

> Before writing `quoteTableName`: `grep -rn "quoteTableName\|QuoteTableName" internal/ commands/` was run during plan-write — confirmed it does not exist. Place it in a new shared file `internal/backend/quote.go` (not inside `odps.go`) so Phase 2b query/meta commands can reuse it without a refactor commit.

Create `internal/backend/quote.go`:

```go
// Copyright (c) 2009-present, Alibaba Cloud All rights reserved.
// (license header — copy verbatim from odps.go)

package backend

import "strings"

// quoteTableName wraps an ODPS table name in backticks, escaping embedded
// backticks. Mirrors Python helpers.quote_table_name. Shared by any backend
// method that needs to interpolate a user-supplied identifier into SQL.
func quoteTableName(name string) string {
    return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
```

Then in `odps.go`, replace `CanIInfo`:

```go
// CanIInfo answers "may I do <operation> on <tableName>?" using the same two-
// stage check Python uses: (1) config-level deny-by-default allow-list, then
// (2) for SELECT, a LIMIT 0 probe against ODPS to surface server-side ACLs.
//
// Divergence vs Python: Python issues the probe through ODPS cost API
// (execute_sql_cost). The Go SDK does not yet have a real EstimateSQLCost
// (only a placeholder); we substitute a plain `SELECT ... LIMIT 0`. When the
// SDK lands real cost support, switch to the cost path to avoid scheduling.
func (b *OdpsBackend) CanIInfo(ctx context.Context, tableName, operation, project string) (CanIResult, error) {
    op := strings.ToUpper(strings.TrimSpace(operation))
    target := project
    if target == "" {
        target = b.Odps.DefaultProjectName()
    }
    res := CanIResult{
        ResourceType: "table",
        TableName:    tableName,
        Project:      target,
        Operation:    op,
    }

    if !contains(b.allowedOps(), op) {
        res.Allowed = false
        res.CheckMode = CheckModeConfigAllowedOperations
        res.Reason = fmt.Sprintf("Configured allowed operations are limited to %s.", strings.Join(b.allowedOps(), ", "))
        res.CheckErrorCode = "PERMISSION_DENIED"
        return res, nil
    }
    if op != "SELECT" {
        res.Allowed = false
        res.CheckMode = CheckModeCLIUnsupportedOperation
        res.Reason = "This CLI currently supports only SELECT read-path permission checks."
        res.CheckErrorCode = "FEATURE_UNAVAILABLE"
        return res, nil
    }

    sql := fmt.Sprintf("SELECT * FROM %s LIMIT 0;", quoteTableName(tableName))
    ins, err := b.Odps.ExecSQl(sql)
    if err != nil {
        return classifyCanIError(res, err)
    }
    if err := ins.WaitForSuccess(); err != nil {
        return classifyCanIError(res, err)
    }
    res.Allowed = true
    res.CheckMode = CheckModeODPSLimit0
    return res, nil
}

// classifyCanIError decides whether an ODPS error is a permission verdict
// (Allowed=false, nil-error so command emits success envelope with allowed=false)
// or a transport-level failure (returns BackendConnectionError so command emits
// status=error). Crude substring-based classification is intentional for
// Phase 2a; Phase 2b introduces typed SDK errors.
func classifyCanIError(res CanIResult, err error) (CanIResult, error) {
    msg := err.Error()
    lower := strings.ToLower(msg)
    connectionMarkers := []string{
        "connection refused", "no such host", "i/o timeout",
        "tls handshake", "context deadline exceeded", "network is unreachable",
        "dial tcp",
    }
    for _, marker := range connectionMarkers {
        if strings.Contains(lower, marker) {
            return CanIResult{}, pkgerr.NewBackendConnectionError(
                fmt.Sprintf("can-i probe failed at transport: %s", msg), "")
        }
    }
    res.Allowed = false
    res.CheckMode = CheckModeODPSLimit0
    res.Reason = msg
    res.CheckErrorCode = "PERMISSION_DENIED"
    return res, nil
}

func contains(xs []string, x string) bool {
    for _, v := range xs {
        if v == x {
            return true
        }
    }
    return false
}
```

Add `"fmt"` import if missing; `"strings"` and `pkgerr "...internal/errors"` should already be present from Phase 1.

> The substring matcher is **not** a long-term solution — it will misclassify some legitimate permission errors as connection failures and vice versa. Acceptable Phase 2a baseline because Phase 2b explicitly takes on typed SDK error classification (tracked in README known-limitations, Task 9).

- [ ] **Step 3.3: Build clean + import-cycle sanity check**

```bash
go list -deps ./internal/config/... | grep "plugin-maxc/internal/backend" || echo "no cycle: config does not import backend"
go build ./...
```

Expected: first command prints the "no cycle" line; second exits 0.

- [ ] **Step 3.4: Run all tests**

```bash
go test ./...
```

Expected: all PASS (FakeBackend tests with new signature already updated in Task 2).

- [ ] **Step 3.5: Commit Tasks 2 + 3 together (atomic interface change)**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/backend/
git commit -m "feat(plugin-maxc/backend): real CanIInfo with config gate + LIMIT 0 probe"
```

> The new `internal/backend/quote.go` file is included by the directory-level `git add`.

---

## Task 4: `aliyun maxc can-i` command

**Files:**
- Create: `aliyun-cli-plugins/plugin-maxc/commands/can_i.go`

Pattern: copy `commands/whoami.go` shape exactly. Persistent `--profile` flag is registered on root.

- [ ] **Step 4.1: Write the command**

```go
// Copyright (c) 2009-present, Alibaba Cloud All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// (header — copy verbatim from whoami.go)

package commands

import (
    "fmt"
    "time"

    "github.com/spf13/cobra"

    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/backend"
    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/config"
    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/envelope"
)

// NewCanICmd builds the `aliyun maxc can-i` cobra command.
func NewCanICmd() *cobra.Command {
    var endpoint, project, table, operation, output string
    var noHints bool

    cmd := &cobra.Command{
        Use:   "can-i",
        Short: "Check whether the current identity may perform <operation> on <table>",
        Long: `Two-stage permission preflight:
  1. Plugin-side allow-list (config: allowed_operations, default ["SELECT"]).
  2. For SELECT: a LIMIT 0 probe against ODPS to surface server-side ACLs.

Other operations (INSERT/CREATE/etc.) are reported as not-supported by this
CLI rather than reaching ODPS.`,
        RunE: func(cmd *cobra.Command, args []string) error {
            profile, _ := cmd.Flags().GetString("profile")
            env := envelope.New("can-i")
            t0 := time.Now()

            be, err := backend.NewOdpsBackend(cmd.Context(), profile, endpoint, project)
            if err != nil {
                env.WithError(err)
                envelope.ApplyErrorHints(env)
                return writeAndReturn(cmd, env, output, noHints, err)
            }
            // Wire plugin config (silently no-op if config missing — defaults apply).
            if cfg, _ := config.LoadFromPath(config.DefaultPath()); cfg != nil {
                be.SetConfig(cfg)
            }

            res, berr := be.CanIInfo(cmd.Context(), table, operation, project)
            env.WithMetadata("elapsed_ms", time.Since(t0).Milliseconds())
            if res.Project != "" {
                env.WithMetadata("project", res.Project)
            }
            if berr != nil {
                env.WithError(berr)
                envelope.ApplyErrorHints(env)
                return writeAndReturn(cmd, env, output, noHints, berr)
            }

            env.WithSuccess().WithData(map[string]any{
                "resource_type":     res.ResourceType,
                "table_name":        res.TableName,
                "project":           res.Project,
                "operation":         res.Operation,
                "allowed":           res.Allowed,
                "check_mode":        res.CheckMode,
                "reason":            res.Reason,
                "check_error_code":  res.CheckErrorCode,
            })

            if !res.Allowed {
                env.Hints.AddInsight(fmt.Sprintf("denied by %s: %s", res.CheckMode, res.Reason))
                if res.CheckMode == backend.CheckModeConfigAllowedOperations {
                    env.Hints.AddNextAction(fmt.Sprintf("edit %s allowed_operations to permit this operation", config.DefaultPath()))
                }
            }
            return writeAndReturn(cmd, env, output, noHints, nil)
        },
    }
    cmd.Flags().StringVar(&endpoint, "endpoint", "", "MaxCompute endpoint (required)")
    cmd.Flags().StringVar(&project, "project", "", "MaxCompute project (defaults to profile)")
    cmd.Flags().StringVar(&table, "table", "", "Table name to check (required)")
    cmd.Flags().StringVar(&operation, "operation", "SELECT", "Operation to probe: SELECT|INSERT|... (default SELECT)")
    cmd.Flags().StringVar(&output, "output", "json", "Output format: json|text|markdown")
    cmd.Flags().BoolVar(&noHints, "no-hints", false, "Suppress agent_hints/warnings/insights")
    _ = cmd.MarkFlagRequired("endpoint")
    _ = cmd.MarkFlagRequired("table")
    return cmd
}
```

- [ ] **Step 4.2: Register in `main/main.go`**

Find the line `maxcCmd.AddCommand(commands.NewWhoamiCmd())` and add directly after:

```go
maxcCmd.AddCommand(commands.NewCanICmd())
```

- [ ] **Step 4.3: Update `manifest.json`**

`manifest.json` is a JSON object with multiple keys (verified during plan-write: `name`, `version`, `command`, `shortDescription`, `description`, `productName`, `author`, `homepage`, `license`, `productCode`, `platforms`, `minCliVersion`, `bin`, `cmdNames`). **Modify only the `cmdNames` line**, leave all other keys intact:

```json
"cmdNames": ["query", "whoami", "can-i"]
```

- [ ] **Step 4.4: Build + smoke help**

```bash
cd aliyun-cli-plugins/plugin-maxc
go build -o aliyun-cli-maxc ./main
./aliyun-cli-maxc maxc can-i --help
```

Expected: usage text shows `--table` + `--operation` + `--endpoint` flags.

- [ ] **Step 4.5: Commit (just the command + registration; e2e in Task 6)**

```bash
git add aliyun-cli-plugins/plugin-maxc/commands/can_i.go \
        aliyun-cli-plugins/plugin-maxc/main/main.go \
        aliyun-cli-plugins/plugin-maxc/manifest.json
git commit -m "feat(plugin-maxc/can-i): wire envelope-formatted permission preflight"
```

---

## Task 5: Backend tests for new CanI logic

**Files:**
- Modify: `aliyun-cli-plugins/plugin-maxc/internal/backend/auth_test.go`

Cover the three pure-logic branches (config deny, non-SELECT, SELECT path) using a `OdpsBackend` constructed by hand with a fake `*odps.Odps` field — but constructing a real `*odps.Odps` requires SDK plumbing we don't want in unit tests. Instead, test the **config-gate logic by direct field manipulation**, and test the SELECT path via the real-ODPS smoke in Task 6.

- [ ] **Step 5.1: Write config-gate tests against `OdpsBackend` (no network)**

```go
func TestOdpsBackend_CanI_DeniedByAllowedOperations(t *testing.T) {
    b := &OdpsBackend{}
    b.SetConfig(&config.MaxCConfig{AllowedOperations: []string{"SELECT"}})
    res, err := b.CanIInfo(context.Background(), "foo", "INSERT", "p")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if res.Allowed || res.CheckMode != CheckModeConfigAllowedOperations {
        t.Errorf("expected config-deny, got %+v", res)
    }
    if res.CheckErrorCode != "PERMISSION_DENIED" {
        t.Errorf("expected PERMISSION_DENIED, got %q", res.CheckErrorCode)
    }
}

func TestOdpsBackend_CanI_DeniedByCLIUnsupported(t *testing.T) {
    b := &OdpsBackend{}
    b.SetConfig(&config.MaxCConfig{AllowedOperations: []string{"SELECT", "DESCRIBE"}})
    res, err := b.CanIInfo(context.Background(), "foo", "DESCRIBE", "p")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if res.Allowed || res.CheckMode != CheckModeCLIUnsupportedOperation {
        t.Errorf("expected cli-unsupported deny, got %+v", res)
    }
    if res.CheckErrorCode != "FEATURE_UNAVAILABLE" {
        t.Errorf("expected FEATURE_UNAVAILABLE, got %q", res.CheckErrorCode)
    }
}

func TestOdpsBackend_CanI_DefaultsToSelectOnly(t *testing.T) {
    b := &OdpsBackend{} // no config set
    res, err := b.CanIInfo(context.Background(), "foo", "INSERT", "p")
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if res.Allowed {
        t.Errorf("with default allowedOps=[SELECT], INSERT should be denied; got %+v", res)
    }
}
```

Add `"github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/config"` import if missing.

- [ ] **Step 5.2: Run, confirm PASS**

```bash
go test ./internal/backend/ -run CanI -v
```

Expected: all PASS.

- [ ] **Step 5.3: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/backend/auth_test.go
git commit -m "test(plugin-maxc/backend): cover CanIInfo config-gate branches"
```

---

## Task 6: Real-ODPS smoke test — `can-i` end-to-end

**Files:** none — verification only; output captured in commit body of Task 9.

> Requires `ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET` + `MAXCOMPUTE_ENDPOINT`/`MAXCOMPUTE_PROJECT` env vars. AK profile sufficient (per Phase-0 Task-4 pattern).

- [ ] **Step 6.1: SELECT allowed path (existing table)**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
./aliyun-cli-maxc maxc can-i \
  --table t1 --operation SELECT \
  --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT"
```

Expected: envelope JSON with `status=success`, `data.allowed=true`, `data.check_mode="odps_limit_0"`, `exit=0`.

- [ ] **Step 6.2: SELECT denied path (nonexistent or no-grant table)**

```bash
./aliyun-cli-maxc maxc can-i \
  --table __no_such_table__ --operation SELECT \
  --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT"
```

Expected: envelope JSON with `status=success` (the *check* succeeded), `data.allowed=false`, `data.check_mode="odps_limit_0"`, `data.reason` populated, `exit=0`.

- [ ] **Step 6.3: Non-SELECT shortcut (no network call)**

```bash
./aliyun-cli-maxc maxc can-i \
  --table t1 --operation INSERT \
  --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT"
```

Expected: envelope with `data.allowed=false`, `data.check_mode="config_allowed_operations"` (since default config only permits SELECT), elapsed_ms small (<50ms — no ODPS round-trip).

- [ ] **Step 6.4: Capture each output to a scratch file for Task 9 commit body**

```bash
mkdir -p /tmp/maxc-phase2a
./aliyun-cli-maxc maxc can-i --table t1 --operation SELECT \
  --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT" \
  > /tmp/maxc-phase2a/canI_select_ok.json 2>&1
./aliyun-cli-maxc maxc can-i --table __no_such_table__ --operation SELECT \
  --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT" \
  > /tmp/maxc-phase2a/canI_select_deny.json 2>&1
./aliyun-cli-maxc maxc can-i --table t1 --operation INSERT \
  --endpoint "$MAXCOMPUTE_ENDPOINT" --project "$MAXCOMPUTE_PROJECT" \
  > /tmp/maxc-phase2a/canI_insert_config_deny.json 2>&1
```

> If the executor agent does not have credentials, this task halts and surfaces to user (per Phase-0 Task-4 precedent). Do not commit a half-verified state.

---

## Task 7: `aliyun maxc login` — informational envelope

**Files:**
- Create: `aliyun-cli-plugins/plugin-maxc/commands/login.go`

Per user's Phase-0 design correction: "maxc login 仅是 aliyun configure 的薄包装，不再独立写 AK". This command does not spawn `aliyun configure` (process model fragility from inside a plugin); it emits an envelope whose `agent_hints.next_actions` give the exact `aliyun configure` invocation for the user's intent. Empty data, success status.

- [ ] **Step 7.1: Write the command**

```go
// Copyright (c) 2009-present, Alibaba Cloud All rights reserved.
// (license header — copy verbatim from whoami.go)

package commands

import (
    "fmt"
    "time"

    "github.com/spf13/cobra"

    "github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/envelope"
)

// NewLoginCmd builds `aliyun maxc login` — a thin guidance command that
// points the user/agent at `aliyun configure` rather than writing AKs itself.
func NewLoginCmd() *cobra.Command {
    var profileHint, mode, output string
    var noHints bool

    cmd := &cobra.Command{
        Use:   "login",
        Short: "Show the aliyun configure invocation for setting up MaxCompute credentials",
        Long: `maxc login does NOT store credentials. Authentication is delegated to
the aliyun-cli profile system. This command emits an envelope whose
agent_hints.next_actions describe the exact 'aliyun configure' invocation
for your case (AK / StsToken / RamRoleArn / EcsRamRole / CloudSSO).`,
        RunE: func(cmd *cobra.Command, args []string) error {
            env := envelope.New("login")
            t0 := time.Now()

            data := map[string]any{
                "delegated_to":     "aliyun configure",
                "profile_hint":     profileHint,
                "mode_hint":        mode,
                "stores_aks_here":  false,
            }
            env.WithSuccess().WithData(data)
            env.WithMetadata("elapsed_ms", time.Since(t0).Milliseconds())

            // Build exact next-action commands.
            base := "aliyun configure"
            if profileHint != "" {
                base += fmt.Sprintf(" --profile %s", profileHint)
            }
            if mode != "" {
                env.Hints.AddNextAction(fmt.Sprintf("%s --mode %s", base, mode))
            } else {
                env.Hints.AddNextAction(base + " --mode AK            # AccessKey + SecretKey")
                env.Hints.AddNextAction(base + " --mode StsToken      # AK + SecurityToken (STS)")
                env.Hints.AddNextAction(base + " --mode RamRoleArn    # assume RAM role")
                env.Hints.AddNextAction(base + " --mode EcsRamRole    # ECS-attached role")
                env.Hints.AddNextAction(base + " --mode CloudSSO      # SSO browser flow")
            }
            env.Hints.AddInsight("After configuring, verify with: aliyun maxc whoami --endpoint <ep> --project <p>")
            return writeAndReturn(cmd, env, output, noHints, nil)
        },
    }
    cmd.Flags().StringVar(&profileHint, "profile-hint", "", "Profile name to embed in the suggested aliyun configure command (does NOT load credentials)")
    cmd.Flags().StringVar(&mode, "mode", "", "Auth mode to narrow suggestion: AK|StsToken|RamRoleArn|EcsRamRole|CloudSSO")
    cmd.Flags().StringVar(&output, "output", "json", "Output format: json|text|markdown")
    cmd.Flags().BoolVar(&noHints, "no-hints", false, "Suppress agent_hints/warnings/insights")
    return cmd
}
```

> **Why `--profile-hint` instead of `--profile`:** `--profile` is registered as a `PersistentFlag` on the root (per Phase 1 Task 1). Cobra `PersistentFlags` propagate to all subcommands; defining a local `--profile` on a subcommand panics at construction time with "flag redefined: profile". The login command does not load credentials, so the persistent `--profile` flag isn't authoritative for it anyway — we want a flag that only shapes the suggestion text, hence the distinct name.

- [ ] **Step 7.2: Register in `main/main.go` + manifest**

Add directly after `NewCanICmd()` registration:

```go
maxcCmd.AddCommand(commands.NewLoginCmd())
```

`manifest.json`:

```json
"cmdNames": ["query", "whoami", "can-i", "login"]
```

- [ ] **Step 7.3: Build + smoke**

```bash
go build -o aliyun-cli-maxc ./main
./aliyun-cli-maxc maxc login
echo "---with profile-hint + mode---"
./aliyun-cli-maxc maxc login --profile-hint prod --mode AK
echo "---text format---"
./aliyun-cli-maxc maxc login --output text
```

Expected: each invocation returns `status=success`, exit=0, agent_hints.next_actions populated, no network access, no file writes (verify with `lsof` or just trust the code).

- [ ] **Step 7.4: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/commands/login.go \
        aliyun-cli-plugins/plugin-maxc/main/main.go \
        aliyun-cli-plugins/plugin-maxc/manifest.json
git commit -m "feat(plugin-maxc/login): guidance-only login command pointing at aliyun configure"
```

---

## Task 8: Local CI re-verification

**Files:** none — runs the existing `scripts/ci-local.sh`.

- [ ] **Step 8.1: Run full local CI**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
bash scripts/ci-local.sh
```

Expected: gofmt clean, vet clean, test PASS, 6-platform CGO_ENABLED=0 cross-compile PASS.

If any failure: stop, fix in a follow-up commit (NOT amend), re-run.

---

## Task 9: README + poc-findings + push

**Files:**
- Modify: `aliyun-cli-plugins/plugin-maxc/README.md` ("What works" section)
- Modify: `aliyun-cli-plugins/plugin-maxc/docs/poc-findings.md` (append Phase 2a entry)

- [ ] **Step 9.1: README**

In the "What works" list, add:

```markdown
- `aliyun maxc can-i --table <t> --operation SELECT` — permission preflight (config gate + LIMIT 0 ODPS probe)
- `aliyun maxc login [--profile-hint <p>] [--mode <m>]` — guidance-only; delegates to `aliyun configure`
```

In a "Known limitations" or equivalent section, add:

```markdown
- `can-i` only supports SELECT permission probes against ODPS; INSERT/CREATE/etc.
  return `check_mode=cli_supported_operations` without reaching the server.
- `can-i` does not yet distinguish PERMISSION_DENIED from BACKEND_CONNECTION_ERROR
  inside the ODPS probe (Phase-2b: typed SDK error classification).
- `can-i` uses a real `LIMIT 0` SELECT (consumes a tiny scheduling slot) instead
  of the cost API. Will switch to `EstimateSQLCost` once the SDK lands real impl.
```

- [ ] **Step 9.2: poc-findings.md — append Phase 2a section**

```markdown
## Phase 2a 总结 (auth family completion)

- can-i 端到端 ✅ — 三模 (allowed / odps-deny / config-deny) 在真 ODPS 验证
- login 命令 ✅ — guidance-only envelope，不写 AK
- backend.CanIInfo 签名对齐 Python (table_name, operation, project)
- config.AllowedOperations 默认 ["SELECT"]，YAML 可覆盖
- 已知降级 1 处：ODPS 探针走 LIMIT 0 而非 cost API（待 SDK EstimateSQLCost 真实现后切回）

Commits: <填 SHA 范围>
```

- [ ] **Step 9.3: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/README.md \
        aliyun-cli-plugins/plugin-maxc/docs/poc-findings.md
git commit -m "docs(plugin-maxc): Phase 2a auth completion (can-i + login)"
```

- [ ] **Step 9.4: Push**

```bash
git push origin feature/plugin-maxc-phase0
```

Capture the GitLab code-review URL printed by the remote.

---

## Exit Criteria

| # | Criterion | How to verify |
|---|---|---|
| 1 | `aliyun maxc can-i` works end-to-end on real ODPS, all 3 paths | Task 6 outputs in `/tmp/maxc-phase2a/` |
| 2 | `aliyun maxc login` returns guidance envelope without network/disk I/O | Task 7.3 manual run |
| 3 | All unit tests PASS (config + backend + envelope + errors) | `go test ./...` exit 0 |
| 4 | Local CI green (`scripts/ci-local.sh`) | exit 0 |
| 5 | `manifest.json` cmdNames includes both new commands | grep |
| 6 | Branch pushed to GitLab origin | push exit 0 + remote URL |

---

## Time Estimate

| Task | Bookkeeping |
|---|---|
| 0 | 5 min |
| 1 | 30 min (TDD) |
| 2 + 3 | 1.5h (atomic interface change + impl) |
| 4 | 45 min |
| 5 | 30 min |
| 6 | 30 min (assumes credentials available) |
| 7 | 45 min |
| 8 | 5 min |
| 9 | 20 min |

Total: ~5 hours hands-on; calendar 1 day with reviews.

---

## Out of Scope (Deferred to Phase 2b or later)

- Typed SDK error classification (separating PERMISSION_DENIED from connection failures inside the ODPS probe).
- INSERT/CREATE/etc. ODPS-level probes (would need separate SDK paths per operation).
- `auth login-external` (process-based credential provider) — Python supports it; Go can wait until a concrete agent need surfaces.
- Real `EstimateSQLCost` SDK implementation — placeholder stays on the local-replace branch per the 2026-04-29 SDK policy decision.

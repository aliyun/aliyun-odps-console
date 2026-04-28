# maxc-cli Go Port — Phase 0 (PoC) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用最小骨架验证 Phase 0 的两个核心假设：(1) aliyun-cli profile 拿到的 STS / AK credential 能直接驱动 odps-sdk-go 完成 ODPS endpoint 调用；(2) 当前 odps-sdk-go 能力是否覆盖 maxc-cli 后续移植所需。

**Architecture:** 在 `aliyun-cli-plugins/plugin-maxc/` 下建最小 Go 插件，复用 `aliyun-cli-runtime/config.ResolveCredential` 取得 `credentials.Credential`，通过 `odps-sdk-go/odps/account.NewStsAccountWithCredential` 桥接到 ODPS。只做一个 `query` 子命令，输出原始 SQL 结果（envelope 等格式化在 Phase 1 做）。再写一个独立的 capability-probe 程序对 SDK 做能力普查。

**Tech Stack:** Go 1.23 / Cobra / aliyun-cli-runtime v* / aliyun-odps-go-sdk (本地 replace 指向 `/Users/dingxin/GolandProjects/odps-sdk-go`) / credentials-go

**Spec:** `/Users/dingxin/pythonProject/maxc-cli/docs/superpowers/specs/2026-04-28-go-port-aliyun-cli-plugin-design.md`

**Working dirs:**
- 插件代码：`/Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc/` (新建)
- SDK fork（如需补丁）：`/Users/dingxin/GolandProjects/odps-sdk-go/`
- 计划本身：`/Users/dingxin/pythonProject/maxc-cli/docs/superpowers/plans/`

**Exit criteria（不达不进 Phase 1）：**
1. `aliyun maxc query --sql "select 1"` 在 AK profile 下成功返回 1 行结果
2. 同上命令在 STS / RAM Role profile 下成功返回 1 行结果（如失败需提交 odps-sdk-go 修复）
3. 输出 SDK capability gap 报告 `docs/sdk-gap.md`，列出 maxc-cli 后续 phase 所需但 SDK 缺失的方法
4. PoC 全部代码 push 到 `aliyun-cli-plugins` 一个 PoC 分支，不合 main

---

## File Structure

新建于 `/Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc/`：

```
plugin-maxc/
├── manifest.json                    # 最小化：仅 query 一条命令
├── go.mod                           # require runtime + odps-sdk-go (replace 指本地 SDK)
├── go.sum
├── main/
│   └── main.go                      # cobra root + maxc 子命令注册
├── commands/
│   └── query.go                     # query 子命令（仅 PoC 所需 flags）
├── internal/
│   └── auth/
│       ├── bridge.go                # ResolveCredential -> odps Account
│       └── bridge_test.go           # 单元测试（mock credential）
├── poc/
│   └── probe/
│       └── main.go                  # 独立可执行：扫一遍 SDK 能力
└── docs/
    ├── poc-findings.md              # AK / STS 端到端跑通的截图/日志
    └── sdk-gap.md                   # SDK 缺失能力清单（喂给后续 phase 与 SDK PR）
```

**职责约束：**
- `commands/query.go` 不超过 80 行；只解析 flag 调用 `internal/auth` + `odps-sdk-go`
- `internal/auth/bridge.go` 只做凭证适配，不引入业务逻辑
- `poc/probe/main.go` 独立 binary，不进插件本体；用完即弃，但代码留作后续测试参考

---

## Task 1: Scaffold plugin-maxc 目录与构建系统

**Files:**
- Create: `plugin-maxc/manifest.json`
- Create: `plugin-maxc/go.mod`
- Create: `plugin-maxc/main/main.go`
- Create: `plugin-maxc/.gitignore`

- [ ] **Step 1.1: 创建分支并切到目标仓库**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins
git checkout -b feature/plugin-maxc-phase0
mkdir -p aliyun-cli-plugins/plugin-maxc/{main,commands,internal/auth,poc/probe,docs}
```

- [ ] **Step 1.2: 写 manifest.json**

参考 `aliyun-cli-plugins/plugin-maxcompute/manifest.json` 格式，PoC 阶段只列 `query`：

```json
{
  "name": "aliyun-cli-maxc",
  "version": "0.0.1-poc",
  "command": "aliyun-cli-maxc",
  "shortDescription": "MaxCompute Agent CLI (PoC)",
  "description": "AI-agent oriented MaxCompute CLI plugin (Phase 0 PoC).",
  "productName": {"en": "MaxCompute", "zh": "云原生大数据计算服务 MaxCompute"},
  "author": "Aliyun MaxCompute Team",
  "homepage": "",
  "license": "Apache-2.0",
  "productCode": "MaxCompute",
  "platforms": ["linux-amd64", "linux-arm64", "darwin-amd64", "darwin-arm64", "windows-amd64", "windows-arm64"],
  "minCliVersion": "3.3.1",
  "bin": {"path": "aliyun-cli-maxc"},
  "cmdNames": ["query"]
}
```

- [ ] **Step 1.3: 初始化 go.mod，加 replace 指本地 SDK**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go mod init github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc
go mod edit -require=github.com/aliyun/aliyun-cli/aliyun-cli-runtime@v0.0.0-00010101000000-000000000000
go mod edit -replace=github.com/aliyun/aliyun-cli/aliyun-cli-runtime=../../aliyun-cli-runtime
go mod edit -require=github.com/aliyun/aliyun-odps-go-sdk@v0.0.0-00010101000000-000000000000
go mod edit -replace=github.com/aliyun/aliyun-odps-go-sdk=/Users/dingxin/GolandProjects/odps-sdk-go
go mod edit -require=github.com/spf13/cobra@v1.8.0
```

> Pseudo-version `v0.0.0-00010101000000-000000000000` 是占位；如果后面 `go mod tidy` 报无法解析，把 `-require` 行换成 `aliyun-cli-runtime/go.mod` 中的真实版本号，或干脆删掉 `-require` 让 tidy 自己从 import + replace 推断。

- [ ] **Step 1.4: 写最小 main.go（仅注册 root + maxc + 占位 query）**

```go
package main

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/commands"
	"github.com/aliyun/aliyun-cli/aliyun-cli-runtime/aly"
	"github.com/aliyun/aliyun-cli/aliyun-cli-runtime/http"
)

var (
	Version   = "0.0.1-poc"
	GitCommit = "unknown"
)

func main() {
	http.SetPluginName("aliyun-cli-maxc")
	http.SetPluginVersion(Version)

	rootCmd := &cobra.Command{
		Use:           "aliyun",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run:           func(cmd *cobra.Command, args []string) { _ = cmd.Help() },
	}

	maxcCmd := &cobra.Command{
		Use:   "maxc",
		Short: aly.I18nT("MaxCompute Agent CLI", "MaxCompute Agent CLI").Text(),
		RunE:  func(cmd *cobra.Command, args []string) error { return cmd.Help() },
	}

	maxcCmd.AddCommand(commands.NewQueryCmd())
	rootCmd.AddCommand(maxcCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
```

- [ ] **Step 1.5: 写占位 commands/query.go（只能 build，不做事）**

```go
package commands

import (
	"github.com/spf13/cobra"
)

func NewQueryCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "query",
		Short: "Run a MaxCompute SQL (PoC)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.Println("query placeholder")
			return nil
		},
	}
}
```

- [ ] **Step 1.6: `.gitignore`**

```
/aliyun-cli-maxc
/dist/
*.test
```

- [ ] **Step 1.7: 构建验证骨架**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go mod tidy
go build -o aliyun-cli-maxc ./main
./aliyun-cli-maxc maxc query
```

Expected stdout: `query placeholder`
Expected exit code: 0

- [ ] **Step 1.8: Commit**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins
git add aliyun-cli-plugins/plugin-maxc
git commit -m "feat(plugin-maxc): scaffold Phase 0 PoC skeleton"
```

---

## Task 2: 实现 auth bridge（profile credential → odps Account）

**Files:**
- Create: `plugin-maxc/internal/auth/bridge.go`
- Create: `plugin-maxc/internal/auth/bridge_test.go`

> 注：不能假装 PoC 跳过测试。`bridge_test.go` 用 fake credential（实现 `credentials.Credential` interface 即可）验证 Account 类型选择和 region 透传逻辑，不做真实网络。真实网络验证留 Task 5/6。

- [ ] **Step 2.1: 写失败的测试**

```go
package auth

import (
	"context"
	"testing"

	"github.com/aliyun/credentials-go/credentials"
	odpsaccount "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
)

type fakeCred struct {
	id, secret, token, ctype string
}

func (f *fakeCred) GetAccessKeyId() (*string, error)     { return &f.id, nil }
func (f *fakeCred) GetAccessKeySecret() (*string, error) { return &f.secret, nil }
func (f *fakeCred) GetSecurityToken() (*string, error)   { return &f.token, nil }
func (f *fakeCred) GetBearerToken() *string              { return nil }
func (f *fakeCred) GetType() *string                     { return &f.ctype }
func (f *fakeCred) GetCredential() (*credentials.CredentialModel, error) {
	return &credentials.CredentialModel{
		AccessKeyId:     &f.id,
		AccessKeySecret: &f.secret,
		SecurityToken:   &f.token,
		Type:            &f.ctype,
	}, nil
}

func TestBuildAccount_AKOnly_ReturnsAliyunAccount(t *testing.T) {
	c := &fakeCred{id: "ak", secret: "sk", token: "", ctype: "access_key"}
	acc, err := BuildAccount(context.Background(), c, "cn-hangzhou")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, ok := acc.(*odpsaccount.AliyunAccount); !ok {
		t.Fatalf("expected *AliyunAccount, got %T", acc)
	}
}

func TestBuildAccount_WithSecurityToken_ReturnsStsAccount(t *testing.T) {
	c := &fakeCred{id: "ak", secret: "sk", token: "st", ctype: "ram_role_arn"}
	acc, err := BuildAccount(context.Background(), c, "cn-hangzhou")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, ok := acc.(*odpsaccount.StsAccount); !ok {
		t.Fatalf("expected *StsAccount, got %T", acc)
	}
}
```

- [ ] **Step 2.2: 跑测试，确认失败**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go test ./internal/auth/...
```

Expected: FAIL（编译错：`BuildAccount` undefined）

- [ ] **Step 2.3: 写最小实现 bridge.go**

```go
package auth

import (
	"context"
	"fmt"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/credentials-go/credentials"
)

// BuildAccount adapts an aliyun-cli profile credential to an odps account.
// Selects StsAccount when SecurityToken is present, AliyunAccount otherwise.
func BuildAccount(ctx context.Context, cred credentials.Credential, regionId string) (account.Account, error) {
	model, err := cred.GetCredential()
	if err != nil {
		return nil, fmt.Errorf("resolve credential: %w", err)
	}
	if model == nil || model.AccessKeyId == nil || model.AccessKeySecret == nil {
		return nil, fmt.Errorf("credential missing access key")
	}
	ak := *model.AccessKeyId
	sk := *model.AccessKeySecret

	var token string
	if model.SecurityToken != nil {
		token = *model.SecurityToken
	}

	if token != "" {
		return account.NewStsAccountWithCredential(cred, regionId), nil
	}
	return account.NewAliyunAccount(ak, sk, regionId), nil
}
```

- [ ] **Step 2.4: 跑测试，确认通过**

```bash
go test ./internal/auth/... -v
```

Expected: PASS（两条 case 都通过）

- [ ] **Step 2.5: 加 ResolveCredentialFromProfile helper（封装 runtime 调用）**

在同文件追加：

```go
import (
    runtimecfg "github.com/aliyun/aliyun-cli/aliyun-cli-runtime/config"
)

// ResolveProfile 从 aliyun-cli profile 拿凭证 + 默认 region
func ResolveProfile(args map[string]any) (credentials.Credential, *runtimecfg.RuntimeProfile, error) {
	cred, profile, err := runtimecfg.ResolveCredential(nil, args)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve aliyun profile: %w", err)
	}
	return cred, profile, nil
}
```

- [ ] **Step 2.6: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/internal/auth
git commit -m "feat(plugin-maxc/auth): bridge aliyun profile credential to odps account"
```

---

## Task 3: 实现 query 子命令（连通 ODPS）

**Files:**
- Modify: `plugin-maxc/commands/query.go` (full rewrite)

> PoC 只输出原始 SELECT 结果（每行 JSON 一行）；envelope/agent_hints 在 Phase 1 才做。flag 集合极小：`--sql`, `--project`, `--endpoint`。

- [ ] **Step 3.1: 重写 query.go**

```go
package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/auth"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

func NewQueryCmd() *cobra.Command {
	var sql, project, endpoint string
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Run a MaxCompute SQL (PoC)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if sql == "" {
				return fmt.Errorf("--sql is required")
			}
			cred, profile, err := auth.ResolveProfile(nil)
			if err != nil {
				return err
			}
			region := profile.RegionId
			acc, err := auth.BuildAccount(context.Background(), cred, region)
			if err != nil {
				return err
			}
			o := odps.NewOdps(acc, endpoint)
			if project != "" {
				o.SetDefaultProjectName(project)
			}
			ins, err := o.ExecSQl(sql)
			if err != nil {
				return fmt.Errorf("submit sql: %w", err)
			}
			if err := ins.WaitForSuccess(); err != nil {
				return fmt.Errorf("wait sql: %w", err)
			}
			results, err := ins.GetResult()
			if err != nil {
				return fmt.Errorf("get result: %w", err)
			}
			return printResults(cmd, results)
		},
	}
	cmd.Flags().StringVar(&sql, "sql", "", "SQL statement (required)")
	cmd.Flags().StringVar(&project, "project", "", "MaxCompute project (optional, fall back to profile)")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "MaxCompute endpoint (required for PoC)")
	_ = cmd.MarkFlagRequired("endpoint")
	_ = cmd.MarkFlagRequired("sql")
	return cmd
}

func printResults(cmd *cobra.Command, results []odps.TaskResult) error {
	for _, r := range results {
		out := map[string]any{
			"task":       r.Name(),
			"content":    r.Content(),
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
		}
		buf, _ := json.Marshal(out)
		cmd.Println(string(buf))
	}
	return nil
}
```

> 注：`odps.ExecSQl` / `Instance.WaitForSuccess` / `Instance.GetResult` / `TaskResult` 的真实方法签名以 odps-sdk-go 当前版本为准；如有出入按 SDK 实际签名调整。**Step 3.2 编译失败时优先看 SDK 实例代码 `odps-sdk-go/examples/sdk/create_table_use_sql/main.go`**。

- [ ] **Step 3.2: 编译验证**

```bash
go build -o aliyun-cli-maxc ./main
```

Expected: 成功，无报错。如有 SDK 接口对不上的编译错，参照 `/Users/dingxin/GolandProjects/odps-sdk-go/examples/sdk/create_table_use_sql/main.go` 调整。

- [ ] **Step 3.3: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/commands/query.go
git commit -m "feat(plugin-maxc/query): wire PoC query command to odps SDK"
```

---

## Task 4: 端到端冒烟（AK profile）

**Files:** 无新文件；只跑命令

- [ ] **Step 4.1: 准备 AK profile**

```bash
# 假设已存在一个名为 'maxc-poc' 的 AK profile；若没有则：
aliyun configure set --profile maxc-poc --mode AK --region cn-hangzhou
# 输入 ak/sk
aliyun configure list
```

确认 `aliyun configure list` 中能看到 `maxc-poc`。

- [ ] **Step 4.2: 跑 select 1**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
./aliyun-cli-maxc maxc query \
    --profile maxc-poc \
    --endpoint http://service.cn-hangzhou.maxcompute.aliyun.com/api \
    --project <你的测试 project> \
    --sql "select 1 as v"
```

Expected: 至少一行 JSON 输出，包含 `task` 与非空 `content`，exit 0。

- [ ] **Step 4.3: 记录证据到 docs/poc-findings.md**

```markdown
# Phase 0 PoC 验证记录

## AK profile 端到端

- 时间：YYYY-MM-DD HH:MM
- profile: maxc-poc (AK)
- endpoint: http://service.cn-hangzhou.maxcompute.aliyun.com/api
- project: <填写>
- 命令：`./aliyun-cli-maxc maxc query --profile maxc-poc --sql "select 1 as v"`
- 输出：

```
<粘贴实际输出>
```

- 结论：✅ 通过
```

- [ ] **Step 4.4: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/docs/poc-findings.md
git commit -m "docs(plugin-maxc): record AK profile e2e success"
```

---

## Task 5: 端到端冒烟（STS / RAM Role profile，关键风险点）

**Files:** 追加 `plugin-maxc/docs/poc-findings.md`

> 这是 Phase 0 最重要的一步。如果 STS token 不被 ODPS endpoint 接受，需要立刻进入 Task 5b 修 SDK，否则不能进 Phase 1。

- [ ] **Step 5.1: 准备 RAM Role profile**

```bash
aliyun configure set --profile maxc-poc-sts --mode RamRoleArn --region cn-hangzhou
# 按提示填入主账号 AK/SK + RAM Role ARN + RoleSessionName
aliyun sts GetCallerIdentity --profile maxc-poc-sts   # 验证 STS 可用
```

- [ ] **Step 5.2: 跑同一条 SQL**

```bash
./aliyun-cli-maxc maxc query \
    --profile maxc-poc-sts \
    --endpoint http://service.cn-hangzhou.maxcompute.aliyun.com/api \
    --project <你的测试 project> \
    --sql "select 1 as v"
```

Expected: 与 Task 4 同样的输出。

- [ ] **Step 5.3: 失败处理决策表**

| 失败现象 | 可能原因 | 处理 |
|---|---|---|
| HTTP 401 / SignatureDoesNotMatch | StsAccount 没有附带 SecurityToken header | 进入 Step 5.4 修 SDK |
| `cred.GetCredential()` 返回空 token | runtime 没有触发 STS assume | 检查 profile mode 是否 RamRoleArn / 是否需要在 args map 透传字段 |
| 其他 ODPS 错误 | 项目权限/endpoint 不对 | 与 Task 4 同 endpoint，调整 project 后重试 |

- [ ] **Step 5.4 (条件)：如 SDK 需修补**

切到 SDK 仓改：

```bash
cd /Users/dingxin/GolandProjects/odps-sdk-go
git checkout -b feature/sts-token-header-poc
```

定位 `odps/account/sts_account.go::SignRequest`，确认是否在调用 `aliyun_account.go` 的 V2/V4 签名前往 header 写入 `x-odps-sts-token` 或等价字段。如缺失则补上，提交：

```bash
git commit -am "fix(account/sts): include security token in request header"
```

回到插件目录重新 `go build`（replace 自动生效），再跑 Step 5.2。

- [ ] **Step 5.5: 记录到 poc-findings.md**

追加 STS section（成功/失败/SDK 修补内容）。

- [ ] **Step 5.6: Commit**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins
git add aliyun-cli-plugins/plugin-maxc/docs/poc-findings.md
git commit -m "docs(plugin-maxc): record STS profile e2e result"
```

---

## Task 6: SDK capability 普查（probe 程序）

**Files:**
- Create: `plugin-maxc/poc/probe/main.go`

> 目标：在 Phase 0 把后续 phase 要用到的 SDK 方法全部点一遍，把缺失的整理成 PR todo。**不是写测试**，是探针程序，用真实 endpoint 跑一次输出 JSON 报告。

- [ ] **Step 6.1: 写 probe 程序**

```go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/aliyun/aliyun-cli/aliyun-cli-plugins/plugin-maxc/internal/auth"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

type probeResult struct {
	Capability string `json:"capability"`
	Supported  bool   `json:"supported"`
	Notes      string `json:"notes,omitempty"`
}

func main() {
	endpoint := flag.String("endpoint", "", "")
	project := flag.String("project", "", "")
	tableForProbe := flag.String("table", "", "existing table name to probe describe/sample")
	flag.Parse()

	cred, profile, err := auth.ResolveProfile(nil)
	must(err)
	acc, err := auth.BuildAccount(context.Background(), cred, profile.RegionId)
	must(err)
	o := odps.NewOdps(acc, *endpoint)
	o.SetDefaultProjectName(*project)

	results := []probeResult{}

	// 1. list tables
	results = append(results, probe("list_tables", func() error {
		tables := o.Tables()
		count := 0
		tables.List(func(t *odps.Table, err error) {
			if err == nil {
				count++
			}
		})
		if count == 0 {
			return fmt.Errorf("no tables listed")
		}
		return nil
	}))

	// 2. describe table
	if *tableForProbe != "" {
		results = append(results, probe("describe_table", func() error {
			t := o.Table(*tableForProbe)
			return t.Load()
		}))
	}

	// 3. get instance (拿一个最近 instance，需要先跑一条 SQL)
	results = append(results, probe("get_instance", func() error {
		ins, err := o.ExecSQl("select 1")
		if err != nil { return err }
		return ins.Load()
	}))

	// TODO 4..N: search_tables, sample, profile, diagnose_job, cost_estimate, explain ...
	// 在 Step 6.2 中按 maxc-cli backend 文件清单逐项补齐

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	must(enc.Encode(results))
}

func probe(name string, fn func() error) probeResult {
	if err := fn(); err != nil {
		return probeResult{Capability: name, Supported: false, Notes: err.Error()}
	}
	return probeResult{Capability: name, Supported: true}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 6.2: 把 maxc-cli 后续 phase 所需方法补齐到 probe**

参照 Python backend 文件，列出所有需要的能力点，加到 probe（每条单独 `probe(...)` 调用）：

来源参照（只读，列清单）：
- `/Users/dingxin/pythonProject/maxc-cli/src/maxc_cli/backend/query.py` — execute_query, estimate_query_cost, explain_query, submit_query
- `/Users/dingxin/pythonProject/maxc-cli/src/maxc_cli/backend/job.py` — get_job, wait_job, fetch_job_result, cancel_job, diagnose_job, list_jobs
- `/Users/dingxin/pythonProject/maxc-cli/src/maxc_cli/backend/meta.py` — list_tables, describe_table, search_tables, search_columns
- `/Users/dingxin/pythonProject/maxc-cli/src/maxc_cli/backend/data.py` — sample_table, profile_table
- `/Users/dingxin/pythonProject/maxc-cli/src/maxc_cli/backend/auth.py` — whoami_info, can_i_info

每个方法在 Go SDK 中找对应（可能是 `odps/instance`、`odps/tables`、`odps/data` 等子包）。**找不到的就是 gap，标 supported=false 写 notes**。

- [ ] **Step 6.3: 跑 probe**

```bash
cd /Users/dingxin/MaxQuery/aliyun-cli-plugins/aliyun-cli-plugins/plugin-maxc
go run ./poc/probe \
    --endpoint http://service.cn-hangzhou.maxcompute.aliyun.com/api \
    --project <你的测试 project> \
    --table <一个已存在的表> \
    > docs/sdk-probe-output.json
```

- [ ] **Step 6.4: 整理 docs/sdk-gap.md**

```markdown
# odps-sdk-go 能力 gap 报告（Phase 0 PoC 输出）

来源：`docs/sdk-probe-output.json`，时间 YYYY-MM-DD

## 已覆盖（无需 SDK 改动）

| Python 方法 | Go SDK 调用 | 备注 |
|---|---|---|
| ... | ... | ... |

## 缺失/部分支持（后续需 PR 到 odps-sdk-go）

| Python 方法 | 现状 | 建议 |
|---|---|---|
| profile_table 分位数 | SDK 无直接 API | 在 odps/data 加 `ProfileColumn(quantiles []float64)` |
| diagnose_job fuxi 详情 | 无 | 暴露 `Instance.GetTaskDetailJson` |
| ... | ... | ... |

## 不阻塞（可在插件层 work around）

| Python 方法 | 解决方式 |
|---|---|
| ... | ... |
```

- [ ] **Step 6.5: Commit**

```bash
git add aliyun-cli-plugins/plugin-maxc/poc aliyun-cli-plugins/plugin-maxc/docs/sdk-probe-output.json aliyun-cli-plugins/plugin-maxc/docs/sdk-gap.md
git commit -m "docs(plugin-maxc): SDK capability probe + gap report"
```

---

## Task 7: PoC 收尾与 Phase 1 入口准备

**Files:**
- Create: `plugin-maxc/README.md`
- Modify: `plugin-maxc/docs/poc-findings.md`（追加结论）

- [ ] **Step 7.1: 写 README.md（PoC 版）**

```markdown
# plugin-maxc (Phase 0 PoC)

PoC for porting maxc-cli to a Go-based aliyun-cli plugin.

**Status: PoC, not for production use.**

## Build

\`\`\`bash
go mod tidy
go build -o aliyun-cli-maxc ./main
\`\`\`

## Run

\`\`\`bash
./aliyun-cli-maxc maxc query \
    --profile <aliyun-cli profile> \
    --endpoint <maxcompute endpoint> \
    --project <project> \
    --sql "select 1"
\`\`\`

## PoC 结论

参见 `docs/poc-findings.md` 与 `docs/sdk-gap.md`。
```

- [ ] **Step 7.2: 在 poc-findings.md 末尾追加结论段**

```markdown
## Phase 0 总结

- AK profile 端到端：✅ / ❌
- STS profile 端到端：✅ / ❌（如需 SDK 修补已 PR：<link>）
- SDK gap 已整理：见 sdk-gap.md，共 N 项需补
- Phase 1 是否可启动：✅ / ❌（如 ❌ 列出阻塞项）
```

- [ ] **Step 7.3: Commit & push 分支（不合 main）**

```bash
git add aliyun-cli-plugins/plugin-maxc/README.md aliyun-cli-plugins/plugin-maxc/docs/poc-findings.md
git commit -m "docs(plugin-maxc): Phase 0 wrap-up + readme"
git push -u origin feature/plugin-maxc-phase0
```

- [ ] **Step 7.4: 验证 Exit Criteria**

依次确认：

1. ☐ `aliyun maxc query --sql "select 1"` 在 AK profile 下成功（Task 4）
2. ☐ 同上在 STS profile 下成功（Task 5）
3. ☐ `docs/sdk-gap.md` 已生成（Task 6）
4. ☐ 分支已 push 到 `aliyun-cli-plugins` 远端（Step 7.3）

四项全 ✅ → 可以启动 Phase 1 计划编写。任一 ❌ → 暂停，回到对应 task 解决或调整 spec。

---

## 预期总耗时

| Task | 估时 |
|---|---|
| 1 — 骨架 | 2~3 小时 |
| 2 — auth bridge + test | 2 小时 |
| 3 — query 命令 | 2 小时 |
| 4 — AK 冒烟 | 1 小时 |
| 5 — STS 冒烟（不含 SDK 修） | 2 小时 |
| 5b — SDK 修补（条件触发） | 0~2 天 |
| 6 — SDK 普查 | 1 天 |
| 7 — 收尾 | 半天 |

**乐观：3 天；包含 SDK 修补：1 周。** 与 spec 中 Phase 0「1~2 周」估算一致。

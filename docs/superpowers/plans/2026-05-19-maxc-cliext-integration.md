# maxc-cli → aliyun-cli cliext Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `maxc-cli` as `aliyun maxc <...>` by adding a Go cliext launcher into `aliyun/aliyun-cli` that downloads a PyInstaller onedir bundle from a public OSS bucket on first use.

**Architecture:** Three single-responsibility layers connected by a fixed OSS directory contract. (1) maxc-cli repo produces per-platform PyInstaller tarballs and uploads to OSS. (2) OSS bucket hosts `versions/latest` + `{version}/{os}-{arch}/maxc.tar.gz{,.sha256}`. (3) aliyun-cli's `cliext/maxc/` Go package mirrors the `cliext/cms2/` pattern — finds/downloads the binary, injects credentials from the active aliyun profile, exec's it. Spec: `docs/superpowers/specs/2026-05-19-maxc-cliext-integration-design.md`.

**Tech Stack:**
- maxc-cli side: Python 3.11, PyInstaller (onedir), bash, `ossutil` (for upload), `pytest`.
- aliyun-cli side: Go 1.25, Cobra (via `aliyun-cli/v3/cli`), `archive/tar`+`compress/gzip`, `crypto/sha256`.
- Distribution: public OSS bucket (hangzhou region, public-read).

**Cross-repo work:** Phases 1, 2 happen in this repo (`/Users/dingxin/pythonProject/maxc-cli`). Phase 3 happens in a separate clone of `aliyun/aliyun-cli` — Task 3.0 sets up that working tree explicitly.

**Human gates marked `[HUMAN]`:** OSS bucket provisioning, Aone CI capability check, upstream issue, manual runbooks. The plan calls these out where they block; the assistant should pause and surface them, not attempt them.

---

## Phase 0 — Pre-flight Validation (spec § 6)

Resolves the open items before any production code is written. Skipping any of these will cause rework later.

### Task 0.1: Fix `.aoneci/cli-publish.yaml` referencing nonexistent `make dist`

**Files:**
- Read: `/Users/dingxin/pythonProject/maxc-cli/Makefile`
- Read: `/Users/dingxin/pythonProject/maxc-cli/.aoneci/cli-publish.yaml`
- Modify: `/Users/dingxin/pythonProject/maxc-cli/.aoneci/cli-publish.yaml`

This is an existing bug surfaced during brainstorming, independent of this plan. Fix it first so the .aoneci file is a trustworthy reference point.

- [ ] **Step 1: Confirm `make dist` is not defined**

Run: `grep -E '^(dist|\.PHONY)' /Users/dingxin/pythonProject/maxc-cli/Makefile`
Expected: no `dist:` target line. Only `test unit build clean smoke build-bin bin-smoke` exist.

- [ ] **Step 2: Decide replacement target**

Two options — pick one and document the decision inline in the .aoneci YAML as a comment:
- (a) replace `make dist` with `make build` (existing target, produces wheel only — current behavior is broken anyway since no onedir was being produced)
- (b) skip-and-defer: comment out the build step with a TODO referencing this plan's Phase 1.

Recommended: (a) for now since cli-hub publish path was already broken; the new pipeline introduced by Phase 2 will replace this YAML entirely.

- [ ] **Step 3: Edit `.aoneci/cli-publish.yaml`**

Change `make dist VERSION="${VERSION}"` → `make build  # TODO(plan 2026-05-19-maxc-cliext-integration): replaced by Phase 2 release pipeline`.

- [ ] **Step 4: Verify YAML still parses**

Run: `python3 -c "import yaml; yaml.safe_load(open('/Users/dingxin/pythonProject/maxc-cli/.aoneci/cli-publish.yaml'))"`
Expected: no exception, no output.

- [ ] **Step 5: Commit**

```bash
git add /Users/dingxin/pythonProject/maxc-cli/.aoneci/cli-publish.yaml
git commit -m "fix(ci): stop calling nonexistent make dist target

Pre-flight cleanup for the cliext integration plan. The .aoneci publish
job referenced make dist which has never existed in this Makefile;
swap to make build until Phase 2 replaces this pipeline.
"
```

### Task 0.2: Verify PyInstaller onedir builds on darwin-arm64 locally

**Files:**
- Read: `/Users/dingxin/pythonProject/maxc-cli/maxc.spec`
- Read: `/Users/dingxin/pythonProject/maxc-cli/Makefile` (targets `build-bin`, `bin-smoke`)

The spec lists this as risk; do it now to either confirm OK or trigger early replan.

- [ ] **Step 1: Clean any stale dist/**

Run: `rm -rf /Users/dingxin/pythonProject/maxc-cli/dist/maxc /Users/dingxin/pythonProject/maxc-cli/build`
Expected: no error.

- [ ] **Step 2: Build onedir**

Run: `cd /Users/dingxin/pythonProject/maxc-cli && make build-bin 2>&1 | tail -20`
Expected: PyInstaller completes, `dist/maxc/maxc` exists.

- [ ] **Step 3: Run smoke checks**

Run: `cd /Users/dingxin/pythonProject/maxc-cli && make bin-smoke`
Expected: all three assertions pass (version, help, skill_exists).

- [ ] **Step 4: Note architecture in shell output**

Run: `file /Users/dingxin/pythonProject/maxc-cli/dist/maxc/maxc`
Expected output contains `arm64` (assuming you're on Apple Silicon). Save the output line for the Phase 0 report.

- [ ] **Step 5: Write findings to short note file (no commit yet)**

Create: `/Users/dingxin/pythonProject/maxc-cli/docs/superpowers/plans/notes/0.2-pyinstaller-arm64-verification.md`

Contents:
```markdown
# Phase 0.2 — PyInstaller darwin-arm64 verification

Date: <run-date>
Host: <uname -a output>
Result: PASS | FAIL
Notes: <any warnings, bundle size, startup-time observations>
```

If FAIL: stop here, surface to human — the whole plan depends on this.

### Task 0.3: STS credential PoC — prove aliyun profile → maxc env chain works end-to-end

**Files:**
- Create (throwaway): `/tmp/maxc-sts-poc/main.go`, `/tmp/maxc-sts-poc/go.mod`

Validates the riskiest design assumption: that an aliyun-cli profile's GetCredential() output can drive maxc through env vars to issue a real ODPS query. If this is broken, the entire credential bridging in Phase 3 needs redesign.

- [ ] **Step 1: Confirm aliyun-cli is installed locally and a working profile exists**

Run: `aliyun configure list 2>/dev/null | head -5` — should list at least one profile with a project/region that has MaxCompute access.

If not, [HUMAN] gate: ask the user which profile to use, or to `aliyun configure` one with MaxCompute access.

- [ ] **Step 2: Write the PoC Go program**

Create `/tmp/maxc-sts-poc/main.go`:
```go
package main

import (
    "fmt"
    "os"
    "os/exec"
    // NOTE: the actual import path for config.LoadProfile is in aliyun-cli;
    // for the PoC, vendor it or call `aliyun` CLI as a subprocess to
    // emit the resolved credentials as JSON. The latter is fewer LOC:
)

func main() {
    profile := os.Getenv("ALIBABA_CLOUD_PROFILE")
    if profile == "" { profile = "default" }

    // Use aliyun CLI itself to resolve credentials; it knows all profile modes.
    out, err := exec.Command("aliyun", "configure", "get",
        "access-key-id", "access-key-secret", "sts-token", "region-id",
        "--profile", profile, "--mode", "AK").Output()
    if err != nil { fmt.Println("ERR:", err); os.Exit(1) }
    fmt.Printf("resolved creds: %s\n", out)

    // Now hand them off to maxc and run a trivial query.
    cmd := exec.Command("./dist/maxc/maxc", "query", "--sql", "select 1")
    cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
    // Inject AK/SK/STS from above (parse them out of 'out' first — left as PoC exercise)
    cmd.Env = append(os.Environ(),
        "ALIBABA_CLOUD_ACCESS_KEY_ID=<from out>",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET=<from out>",
        // "ALIBABA_CLOUD_SECURITY_TOKEN=<from out>",  // if STS profile
    )
    if err := cmd.Run(); err != nil { os.Exit(1) }
}
```

The exact import path for `aliyun-cli/v3/config.LoadProfile` is what we'll actually use in Phase 3; the PoC just needs to prove the env-bridge works. Use whichever shape is easier to write in 50 lines.

- [ ] **Step 3: Run PoC**

```bash
cd /tmp/maxc-sts-poc && go run main.go
```

Expected: maxc envelope output with `"status": "success"` and `data` containing the result of `select 1`.

- [ ] **Step 4: Repeat with a RamRoleArn profile if one is available**

[HUMAN] gate: requires a profile with `mode: RamRoleArn`. If unavailable, document the gap in `notes/0.3-sts-poc.md` and flag for Phase 3 test plan.

- [ ] **Step 5: Write findings**

Create: `/Users/dingxin/pythonProject/maxc-cli/docs/superpowers/plans/notes/0.3-sts-poc.md`

Record: which credential modes were verified (AK, RamRoleArn, EcsRamRole, CloudSSO, StsToken), which were skipped due to environment, any quirks discovered.

If any tested mode fails: stop and surface — Phase 3's `credentials.go` design needs rework before proceeding.

### Task 0.4: CI platform decision

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/docs/superpowers/plans/notes/0.4-ci-decision.md`

[HUMAN] heavy task. Three sub-checks:

- [ ] **Step 1: Investigate Aone CI runner matrix**

Ask Aone platform owners (or read internal docs) — does Aone CI provide non-linux-amd64 runners? Specifically `{darwin, windows} × {amd64, arm64}`?

- [ ] **Step 2: Investigate `cli-hub` semantics**

Can `cli-hub` (the existing publish step in `.aoneci/cli-publish.yaml`) host generic tarballs for cliext to fetch via stable URL, OR is it tied to the aliyun-cli plugin index? If the latter, cliext can't use it directly.

- [ ] **Step 3: Decide one of: full-Aone | hybrid (GH Actions matrix + Aone publish) | full-GH-Actions**

Record decision and reasoning in `notes/0.4-ci-decision.md`. This decision selects which Phase 2 task variant gets used (the plan provides all three; one will be skipped).

### Task 0.5: OSS bucket provisioning [HUMAN]

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/docs/superpowers/plans/notes/0.5-oss-bucket.md`

- [ ] **Step 1: Provision bucket**

Coordinate with the team to create an OSS bucket. Constraints:
- Region: cn-hangzhou (matches cms2 precedent for latency)
- ACL: public-read on objects (cliext does anonymous GET)
- CORS: not required (cliext is a CLI, not a browser)
- Lifecycle: retain old versions ≥ 90 days (rollback safety)

- [ ] **Step 2: Verify with anonymous curl**

After bucket exists, place a test file at `versions/test` (one line `hello`).
Run: `curl -fsSL https://<bucket>.oss-cn-hangzhou.aliyuncs.com/versions/test`
Expected output: `hello`.

- [ ] **Step 3: Document bucket name + region in `notes/0.5-oss-bucket.md`**

This is the canonical reference for the rest of the plan. Every subsequent script and the Go cliext code will use this exact name.

### Task 0.6: Aliyun-cli upstream sign-off [HUMAN]

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/docs/superpowers/plans/notes/0.6-upstream.md`

- [ ] **Step 1: Fork `aliyun/aliyun-cli` to your GitHub account**

Via web UI or `gh repo fork aliyun/aliyun-cli --remote=false`. Record the fork URL in `notes/0.6-upstream.md`. Phase 3 Task 3.0 will clone this fork.

- [ ] **Step 2: Open issue on `aliyun/aliyun-cli`**

Title suggestion: `Proposal: add cliext/maxc/ launcher for maxc-cli MaxCompute agent tool`

Body should reference: spec link (after pushing), the cms2/saectl pattern as precedent, OSS hosting plan, naming choice (`maxc`).

- [ ] **Step 3: Wait for maintainer response, document outcome**

If maintainers reject: fall back to `cli/plugin/` route — this requires a separate spec; stop the plan and replan.
If maintainers approve: record the assigned reviewer name(s) and any naming/scope adjustments in `notes/0.6-upstream.md`.

---

## Phase 1 — maxc-cli Packaging Pipeline

Produces per-platform tarballs locally. Independent of OSS and CI — verifiable on a single laptop first.

### Task 1.1: `scripts/build_release.sh` — wraps PyInstaller + tar + sha256

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/scripts/build_release.sh`
- Test: `/Users/dingxin/pythonProject/maxc-cli/tests/test_build_release_script.py`

- [ ] **Step 1: Write the failing pytest**

Create `tests/test_build_release_script.py`:
```python
import os
import subprocess
import tarfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "build_release.sh"


@pytest.mark.slow  # full PyInstaller build, gate behind marker
def test_build_release_produces_tarball_and_sha256(tmp_path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    env = os.environ.copy()
    env["OUTPUT_DIR"] = str(out_dir)
    subprocess.run(["bash", str(SCRIPT)], check=True, env=env, cwd=REPO)

    tarball = out_dir / "maxc.tar.gz"
    sha = out_dir / "maxc.tar.gz.sha256"
    assert tarball.exists(), f"missing {tarball}"
    assert sha.exists(), f"missing {sha}"

    # sha256 file is single-line hex
    digest = sha.read_text().strip()
    assert len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)

    # tarball top-level is the directory 'maxc/'
    with tarfile.open(tarball) as tf:
        roots = {m.name.split("/", 1)[0] for m in tf.getmembers() if m.name}
        assert roots == {"maxc"}, f"unexpected roots: {roots}"

        # maxc/maxc (or maxc.exe on windows) must exist
        names = [m.name for m in tf.getmembers()]
        assert any(n in ("maxc/maxc", "maxc/maxc.exe") for n in names)
```

Add the `slow` marker to `pytest.ini` if not present.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/dingxin/pythonProject/maxc-cli && pytest tests/test_build_release_script.py -v -m slow`
Expected: FAIL with `No such file or directory: scripts/build_release.sh`.

- [ ] **Step 3: Write the script**

Create `scripts/build_release.sh`:
```bash
#!/usr/bin/env bash
# Build maxc PyInstaller onedir, tar it, compute sha256.
# Outputs ${OUTPUT_DIR:-dist/release}/maxc.tar.gz + maxc.tar.gz.sha256
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-${REPO_DIR}/dist/release}"
mkdir -p "${OUTPUT_DIR}"

cd "${REPO_DIR}"

# Clean prior build artifacts (PyInstaller can pick up stale caches)
rm -rf dist/maxc build

# Run PyInstaller (uses maxc.spec, produces dist/maxc/)
pyinstaller --noconfirm maxc.spec

# Sanity check
test -d dist/maxc || { echo "ERR: dist/maxc not produced"; exit 1; }
if [ -f dist/maxc/maxc.exe ]; then
  BIN="maxc.exe"
else
  test -f dist/maxc/maxc || { echo "ERR: dist/maxc/maxc not produced"; exit 1; }
  BIN="maxc"
fi

# Tar the onedir, preserving top-level 'maxc/' directory
cd dist
tar -czf "${OUTPUT_DIR}/maxc.tar.gz" maxc

# Compute sha256
cd "${OUTPUT_DIR}"
if command -v shasum >/dev/null; then
  shasum -a 256 maxc.tar.gz | awk '{print $1}' > maxc.tar.gz.sha256
else
  sha256sum maxc.tar.gz | awk '{print $1}' > maxc.tar.gz.sha256
fi

echo "==> built: ${OUTPUT_DIR}/maxc.tar.gz"
echo "==> sha256: $(cat maxc.tar.gz.sha256)"
echo "==> entry binary: ${BIN}"
```

Make executable: `chmod +x scripts/build_release.sh`.

- [ ] **Step 4: Run test, verify it passes**

Run: `cd /Users/dingxin/pythonProject/maxc-cli && pytest tests/test_build_release_script.py -v -m slow`
Expected: PASS (will take ~30s for PyInstaller).

- [ ] **Step 5: Commit**

```bash
git add scripts/build_release.sh tests/test_build_release_script.py pytest.ini
git commit -m "feat(release): scripts/build_release.sh produces onedir tarball + sha256

Wraps pyinstaller, tars the dist/maxc/ output preserving the top-level
'maxc/' dir per spec § 3, computes sha256. Output dir overridable via
OUTPUT_DIR for CI use."
```

### Task 1.2: `tests/test_pyinstaller_bundle.py` — pytest replacement for bin-smoke

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/tests/test_pyinstaller_bundle.py`

Spec § 5 lists 4 assertions for the built bundle. Convert from Makefile `bin-smoke` into pytest so CI can produce structured results.

- [ ] **Step 1: Write the failing test file**

```python
"""Smoke tests against the PyInstaller onedir bundle in dist/maxc/.

Requires that `make build-bin` or `scripts/build_release.sh` has been run.
Skips cleanly if the bundle isn't present.
"""
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BUNDLE = REPO / "dist" / "maxc"
BIN = BUNDLE / ("maxc.exe" if os.name == "nt" else "maxc")

pytestmark = pytest.mark.skipif(
    not BIN.exists(),
    reason=f"PyInstaller bundle not built at {BIN}; run `make build-bin` first",
)


def _run(*args, **kwargs):
    return subprocess.run(
        [str(BIN), *args],
        capture_output=True,
        text=True,
        timeout=30,
        **kwargs,
    )


def test_version_is_semver():
    r = _run("--version")
    assert r.returncode == 0, r.stderr
    # Permissive — must contain something like X.Y.Z
    assert re.search(r"\d+\.\d+\.\d+", r.stdout), f"no semver in: {r.stdout!r}"


def test_help_works():
    r = _run("--help")
    assert r.returncode == 0
    assert "usage" in r.stdout.lower() or "Usage" in r.stdout


def test_skill_resource_bundled():
    r = _run("--format", "json", "agent", "skill")
    assert r.returncode == 0, r.stderr
    payload = json.loads(r.stdout)
    assert payload["data"]["skill_exists"] is True


def test_no_plain_source_in_bundle():
    """Spec § 5: bundle should not leak .py source files (other than .pyc)."""
    src_files = []
    for root, _, files in os.walk(BUNDLE):
        for f in files:
            if f.endswith(".py"):
                src_files.append(os.path.join(root, f))
    # Some PyInstaller hooks pull in .py files intentionally (e.g. pkg_resources);
    # the assertion is "no maxc_cli source files" specifically.
    leaks = [p for p in src_files if "maxc_cli" in p and "skills" not in p]
    assert not leaks, f"maxc_cli source files leaked: {leaks[:5]}"
```

- [ ] **Step 2: Run test to verify it works**

If `dist/maxc/` exists from Task 0.2:
Run: `cd /Users/dingxin/pythonProject/maxc-cli && pytest tests/test_pyinstaller_bundle.py -v`
Expected: 4 passes (or skips if bundle missing — should not happen if Task 0.2 left dist/ in place).

If bundle was cleaned: `make build-bin && pytest tests/test_pyinstaller_bundle.py -v`.

- [ ] **Step 3: Commit**

```bash
git add tests/test_pyinstaller_bundle.py
git commit -m "test(bundle): pytest assertions for PyInstaller onedir bundle

Replaces Makefile bin-smoke checks with structured pytest output so CI
can attribute failures to specific bundle properties (version, help,
skill resources, no source leakage). Spec § 5."
```

### Task 1.3: `scripts/upload_to_oss.sh` — ossutil cp to bucket

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/scripts/upload_to_oss.sh`

Requires Task 0.5 (bucket exists) to have completed. Until then, the script can be written and unit-tested with `ossutil --dry-run`, but the live upload step blocks.

- [ ] **Step 1: Write the script**

Create `scripts/upload_to_oss.sh`:
```bash
#!/usr/bin/env bash
# Upload a built {maxc.tar.gz, maxc.tar.gz.sha256} pair to the OSS bucket
# under the canonical path: {version}/{platform}/.
#
# Required env:
#   VERSION       — semver string, e.g. 0.2.5
#   PLATFORM      — one of: linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64 windows-arm64
#   OSS_BUCKET    — bucket name (from Phase 0.5)
#   OSS_REGION    — e.g. cn-hangzhou
#   INPUT_DIR     — directory containing maxc.tar.gz + maxc.tar.gz.sha256
# Optional:
#   OSS_ENDPOINT  — override; default derived from OSS_REGION
#   DRY_RUN=1     — print ossutil commands without executing
set -euo pipefail

: "${VERSION:?VERSION env required}"
: "${PLATFORM:?PLATFORM env required}"
: "${OSS_BUCKET:?OSS_BUCKET env required}"
: "${OSS_REGION:?OSS_REGION env required}"
: "${INPUT_DIR:?INPUT_DIR env required}"

case "$PLATFORM" in
  linux-amd64|linux-arm64|darwin-amd64|darwin-arm64|windows-amd64|windows-arm64) ;;
  *) echo "ERR: unsupported PLATFORM=${PLATFORM}"; exit 1 ;;
esac

ENDPOINT="${OSS_ENDPOINT:-oss-${OSS_REGION}.aliyuncs.com}"
DEST="oss://${OSS_BUCKET}/${VERSION}/${PLATFORM}/"

TARBALL="${INPUT_DIR}/maxc.tar.gz"
SHA="${INPUT_DIR}/maxc.tar.gz.sha256"

test -f "$TARBALL" || { echo "ERR: missing $TARBALL"; exit 1; }
test -f "$SHA" || { echo "ERR: missing $SHA"; exit 1; }

run() {
  if [ "${DRY_RUN:-0}" = "1" ]; then
    echo "DRY: $*"
  else
    "$@"
  fi
}

run ossutil cp "$TARBALL" "$DEST" --endpoint "$ENDPOINT" --acl public-read --force
run ossutil cp "$SHA"     "$DEST" --endpoint "$ENDPOINT" --acl public-read --force

echo "==> uploaded ${PLATFORM} v${VERSION} to ${DEST}"
```

`chmod +x scripts/upload_to_oss.sh`.

- [ ] **Step 2: Dry-run verification**

```bash
INPUT_DIR=dist/release VERSION=0.0.0-test PLATFORM=darwin-arm64 \
  OSS_BUCKET=dummy OSS_REGION=cn-hangzhou DRY_RUN=1 \
  bash scripts/upload_to_oss.sh
```
Expected: prints two `DRY: ossutil cp ...` lines, exits 0.

- [ ] **Step 3: Commit**

```bash
git add scripts/upload_to_oss.sh
git commit -m "feat(release): scripts/upload_to_oss.sh uploads tarball+sha to OSS

Publishes the {version}/{platform}/maxc.tar.gz{,.sha256} pair per the
spec § 3 OSS contract. Reads bucket/region/version from env so the
same script works in any CI environment. DRY_RUN=1 for local testing."
```

### Task 1.4: `scripts/publish_latest.sh` — atomically flip `versions/latest`

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/scripts/publish_latest.sh`

Per spec § 3: this is the LAST step of a release. Calling it before all 6 platforms uploaded would surface a half-baked release to users.

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Flip the OSS pointer file versions/latest to a new version.
# Pre-condition: all 6 platforms for $VERSION must already be in OSS.
# This script verifies that pre-condition before flipping.
set -euo pipefail

: "${VERSION:?VERSION env required}"
: "${OSS_BUCKET:?OSS_BUCKET env required}"
: "${OSS_REGION:?OSS_REGION env required}"

ENDPOINT="${OSS_ENDPOINT:-oss-${OSS_REGION}.aliyuncs.com}"
BASE_URL="https://${OSS_BUCKET}.${ENDPOINT}"

PLATFORMS=(linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64 windows-arm64)

echo "==> verifying all 6 platforms exist for v${VERSION}"
missing=()
for p in "${PLATFORMS[@]}"; do
  for f in maxc.tar.gz maxc.tar.gz.sha256; do
    if ! curl -fsI "${BASE_URL}/${VERSION}/${p}/${f}" >/dev/null; then
      missing+=("${p}/${f}")
    fi
  done
done

if [ ${#missing[@]} -gt 0 ]; then
  echo "ERR: refusing to flip latest, missing artifacts:"
  printf '  - %s\n' "${missing[@]}"
  exit 1
fi

# All present — write the pointer
TMP=$(mktemp)
printf '%s' "$VERSION" > "$TMP"

ossutil cp "$TMP" "oss://${OSS_BUCKET}/versions/latest" \
  --endpoint "$ENDPOINT" --acl public-read --force

echo "==> versions/latest is now ${VERSION}"
rm -f "$TMP"
```

`chmod +x scripts/publish_latest.sh`.

- [ ] **Step 2: Commit**

```bash
git add scripts/publish_latest.sh
git commit -m "feat(release): scripts/publish_latest.sh flips versions/latest

Last step of release — only runs after verifying all 6 platforms have
both the tarball and sha256 file in OSS. Prevents the cliext launcher
from ever seeing latest pointing at an incomplete version. Spec § 3."
```

### Task 1.5: Wire scripts into `make release`

**Files:**
- Modify: `/Users/dingxin/pythonProject/maxc-cli/Makefile`

- [ ] **Step 1: Add Makefile targets**

Append to `Makefile`:
```makefile
# ─────────────────────────────────────────────
# Release pipeline (spec 2026-05-19-maxc-cliext-integration-design)
# ─────────────────────────────────────────────

# Detect current platform — overridable for cross-build CI (where actual build
# still happens on the target runner, this just labels the artifact).
PLATFORM ?= $(shell \
  os=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
  arch=$$(uname -m); \
  case "$$arch" in x86_64) arch=amd64;; aarch64|arm64) arch=arm64;; esac; \
  case "$$os" in darwin|linux) echo "$$os-$$arch";; *) echo "unknown-$$arch";; esac)

VERSION ?= $(shell grep -E '^__version__' src/maxc_cli/__init__.py | sed -E 's/.*"([^"]+)".*/\1/')

release-build:
	OUTPUT_DIR=dist/release bash scripts/build_release.sh

release-upload: release-build
	INPUT_DIR=dist/release VERSION=$(VERSION) PLATFORM=$(PLATFORM) \
	  bash scripts/upload_to_oss.sh

release-publish-latest:
	VERSION=$(VERSION) bash scripts/publish_latest.sh

# Convenience target for a one-platform local rehearsal
release-local: release-build
	@echo "==> built ${VERSION} for ${PLATFORM}, output in dist/release/"
	@ls -la dist/release/

.PHONY: release-build release-upload release-publish-latest release-local
```

- [ ] **Step 2: Verify the new targets parse**

Run: `cd /Users/dingxin/pythonProject/maxc-cli && make -n release-local`
Expected: prints the recipe without running it.

- [ ] **Step 3: Run local rehearsal**

Run: `cd /Users/dingxin/pythonProject/maxc-cli && make release-local`
Expected: builds, ends with `==> built X.Y.Z for darwin-arm64, output in dist/release/` and lists `maxc.tar.gz` + `maxc.tar.gz.sha256`.

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "feat(release): make release-* targets wrap the release scripts

release-build  → build tarball+sha
release-upload → upload one platform's artifacts (CI runs once per runner)
release-publish-latest → flip the latest pointer (CI runs once after fan-in)
release-local  → developer rehearsal of the build-only path"
```

---

## Phase 2 — CI Integration

**Variant selection:** Phase 0.4 produced a decision file `notes/0.4-ci-decision.md`. Use it to pick exactly ONE of Tasks 2.1a / 2.1b / 2.1c. Skip the others.

### Task 2.1a: Full GitHub Actions matrix (if decision = full-GH-Actions)

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/.github/workflows/release.yml`

- [ ] **Step 1: Write workflow**

```yaml
name: Release maxc binaries
on:
  push:
    tags: ['v*']
  workflow_dispatch:
    inputs:
      version:
        description: 'Version override (defaults to tag)'
        required: false

jobs:
  build:
    strategy:
      fail-fast: false
      matrix:
        include:
          - { runner: ubuntu-22.04,         platform: linux-amd64 }
          - { runner: ubuntu-22.04-arm,     platform: linux-arm64 }
          - { runner: macos-13,             platform: darwin-amd64 }
          - { runner: macos-14,             platform: darwin-arm64 }
          - { runner: windows-2022,         platform: windows-amd64 }
          - { runner: windows-11-arm,       platform: windows-arm64 }
    runs-on: ${{ matrix.runner }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          python -m pip install -e ".[dev]" || python -m pip install -e .
          python -m pip install pyinstaller pytest
      - name: Build release tarball
        shell: bash
        env:
          OUTPUT_DIR: dist/release
        run: bash scripts/build_release.sh
      - name: Smoke test the bundle
        run: pytest tests/test_pyinstaller_bundle.py -v
      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: maxc-${{ matrix.platform }}
          path: dist/release/

  publish:
    needs: build
    runs-on: ubuntu-22.04
    if: startsWith(github.ref, 'refs/tags/v')
    steps:
      - uses: actions/checkout@v4
      - uses: actions/download-artifact@v4
        with: { path: ./artifacts }
      - name: Install ossutil
        run: |
          curl -fsSL https://gosspublic.alicdn.com/ossutil/install.sh | sudo bash
      - name: Upload each platform
        env:
          OSS_BUCKET: ${{ secrets.OSS_BUCKET }}
          OSS_REGION: ${{ secrets.OSS_REGION }}
          OSS_ACCESS_KEY_ID: ${{ secrets.OSS_AK_ID }}
          OSS_ACCESS_KEY_SECRET: ${{ secrets.OSS_AK_SECRET }}
        run: |
          ossutil config -e oss-${OSS_REGION}.aliyuncs.com -i $OSS_ACCESS_KEY_ID -k $OSS_ACCESS_KEY_SECRET
          VERSION=${GITHUB_REF_NAME#v}
          for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64 windows-arm64; do
            INPUT_DIR=./artifacts/maxc-${platform} VERSION=$VERSION PLATFORM=$platform \
              bash scripts/upload_to_oss.sh
          done
      - name: Flip latest
        env:
          OSS_BUCKET: ${{ secrets.OSS_BUCKET }}
          OSS_REGION: ${{ secrets.OSS_REGION }}
        run: VERSION=${GITHUB_REF_NAME#v} bash scripts/publish_latest.sh
```

- [ ] **Step 2: Add OSS secrets to repo settings [HUMAN]**

In GitHub repo settings → Secrets and variables → Actions, add:
- `OSS_BUCKET`, `OSS_REGION`, `OSS_AK_ID`, `OSS_AK_SECRET`

- [ ] **Step 3: Trigger a workflow_dispatch dry-run on a non-prod bucket**

Use a staging bucket (or temporary `OSS_BUCKET` override pointing to a sandbox bucket).
Expected: 6 matrix jobs pass, artifacts uploaded, latest pointer updated.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci(release): GitHub Actions 6-platform matrix for maxc binaries

Builds PyInstaller onedir on linux/darwin/windows × amd64/arm64,
smoke-tests each bundle, uploads to OSS, flips versions/latest only
after all 6 succeed. Triggered by v* tags or manual dispatch.
"
```

### Task 2.1b: Hybrid — GH Actions matrix + Aone publish (if decision = hybrid)

**Files:**
- Create: `/Users/dingxin/pythonProject/maxc-cli/.github/workflows/release.yml` (build-only variant)
- Modify: `/Users/dingxin/pythonProject/maxc-cli/.aoneci/cli-publish.yaml`

Same as 2.1a but the `publish` job is removed from the GH Actions workflow; artifacts are downloaded by Aone CI which runs the OSS upload + flip.

Skip if not selected by Phase 0.4.

- [ ] **Step 1: GH Actions workflow without publish job** (same matrix as 2.1a, drops `publish:` job)

- [ ] **Step 2: Aone CI consumes artifacts**

Modify `.aoneci/cli-publish.yaml` to add a job that fetches GH Actions artifact (via API + token) and runs `scripts/upload_to_oss.sh` + `scripts/publish_latest.sh`. Exact YAML depends on Aone's HTTP-fetch action availability — discover during implementation.

- [ ] **Step 3: Verify end-to-end** (tag → GH build → Aone publish → bucket populated).

- [ ] **Step 4: Commit both files**

### Task 2.1c: Full Aone CI matrix (if decision = full-Aone)

Only viable if Phase 0.4 confirmed Aone has all 6 runner types.

- [ ] **Step 1: Rewrite `.aoneci/cli-publish.yaml` with matrix**

Will need Aone-specific matrix syntax — consult Aone docs.

- [ ] **Step 2: Verify all platforms succeed** as in 2.1a Step 3.

- [ ] **Step 3: Commit**

### Task 2.2: Release dry-run on beta channel

**Files:**
- Read: `notes/0.5-oss-bucket.md` for bucket name

[HUMAN GATE] — requires bucket from 0.5 and CI from 2.1.

- [ ] **Step 1: Push a `v0.0.0-beta1` tag**

```bash
git tag v0.0.0-beta1
git push origin v0.0.0-beta1
```

- [ ] **Step 2: Watch CI complete, verify OSS layout**

```bash
for p in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64 windows-arm64; do
  echo "--- $p ---"
  curl -fsI "https://<bucket>.oss-cn-hangzhou.aliyuncs.com/0.0.0-beta1/${p}/maxc.tar.gz"
  curl -fsI "https://<bucket>.oss-cn-hangzhou.aliyuncs.com/0.0.0-beta1/${p}/maxc.tar.gz.sha256"
done
curl -fsS "https://<bucket>.oss-cn-hangzhou.aliyuncs.com/versions/latest"
```

Expected: all 200, latest = `0.0.0-beta1`.

- [ ] **Step 3: Download one platform manually, run smoke check**

```bash
curl -fsLO "https://<bucket>.oss-cn-hangzhou.aliyuncs.com/0.0.0-beta1/darwin-arm64/maxc.tar.gz"
curl -fsLO "https://<bucket>.oss-cn-hangzhou.aliyuncs.com/0.0.0-beta1/darwin-arm64/maxc.tar.gz.sha256"
shasum -a 256 -c <(echo "$(cat maxc.tar.gz.sha256)  maxc.tar.gz")
tar -xzf maxc.tar.gz
./maxc/maxc --version
```

Expected: sha matches, version prints `0.0.0-beta1`.

- [ ] **Step 4: Document beta channel results in `notes/2.2-beta-release.md`**

---

## Phase 3 — cliext/maxc/ Go Launcher (in aliyun-cli fork)

**Working directory changes here.** Phases 1 and 2 happened in maxc-cli. Phase 3 happens in a fresh clone of aliyun-cli.

### Task 3.0: Set up aliyun-cli working tree

**Files:**
- External: clone of `https://github.com/aliyun/aliyun-cli` to a directory of your choice (suggested `~/work/aliyun-cli`)

- [ ] **Step 1: Clone the fork** (after Phase 0.6 fork is set up)

```bash
git clone https://github.com/<your-fork>/aliyun-cli ~/work/aliyun-cli
cd ~/work/aliyun-cli
git remote add upstream https://github.com/aliyun/aliyun-cli
git fetch upstream
git checkout -b feat/cliext-maxc upstream/master
```

- [ ] **Step 2: Verify the build works on master baseline**

```bash
cd ~/work/aliyun-cli
go build ./...
go test ./cliext/cms2/...   # use cms2 as a reference smoke test
```

Expected: both succeed.

- [ ] **Step 3: Read the cms2 source as the canonical template**

```bash
cat cliext/cms2/main.go cliext/cms2/cms2.go cliext/cms2/main_test.go cliext/cms2/cms2_test.go
```

Internalize the pattern; every Go file in subsequent tasks should diverge from this only where spec § 2 prescribes.

### Task 3.1: `cliext/maxc/main.go` — `NewMaxcCommand()` factory skeleton

**Files:**
- Create: `~/work/aliyun-cli/cliext/maxc/main.go`
- Create: `~/work/aliyun-cli/cliext/maxc/main_test.go`

- [ ] **Step 1: Write failing test**

`cliext/maxc/main_test.go`:
```go
package maxc

import (
	"testing"
)

func TestNewMaxcCommand_BasicShape(t *testing.T) {
	cmd := NewMaxcCommand()
	if cmd == nil {
		t.Fatal("NewMaxcCommand returned nil")
	}
	if cmd.Name != "maxc" {
		t.Errorf("expected Name=maxc, got %q", cmd.Name)
	}
	if !cmd.EnableUnknownFlag {
		t.Error("EnableUnknownFlag must be true so aliyun root does not parse maxc subcommand flags")
	}
	if !cmd.KeepArgs {
		t.Error("KeepArgs must be true")
	}
}
```

- [ ] **Step 2: Run test, see it fail**

```bash
cd ~/work/aliyun-cli && go test ./cliext/maxc/...
```

Expected: build error `undefined: NewMaxcCommand`.

- [ ] **Step 3: Implement `main.go`**

Adapted from `cliext/cms2/main.go`:
```go
package maxc

import (
	"github.com/aliyun/aliyun-cli/v3/cli"
	"github.com/aliyun/aliyun-cli/v3/i18n"
)

func NewMaxcCommand() *cli.Command {
	return &cli.Command{
		Name: "maxc",
		Short: i18n.T(
			"MaxCompute CLI for AI agents — structured envelope output, query/job/meta/data tools.",
			"MaxCompute CLI 工具层（供 AI agent 调用） — 结构化输出，覆盖 SQL/作业/元数据/数据采样。"),
		Usage:             "aliyun maxc <command> [args...] [options...]",
		Hidden:            false,
		EnableUnknownFlag: true,
		KeepArgs:          true,
		SkipDefaultHelp:   true,
		Run: func(ctx *cli.Context, args []string) error {
			// cms2 pattern: translate --help → help so the child sees the right token
			if ctx.IsHelp() {
				hasHelp := false
				for i, arg := range args {
					if arg == "help" {
						hasHelp = true
						break
					} else if arg == "--help" {
						args[i] = "help"
						hasHelp = true
						break
					}
				}
				if !hasHelp {
					args = append(args, "help")
				}
			}
			c := NewContext(ctx)
			return c.Run(args)
		},
	}
}
```

- [ ] **Step 4: Run test, see it pass**

```bash
go test ./cliext/maxc/...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd ~/work/aliyun-cli
git add cliext/maxc/
git commit -m "feat(cliext/maxc): NewMaxcCommand skeleton + KeepArgs/EnableUnknownFlag

Initial cobra command factory that registers 'aliyun maxc'. Mirrors
cliext/cms2 structure. Delegates to a forthcoming Context.Run().
"
```

### Task 3.2: `cliext/maxc/maxc.go` — Context, InitBasicInfo, CheckOsTypeAndArch

**Files:**
- Create: `~/work/aliyun-cli/cliext/maxc/maxc.go`
- Create: `~/work/aliyun-cli/cliext/maxc/maxc_test.go`

- [ ] **Step 1: Write failing tests for the 6-platform whitelist**

`maxc_test.go`:
```go
package maxc

import (
	"testing"
)

func TestPlatformWhitelist(t *testing.T) {
	cases := map[string]bool{
		"linux-amd64":   true,
		"linux-arm64":   true,
		"darwin-amd64":  true,
		"darwin-arm64":  true,
		"windows-amd64": true,
		"windows-arm64": true,
		"freebsd-amd64": false,
		"linux-386":     false,
	}
	for k, want := range cases {
		_, got := platformPaths[k]
		if got != want {
			t.Errorf("platformPaths[%q] = %v, want %v", k, got, want)
		}
	}
}

func TestInitBasicInfo_DerivesPathFromConfig(t *testing.T) {
	origGetConfigurePath := getConfigurePathFunc
	defer func() { getConfigurePathFunc = origGetConfigurePath }()
	getConfigurePathFunc = func() string { return "/tmp/test-aliyun" }

	c := &Context{}
	c.InitBasicInfo()

	if c.installDir != "/tmp/test-aliyun/maxc" {
		t.Errorf("installDir = %q, want /tmp/test-aliyun/maxc", c.installDir)
	}
	wantExec := "/tmp/test-aliyun/maxc/maxc"
	if runtimeGOOSFunc() == "windows" {
		wantExec += ".exe"
	}
	if c.execFilePath != wantExec {
		t.Errorf("execFilePath = %q, want %q", c.execFilePath, wantExec)
	}
}
```

- [ ] **Step 2: Run, see fail**

`go test ./cliext/maxc/...` → fail on undefined `platformPaths`, `Context`, etc.

- [ ] **Step 3: Implement `maxc.go` skeleton (without download logic yet)**

```go
package maxc

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/aliyun/aliyun-cli/v3/cli"
	"github.com/aliyun/aliyun-cli/v3/config"
)

type Context struct {
	originCtx       *cli.Context
	configPath      string
	installDir      string  // ~/.aliyun/maxc
	execFilePath    string  // ~/.aliyun/maxc/maxc[.exe]
	versionCachePath string // ~/.aliyun/maxc/.version_check
	versionFilePath  string // ~/.aliyun/maxc/.version
	installed       bool
	versionLocal    string
	versionRemote   string
	osType          string
	osArch          string
	osSupport       bool
	platformKey     string
	envMap          map[string]string
}

type ExitError struct{ Code int }

func (e *ExitError) Error() string  { return fmt.Sprintf("subprocess exited with code %d", e.Code) }
func (e *ExitError) ExitCode() int  { return e.Code }

var (
	getConfigurePathFunc = func() string { return config.GetConfigPath() }
	runtimeGOOSFunc      = func() string { return runtime.GOOS }
	runtimeGOARCHFunc    = func() string { return runtime.GOARCH }
)

var platformPaths = map[string]struct{}{
	"linux-amd64":   {},
	"linux-arm64":   {},
	"darwin-amd64":  {},
	"darwin-arm64":  {},
	"windows-amd64": {},
	"windows-arm64": {},
}

// Default download base; overridable via ALIBABA_CLOUD_MAXC_DOWNLOAD_BASE_URL.
// TODO(Phase 0.5): replace with the real bucket URL once provisioned.
var downloadBaseURL = "https://<bucket>.oss-cn-hangzhou.aliyuncs.com"

const VersionCheckTTL = 86400 // seconds

func NewContext(origin *cli.Context) *Context {
	return &Context{originCtx: origin}
}

func (c *Context) InitBasicInfo() {
	c.configPath = getConfigurePathFunc()
	c.installDir = filepath.Join(c.configPath, "maxc")
	binName := "maxc"
	if runtimeGOOSFunc() == "windows" {
		binName += ".exe"
	}
	c.execFilePath = filepath.Join(c.installDir, binName)
	c.versionCachePath = filepath.Join(c.installDir, ".version_check")
	c.versionFilePath = filepath.Join(c.installDir, ".version")

	if envPath := os.Getenv("ALIBABA_CLOUD_MAXC_EXEC_PATH"); envPath != "" {
		c.execFilePath = envPath
	}
	c.installed = fileExists(c.execFilePath)
}

func (c *Context) CheckOsTypeAndArch() {
	c.osType = runtimeGOOSFunc()
	c.osArch = runtimeGOARCHFunc()
	c.platformKey = c.osType + "-" + c.osArch
	if _, ok := platformPaths[c.platformKey]; ok {
		c.osSupport = true
	}
}

func (c *Context) Run(args []string) error {
	c.InitBasicInfo()
	c.CheckOsTypeAndArch()
	if !c.osSupport {
		return fmt.Errorf("your os type %s and arch %s is not supported", c.osType, c.osArch)
	}
	// Subsequent tasks fill in download/credentials/exec logic.
	return fmt.Errorf("not implemented yet")
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
```

- [ ] **Step 4: Run test, see pass**

```bash
go test ./cliext/maxc/... -run 'TestPlatformWhitelist|TestInitBasicInfo'
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cliext/maxc/maxc.go cliext/maxc/maxc_test.go
git commit -m "feat(cliext/maxc): Context + platform whitelist + InitBasicInfo

Implements the 6-platform whitelist and resolves the on-disk install
paths (~/.aliyun/maxc/{maxc,.version,.version_check}). Download, update
check, credential injection, and exec come in subsequent commits.
"
```

### Task 3.3: Download flow — fetch tarball, verify sha256, atomic install

**Files:**
- Modify: `~/work/aliyun-cli/cliext/maxc/maxc.go`
- Modify: `~/work/aliyun-cli/cliext/maxc/maxc_test.go`

- [ ] **Step 1: Write failing tests for download/verify/extract**

Append to `maxc_test.go`. Use `cliext/cms2/cms2_test.go` as the canonical template for `httpGetFunc` / `httptest.NewServer` patterns — don't re-derive:

```go
func TestDownloadAndInstall_VerifiesSha256(t *testing.T) {
	// See cliext/cms2/cms2_test.go for httptest.NewServer pattern.
	// Setup a fake "OSS" serving a known tarball + matching sha256 file.
	// Test: success path leaves c.execFilePath present.
}

func TestDownloadAndInstall_RejectsBadSha(t *testing.T) {
	// Same server, but sha256 file contents do not match the tarball.
	// Test: returns error, leaves c.installDir empty / unchanged.
}

func TestDownloadAndInstall_AtomicRenameKeepsOldOnFailure(t *testing.T) {
	// Pre-populate c.installDir with a 'maxc' binary, then trigger a
	// failed extract mid-way. Old binary should still be intact.
}

func TestDownloadURL_RespectsBaseURLEnvOverride(t *testing.T) {
	// Set ALIBABA_CLOUD_MAXC_DOWNLOAD_BASE_URL=https://override.example.com.
	// Assert: the URL the launcher GETs starts with https://override.example.com
	// for both versions/latest and {ver}/{platform}/maxc.tar.gz.
	// Spec § 4: this overrides the bucket root, not per-path.
}
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement download + verify + atomic install in `maxc.go`**

Add helpers:
- `func (c *Context) effectiveBaseURL() string` — returns `os.Getenv("ALIBABA_CLOUD_MAXC_DOWNLOAD_BASE_URL")` if non-empty, otherwise the package-level `downloadBaseURL` constant. ALL subsequent URL construction (`versions/latest`, `{ver}/{platform}/maxc.tar.gz`, `.sha256`) goes through this. Spec § 4: env override replaces the bucket root, not per-path.
- `func (c *Context) downloadTarball(version string) (tarPath string, err error)` — `httpGetFunc(c.effectiveBaseURL() + "/" + version + "/" + c.platformKey + "/maxc.tar.gz")` to a temp file
- `func verifyShaFromFile(tarPath, shaURL string) error` — download .sha256 via same baseURL helper, compare
- `func extractAndAtomicRename(tarPath, dest string) error` — `archive/tar` → `os.Rename(installDir → installDir.old.<ts>)` → `os.Rename(extractedMaxc → installDir)` → `os.RemoveAll(old)`

Inject `httpGetFunc` and `httpDoFunc` vars analogous to cms2.go for testability.

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(cliext/maxc): tarball download + sha256 verify + atomic rename

Spec § 3 install flow: fetch tar.gz + .sha256, refuse to write if hashes
mismatch, extract to tmp, swap install dir atomically (old → .old.<ts>,
new → live), async clean up old. Mockable via httpGetFunc/execCommandFunc.
"
```

### Task 3.4: Update check — TTL 86400s + EnsureInstalledAndUpdated

**Files:**
- Modify: `~/work/aliyun-cli/cliext/maxc/maxc.go`
- Modify: `~/work/aliyun-cli/cliext/maxc/maxc_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestEnsureInstalled_NotInstalled_DownloadsLatest(t *testing.T) { /* ... */ }
func TestEnsureInstalled_FreshCache_Skips(t *testing.T) { /* ... */ }
func TestEnsureInstalled_StaleCache_ChecksRemote(t *testing.T) { /* ... */ }
func TestEnsureInstalled_RemoteCheckFails_KeepsOldVersion(t *testing.T) {
	// Existing install + remote 500 → no error returned (warning to stderr ok),
	// installed binary still usable.
}
func TestEnsureInstalled_NoUpdateCheckEnv_Skips(t *testing.T) {
	// ALIBABA_CLOUD_MAXC_NO_UPDATE_CHECK=1 → never hit remote.
}
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

```go
func (c *Context) EnsureInstalledAndUpdated() error {
	if os.Getenv("ALIBABA_CLOUD_MAXC_EXEC_PATH") != "" { return nil }
	if os.Getenv("ALIBABA_CLOUD_MAXC_NO_UPDATE_CHECK") == "1" { return nil }

	if !c.installed {
		latest, err := getLatestVersionFunc()
		if err != nil { return err }
		return c.downloadAndInstall(latest)
	}

	// TTL check
	if !c.cacheStale() { return nil }
	defer c.touchCache()

	latest, err := getLatestVersionFunc()
	if err != nil {
		fmt.Fprintf(c.originCtx.Stderr(),
			"Warning: failed to check maxc updates: %v\n", err)
		return nil
	}
	if latest == c.readLocalVersion() { return nil }
	return c.downloadAndInstall(latest)
}
```

Plus helpers `cacheStale`, `touchCache`, `readLocalVersion`, `getLatestVersionFunc` (injectable var).

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

### Task 3.5: `credentials.go` — profile → env injection

**Files:**
- Create: `~/work/aliyun-cli/cliext/maxc/credentials.go`
- Create: `~/work/aliyun-cli/cliext/maxc/credentials_test.go`

- [ ] **Step 1: Write failing tests covering 5 branches**

```go
package maxc

import "testing"

func TestInjectCreds_AK(t *testing.T) {
	// Mock loadProfileFunc returns AK profile → env has _ID/_SECRET, no _SECURITY_TOKEN.
}
func TestInjectCreds_StsToken(t *testing.T) {
	// Profile mode=StsToken → env has _ID/_SECRET/_SECURITY_TOKEN.
}
func TestInjectCreds_RamRoleArn(t *testing.T) {
	// Profile mode=RamRoleArn, GetCredential() returns temp creds → env has all 3.
}
func TestInjectCreds_NoProfile_Silent(t *testing.T) {
	// loadProfileFunc returns error "profile not found" → no env injected, no error returned.
}
func TestInjectCreds_GetCredentialFails(t *testing.T) {
	// Profile loads but GetCredential errors (network/STS failure) → return error.
}
```

- [ ] **Step 2: Run, see fail**

- [ ] **Step 3: Implement**

Profile flag extraction from `*cli.Context`: cms2 doesn't read --profile itself (it just forwards), so this is a new pattern for cliext. The canonical source is `config/configure_*.go` in aliyun-cli — grep for `Get("profile")` or `ProfileFlag` to find the exact accessor before writing the code below.

```go
package maxc

import (
	"context"
	"fmt"

	"github.com/aliyun/aliyun-cli/v3/cli"
	"github.com/aliyun/aliyun-cli/v3/config"
)

var loadProfileFunc = func(ctx *cli.Context, name string) (config.Profile, error) {
	cfg, err := config.LoadCurrentConfiguration()
	if err != nil { return config.Profile{}, err }
	return cfg.GetProfile(name)
}

// InjectAliyunCredentials reads the active aliyun profile and populates
// c.envMap with ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET[/SECURITY_TOKEN] plus
// MAXCOMPUTE_REGION. On profile-missing it returns nil silently so the
// child maxc process can fall back to its own config chain (spec § 2).
func (c *Context) InjectAliyunCredentials(args []string) error {
	if c.envMap == nil { c.envMap = map[string]string{} }

	profileName := c.originCtx.Flags().Get("profile").GetValue() // or empty for default
	profile, err := loadProfileFunc(c.originCtx, profileName)
	if err != nil {
		// silent fallback — spec § 2 step 5
		return nil
	}

	cred, err := profile.GetCredential(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("failed to resolve credentials for profile %q: %w", profileName, err)
	}

	model, err := cred.GetCredential()
	if err != nil { return err }

	if model.AccessKeyId != nil {
		c.envMap["ALIBABA_CLOUD_ACCESS_KEY_ID"] = *model.AccessKeyId
	}
	if model.AccessKeySecret != nil {
		c.envMap["ALIBABA_CLOUD_ACCESS_KEY_SECRET"] = *model.AccessKeySecret
	}
	if model.SecurityToken != nil && *model.SecurityToken != "" {
		c.envMap["ALIBABA_CLOUD_SECURITY_TOKEN"] = *model.SecurityToken
	}
	if profile.RegionId != "" {
		c.envMap["MAXCOMPUTE_REGION"] = profile.RegionId
	}
	return nil
}
```

(Exact `cli.Context` profile flag accessor may differ — discover and adjust during impl.)

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

### Task 3.6: `Execute` — exec subprocess, propagate exit code

**Files:**
- Modify: `~/work/aliyun-cli/cliext/maxc/maxc.go`
- Modify: `~/work/aliyun-cli/cliext/maxc/maxc_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestExecute_ForwardsExitCode(t *testing.T) {
	// Mock execCommandFunc to return a fake exit code 42 → c.Execute returns
	// ExitError{Code:42}.
}
func TestExecute_PassesArgsAndEnv(t *testing.T) {
	// Spy on execCommandFunc invocation: args match, env contains injected creds.
}
func TestRemoveFlagsForMainCli_StripsProfile(t *testing.T) {
	// Input: ["--profile", "prod", "query", "--sql", "x"]
	// Output: ["query", "--sql", "x"]
}
```

- [ ] **Step 2: Run, fail**

- [ ] **Step 3: Implement**

```go
func (c *Context) Execute(args []string) error {
	cmd := execCommandFunc(c.execFilePath, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = c.originCtx.Stdout()
	cmd.Stderr = c.originCtx.Stderr()

	env := os.Environ()
	for k, v := range c.envMap {
		env = append(env, k+"="+v)
	}
	cmd.Env = env

	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return &ExitError{Code: ee.ExitCode()}
		}
		return err
	}
	return nil
}

func (c *Context) RemoveFlagsForMainCli(args []string) []string {
	// Strip --profile <val> and similar aliyun root flags; leave maxc subcommands intact.
	// Conservative: strip exactly the set of flags aliyun root knows about.
	// See cms2.go for the precise list to keep parity.
}
```

- [ ] **Step 4: Tests pass**

- [ ] **Step 5: Commit**

### Task 3.7: Wire `Context.Run` end-to-end + integration test

**Files:**
- Modify: `~/work/aliyun-cli/cliext/maxc/maxc.go`
- Modify: `~/work/aliyun-cli/cliext/maxc/maxc_test.go`

- [ ] **Step 1: Update `Context.Run` to chain all the pieces**

```go
func (c *Context) Run(args []string) error {
	c.InitBasicInfo()
	c.CheckOsTypeAndArch()
	if !c.osSupport {
		return fmt.Errorf("your os type %s and arch %s is not supported", c.osType, c.osArch)
	}
	if err := c.EnsureInstalledAndUpdated(); err != nil {
		if !c.installed { return err }
		fmt.Fprintf(c.originCtx.Stderr(), "Warning: maxc update check failed: %v\n", err)
	}
	if err := c.InjectAliyunCredentials(args); err != nil {
		return err
	}
	childArgs := c.RemoveFlagsForMainCli(args)
	return c.Execute(childArgs)
}
```

- [ ] **Step 2: Write integration test that exercises the full chain with mocks**

```go
func TestRun_FullChain_Mocked(t *testing.T) {
	// Inject: http server serving a tiny tarball, mock loadProfile returning AK profile,
	// mock execCommandFunc capturing args/env. Run Context.Run(["query", "--sql", "1"]).
	// Assert: env has _ID/_SECRET, args == ["query","--sql","1"], exit 0.
}
```

- [ ] **Step 3: Tests pass**

- [ ] **Step 4: Commit**

### Task 3.8: Register `NewMaxcCommand` into aliyun root

**Files:**
- Modify: a registration site in aliyun-cli main — discover by grepping for `NewCms2Command`

- [ ] **Step 1: Find registration site**

```bash
cd ~/work/aliyun-cli
grep -rn "NewCms2Command" --include="*.go" | grep -v cliext/
```

Expected: one or two hits in the aliyun root setup (e.g. `main/main.go` or `cli/setup.go`).

- [ ] **Step 2: Add `NewMaxcCommand` registration alongside cms2**

Wherever `NewCms2Command()` is called and added to root, add the same for `NewMaxcCommand`:
```go
import "github.com/aliyun/aliyun-cli/v3/cliext/maxc"
...
rootCmd.AddSubCommand(maxc.NewMaxcCommand())
```

- [ ] **Step 3: Build the full CLI, check command appears**

```bash
go build -o /tmp/aliyun ./main/
/tmp/aliyun help 2>&1 | grep -i maxc
/tmp/aliyun maxc --help
```

Expected: `maxc` appears in root help; `aliyun maxc --help` calls into our cliext (will likely error on "binary not found" since nothing's downloaded yet).

- [ ] **Step 4: Set `ALIBABA_CLOUD_MAXC_EXEC_PATH` to a locally-built maxc, test end-to-end**

```bash
# Build maxc onedir in the maxc-cli repo first (Task 0.2 output)
ALIBABA_CLOUD_MAXC_EXEC_PATH=/Users/dingxin/pythonProject/maxc-cli/dist/maxc/maxc \
  /tmp/aliyun maxc --version
```

Expected: maxc's version output.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(cli): register cliext/maxc/ NewMaxcCommand on root

Wires 'aliyun maxc' into the root command tree alongside cms2/saectl/
appmanagerutil. End-to-end smoke verified with a locally-built maxc
binary via ALIBABA_CLOUD_MAXC_EXEC_PATH.
"
```

---

## Phase 4 — Integration Testing & Runbook

Manual, per spec § 5. Produce reusable runbook artifacts so future maintainers can repeat.

### Task 4.1: Build & manual smoke against staging bucket

**Files:**
- Create: `~/work/aliyun-cli/cliext/maxc/RUNBOOK.md`

- [ ] **Step 1: Replace `downloadBaseURL` with the real bucket URL** (from Phase 0.5)

In `cliext/maxc/maxc.go`, replace the TODO placeholder with the actual bucket URL.

- [ ] **Step 2: Build aliyun**

```bash
cd ~/work/aliyun-cli
go build -o /tmp/aliyun ./main/
```

- [ ] **Step 3: Wipe any local maxc install**

```bash
rm -rf ~/.aliyun/maxc ~/.aliyun/maxc.old.*
```

- [ ] **Step 4: Run first-install path**

```bash
/tmp/aliyun maxc --version
```

Expected stderr: `maxc: not installed locally, downloading...`. Expected stdout: maxc version.

- [ ] **Step 5: Confirm install layout**

```bash
ls -la ~/.aliyun/maxc/
file ~/.aliyun/maxc/maxc
```

Expected: `maxc` binary present, native arch.

- [ ] **Step 6: Run real query (requires a configured aliyun profile with MaxCompute access)**

```bash
/tmp/aliyun configure list
/tmp/aliyun maxc query --sql "select 1"
```

Expected: maxc envelope with `"status": "success"`.

- [ ] **Step 7: Write `cliext/maxc/RUNBOOK.md` capturing each step + the expected output**

This is what we link in the upstream PR description.

### Task 4.2: Cross-platform manual runbook [HUMAN]

For each of macOS, Linux, Windows (one each is enough for the first PR):

- [ ] **Step 1: Use a clean machine / VM / container**
- [ ] **Step 2: Install aliyun-cli built from the PR branch**
- [ ] **Step 3: `aliyun configure` with an AK profile**
- [ ] **Step 4: `aliyun maxc --version`** → first-install download succeeds
- [ ] **Step 5: `aliyun maxc query --sql "select 1"`** → success envelope
- [ ] **Step 6: Append to RUNBOOK.md**: machine specs, dates, OK/FAIL

### Task 4.3: STS / RamRoleArn manual transparency check [HUMAN]

- [ ] **Step 1: Set up a profile with `mode: RamRoleArn`**

[HUMAN] requires a real RAM role + parent AK.

- [ ] **Step 2: Activate that profile via `--profile`**

```bash
/tmp/aliyun --profile sts-role maxc whoami
```

Expected: maxc's `whoami` output reflects the assumed role's identity, NOT the parent AK.

- [ ] **Step 3: Document in RUNBOOK**

### Task 4.4: Version-update manual check

- [ ] **Step 1: With a current install, manually rewind `.version_check` file**

```bash
echo 0 > ~/.aliyun/maxc/.version_check  # or `touch -t 202001010000` it
```

- [ ] **Step 2: Run any maxc command**

```bash
/tmp/aliyun maxc --version
```

Expected: stderr shows update check; if a newer version exists on OSS, downloads it; if not, silent.

- [ ] **Step 3: Test failure path**: temporarily point `ALIBABA_CLOUD_MAXC_DOWNLOAD_BASE_URL` at a nonexistent host

```bash
ALIBABA_CLOUD_MAXC_DOWNLOAD_BASE_URL=https://nonexistent.example.com \
  /tmp/aliyun maxc --version
```

Expected: stderr warning, but maxc still runs (uses cached install).

---

## Phase 5 — Upstream PR

### Task 5.1: Polish branch for PR

**Files:**
- All of `~/work/aliyun-cli/cliext/maxc/`

- [ ] **Step 1: Run linters from aliyun-cli**

```bash
cd ~/work/aliyun-cli
make lint  # or whatever the aliyun-cli convention is — read CONTRIBUTING.md
go vet ./cliext/maxc/...
go test -race ./cliext/maxc/...
```

Fix any warnings.

- [ ] **Step 2: Verify license headers / coverage match other cliext packages**

Compare against `cliext/cms2/` boilerplate.

- [ ] **Step 3: Squash WIP commits into reviewable commits**

Recommended split:
- `feat(cliext/maxc): bootstrap NewMaxcCommand and Context skeleton`
- `feat(cliext/maxc): tarball download, sha256 verify, atomic install`
- `feat(cliext/maxc): TTL update check`
- `feat(cliext/maxc): aliyun profile → maxc env credential injection`
- `feat(cliext/maxc): exec, exit code propagation, arg cleanup`
- `feat(cli): register NewMaxcCommand on root`
- `docs(cliext/maxc): runbook`

### Task 5.2: Open upstream PR

- [ ] **Step 1: Push branch to fork**

```bash
git push -u origin feat/cliext-maxc
```

- [ ] **Step 2: Open PR via web or `gh pr create`**

Title: `feat(cliext): add maxc launcher for MaxCompute agent CLI`

Body must include:
- Link to the spec: `docs/superpowers/specs/2026-05-19-maxc-cliext-integration-design.md` (after merging the spec into a doc-accessible location — or paste relevant sections inline)
- Link to the OSS bucket and how to verify a tarball download
- Reference to Phase 0.6 issue (the sign-off conversation)
- Attach RUNBOOK.md results from Phase 4

### Task 5.3: Iterate on reviewer feedback

- [ ] **Step 1: For each review comment, decide accept / push back**

Use superpowers:receiving-code-review for the loop.

- [ ] **Step 2: Re-run smoke (Phase 4.1) after every behavioural change**

- [ ] **Step 3: On merge, update the OSS bucket public-read permissions audit log [HUMAN]**

---

## Done definition

- `aliyun/aliyun-cli` master contains `cliext/maxc/` package
- OSS bucket serves `versions/latest` and `{version}/{6 platforms}/maxc.tar.gz{,.sha256}`
- A user with a fresh `aliyun` install can run `aliyun maxc query --sql "select 1"` and get a maxc envelope back, using AK or STS credentials from their configured profile
- All Phase 4 runbook entries are PASS
- Plan notes folder (`docs/superpowers/plans/notes/`) is checked into maxc-cli with all Phase 0 verification artifacts

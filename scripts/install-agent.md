# maxc-cli Agent-Driven Install Guide

You are an AI coding agent (Claude Code, Cursor, Codex, Windsurf, Qwen, Qoder, etc.). The
user has asked you to install `maxc-cli` on this machine by following this document. Run
the steps below in order, using your shell tool. Stop and ask the user only at the points
explicitly marked **ASK USER**.

The end state after a successful run:

- `maxc` executable on PATH, `maxc --version` works
- Credentials persisted via `maxc auth login` so `maxc auth whoami` returns `verified`
- An editor skill installed for the agent currently running this doc

---

## Phase 0 — Detect the environment

Run these and remember the values; they decide every later URL and path.

```bash
uname -s     # Linux | Darwin | (Windows: use $env:OS in PowerShell)
uname -m     # x86_64 | aarch64 | arm64
echo "$SHELL"
```

Map to a `PLATFORM` string used by the OSS layout:

| uname -s | uname -m            | PLATFORM        |
|----------|---------------------|-----------------|
| Linux    | x86_64              | `linux-amd64`   |
| Linux    | aarch64 / arm64     | `linux-arm64`   |
| Darwin   | x86_64              | `darwin-amd64`  |
| Darwin   | arm64               | `darwin-arm64`  |
| Windows  | AMD64               | `windows-amd64` |

> **Known gap (2026-05):** today only `linux-amd64` is reliably published. For other
> platforms the OSS GET in Phase 2 will 404. If that happens, jump to **Fallback A**
> at the bottom of this doc (pip install) instead of failing.

---

## Phase 1 — Resolve the latest version

```bash
BASE_URL="https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli"
VERSION=$(curl -fsSL "${BASE_URL}/versions/latest")
echo "Latest version: ${VERSION}"
```

`versions/latest` is a plain-text file containing a semver string like `0.3.0`. If the
`curl` fails, the OSS bucket or your network is unreachable — stop and report to the
user.

---

## Phase 2 — Download and verify the tarball

```bash
INSTALL_ROOT="${HOME}/.maxc/bin"
mkdir -p "${INSTALL_ROOT}"
cd "$(mktemp -d)"

TARBALL_URL="${BASE_URL}/${VERSION}/${PLATFORM}/maxc.tar.gz"
SHA_URL="${TARBALL_URL}.sha256"

curl -fsSL -o maxc.tar.gz       "${TARBALL_URL}"
curl -fsSL -o maxc.tar.gz.sha256 "${SHA_URL}"
```

**Verify sha256 before extracting** — never extract an unverified bundle:

```bash
# .sha256 file is in the format: "<hex>  maxc.tar.gz"
EXPECTED=$(awk '{print $1}' maxc.tar.gz.sha256)

# Use sha256sum on Linux, shasum -a 256 on macOS.
if command -v sha256sum >/dev/null; then
  ACTUAL=$(sha256sum maxc.tar.gz | awk '{print $1}')
else
  ACTUAL=$(shasum -a 256 maxc.tar.gz | awk '{print $1}')
fi

if [ "${EXPECTED}" != "${ACTUAL}" ]; then
  echo "FATAL: sha256 mismatch (expected ${EXPECTED}, got ${ACTUAL})"
  exit 1
fi
echo "sha256 OK: ${ACTUAL}"
```

If the tarball 404s, the platform isn't published yet → use **Fallback A**.

---

## Phase 3 — Extract and link

The tarball is a PyInstaller `onedir` bundle. The `maxc` entry binary lives inside.

```bash
tar -xzf maxc.tar.gz -C "${INSTALL_ROOT}" --strip-components=0

# After extraction there is a versioned subdir; symlink the entry binary to a
# stable name so PATH doesn't need to know the version.
EXTRACTED_DIR=$(find "${INSTALL_ROOT}" -maxdepth 1 -mindepth 1 -type d -name 'maxc*' | sort -V | tail -1)
ln -sfn "${EXTRACTED_DIR}/maxc" "${INSTALL_ROOT}/maxc"
```

> **Adapt if the tarball layout differs**: list the extracted directory first
> (`ls "${INSTALL_ROOT}"`); whatever folder holds the `maxc` executable, symlink it
> at `${INSTALL_ROOT}/maxc`.

---

## Phase 4 — Put `maxc` on PATH

Detect the user's login shell and append the export line to its rc file **idempotently**
(don't double-append if it's already there).

```bash
case "$(basename "${SHELL:-/bin/bash}")" in
  zsh)  RC="${HOME}/.zshrc" ;;
  bash) RC="${HOME}/.bashrc" ;;
  fish) RC="${HOME}/.config/fish/config.fish" ;;
  *)    RC="${HOME}/.profile" ;;
esac

LINE='export PATH="${HOME}/.maxc/bin:${PATH}"'
if ! grep -qF "${LINE}" "${RC}" 2>/dev/null; then
  printf '\n# Added by maxc-cli installer\n%s\n' "${LINE}" >> "${RC}"
  echo "Added PATH entry to ${RC}"
fi

# Activate for the rest of this install session.
export PATH="${HOME}/.maxc/bin:${PATH}"

maxc --version    # must print: maxc <semver>
```

If `maxc --version` fails, stop and report. Do not continue to auth.

---

## Phase 5 — Configure authentication

**ASK USER** for the four credentials. Present them as one prompt with four labelled
fields; do not store them in conversation transcripts or commit them anywhere:

- **Access Key ID** (Aliyun RAM user, e.g. `LTAI5t...`)
- **Access Key Secret** (treat as secret; mask in any logs)
- **Project** (the MaxCompute project, e.g. `my_project`)
- **Endpoint** (the API endpoint; ask the user, or offer the common ones below)

Common endpoints (China public cloud):

| Region        | Endpoint                                                       |
|---------------|----------------------------------------------------------------|
| 华东1 杭州     | `https://service.cn-hangzhou.maxcompute.aliyun.com/api`        |
| 华东2 上海     | `https://service.cn-shanghai.maxcompute.aliyun.com/api`        |
| 华北2 北京     | `https://service.cn-beijing.maxcompute.aliyun.com/api`         |
| 华南1 深圳     | `https://service.cn-shenzhen.maxcompute.aliyun.com/api`        |

Once the user has provided them, run:

```bash
maxc auth login \
  --access-id "${AK_ID}" \
  --secret-access-key "${AK_SECRET}" \
  --project "${PROJECT}" \
  --endpoint "${ENDPOINT}" \
  --no-picker \
  --json
```

> **Why `--no-picker`**: the default `auth login` opens an interactive project picker
> when `--project` is omitted. Since the user gave a project, `--no-picker` skips the
> TTY interaction so this works under an agent that does not allocate a TTY.

Then verify:

```bash
maxc auth whoami --json
```

The envelope's `data.identity.validation_status` should be `verified`. If it is
`configuration_only`, credentials are saved but the network call to MaxCompute didn't
succeed — surface the envelope's `error` and `agent_hints.warnings` to the user and ask
how they want to proceed.

> **Security note:** `maxc auth login` writes AK/SK in plaintext to
> `~/.maxc/config.yaml` with mode `0600`. Tell the user this once.

---

## Phase 6 — Install the editor skill

Pick the skill that matches the agent following this doc:

| Agent following this doc | `<platform>` argument |
|--------------------------|-----------------------|
| Claude Code              | `claude-code`         |
| Cursor                   | `cursor`              |
| Windsurf                 | `windsurf`            |
| Codex                    | `codex`               |
| Qwen                     | `qwen`                |
| Qoder                    | `qoder`               |
| QoderWork                | `qoderwork`           |

If you (the running agent) don't know which one you are, **ASK USER** to pick.

```bash
maxc agent skill install <platform> --json
```

The envelope reports the destination directory; show that path to the user.

---

## Phase 7 — Final report

Print a one-paragraph summary to the user with:

1. The installed version (`maxc --version`)
2. The authenticated identity (`principal_display` and `project` from
   `maxc auth whoami --json`)
3. The skill destination path from Phase 6
4. Three suggested next commands:
   - `maxc meta list-tables --json`
   - `maxc query "SELECT 1" --json`
   - `maxc cache build --json`

---

## Fallback A — pip install when the OSS bundle is unavailable

Use this if the user's platform 404s in Phase 2, or if the user prefers pip.

Requires Python ≥ 3.8.

```bash
python3 -m pip install --user maxc-cli

# Ensure pip's user-site bin is on PATH (same as Phase 4 but with a different dir):
USER_BIN="$(python3 -m site --user-base)/bin"
case ":$PATH:" in
  *":${USER_BIN}:"*) ;;
  *) export PATH="${USER_BIN}:${PATH}" ;;
esac

maxc --version
```

Then resume at **Phase 5**.

---

## Failure handling

If any phase fails with a non-zero exit:

1. Print the full stderr to the user — do not summarize it away.
2. Do not silently retry. Ask the user how to proceed.
3. Never use `--no-verify`, never disable sha256 checking, never `chmod -R 777`.
4. If `maxc auth login` fails, do not retry with different credentials autonomously —
   surface the error and ask the user.

A failed install is recoverable; a half-installed binary with broken auth or a wrong
checksum is not.

# maxc-cli Agent-Driven Install Guide

You are an AI coding agent (Claude Code, Cursor, Codex, Windsurf, Qwen, Qoder, etc.). The
user has asked you to install `maxc-cli` on this machine by following this document. Run
the steps below in order, using your shell tool. Stop and ask the user only at the points
explicitly marked **ASK USER**.

This guide installs the standalone Python-compatible `maxc` entry. For public-cloud use,
the preferred entry is `aliyun maxc`; follow the current quick start in
[`README.md`](../README.md) or [`docs/install-guide.md`](../docs/install-guide.md).
Runtime commands and flags come from `agent manifest` and `--help`. `CHANGELOG.md` and
`docs/superpowers/` are historical records, not runtime contracts.

The end state after a successful run:

- `maxc` executable on PATH, `maxc --version` works
- Authentication selected without exposing secrets, and `agent doctor --online` reports
  `data.ready=true`
- The `alibabacloud-maxcompute-cli` Skill installed for the current agent

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

> Some platform bundles may be unavailable. If the server returns HTTP 404 for the
> tarball in Phase 2, jump to **Fallback A** and install with pip.

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
MAXC_HOME="${HOME}/.maxc"
INSTALL_ROOT="${MAXC_HOME}/bin"
RELEASE_ROOT="${MAXC_HOME}/releases"
mkdir -p "${INSTALL_ROOT}" "${RELEASE_ROOT}"
DOWNLOAD_DIR="$(mktemp -d)"
cd "${DOWNLOAD_DIR}"

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

If the server returns HTTP 404 for the tarball, use **Fallback A**.

---

## Phase 3 — Extract and link

The tarball is a PyInstaller `onedir` bundle. The `maxc` entry binary lives inside.
Install it under an immutable release directory first. Do not extract a `maxc/` bundle
directly into `${INSTALL_ROOT}`: that would create `${INSTALL_ROOT}/maxc` as a directory
and make the stable link point into itself.

```bash
set -euo pipefail

if ! printf '%s\n' "${VERSION}" \
  | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.+-]*)?$'; then
  echo "FATAL: unsafe release version: ${VERSION}"
  exit 1
fi
case "${PLATFORM}" in
  ""|"."|".."|*[!0-9A-Za-z._-]*) \
    echo "FATAL: unsafe platform: ${PLATFORM}"; exit 1 ;;
esac

# The published bundle contract has exactly one top-level maxc/ directory.
if ! tar -tzf maxc.tar.gz | awk '
  /^\// || $0 ~ /(^|\/)\.\.($|\/)/ || $0 !~ /^maxc(\/|$)/ { bad = 1 }
  $0 == "maxc/maxc" || $0 == "maxc/maxc.exe" { entry = 1 }
  END { exit(bad || !entry) }
'; then
  echo "FATAL: unsafe or unsupported tarball layout"
  exit 1
fi

VERSION_ROOT="${RELEASE_ROOT}/${VERSION}"
RELEASE_DIR="${VERSION_ROOT}/${PLATFORM}-${ACTUAL}"
mkdir -p "${VERSION_ROOT}"
STAGING_DIR="$(mktemp -d "${VERSION_ROOT}/.${PLATFORM}.XXXXXX")"
tar -xzf maxc.tar.gz -C "${STAGING_DIR}"

ENTRY_NAME="maxc"
if [ ! -f "${STAGING_DIR}/maxc/${ENTRY_NAME}" ] && \
   [ -f "${STAGING_DIR}/maxc/maxc.exe" ]; then
  ENTRY_NAME="maxc.exe"
fi
CANDIDATE="${STAGING_DIR}/maxc/${ENTRY_NAME}"
if [ -L "${STAGING_DIR}/maxc" ] || [ -L "${CANDIDATE}" ] || \
   [ ! -f "${CANDIDATE}" ]; then
  echo "FATAL: bundle is missing maxc/maxc[.exe]"
  rm -rf -- "${STAGING_DIR}"
  exit 1
fi
chmod u+x "${CANDIDATE}"

# Smoke-test the candidate before it can become the stable executable.
if ! CANDIDATE_OUTPUT=$("${CANDIDATE}" --version 2>/dev/null); then
  echo "FATAL: candidate failed the maxc --version smoke test"
  rm -rf -- "${STAGING_DIR}"
  exit 1
fi
CANDIDATE_VERSION=$(printf '%s\n' "${CANDIDATE_OUTPUT}" | awk '{print $NF}')
if [ "${CANDIDATE_VERSION}" != "${VERSION}" ]; then
  echo "FATAL: bundle version ${CANDIDATE_VERSION:-unknown} does not match ${VERSION}"
  rm -rf -- "${STAGING_DIR}"
  exit 1
fi

# A digest-qualified release is immutable. Serialize first publication so two
# installers cannot accidentally move one staging directory inside the other.
if [ ! -e "${RELEASE_DIR}" ]; then
  PUBLISH_LOCK="${RELEASE_DIR}.install-lock"
  if ! mkdir "${PUBLISH_LOCK}" 2>/dev/null; then
    echo "FATAL: another installer is publishing this release; retry when it finishes"
    rm -rf -- "${STAGING_DIR}"
    exit 1
  fi
  if [ ! -e "${RELEASE_DIR}" ] && ! mv "${STAGING_DIR}" "${RELEASE_DIR}"; then
    rmdir "${PUBLISH_LOCK}"
    echo "FATAL: could not publish release: ${RELEASE_DIR}"
    rm -rf -- "${STAGING_DIR}"
    exit 1
  fi
  rmdir "${PUBLISH_LOCK}"
fi

EXISTING_ENTRY="${RELEASE_DIR}/maxc/${ENTRY_NAME}"
EXISTING_VERSION=$("${EXISTING_ENTRY}" --version 2>/dev/null | awk '{print $NF}' || true)
if [ "${EXISTING_VERSION}" != "${VERSION}" ]; then
  echo "FATAL: existing release is incomplete or corrupt: ${RELEASE_DIR}"
  [ ! -d "${STAGING_DIR}" ] || rm -rf -- "${STAGING_DIR}"
  exit 1
fi
[ ! -d "${STAGING_DIR}" ] || rm -rf -- "${STAGING_DIR}"

RELEASE_ENTRY="${RELEASE_DIR}/maxc/${ENTRY_NAME}"
STABLE_ENTRY="${INSTALL_ROOT}/maxc"

# Migrate the directory produced by installers older than this guide without
# deleting it. This one-time move makes room for the stable executable link.
if [ -d "${STABLE_ENTRY}" ] && [ ! -L "${STABLE_ENTRY}" ]; then
  LEGACY_BACKUP="${RELEASE_ROOT}/legacy-maxc.$(date +%Y%m%d%H%M%S).$$"
  mv "${STABLE_ENTRY}" "${LEGACY_BACKUP}"
  echo "Preserved legacy install at ${LEGACY_BACKUP}"
fi

# A legacy link to a directory also makes portable `mv` implementations treat
# the destination as a directory. Preserve that bad link before switching.
if [ -L "${STABLE_ENTRY}" ] && [ -d "${STABLE_ENTRY}" ]; then
  LEGACY_LINK="${RELEASE_ROOT}/legacy-maxc-link.$(date +%Y%m%d%H%M%S).$$"
  mv "${STABLE_ENTRY}" "${LEGACY_LINK}"
  echo "Preserved legacy directory link at ${LEGACY_LINK}"
fi

# Build the link under the same filesystem, then rename it over the stable
# entry. POSIX rename keeps an existing working executable visible until the
# new, already-smoke-tested release is ready.
LINK_STAGE="$(mktemp -d "${INSTALL_ROOT}/.link.XXXXXX")"
ln -s "${RELEASE_ENTRY}" "${LINK_STAGE}/maxc"
mv -f "${LINK_STAGE}/maxc" "${STABLE_ENTRY}"
rmdir "${LINK_STAGE}"

"${STABLE_ENTRY}" --version
```

If the archive does not match the documented `maxc/maxc[.exe]` layout, stop and use the
pip fallback. Do not guess another executable path or publish a link to an unverified file.

---

## Phase 4 — Put `maxc` on PATH

Detect the login shell configured for the user and append the export line to its rc file
**idempotently**. Do not append the line when it already exists.

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

Preserve a usable existing identity. Run:

```bash
maxc auth whoami --json
```

If `data.identity.validation_status` is `verified`, show the principal and project to the
user and continue without replacing the configuration. Otherwise, start OAuth login:

```bash
maxc auth login --oauth --json
```

For a headless environment, add `--no-browser` and show the returned sign-in URL to the
user. If the command returns `status=pending`, follow its structured
`agent_hints.actions`; ask the user to choose a project only when the CLI requires that
choice.

Do not ask the user to paste a secret into chat. Do not put credentials in command-line
arguments, logs, shell history, or the final report. If OAuth is unavailable and the user
or runtime explicitly requires another provider, use one of these safe routes:

- Import credentials already injected into the process environment with
  `maxc auth login --from-env --json`.
- Configure an approved external credential process with `maxc auth login-external`.
- Ask the user to complete credential configuration outside the agent transcript, then
  rerun the checks below.

Verify local command discovery and live readiness:

```bash
maxc agent context --json
maxc agent manifest --json
maxc agent doctor --online --json
```

`agent context` is local-only and does not prove authentication or network reachability.
Continue to data operations only when `agent doctor --online` returns `data.ready=true`.
If it does not, surface the error and warnings in the envelope without exposing credentials.

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
| OpenClaw                 | `openclaw`            |
| Hermes                   | `hermes`              |
| Other supported agent    | `others`              |

If the running agent cannot identify the platform, **ASK USER** to pick one.

```bash
maxc agent skill install <platform> --invocation maxc --json
```

The envelope reports the `alibabacloud-maxcompute-cli` destination directory; show that
path to the user. Treat `maxc agent skill install --help` as the current platform list.

---

## Phase 7 — Final report

Print a one-paragraph summary to the user with:

1. The installed version (`maxc --version`)
2. The authenticated identity (`principal_display` and `project` from
   `maxc auth whoami --json`)
3. The online readiness result from `maxc agent doctor --online --json`
4. The Skill destination path from Phase 6
5. Three suggested next commands:
   - `maxc meta list-tables --json`
   - `maxc query "SELECT 1" --json`
   - `maxc cache build --json`

---

## Fallback A — pip install when the OSS bundle is unavailable

Use this if the server returns HTTP 404 for the platform bundle in Phase 2, or if the user
prefers pip.

Requires Python ≥ 3.9.

```bash
python3 -m pip install --user maxc-cli

# Ensure the pip user-site bin directory is on PATH (same as Phase 4 but with a different dir):
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
4. If OAuth login fails, do not switch credential providers autonomously. Surface the
   error and ask the user how to proceed.

A failed install is recoverable; a half-installed binary with broken auth or a wrong
checksum is not.

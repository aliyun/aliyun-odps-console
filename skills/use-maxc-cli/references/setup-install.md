# MaxC Environment Setup And Install

Read this file before any ODPS task when the target machine may not already have Python, `maxc-cli`, or `ncs`.

## Scope

This skill is designed to be distributed to internal users. It should bootstrap the user machine before handling ODPS questions.

Current assumptions:

- `maxc-cli` supports Python `3.6` through `3.12`
- the bundled `ncs` installer script is macOS-only
- the preferred auth path is `ncs`, not access key or STS

## Setup Order

Always bootstrap in this order:

1. Python
2. `maxc-cli`
3. `ncs`
4. `auth login-ncs`
5. metadata bootstrap such as `cache build`

Do not skip to auth before the three local prerequisites are ready.

## Step 1: Check Python

Verify Python first:

```bash
python3 --version
python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")'
python3 -m pip --version
```

Interpretation:

- if `python3` is missing, install Python before anything else
- if the version is outside `3.6` to `3.12`, explain the supported range and ask the user for confirmation before changing Python
- prefer `python3 -m pip ...` over bare `pip`
- never perform Python installation or upgrade commands without explicit user confirmation

### Install Python On macOS

Only after the user confirms, a common macOS path when Homebrew is available is:

```bash
brew install python@3.12
```

If `brew` is not installed, use the official macOS Python installer for a supported version such as `3.12`, then rerun the Python checks above.

Do not install or upgrade Python proactively. First tell the user why the current interpreter is insufficient, then ask whether they want you to make the system-level change.

Do not install `maxc-cli` into unsupported interpreters. The package metadata currently targets Python `>=3.6,<3.13`.

## Step 2: Install `maxc-cli`

Install with pip:

```bash
python3 -m pip install --upgrade maxc-cli
```

If the environment requires a user-local install:

```bash
python3 -m pip install --user --upgrade maxc-cli
```

Verify install with either the console script or module path:

```bash
maxc --help
python3 -m maxc_cli --help
```

Notes:

- if `maxc --help` fails but `python3 -m maxc_cli --help` works, continue using `python3 -m maxc_cli ...`

## Step 3: Install `ncs`

Check the current `ncs` binary:

```bash
command -v ncs
which ncs
```

Treat these cases as "needs install":

- no `ncs` on `PATH`
- the path points to `.real/third_party/cli/aone-kit/bin`

### Run The Bundled Installer

The installer script lives at `<skill_root>/scripts/install_ncs.sh`. The exact path depends on the platform:

```bash
# Find the installer dynamically
INSTALL_SCRIPT=""
for candidate in \
    "$HOME/.codex/skills/use-maxc-cli/scripts/install_ncs.sh" \
    "$(git rev-parse --show-toplevel 2>/dev/null)/skills/use-maxc-cli/scripts/install_ncs.sh"; do
    [ -f "$candidate" ] && INSTALL_SCRIPT="$candidate" && break
done

if [ -z "$INSTALL_SCRIPT" ]; then
    echo "install_ncs.sh not found — locate it in the skill's scripts/ directory"
else
    chmod +x "$INSTALL_SCRIPT" 2>/dev/null
    "$INSTALL_SCRIPT"
fi
```

The script:

- downloads a standalone `ncs` binary into `~/.ncs`
- writes `NCS_HOME` and `PATH` updates into `~/.zshrc` or `~/.bash_profile`
- avoids reusing the aone-kit copy

### Refresh The Shell After Install

Because shell profile changes written by the script may not affect a fresh subprocess automatically, refresh the environment before the next command:

```bash
source ~/.zshrc 2>/dev/null || source ~/.bash_profile 2>/dev/null
export PATH="$HOME/.ncs:$PATH"
hash -r 2>/dev/null || true
command -v ncs
```

If `ncs` is still missing after the installer and shell refresh, stop and surface that as an environment setup failure.

## Step 4: Bootstrap Auth

Once Python, `maxc-cli`, and `ncs` are ready:

```bash
maxc auth whoami --json
maxc auth login-ncs --list-accounts --account-type user --json
maxc auth login-ncs --interactive
maxc auth whoami --json
```

If `maxc` is not on `PATH`, replace it with:

```bash
python3 -m maxc_cli ...
```

For the account-type mapping and explicit `login-ncs` command forms, read [ncs-auth.md](ncs-auth.md).

## Step 5: Bootstrap Metadata

After login succeeds, prepare metadata for discovery workflows:

```bash
maxc cache build --json
maxc meta list-tables --json
```

## Working Rules

- On user machines, own the setup instead of assuming prerequisites exist.
- Prefer install-and-verify loops over long speculative explanations.
- Before any Python install or upgrade, ask the user for explicit confirmation.
- Do not switch to access-key or STS auth as a workaround for missing `ncs`.
- If the machine is not macOS, be explicit that the bundled `ncs` installer script does not support that platform.
- If only `python3 -m maxc_cli` works, continue with it rather than blocking on `PATH` cleanup.

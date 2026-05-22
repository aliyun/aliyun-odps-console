# MaxC Environment Setup And Install

Read this file before any ODPS task when the target machine may not already have Python or `maxc-cli`.

## Setup Order

Always bootstrap in this order:

1. Python
2. `maxc-cli`
3. `auth login`
4. metadata bootstrap such as `cache build`

Do not skip to auth before the local prerequisites are ready.

## Step 1: Check Python

Verify Python first:

```bash
python3 --version
python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")'
python3 -m pip --version
```

Interpretation:

- if `python3` is missing, install Python before anything else
- if the version is below `3.8`, explain the supported range and ask the user for confirmation before changing Python
- prefer `python3 -m pip ...` over bare `pip`
- never perform Python installation or upgrade commands without explicit user confirmation

### Install Python On macOS

Only after the user confirms, a common macOS path when Homebrew is available is:

```bash
brew install python@3.12
```

If `brew` is not installed, use the official macOS Python installer for a supported version such as `3.12`, then rerun the Python checks above.

Do not install or upgrade Python proactively. First tell the user why the current interpreter is insufficient, then ask whether they want you to make the system-level change.

## Step 2: Install `maxc-cli`

`maxc-cli` is published on PyPI:

```bash
python3 -m pip install --upgrade maxc-cli
```

If the environment requires a user-local install:

```bash
python3 -m pip install --user --upgrade maxc-cli
```

Verify install:

```bash
{{cli}} --help
```
<!-- @if cli_module_differs -->

If the console script is not on `PATH` after install, the module path works as a fallback:

```bash
{{cli_module}} --help
```

Notes:

- if `{{cli}} --help` fails but `{{cli_module}} --help` works, continue using `{{cli_module}} ...`
<!-- @endif -->

## Step 3: Bootstrap Auth

Once Python and `maxc-cli` are ready:

```bash
{{cli}} auth whoami --json
```

If not authenticated, follow the auth bootstrap flow in [bootstrap-auth.md](bootstrap-auth.md).

```bash
{{cli}} auth whoami --json
```

## Step 4: Bootstrap Metadata

After login succeeds, verify connectivity by listing tables:

```bash
{{cli}} meta list-tables --json
```

## Working Rules

- On user machines, own the setup instead of assuming prerequisites exist.
- Prefer install-and-verify loops over long speculative explanations.
- Before any Python install or upgrade, ask the user for explicit confirmation.
<!-- @if cli_module_differs -->
- If only `{{cli_module}}` works, continue with it rather than blocking on `PATH` cleanup.
<!-- @endif -->

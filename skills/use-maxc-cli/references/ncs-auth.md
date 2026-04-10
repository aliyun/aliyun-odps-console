# MaxC NCS Authentication

Read this file when the task specifically involves `ncs`, internal MaxCompute account selection, or `auth login-ncs`.

## Scope

This skill uses `ncs` as the auth provider for `maxc` and ships an installer script for internal release workflows. It remains `maxc`-oriented, not ODPSCMD-oriented.

Adapt the flow to the distributed skill:

- save config in `~/.maxc/config.yaml`, not `~/.odps_config.ini`
- use `ncs` as the only supported login path in this skill release
- use `auth login-ncs`, not raw config-file editing
- do not assume `_dev` will be appended to project names automatically
- do not persist temporary access keys returned by `ncs`; save the provider config and let runtime invoke `ncs`

If the target machine may not have Python, `maxc-cli`, or `ncs`, read [setup-install.md](setup-install.md) first.

## Preflight

Use the installed command surface:

```bash
maxc auth whoami --json
```

If the console script is not on `PATH`, use:

```bash
python3 -m maxc_cli auth whoami --json
```

Before using an ncs flow, check that `ncs` is actually available:

```bash
command -v ncs
```

If `ncs` is missing from `PATH`, or the resolved binary is the aone-kit copy under `.real/third_party/cli/aone-kit/bin`, install the bundled standalone copy first.

The bundled installer lives alongside this skill at:

```text
<skill_root>/scripts/install_ncs.sh
```

The exact path depends on how the skill was deployed:

| Platform | Typical skill root |
|----------|-------------------|
| Codex | `~/.codex/skills/use-maxc-cli` |
| Claude Code | `skills/use-maxc-cli` (relative to repo root) |

To find the installer dynamically:

```bash
# Try common locations
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

The installer:

- downloads `ncs` into `~/.ncs`
- ensures `/usr/sbin` is present in `PATH`
- writes `NCS_HOME` and `PATH` updates into `~/.zshrc` or `~/.bash_profile`

After running the installer in a non-interactive subprocess, refresh the shell environment before re-checking `ncs`:

```bash
source ~/.zshrc 2>/dev/null || source ~/.bash_profile 2>/dev/null
export PATH="$HOME/.ncs:$PATH"
hash -r 2>/dev/null || true
command -v ncs
```

Do not fall back to access-key or STS login just because `ncs` is missing. Install `ncs` and continue on the ncs path.

## Account Types

`auth login-ncs` supports three account types:

| `--account-type` | Required identifier | Account-list command | Generated process command |
| --- | --- | --- | --- |
| `user` | `--employee-id` | `ncs list authorizations odpsuser -o custom-columns=BUC_USER_ID:.extension.bucUserId,BUC_USER_TYPE:.extension.bucUserType,BUC_ACCOUNT_NAME:.extension.bucDomainAccount` | `ncs create credential odpsuser --employee-id <id> -o template -t odpscmd` |
| `account` | `--account-name` | `ncs list authorizations odpsaccount --scenario app -o custom-columns=accountName:.extension.accountName` | `ncs create credential odpsaccount --account-name <name> -o template -t odpscmd` |
| `app` | `--app-name` | `ncs list authorizations odpsapp -o custom-columns=AppName:.extension.appName` | `ncs create credential odpsapp --app-name <name> -o template -t odpscmd` |

Notes:

- `user` covers personal or shared employee-style identities exposed by the `odpsuser` listing.
- `auth login-ncs --list-accounts` defaults to `user` if no account type is given and no saved ncs config exists.
- `--list-accounts` returns `data.account_type`, `data.raw_lines`, and `data.raw_output`. It does not normalize rows into structured objects yet.

## Common Flow

List candidate accounts first:

```bash
maxc auth login-ncs --list-accounts --account-type user --json
maxc auth login-ncs --list-accounts --account-type account --json
maxc auth login-ncs --list-accounts --account-type app --json
```

If `maxc` is not on `PATH`, replace it with `python3 -m maxc_cli`.

Then save the selected ncs-backed config:

```bash
maxc auth login-ncs \
  --account-type user \
  --employee-id 123456 \
  --project your_project \
  --endpoint http://service-corp.odps.aliyun-inc.com/api \
  --json
```

If you only need to save config and remote validation is not possible yet:

```bash
maxc auth login-ncs \
  --account-type user \
  --employee-id 123456 \
  --project your_project \
  --endpoint http://service-corp.odps.aliyun-inc.com/api \
  --no-validate \
  --json
```

Interactive mode is available, but it only prompts for free-text values:

```bash
maxc auth login-ncs --interactive
```

Prefer the explicit flag form when you already know the account identifier, project, and endpoint.

## What `auth login-ncs` Saves

The command persists an ncs provider config under `~/.maxc/config.yaml`. The important shape is:

```yaml
auth:
  provider: ncs
  project: your_project
  endpoint: http://service-corp.odps.aliyun-inc.com/api
  region_name: cn-hangzhou  # optional
  tunnel_endpoint: https://dt.odps.aliyun.com  # optional
  ncs:
    account_type: user
    employee_id: "123456"
    process_command: "ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd"
    process_timeout: 20
```

Important constraints:

- `process_command` is derived automatically from the selected account type and identifier.
- Use the raw project the user wants. The CLI does not append `_dev`.
- `--no-validate` saves config without a remote identity check and returns `validation_status=configuration_only`.
- When validation is enabled, `auth login-ncs` may execute the configured ncs process command during auth verification.

## Environment Override Rules

Saved ncs config is not the only source of truth. MaxCompute environment variables can override parts of it:

- `ALIBABA_CLOUD_ACCESS_KEY_ID`
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`
- `ALIBABA_CLOUD_SECURITY_TOKEN`
- `MAXCOMPUTE_PROJECT`
- `MAXCOMPUTE_ENDPOINT`
- aliases such as `ODPS_PROJECT` and `ODPS_ENDPOINT`

If those variables are present, `auth whoami --json` may report `identity_source=mixed` and the saved ncs config may not be the effective runtime configuration.

## Endpoint Guidance

The CLI does not provide endpoint presets or auto-discovery. Use an endpoint the user explicitly gave you, an endpoint already present in config, or one confirmed by local context.

Common internal examples from the earlier ODPS skill draft:

| Region | Endpoint |
| --- | --- |
| Mainland China (Hangzhou) | `http://service-corp.odps.aliyun-inc.com/api` |
| Singapore | `http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api` |
| Germany | `http://service-corp.de-internal.odps.aliyun-inc.com/api` |
| US Ant | `http://service-corp-us.odps.aliyun-inc.com/api` |
| Vietnam Ant | `http://service-all.vn-ant.odps.aliyun-inc.com/api` |

Treat this table as a user-provided reference, not a guarantee that a given endpoint is valid in the current environment.

## Do And Do Not

Do:

- install `ncs` with `scripts/install_ncs.sh` when it is missing or only the aone-kit copy is present
- use `auth login-ncs` to persist config
- use `--list-accounts` before asking the user to choose an identifier
- rerun `auth whoami --json` after login or config changes
- inspect `agent_hints.warnings` for env-override messages

Do not:

- invent account identifiers, projects, or endpoints
- recommend `auth login`, `--from-env`, access keys, or STS tokens through this skill
- manually paste temporary access keys from `ncs` output into config
- document ODPSCMD-only behavior as if `maxc` supported it

For config precedence, `auth whoami` interpretation, and non-ncs login methods, also read [bootstrap-auth.md](bootstrap-auth.md).

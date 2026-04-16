# MaxC Bootstrap And Auth

## Runtime Target

If Python or `maxc` might be missing, read [setup-install.md](setup-install.md) first.

Prefer the installed command:

```bash
maxc ...
```

If the console script is not on `PATH`, use:

```bash
python3 -m maxc_cli ...
```

maxc writes local files under `~/.maxc` by default:

- `config.yaml`
- `session_override.yaml`
- `state/`
- `cache/cache.db`

In sandboxes or CI, make sure those paths are writable.

### Migrating From odpscmd

If the user already has `odpscmd` configured, reuse their credentials — see [migrate-from-odpscmd.md](migrate-from-odpscmd.md) for the field mapping and config conversion steps.

---

## Step 1: Check Current Auth Status

Always run this first:

```bash
maxc auth whoami --json
```

Inspect `data.identity`:

| `authenticated` | `configured` | `validation_status` | Meaning |
|----------------|--------------|--------------------|-|
| `true` | `true` | `verified` | Authenticated — ask whether to continue or re-configure |
| `false` | `false` | `missing_configuration` | No auth configured → go to Step 2 |
| `false` | `true` | `failed` | Config exists but remote check failed → fix or re-login → go to Step 2 |

### If already authenticated

Show the current identity and ask the user before proceeding:

> "Currently authenticated as `<principal_display>` on project `<project>` (`<auth_type>`).
> Continue with this, or re-configure auth?"

- **Continue** → skip to the task. Do not modify auth config.
- **Re-configure** → go to Step 2 and ask which method.

Never silently continue if the user might want a different account or project.

If `data.metadata.config_sources` is present, it lists which config files are active. Use this to diagnose conflicts when auth is not behaving as expected.

---

## Step 2: Ask the User Which Auth Method to Use

**Always ask before choosing a path.** Do not assume any particular method.

> "Which auth method would you like to use?
> **(A) Access Key / Secret Key** — long-lived AK/SK pair, saved to `~/.maxc/config.yaml`
> **(B) Environment variables** — keys already set in the current shell (ALIBABA_CLOUD_ACCESS_KEY_ID etc.)"

Then follow the matching section below.

### Always ask for project and endpoint

**Regardless of auth method, always ask the user for `project` and `endpoint` explicitly.** Do not silently reuse values from an existing config file or environment variables.

If a current value is visible in the config or env, present it as a default option — but the user must confirm or change it:

> "Which MaxCompute project would you like to use? (current config shows: `<existing_project>`)"
> "Which endpoint? (current config shows: `<existing_endpoint>`)"

Never assume the existing project/endpoint is still correct.

---

## Path A: Access Key / Secret Key

Use when the user has a long-lived AK/SK pair.

### What you need

- `access_key_id`
- `access_key_secret`
- `project` (MaxCompute project name)
- `endpoint` (e.g. `http://service.cn-shanghai.maxcompute.aliyun.com/api`)
- `region` (optional, e.g. `cn-shanghai`)

### Login command

```bash
maxc auth login \
  --access-id "<access_key_id>" \
  --secret-access-key "<access_key_secret>" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --region "<region>" \
  --json
```

Add `--no-validate` to save config without a remote identity check:

```bash
maxc auth login \
  --access-id "<access_key_id>" \
  --secret-access-key "<access_key_secret>" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --no-validate \
  --json
```

### What it saves

```yaml
auth:
  provider: access_key
  access_id: "<access_key_id>"
  secret_access_key: "<access_key_secret>"
  project: "<project>"
  endpoint: "<endpoint>"
  region_name: "<region>"   # if provided
```

Config is saved to `~/.maxc/config.yaml` with permissions `0600`.

Note: `auth login` also writes `default_project` (and `default_region` if provided) to the top level of the config file, so the project is available even without the `auth` block.

### STS token variant

If the user has a temporary STS token, add `--security-token`:

```bash
maxc auth login \
  --access-id "<access_key_id>" \
  --secret-access-key "<access_key_secret>" \
  --security-token "<sts_token>" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --json
```

---

## Path B: Environment Variables

Use when the relevant environment variables are already set in the current shell — for example, in CI pipelines or developer environments where keys are injected automatically.

### Check which vars are set

```bash
env | grep -E 'ALIBABA_CLOUD|MAXCOMPUTE|ODPS'
```

Primary variables:

```bash
ALIBABA_CLOUD_ACCESS_KEY_ID
ALIBABA_CLOUD_ACCESS_KEY_SECRET
ALIBABA_CLOUD_SECURITY_TOKEN   # only for STS
MAXCOMPUTE_PROJECT
MAXCOMPUTE_ENDPOINT
MAXCOMPUTE_REGION              # optional
```

Supported aliases (the CLI resolves these automatically):

- access id: `ODPS_ACCESS_ID`, `ACCESS_KEY_ID`
- secret: `ODPS_ACCESS_KEY`, `ODPS_ACCESS_KEY_SECRET`, `ACCESS_KEY_SECRET`
- token: `ODPS_STS_TOKEN`, `SECURITY_TOKEN`
- project: `ODPS_PROJECT`
- endpoint: `ODPS_ENDPOINT`
- region: `ALIBABA_CLOUD_REGION`

### Save to config from env vars

```bash
maxc auth login --from-env --json
```

This reads the current env vars and writes them to `~/.maxc/config.yaml`. If a required variable (e.g. `ALIBABA_CLOUD_ACCESS_KEY_ID`) is not set, the command will fail with a clear error rather than silently falling back to existing config.

If you only want to verify env vars work without saving to config, run `auth whoami --json` directly — the CLI reads env vars at runtime with or without a config file.

### Important: env vars override config at runtime

If env vars and config file are both active, env vars win for `access_id`, `secret_access_key`, `project`, and `endpoint`. `auth whoami` will report `identity_source=mixed` in this case. Run `auth whoami --json` to confirm which source is effective.

---

## Path C: External Credential Provider (NCS)

Use when the user authenticates via an external command — most commonly `ncs` (Alibaba internal credential service). The command outputs JSON with `AccessKeyId`, `AccessKeySecret`, and optionally `SecurityToken` / `Expiration`.

This is the recommended path for internal users who already use `ncs` with odpscmd.

### What you need

- `process_command` — the command that outputs credentials (e.g. `ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd`)
- `project`
- `endpoint`

### Migrating from odpscmd

If the user has odpscmd already configured with `account_provider=external`, reuse their settings — see [migrate-from-odpscmd.md](migrate-from-odpscmd.md). The key mapping:

| odpscmd field | maxc field |
|---|---|
| `account_provider=external` | `provider: external` |
| `processCommand=ncs create credential ...` | `process_command: ncs create credential ...` |
| `processCommandTimeout=20` | `process_timeout: 20` |

### Login command

```bash
maxc auth login-external \
  --process-command "ncs create credential odpsuser --employee-id <employee_id> -o template -t odpscmd" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --json
```

For department or app accounts:

```bash
# Department account
maxc auth login-external \
  --process-command "ncs create credential odpsaccount --account-name <account_name> -o template -t odpscmd" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --json

# App account
maxc auth login-external \
  --process-command "ncs create credential odpsapp --app-name <app_name> -o template -t odpscmd" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --json
```

### What it saves

```yaml
auth:
  provider: external
  project: "<project>"
  endpoint: "<endpoint>"
  external:
    process_command: "ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd"
    process_timeout: 60
```

### Backward compatibility with `provider: ncs`

Old config files with `provider: ncs` continue to work — at runtime, maxc transparently converts them to `provider: external` and uses the same `ExternalCredentialProvider`. No manual migration needed.

### Credential caching

- **L1 (in-process)**: Credentials are cached in memory and refreshed when they approach expiration.
- **L2 (kv_store)**: Temporary credentials (with `Expiration`) are persisted to `~/.maxc/cache/cache.db`, avoiding repeated command execution across processes. Long-lived AKs (no `Expiration`) are NOT cached in kv_store to ensure revocation is respected.

---

## Step 3: Verify Auth After Login

After any login command, always re-run:

```bash
maxc auth whoami --json
```

Confirm `data.identity.authenticated=true` and `validation_status=verified`.

`data.metadata.config_sources` lists the active config files — useful to confirm the right file was written and no local config is overriding it.

### If verification still fails after login

Check whether environment variables are overriding the saved config:

```bash
env | grep -E 'ALIBABA_CLOUD|MAXCOMPUTE|ODPS'
```

If `MAXCOMPUTE_PROJECT`, `MAXCOMPUTE_ENDPOINT`, or similar vars are set, they will override the project/endpoint you just saved — even if login reported success. Tell the user:

> "Environment variables are overriding your saved config. To use the values you just configured, unset them:
> `unset MAXCOMPUTE_PROJECT MAXCOMPUTE_ENDPOINT`
> Then re-run `auth whoami --json` to confirm."

Do not proceed with the task until `authenticated=true` and the reported `project`/`endpoint` match what the user intended.

---

## Config Discovery

Without top-level `--config`, the loader checks files in this order (later files override earlier ones via deep merge):

1. `~/.maxc/config.yaml` (global)
2. `./.maxc/config.yaml` (project-local)
3. `./.maxc.yaml` (project-local)
4. `./.maxc` (project-local)

**Project-local config files can override global auth settings.** If `auth whoami` reports an unexpected `auth_type` or `identity_source`, check whether a local `.maxc` or `.maxc.yaml` file has a conflicting `auth` block.

Session overrides (project and schema only) come from:

```text
~/.maxc/session_override.yaml
```

`session_override.yaml` has higher priority than env vars and config files for `default_project` and `default_schema`, but it does **not** affect which auth provider is used.

---

## How To Read `auth whoami`

```json
{
  "data": {
    "identity": {
      "authenticated": true,
      "configured": true,
      "validation_status": "verified",
      "auth_type": "access_key",
      "identity_source": "config_file",
      "principal_display": "ALIYUN$xxx",
      "project": "demo_project"
    }
  },
  "metadata": {
    "config_sources": ["/Users/you/.maxc/config.yaml"]
  }
}
```

Key fields:

- `authenticated=false` — maxc could not complete a valid remote identity check
- `configured=false` — required auth settings are incomplete
- `validation_status` — one of `verified`, `missing_configuration`, `failed`, `configuration_only`
- `identity_source` — one of `environment`, `config_file`, `mixed`, `unknown`
- `config_sources` — list of config files currently active (use to diagnose override conflicts)
- `auth_options` — present when auth is not ready; an array of login suggestions, each with `type` (e.g. `access_key`, `sts_token`), `description`, and `command` (a runnable maxc command)

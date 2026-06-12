> Loaded on demand — covers AK/SK runtime targets and auth backend details. Skip unless the agent is configuring credentials on a fresh machine or debugging auth.

# MaxC Bootstrap And Auth

## Runtime Target

If Python or `{{cli}}` might be missing, read [setup-install.md](setup-install.md) first.

Prefer the installed command:

```bash
{{cli}} ...
```
<!-- @if cli_module_differs -->

If the console script is not on `PATH`, use:

```bash
{{cli_module}} ...
```
<!-- @endif -->

{{cli}} writes local files under `~/.maxc` by default:

- `config.yaml`
- `state/`
- `cache/cache.db`

In sandboxes or CI, make sure those paths are writable.

---

## Step 1: Check Current Auth Status

Always run this first:

```bash
{{cli}} auth whoami --json
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

### If `auth_type=external` is already configured

If `auth whoami` shows `auth_type=external` (or the saved config has `provider: external`), the user is on an externally-managed credential provider set up by another tool. **Do not run Step 2 or write a new auth block.** Treat the auth as already valid. Only `project`/`endpoint`/`schema` may be changed — via `{{cli}} session set ...` or by re-running `auth login-external` with the *same* `--process-command` and the new project/endpoint values.

### Project and endpoint selection

**Path A (AK/SK):** omit `--project` — the CLI queries the Catalog API and returns a `status="pending"` envelope listing all projects visible to the AK/SK, with endpoint/region pre-computed per project. Pick one from the list (ask the user if ambiguous), then re-run with `--project <id>` to complete login.

**Path B (env vars):** `--from-env` reads `MAXCOMPUTE_PROJECT` and `MAXCOMPUTE_ENDPOINT` from the shell environment. If either is missing, the command fails with a clear error — ask the user to set them.

**Explicit override:** if the user already knows the target project, pass `--project` and `--endpoint` directly to skip project discovery.

---

## Path A: Access Key / Secret Key

Use when the user has a long-lived AK/SK pair.

### What you need

- `access_key_id`
- `access_key_secret`

The Catalog API provides `project`, `endpoint`, `region`, and `tunnel_endpoint` automatically. Only ask the user for these if they pass `--no-picker` or already know the target project.

### Login command — Step 1: discover projects (recommended)

```bash
{{cli}} auth login \
  --access-id "<access_key_id>" \
  --secret-access-key "<access_key_secret>" \
  --json
```

Returns `status="pending"` with `data.projects` — a list of `{project_id, region, endpoint, owner, schema_enabled, description}`. Pick a project from the list and proceed to Step 2.

### Login command — Step 2: complete login

```bash
{{cli}} auth login \
  --access-id "<access_key_id>" \
  --secret-access-key "<access_key_secret>" \
  --project "<project_id>" \
  --json
```

Returns `status="success"` with the configured identity. Endpoint/region are auto-derived from the project when omitted.

### Login command (explicit — when user knows the target)

```bash
{{cli}} auth login \
  --access-id "<access_key_id>" \
  --secret-access-key "<access_key_secret>" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --region "<region>" \
  --json
```

Add `--no-validate` to either form to save config without a remote identity check.

### Picker flags

| Flag | Effect |
|------|--------|
| (omit `--project`) | Return pending envelope with project list from Catalog API (non-TTY) or interactive picker (TTY) |
| `--no-picker` | Disable project discovery; fall back to manual prompt for project/endpoint (CI escape hatch) |
| `--reselect` | Force project discovery even when a project is already saved in config (no effect with `--project` or `--no-picker`) |
| `--catalog-endpoint <url>` | Override the Catalog API URL (for non-China regions where auto-routing is unavailable) |

Fallback: if the Catalog API call fails (network, permissions, etc.), the CLI falls back to a manual prompt (TTY) or returns project=None (non-TTY).

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
{{cli}} auth login \
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
{{cli}} auth login --from-env --json
```

This reads the current env vars and writes them to `~/.maxc/config.yaml`. If a required variable (e.g. `ALIBABA_CLOUD_ACCESS_KEY_ID`) is not set, the command will fail with a clear error rather than silently falling back to existing config.

If you only want to verify env vars work without saving to config, run `auth whoami --json` directly — the CLI reads env vars at runtime with or without a config file.

### Important: env vars override config at runtime

If env vars and config file are both active, env vars win for `access_id`, `secret_access_key`, `project`, and `endpoint`. `auth whoami` will report `identity_source=mixed` in this case. Run `auth whoami --json` to confirm which source is effective.

---

## Step 3: Verify Auth After Login

After any login command, always re-run:

```bash
{{cli}} auth whoami --json
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

`{{cli}} session set --project/--schema` writes `default_project` / `default_schema` directly into `~/.maxc/config.yaml` — there is no separate override file. A project-local config can still shadow the user-level value; `session set` warns when it does.

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

- `authenticated=false` — {{cli}} could not complete a valid remote identity check
- `configured=false` — required auth settings are incomplete
- `validation_status` — one of `verified`, `missing_configuration`, `failed`, `configuration_only`
- `identity_source` — one of `environment`, `config_file`, `mixed`, `unknown`
- `config_sources` — list of config files currently active (use to diagnose override conflicts)
- `auth_options` — present when auth is not ready; an array of login suggestions, each with `type` (e.g. `access_key`, `sts_token`), `description`, and `command` (a runnable {{cli}} command)

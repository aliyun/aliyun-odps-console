# MaxC Bootstrap And Auth

## Runtime Target

If Python, `maxc`, or `ncs` might be missing, read [setup-install.md](setup-install.md) first.

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

---

## Step 1: Check Current Auth Status

Always run this first:

```bash
maxc auth whoami --json
```

Inspect `data.identity`:

| `authenticated` | `configured` | `validation_status` | Meaning |
|----------------|--------------|--------------------|-|
| `true` | `true` | `verified` | Ready — continue with task |
| `false` | `false` | `missing_configuration` | No auth configured → go to Step 2 |
| `false` | `true` | `failed` | Config exists but remote check failed → fix or re-login |

If `data.metadata.config_sources` is present, it lists which config files are active. Use this to diagnose conflicts when auth is not behaving as expected.

---

## Step 2: Ask the User Which Auth Method to Use

**Always ask before choosing a path.** Do not assume NCS or any other method.

> "Which auth method would you like to use?
> **(A) Access Key / Secret Key** — long-lived AK/SK pair, saved to `~/.maxc/config.yaml`
> **(B) Environment variables** — keys already set in the current shell (ALIBABA_CLOUD_ACCESS_KEY_ID etc.)
> **(C) NCS** — internal machine account issued by ncs CLI (requires `ncs` on PATH)"

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
- `endpoint` (e.g. `http://service-corp.odps.aliyun-inc.com/api`)
- `region` (optional, e.g. `cn-hangzhou`)

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

This reads the current env vars and writes them to `~/.maxc/config.yaml`.

If you only want to verify env vars work without saving to config, run `auth whoami --json` directly — the CLI reads env vars at runtime with or without a config file.

### Important: env vars override config at runtime

If env vars and config file are both active, env vars win for `access_id`, `secret_access_key`, `project`, and `endpoint`. `auth whoami` will report `identity_source=mixed` in this case. Run `auth whoami --json` to confirm which source is effective.

**After `auth login-ncs`, if `MAXCOMPUTE_PROJECT` or `MAXCOMPUTE_ENDPOINT` are still set, they will override the NCS-configured project/endpoint at runtime.** Unset them if you want the config file to be authoritative.

---

## Path C: NCS (Internal Machine Auth)

Use when the user has access to the internal `ncs` CLI for machine-account authentication.

Read [ncs-auth.md](ncs-auth.md) for the full account-type mapping, installer instructions, and config-persistence rules.

### Quick flow

```bash
# 1. Verify ncs is available
command -v ncs

# 2. List candidate accounts
maxc auth login-ncs --list-accounts --account-type user --json
maxc auth login-ncs --list-accounts --account-type account --json
maxc auth login-ncs --list-accounts --account-type app --json

# 3. Save the selected ncs-backed config
maxc auth login-ncs \
  --account-type user \
  --employee-id "<id>" \
  --project "<project>" \
  --endpoint "<endpoint>" \
  --json

# 4. Or use interactive mode
maxc auth login-ncs --interactive
```

If `ncs` is missing from `PATH`, install it with the bundled installer before proceeding. See [setup-install.md](setup-install.md).

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
      "auth_type": "ncs",
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
- `auth_options` — present when maxc wants to suggest a next login step; for Path C, ignore non-ncs options

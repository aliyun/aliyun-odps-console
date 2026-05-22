# Migrate From odpscmd

When the user already has `odpscmd` (ODPS Console) configured, reuse their existing credentials instead of asking for them again.

## Step 1: Locate odpscmd Config

odpscmd reads an INI file (key=value, no sections). Common paths:

```bash
# Default location
~/.odpscmd/odps_config.ini

# Custom location (set via odpscmd --config or ODPS_CONFIG)
# Ask the user if the default is missing
```

## Step 2: Read Config Fields

Parse the file — it is plain `key=value` (no `[section]` headers). Relevant fields:

| odpscmd key | {{cli}} key | Notes |
|---|---|---|
| `access_id` | `auth.access_id` | |
| `access_key` | `auth.secret_access_key` | Renamed to clarify it is a secret |
| `project_name` | `auth.project` + `default_project` | |
| `end_point` | `auth.endpoint` | |
| `schema_name` | `auth.schema` | Optional, 2.0 projects only |
| `tunnel_endpoint` | `auth.tunnel_endpoint` | Optional |
| `region_id` | `auth.region_name` | Optional |
| `quota_name` | `auth.quota_name` | Optional |
| `account_provider` | `auth.provider` | See mapping below |
| `process_command` | `auth.external.process_command` | Only when `account_provider=external` |
| `process_timeout` | `auth.external.process_timeout` | Optional, default 60 |

**Provider mapping:**

| `account_provider` | {{cli}} `auth.provider` |
|---|---|
| *(missing)* | `access_key` — odpscmd defaults to `aliyun` |
| `aliyun` | `access_key` |
| `sts` | `access_key` — set `auth.security_token` if the odpscmd config has `security_token` |
| `external` | `external` — already managed by external tooling, see "External provider" note below |

`aliyun` and `sts` are the same in maxc: plain AK/SK auth. The only difference is whether a `security_token` is present.

**External provider note**: if `account_provider=external`, the user already has an externally-managed credential pipeline set up. **Do not write a new auth block from scratch** — preserve the existing `process_command` and only update `project`/`endpoint` if needed. See SKILL.md §"Bootstrap Flow" and the `auth_type=external` safeguard.

**Ignored fields** (no {{cli}} equivalent): `app_access_id`, `app_access_key`, `log_view_host`, `log_view_version`, `log_view_life`, `proxy_host`, `proxy_port`, `LABEL`, `data_size_confirm`, `update_url`, `signature_v4_corporation`, `https_end_point`.

## Step 3: Build {{cli}} Config

Construct `~/.maxc/config.yaml` from the parsed fields. Three common patterns:

### Pattern A: Access Key (most common)

odpscmd config:
```ini
access_id=LTAI5t...
access_key=kLnSHC...
end_point=http://service.cn-shanghai.maxcompute.aliyun.com/api
project_name=my_project
```

{{cli}} config:
```yaml
auth:
  provider: access_key
  access_id: LTAI5t...
  secret_access_key: kLnSHC...
  project: my_project
  endpoint: http://service.cn-shanghai.maxcompute.aliyun.com/api
default_project: my_project
```

### Pattern B: External Provider

odpscmd config:
```ini
account_provider=external
process_command=/path/to/credential-helper.sh
end_point=http://service.cn-shanghai.maxcompute.aliyun.com/api
project_name=my_project
```

{{cli}} config:
```yaml
auth:
  provider: external
  project: my_project
  endpoint: http://service.cn-shanghai.maxcompute.aliyun.com/api
  external:
    process_command: /path/to/credential-helper.sh
    process_timeout: 60
default_project: my_project
```

Or via CLI:
```bash
{{cli}} auth login-external \
  --process-command "/path/to/credential-helper.sh" \
  --project my_project \
  --endpoint http://service.cn-shanghai.maxcompute.aliyun.com/api
```

### Pattern C: External provider already in use

If the user's odpscmd config has `account_provider=external`, their credentials are managed by an external command pipeline that another tool already set up. **Do not regenerate the {{cli}} config from scratch.** Instead, preserve the existing `process_command` verbatim and only update `project`/`endpoint` if the user wants to point at a different workspace:

```bash
{{cli}} auth login-external \
  --process-command "<existing process_command from odpscmd>" \
  --project <project> \
  --endpoint <endpoint> \
  --no-validate
```

## Step 4: Verify

After writing the config, always verify:

```bash
{{cli}} auth whoami --json
```

Confirm `"authenticated": true` and `"validation_status": "verified"`. If validation fails, check the endpoint format and credentials.

## Edge Cases

- **`end_point` vs `https_end_point`**: odpscmd has both; {{cli}} uses `endpoint`. Prefer `https_end_point` if available.
- **No `account_provider` field**: Treat as `access_key`.
- **STS token in odpscmd**: STS tokens are short-lived and not worth persisting. If the odpscmd config has `security_token`, skip it — the user should use `external` provider with a command that refreshes STS automatically.

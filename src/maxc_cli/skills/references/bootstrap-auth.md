> Load only for authentication setup beyond the normal OAuth path, or to
> diagnose a failed identity check.

# Authentication Setup And Recovery

Append the session User-Agent declared in SKILL.md to each `{{cli}}` command
that calls a cloud API: `--user-agent "$UA"`. Local help, `agent context`,
`agent manifest`, `session show`, and `cache status` may omit it.

## Start With The Effective Identity

```bash
{{cli}} auth whoami --json
{{cli}} agent doctor --online --json
```

Keep an authenticated identity unless the user asked to change it. Project
selection and credential selection are separate decisions; a permission error
does not by itself prove that credentials are invalid.

## OAuth

OAuth is the preferred interactive public-cloud method because the Agent does
not need to handle a long-lived secret.

```bash
{{cli}} auth login --oauth --json
```

Use `--site-type INTL` only when the user's account/site requires it. Use
`--no-browser` only to suppress automatic browser opening and print the sign-in
URL. The CLI still binds its OAuth callback to `127.0.0.1` on the CLI host; this
is not a device-code/headless flow. Over SSH, forward the selected loopback
port back to the CLI host or use a browser on that host. When login returns
`status=pending`, resolve the project from the returned project list/actions
and run the exact structured completion action.

## Environment Or STS Credentials

Use this path only when the user or managed runtime explicitly states that it
already injects credentials. Do not enumerate the process environment to find
them; invoke the supported import directly and let its sanitized envelope
report missing configuration.

Primary variables:

- `ALIBABA_CLOUD_ACCESS_KEY_ID`
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`
- `ALIBABA_CLOUD_SECURITY_TOKEN` for STS
- `MAXCOMPUTE_PROJECT`
- `MAXCOMPUTE_ENDPOINT`
- `MAXCOMPUTE_REGION` when needed

Then:

```bash
{{cli}} auth login --from-env --json
```

Environment values may override saved configuration. Use `auth whoami` and
`session show` to inspect the effective project and config sources. Ask before
unsetting or changing variables owned by the user's runtime.

## Remove Saved Authentication

Use this only when the user asks to sign out or remove the saved provider:

```bash
{{cli}} auth logout --json
```

The command removes the `auth` block from the selected config and clears
cached external temporary credentials. It preserves project and region
preferences. Check `data.environment_auth_variables` and
`data.remaining_auth_sources`: a child process cannot unset credentials in the
parent shell, and another loaded config may still provide authentication.

## External Credential Process

Use an existing credential helper when the environment manages short-lived
credentials:

```bash
MAXCOMPUTE_ENDPOINT='<endpoint>' \
MAXCOMPUTE_PROJECT='<project>' \
{{cli}} auth login-external \
  --process-command '<credential-helper-command>' \
  --project '<project>' \
  --json
```

Using `MAXCOMPUTE_ENDPOINT` works for both supported invocations and avoids the
Alibaba Cloud CLI root consuming `--endpoint` before the `aliyun maxc`
extension parser receives it.

The helper command is sensitive operational configuration. Do not log it, and
do not replace an already configured external provider merely because a remote
operation returned `PERMISSION_DENIED`. It must come from trusted user-level
configuration or an explicit `--config` selected by the user; automatically
discovered workspace configuration is not allowed to define `auth`.

The process string is parsed into one executable plus argv and runs with
`shell=false`. Shell pipelines, redirections, command substitution, and other
shell syntax are not supported. Use a dedicated trusted helper executable
rather than wrapping credential output in a shell expression.

## Direct AK/SK

Avoid secrets in process arguments or chat. Prefer OAuth, an Alibaba Cloud CLI
profile, injected environment variables, or an external credential process. If
the user explicitly chooses direct AK/SK, obtain it through an approved secret
input channel and follow the live `auth login --help`; never reproduce the
secret in the response.

## Project And Endpoint

- Use values supplied by the profile, Catalog result, current configuration, or
  the user.
- Never invent an endpoint or derive a related project from a name suffix.
- A project-local config may override the user-level config. Inspect
  `metadata.config_sources` and `session show --json` when results are
  unexpected.
- Use `session set` for an authorized project/schema preference change; do not
  hand-edit auth YAML.

## Recovery

| Symptom | Action |
|---|---|
| `configured=false` | Configure OAuth or the user-selected provider |
| `validation_status=failed` | Read the warning and verify endpoint/profile |
| wrong effective project | Inspect config sources and environment overrides |
| `PERMISSION_DENIED` | Check the exact object with `auth can-i`; do not assume bad credentials |
| OAuth callback timeout | Re-run once after checking local callback/browser access |
| external helper failure | Run the approved helper through its owner workflow; do not expose its output |

Finish every repair with:

```bash
{{cli}} agent doctor --online --json
```

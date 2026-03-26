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

In sandboxes or CI, make sure those paths are writable. The simplest workaround is a writable `HOME` or an explicit config whose `state_dir` and `cache_dir` point to writable paths.

## Current Auth Flow

This skill is `ncs`-first for the current internal release. Even if the live CLI still exposes access-key or STS login flows, do not guide users through them from this skill.

1. Check whether `ncs` is available:
   - run `command -v ncs`
   - if the result is empty, or points to `.real/third_party/cli/aone-kit/bin`, run the bundled installer described in [setup-install.md](setup-install.md)
2. Run `auth whoami --json`.
3. Inspect `data.identity`.
4. If `data.identity.authenticated` is `false`, check:
   - `configured=false` and `validation_status=missing_configuration`: required settings are missing
   - `configured=true` and `validation_status=failed`: config exists, but the remote `whoami` probe failed
5. Use the `ncs` login path only:
   - `auth login-ncs --list-accounts --account-type user --json`
   - `auth login-ncs --interactive`
   - `auth login-ncs --account-type user --employee-id <id> --project <project> --endpoint <endpoint> --json`
6. Ignore access-key or STS suggestions that may still appear in `data.auth_options`.
7. Read [ncs-auth.md](ncs-auth.md) before persisting or validating an ncs config.
8. If you only need to persist config and cannot validate remotely yet, add `--no-validate`.
9. After any login or config change, rerun `auth whoami --json`.
10. If metadata discovery is next, build cache before expecting `meta list-tables` to succeed:

```bash
maxc cache build --json
```

`auth whoami` now performs a remote security `whoami` call when config is present. A successful `authenticated=true` result means credential shape, endpoint reachability, and the security probe all worked. It still does not prove table-level permissions or data-path access.

## Config Discovery

Without top-level `--config`, the loader checks files in this order:

1. `~/.maxc/config.yaml`
2. `./.maxc/config.yaml`
3. `./.maxc.yaml`
4. `./.maxc`

Project and schema session overrides come from:

```text
~/.maxc/session_override.yaml
```

`session_override.yaml` has higher priority than both environment variables and config files for project/schema selection.

For `auth login` and `auth login-ncs`, top-level `--config <path>` may point to a file that does not exist yet; the command can create it.

## Environment Variables And Aliases

These variables still matter even in an `ncs`-first skill, because they can override saved config and change the effective runtime identity.

Primary variables for real access:

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="<access_key_id>"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="<access_key_secret>"
export MAXCOMPUTE_PROJECT="<project>"
export MAXCOMPUTE_ENDPOINT="<endpoint>"
```

STS adds:

```bash
export ALIBABA_CLOUD_SECURITY_TOKEN="<security_token>"
```

Supported aliases in the current CLI:

- access id: `ODPS_ACCESS_ID`, `ODPS_STS_ACCESS_KEY_ID`, `ACCESS_KEY_ID`
- secret: `ODPS_ACCESS_KEY`, `ODPS_ACCESS_KEY_SECRET`, `ODPS_STS_ACCESS_KEY_SECRET`, `ACCESS_KEY_SECRET`
- token: `ODPS_STS_TOKEN`, `SECURITY_TOKEN`
- project: `ODPS_PROJECT`
- endpoint: `ODPS_ENDPOINT`, `odps_endpoint`
- region: `MAXCOMPUTE_REGION`, `ALIBABA_CLOUD_REGION`
- tunnel endpoint: `MAXCOMPUTE_TUNNEL_ENDPOINT`, `ODPS_TUNNEL_ENDPOINT`

Environment variables override saved config. If `identity_source` is `mixed`, env and config are both active and env wins where values overlap.

## How To Read `auth whoami`

With `--json`, the normalized shape is:

```json
{
  "data": {
    "identity": {
      "authenticated": true,
      "configured": true,
      "validation_status": "verified",
      "backend": "odps",
      "auth_type": "ncs",
      "identity_source": "config_file",
      "principal_display": "ALIYUN$xxx or masked access id",
      "project": "demo_project"
    },
    "auth_options": []
  }
}
```

Key interpretations:

- `authenticated=false` means maxc could not complete a valid remote identity check
- `configured=false` means required auth settings are incomplete
- `validation_status` is one of `verified`, `missing_configuration`, or `failed`
- `identity_source` is one of `environment`, `config_file`, `mixed`, or `unknown`
- `principal_display` may fall back to a masked access key id when the security API does not provide an owner name
- `auth_options` is present when maxc wants to guide the next login step

## Out-Of-Scope Login Modes

The current CLI still implements `auth login` for access-key and STS-based auth, but this internal-release skill intentionally does not guide users through those flows.

Do not recommend:

- `auth login --from-env`
- `auth login --access-id ... --secret-access-key ...`
- `auth login --security-token ...`

## NCS Login

Use `ncs` only when that environment is already approved and available:

```bash
maxc auth login-ncs --interactive
maxc auth login-ncs --list-accounts
maxc auth login-ncs --account-type user --employee-id <id> --project <project> --endpoint <endpoint> --json
```

`auth login-ncs` saves `auth.provider=ncs` plus `auth.ncs.*` fields in config. It does not persist temporary access keys returned by `ncs`.

If `ncs` is missing from `PATH`, the current implementation raises a feature-unavailable error. It does not fall back to a mock backend.

See [ncs-auth.md](ncs-auth.md) for the full account-type mapping and guidance adapted from the earlier ODPSCMD-oriented workflow.

## Missing-Credential Handling

- Do not invent credentials.
- Do not assume a runtime mock/local backend exists.
- Prefer `auth login-ncs` over manual YAML edits.
- If `whoami` fails validation, inspect the warning text before retrying.
- If you only need local session inspection, `session show` and `agent context` can run without authenticated backend startup.

> Loaded on demand — covers the three-phase setup walkthrough. Skip unless `auth whoami --json` returned `configured=false`.

# Bootstrap Flow

When `auth whoami --json` returns `configured=false`, follow the three phases below in order. Each phase delegates to a focused reference — read those for command-level detail.

```
Phase 1: Prerequisites    → setup-install.md
            ↓
Phase 2: Auth             → bootstrap-auth.md
            ↓
Phase 3: Verify           → {{cli}} auth whoami --json
```

---

## Phase 1: Ensure Prerequisites

Goal: `python3` (>= 3.8), `pip`, and `{{cli}}` available.

```bash
python3 --version
{{cli}} --version
```

- Missing or too-old Python / missing `{{cli}}` → see [setup-install.md](setup-install.md). Never install or upgrade Python without explicit user confirmation.
<!-- @if cli_module_differs -->
- `{{cli}}` not on PATH but `{{cli_module}}` works → continue with the module form, do not block on PATH cleanup.
<!-- @endif -->

Skip whatever the user already has.

---

## Phase 2: Configure Auth

**Always ask the user which method first — never pick one yourself.**

> "Which auth method would you like to use?
> **(A) Access Key / Secret Key** — long-lived AK/SK pair
> **(B) Environment variables** — keys already exported in the current shell"

Then jump to the matching path in [bootstrap-auth.md](bootstrap-auth.md):

| User chose | Section in `bootstrap-auth.md` |
|---|---|
| (A) AK/SK | Path A: Access Key / Secret Key |
| (B) Env vars | Path B: Environment Variables |

### If `auth_type=external` is already configured

If `auth whoami --json` shows `auth_type=external` (or `provider: external` in the saved config), the user is on an externally-managed credential provider. **Do not run Phase 2.** The auth is already set up — only `project`/`endpoint`/`schema` are safe to change via `session set` or by re-running the original `auth login-external` with updated `--project`/`--endpoint`. Treat bootstrap as complete and move to Phase 3.

### Always confirm project and endpoint

Regardless of method, ask the user explicitly for `project` and `endpoint`. If a value is already in the config or env, present it as a default but require confirmation. See [bootstrap-auth.md](bootstrap-auth.md) §"Always ask for project and endpoint".

### Dev vs production project check

If the project name does **not** end with `_dev`, warn the user:

> "Project `<project>` does not end with `_dev`. Personal accounts usually only have access to dev projects — would you like to switch to `<project>_dev`?"

See SKILL.md §"Dev vs Production Projects" for the full rationale.

---

## Phase 3: Verify

```bash
{{cli}} auth whoami --json
```

Expected: `data.identity.authenticated=true`. `validation_status` interpretation:

| `validation_status` | Meaning | Action |
|---|---|---|
| `verified` | Remote check passed | ✓ ready |
| `configuration_only` | Saved but not remote-checked | ✓ ready, credentials resolve at first query |
| `validation_failed` | Remote check failed (also rendered as literal `failed` in some code paths — treat both the same) | Recheck AK/SK and endpoint |
| `missing_configuration` | Phase 2 did not save anything | Go back to Phase 2 |

### Common pitfalls when `whoami` looks wrong

- **Cwd config shadows global**: a `cwd/.maxc/config.yaml` (or `cwd/.maxc.yaml`, `cwd/.maxc`) deep-merges over `~/.maxc/config.yaml`. Run `{{cli}} session show --json` to see `config_sources`; `{{cli}} session unset --json` clears `default_project` / `default_schema` from the user-level file (cwd files are left alone).
- **Env vars override config**: `ALIBABA_CLOUD_ACCESS_KEY_ID` / `MAXCOMPUTE_PROJECT` etc. shadow saved values. `auth whoami` reports `identity_source=mixed` when this happens. Ask the user before unsetting.
- **Wrong project default**: if `project` shows production name but you wanted dev, `{{cli}} session set --project <name>_dev`.

---

## What this file does NOT cover

- Step-by-step Python / maxc-cli installation — see [setup-install.md](setup-install.md).
- Each auth method's exact CLI flags and saved YAML — see [bootstrap-auth.md](bootstrap-auth.md).
- Public cloud endpoint catalog — present in [bootstrap-auth.md](bootstrap-auth.md) Path A.

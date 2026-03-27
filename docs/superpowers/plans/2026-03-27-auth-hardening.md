# Auth Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix five auth-layer bugs where NCS authentication can be silently overridden, config source is opaque, session project conflicts with auth project, interactive prompts drop existing values, and the env-var warning is too broad.

**Architecture:** All fixes are localised to `config.py`, `helpers.py`, `auth_providers.py`, and `app.py`. No new modules. Tests live in `tests/test_cli_mock.py` (existing mock harness).

**Tech Stack:** Python 3.6+, pytest, PyYAML, argparse (no new deps)

---

## File Map

| File | Changes |
|------|---------|
| `src/maxc_cli/helpers.py` | Fix `missing_odps_settings` NCS tolerance |
| `src/maxc_cli/app.py` | Fix `auth_whoami`/`session_show` config sources; `session_set` project conflict warn; `auth_login_ncs` interactive defaults; narrow env-var warning |
| `tests/test_cli_mock.py` | Tests for every fix |

---

## Task 1: Fix `missing_odps_settings` — NCS requires `ncs_process_command` even when account fields are present

**Files:**
- Modify: `src/maxc_cli/helpers.py:200-210`
- Test: `tests/test_cli_mock.py`

**Background:** `missing_odps_settings(settings, auth_type="ncs")` requires `ncs_process_command`. But `build_ncs_account` already reconstructs the command from `ncs_account_type + ncs_employee_id/ncs_account_name/ncs_app_name` if `ncs_process_command` is absent. `missing_odps_settings` doesn't know this — so `auth_settings_available` incorrectly returns `False` for hand-edited configs that only set account_type+identifier.

**Current code (`helpers.py:208-210`):**
```python
if auth_type == "ncs":
    required = ["project", "endpoint", "ncs_process_command"]
return [name for name in required if not settings.get(name)]
```

**Fix:** if `ncs_process_command` is missing, check whether enough fields exist to reconstruct it.

- [ ] **Step 1: Write failing test**

Add to `tests/test_cli_mock.py`:

```python
def test_missing_odps_settings_ncs_tolerates_missing_process_command_when_account_fields_present() -> None:
    from maxc_cli.helpers import missing_odps_settings

    # Has account type + identifier but no process_command → should NOT be missing
    settings = {
        "project": "myproj",
        "endpoint": "http://service.cn.maxcompute.aliyun.com/api",
        "ncs_account_type": "user",
        "ncs_employee_id": "123456",
        "ncs_process_command": None,
    }
    assert missing_odps_settings(settings, auth_type="ncs") == []


def test_missing_odps_settings_ncs_reports_missing_when_no_account_fields() -> None:
    from maxc_cli.helpers import missing_odps_settings

    # No process_command AND no account fields → truly missing
    settings = {
        "project": "myproj",
        "endpoint": "http://service.cn.maxcompute.aliyun.com/api",
        "ncs_account_type": None,
        "ncs_employee_id": None,
        "ncs_process_command": None,
    }
    result = missing_odps_settings(settings, auth_type="ncs")
    assert "ncs_process_command" in result
```

- [ ] **Step 2: Run to verify FAIL**

```bash
pytest tests/test_cli_mock.py::test_missing_odps_settings_ncs_tolerates_missing_process_command_when_account_fields_present tests/test_cli_mock.py::test_missing_odps_settings_ncs_reports_missing_when_no_account_fields -v
```

Expected: first test FAILS (reports ncs_process_command as missing), second PASSES.

- [ ] **Step 3: Implement fix in `helpers.py`**

Replace the ncs block in `missing_odps_settings`:

```python
if auth_type == "ncs":
    missing = [f for f in ("project", "endpoint") if not settings.get(f)]
    # ncs_process_command can be derived from account_type + identifier,
    # so only require it when no derivable fields are present either.
    has_process_command = bool(settings.get("ncs_process_command"))
    has_account_fields = bool(
        settings.get("ncs_account_type") and (
            settings.get("ncs_employee_id")
            or settings.get("ncs_account_name")
            or settings.get("ncs_app_name")
        )
    )
    if not has_process_command and not has_account_fields:
        missing.append("ncs_process_command")
    return missing
```

- [ ] **Step 4: Run tests to verify PASS**

```bash
pytest tests/test_cli_mock.py::test_missing_odps_settings_ncs_tolerates_missing_process_command_when_account_fields_present tests/test_cli_mock.py::test_missing_odps_settings_ncs_reports_missing_when_no_account_fields -v
```

Expected: both PASS.

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
pytest tests/test_cli_mock.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/maxc_cli/helpers.py tests/test_cli_mock.py
git commit -m "fix: tolerate missing ncs_process_command when account type+id fields are present"
```

---

## Task 2: `auth whoami` and `session show` expose active config file paths

**Files:**
- Modify: `src/maxc_cli/app.py` — `auth_whoami`, `_whoami_validation_failed_envelope`, `_unauthenticated_whoami_envelope`, `session_show`
- Test: `tests/test_cli_mock.py`

**Background:** When NCS auth is silently overridden by a local `.maxc` file, there is no way for the user or agent to detect which config file "won". Adding `config_sources` to metadata of `auth whoami` and `session show` makes the conflict diagnosable.

`MaxCConfig` already carries `sources: list[Path]` (set in `load_config`, `config.py:379`). It just needs to be forwarded into the response metadata.

- [ ] **Step 1: Write failing test**

```python
def test_auth_whoami_metadata_includes_config_sources(tmp_path: 'Path', monkeypatch) -> None:
    """auth whoami metadata should list the active config file paths."""
    import maxc_cli.backend.odps as odps_module
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n"
        "  provider: access_key\n"
        "  access_id: TESTID1234\n"
        "  secret_access_key: TESTSECRET1234\n"
        "  project: test_project\n"
        "  endpoint: http://service.cn.maxcompute.aliyun.com/api\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(odps_module, "ODPS", FakeODPS)

    code, payload, _ = run_json_command(tmp_path, config_path, ["auth", "whoami", "--json"])
    assert code == 0
    assert "config_sources" in payload["metadata"]
    assert isinstance(payload["metadata"]["config_sources"], list)
    assert any(str(config_path) in s for s in payload["metadata"]["config_sources"])


def test_session_show_data_includes_config_sources(tmp_path: 'Path', monkeypatch) -> None:
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "default_project: demo\n"
        "default_format: json\n"
        "state_dir: .maxc/state\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(tmp_path, config_path, ["session", "show", "--json"])
    assert code == 0
    assert "config_sources" in payload["data"]
    assert isinstance(payload["data"]["config_sources"], list)
```

- [ ] **Step 2: Run to verify FAIL**

```bash
pytest tests/test_cli_mock.py::test_auth_whoami_metadata_includes_config_sources tests/test_cli_mock.py::test_session_show_data_includes_config_sources -v
```

- [ ] **Step 3: Add `config_sources` to `auth_whoami` in `app.py`**

In `auth_whoami` (around line 2080), change the `metadata` dict:
```python
metadata={
    "project": self.config.default_project,
    "config_sources": [str(p) for p in self.config.sources],
},
```

In `_whoami_validation_failed_envelope` (around line 106), change the `metadata` dict:
```python
metadata={
    "project": self.config.default_project,
    "config_sources": [str(p) for p in self.config.sources],
},
```

In `_unauthenticated_whoami_envelope` (around line 2142), add same `config_sources` to metadata. First read that method to confirm its shape:

```python
metadata={
    "project": self.config.default_project,
    "config_sources": [str(p) for p in self.config.sources],
},
```

- [ ] **Step 4: Add `config_sources` to `session_show` in `app.py`**

In `session_show`, add `config_sources` inside the `data={...}` literal at line 1703 (alongside `project_info`):
```python
data={
    "project": { ... },
    "schema": { ... },
    "override_path": str(override_path),
    "config_path": str(config_path) if config_path.exists() else None,
    "project_info": project_info,
    "config_sources": [str(p) for p in self.config.sources],
},
```
Only add the `"config_sources"` line — do not duplicate the other keys.

- [ ] **Step 5: Run tests to verify PASS**

```bash
pytest tests/test_cli_mock.py::test_auth_whoami_metadata_includes_config_sources tests/test_cli_mock.py::test_session_show_data_includes_config_sources -v
```

- [ ] **Step 6: Run full suite**

```bash
pytest tests/test_cli_mock.py -v
```

- [ ] **Step 7: Commit**

```bash
git add src/maxc_cli/app.py tests/test_cli_mock.py
git commit -m "feat: expose active config file paths in auth whoami and session show metadata"
```

---

## Task 3: `session set` warns when override project differs from auth project

**Files:**
- Modify: `src/maxc_cli/app.py:session_set` (around line 1570)
- Test: `tests/test_cli_mock.py`

**Background:** `session_override.yaml` project takes highest priority over everything, including the project saved in `auth login-ncs`. If an agent did `session set --project old_project` and then `auth login-ncs --project new_project`, operations silently run against `old_project` with NCS credentials that may only cover `new_project`.

The fix: after saving the session override project, check if it differs from `config.auth.project`. If so, emit a warning in `agent_hints.warnings`.

- [ ] **Step 1: Write failing test**

```python
def test_session_set_warns_when_project_differs_from_auth_project(tmp_path: 'Path', monkeypatch) -> None:
    """session set should warn when override project differs from auth.project."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "auth:\n"
        "  provider: ncs\n"
        "  project: ncs_project\n"
        "  endpoint: http://service.cn.maxcompute.aliyun.com/api\n"
        "  ncs:\n"
        "    account_type: user\n"
        "    employee_id: '123456'\n"
        "    process_command: 'ncs create credential odpsuser --employee-id 123456 -o template -t odpscmd'\n"
        "default_project: ncs_project\n"
        "allowed_operations:\n  - SELECT\n",
        encoding="utf-8",
    )

    code, payload, _ = run_json_command(
        tmp_path, config_path, ["session", "set", "--project", "other_project", "--json"]
    )
    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert any("ncs_project" in w and "other_project" in w for w in warnings), (
        f"Expected a warning about project mismatch, got: {warnings}"
    )
```

- [ ] **Step 2: Run to verify FAIL**

```bash
pytest tests/test_cli_mock.py::test_session_set_warns_when_project_differs_from_auth_project -v
```

- [ ] **Step 3: Implement fix in `app.py:session_set`**

After the `if project:` block (after `changes.append(...)`, before `save_config_mapping`), add:

```python
        # Warn if session override project differs from the project saved in auth config
        if project and self.config.auth.project and project != self.config.auth.project:
            warnings.append(
                f"Session project override (`{project}`) differs from the project saved in auth config "
                f"(`{self.config.auth.project}`). Operations will use `{project}`, but credentials "
                f"were configured for `{self.config.auth.project}`. Run `auth whoami` to verify access."
            )
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_cli_mock.py::test_session_set_warns_when_project_differs_from_auth_project -v
```

- [ ] **Step 5: Run full suite**

```bash
pytest tests/test_cli_mock.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/maxc_cli/app.py tests/test_cli_mock.py
git commit -m "fix: warn in session set when override project differs from auth configured project"
```

---

## Task 4: `auth login-ncs` interactive mode shows existing values as defaults

**Files:**
- Modify: `src/maxc_cli/app.py:_prompt_text` (around line 2202) and `auth_login_ncs` interactive block (around line 1959)
- Test: `tests/test_cli_mock.py`

**Background:** When `auth login-ncs --interactive` is run to update an existing NCS config, `_prompt_text` doesn't show the current value. Users don't know they can press Enter to keep it, and the code path (`project = project or self._prompt_text(...)`) means if the user presses Enter (empty input), it calls `_prompt_text` which raises `ValidationError` for required fields.

Fix:
1. Add `default` param to `_prompt_text`: show `[current: X]` in prompt, accept empty input to return the default.
2. In `auth_login_ncs` interactive block, pass `existing_auth.*` values as `default`.

- [ ] **Step 1: Write failing test**

Note: `StringIO.isatty()` returns `False`, so `_prompt_text` will reach the `if not sys.stdin.isatty(): return default` branch after the fix rather than the live `input()` branch. That is the correct path to test: in non-TTY contexts (CI, agents), existing values must be preserved as defaults. Use `monkeypatch` on `builtins.input` to test genuine TTY interaction separately if needed, but the non-TTY test is sufficient to verify the fix.

```python
def test_auth_login_ncs_interactive_uses_existing_project_on_empty_input(
    tmp_path: 'Path', monkeypatch
) -> None:
    """Interactive login-ncs preserves existing values when stdin is non-TTY (empty input).

    After the fix, _prompt_text returns `default` when stdin is not a TTY,
    so existing config values survive a --interactive run in non-interactive environments.
    """
    import builtins
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")

    config_path = tmp_path / "existing.yaml"
    config_path.write_text(
        "auth:\n"
        "  provider: ncs\n"
        "  project: existing_project\n"
        "  endpoint: http://service.cn.maxcompute.aliyun.com/api\n"
        "  ncs:\n"
        "    account_type: user\n"
        "    employee_id: '111'\n"
        "    process_command: 'ncs create credential odpsuser --employee-id 111 -o template -t odpscmd'\n",
        encoding="utf-8",
    )

    # Patch stdin.isatty to return True so _prompt_text enters the interactive branch,
    # then patch builtins.input to return "" (simulating the user pressing Enter).
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr(builtins, "input", lambda _: "")

    stdout = StringIO()
    from maxc_cli.cli import run
    code = run(
        ["--config", str(config_path), "auth", "login-ncs", "--interactive", "--no-validate", "--json"],
        cwd=tmp_path,
        stdout=stdout,
        stderr=StringIO(),
    )
    payload = json.loads(stdout.getvalue())
    assert code == 0
    # Empty input should keep existing project via the default parameter
    assert payload["data"]["identity"]["project"] == "existing_project"
```

- [ ] **Step 2: Run to verify FAIL**

```bash
pytest tests/test_cli_mock.py::test_auth_login_ncs_interactive_uses_existing_project_on_empty_input -v
```

- [ ] **Step 3: Update `_prompt_text` to accept a `default` parameter**

```python
def _prompt_text(
    self,
    prompt: 'str',
    *,
    required: 'bool' = True,
    default: 'str | None' = None,
) -> 'str | None':
    if not sys.stdin.isatty():
        return default
    display_prompt = prompt
    if default:
        display_prompt = f"{prompt} [current: {default}]"
    value = input(f"{display_prompt}: ").strip()
    if value:
        return value
    if default:
        return default
    if required:
        raise ValidationError(f"{prompt} is required.")
    return None
```

- [ ] **Step 4: Pass existing values as `default` in `auth_login_ncs` interactive block**

Replace the interactive block in `auth_login_ncs` (lines 1959–1974):

```python
        if interactive:
            account_type = account_type or self._prompt_text(
                "ncs account type (user/account/app)",
                default=existing_auth.ncs.account_type,
            )
            normalized_type = (account_type or "").strip().lower()
            if normalized_type == "user":
                employee_id = employee_id or self._prompt_text(
                    "Employee ID", default=existing_auth.ncs.employee_id
                )
            elif normalized_type == "account":
                account_name = account_name or self._prompt_text(
                    "Account name", default=existing_auth.ncs.account_name
                )
            elif normalized_type == "app":
                app_name = app_name or self._prompt_text(
                    "App name", default=existing_auth.ncs.app_name
                )
            project = project or self._prompt_text(
                "MaxCompute Project", default=existing_auth.project
            )
            endpoint = endpoint or self._prompt_text(
                "MaxCompute Endpoint", default=existing_auth.endpoint
            )
            region_name = region_name or self._prompt_text(
                "MaxCompute Region (optional)",
                required=False,
                default=existing_auth.region_name,
            )
            tunnel_endpoint = tunnel_endpoint or self._prompt_text(
                "MaxCompute Tunnel Endpoint (optional)",
                required=False,
                default=existing_auth.tunnel_endpoint,
            )
```

- [ ] **Step 5: Run tests**

```bash
pytest tests/test_cli_mock.py::test_auth_login_ncs_interactive_uses_existing_project_on_empty_input -v
```

- [ ] **Step 6: Run full suite**

```bash
pytest tests/test_cli_mock.py -v
```

- [ ] **Step 7: Commit**

```bash
git add src/maxc_cli/app.py tests/test_cli_mock.py
git commit -m "fix: auth login-ncs interactive mode shows and preserves existing config values"
```

---

## Task 5: Narrow the env-var override warning in `auth login-ncs`

**Files:**
- Modify: `src/maxc_cli/app.py:auth_login_ncs` (around line 2007)
- Test: `tests/test_cli_mock.py`

**Background:** The current warning fires whenever `access_id`, `secret_access_key`, `security_token`, `project`, or `endpoint` env vars are set. But when `provider=ncs` is explicitly configured, access_id/secret env vars do **not** change the provider selection (see `infer_auth_provider` — explicit wins). The warning is therefore misleading: it implies env vars might override NCS when they cannot override the provider.

A more precise warning:
- Only warn about `project` and `endpoint` env vars (they DO override the NCS-configured values at runtime via `resolve_odps_settings`)
- Do not warn about `access_id`/`secret_access_key`/`security_token` — they don't change the NCS provider

- [ ] **Step 1: Write failing test**

```python
def test_auth_login_ncs_no_spurious_warning_when_only_credential_env_vars_set(
    tmp_path: 'Path', monkeypatch
) -> None:
    """login-ncs should NOT warn about env vars when only access_id/secret envs are set.

    Those env vars don't override the ncs provider selection, so warning is misleading.
    """
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")
    # Only set access_id/secret — these don't affect NCS provider
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_ID", "some_key")
    monkeypatch.setenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "some_secret")

    config_path = tmp_path / "ncs.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login-ncs",
            "--account-type", "user",
            "--employee-id", "999",
            "--project", "my_project",
            "--endpoint", "http://service.cn.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    # No warning about env vars overriding NCS config
    assert not any("environment variable" in w.lower() or "env" in w.lower() for w in warnings), (
        f"Should not warn about env vars when only credential vars are set: {warnings}"
    )


def test_auth_login_ncs_warns_when_project_or_endpoint_env_var_set(
    tmp_path: 'Path', monkeypatch
) -> None:
    """login-ncs SHOULD warn when MAXCOMPUTE_PROJECT or MAXCOMPUTE_ENDPOINT env vars are set."""
    clear_odps_env(monkeypatch)
    isolate_home(monkeypatch, tmp_path)
    monkeypatch.setattr("maxc_cli.auth_providers.shutil.which", lambda _: "/usr/bin/ncs")
    monkeypatch.setenv("MAXCOMPUTE_PROJECT", "env_override_project")

    config_path = tmp_path / "ncs.yaml"
    code, payload, _ = run_json_command(
        tmp_path,
        config_path,
        [
            "auth", "login-ncs",
            "--account-type", "user",
            "--employee-id", "999",
            "--project", "config_project",
            "--endpoint", "http://service.cn.maxcompute.aliyun.com/api",
            "--no-validate", "--json",
        ],
    )
    assert code == 0
    warnings = payload["agent_hints"]["warnings"]
    assert any("env" in w.lower() or "environment" in w.lower() for w in warnings), (
        f"Expected a warning about project/endpoint env var override: {warnings}"
    )
```

- [ ] **Step 2: Run to verify FAIL/PASS**

```bash
pytest tests/test_cli_mock.py::test_auth_login_ncs_no_spurious_warning_when_only_credential_env_vars_set tests/test_cli_mock.py::test_auth_login_ncs_warns_when_project_or_endpoint_env_var_set -v
```

Expected: first test FAILS (currently warns), second PASSES.

- [ ] **Step 3: Implement narrowed warning in `app.py:auth_login_ncs`**

Replace (around line 2007):
```python
        if any(load_odps_env().get(name) for name in ("access_id", "secret_access_key", "security_token", "project", "endpoint")):
            warnings.append(
                "MaxCompute-related environment variables are set in the current shell; they may override the ncs config you just saved."
            )
```

With:
```python
        env_settings = load_odps_env()
        overriding_env_fields = [
            name for name in ("project", "endpoint")
            if env_settings.get(name)
        ]
        if overriding_env_fields:
            warnings.append(
                f"Environment variable(s) for {', '.join(overriding_env_fields)} are set and will override "
                f"the values you just saved at runtime. Unset them or they will take precedence over this ncs config."
            )
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_cli_mock.py::test_auth_login_ncs_no_spurious_warning_when_only_credential_env_vars_set tests/test_cli_mock.py::test_auth_login_ncs_warns_when_project_or_endpoint_env_var_set -v
```

- [ ] **Step 5: Run full suite**

```bash
pytest tests/test_cli_mock.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/maxc_cli/app.py tests/test_cli_mock.py
git commit -m "fix: narrow env-var override warning in auth login-ncs to project/endpoint only"
```

---

## Task 6: Verify all tests pass and do final cleanup

- [ ] **Step 1: Run full test suite**

```bash
pytest -v
```

Expected: all tests PASS, no regressions.

- [ ] **Step 2: Quick smoke-check of changed behaviour**

```bash
# Verify missing_odps_settings is importable and returns expected values
python3 -c "
from maxc_cli.helpers import missing_odps_settings
s = {'project': 'p', 'endpoint': 'e', 'ncs_account_type': 'user', 'ncs_employee_id': '1', 'ncs_process_command': None}
assert missing_odps_settings(s, auth_type='ncs') == [], 'Fix 1 failed'
print('Fix 1 OK: ncs missing_odps_settings with account fields')
"
```

- [ ] **Step 3: No trailing summary needed — diff speaks for itself**

> Load only when the preferred CLI entry or its runtime is missing or too old.

# Install Or Upgrade The CLI

After the CLI is available, append the session User-Agent declared in SKILL.md
to each MaxCompute command that calls a cloud API: `--user-agent "$UA"`.
Local help, `agent context`, `agent manifest`, `session show`, and `cache status`
may omit it.

## Preferred Public-Cloud Entry

Use Alibaba Cloud CLI for the public-cloud Skill:

```bash
aliyun version
aliyun maxc --help
```

`aliyun maxc` requires Alibaba Cloud CLI 3.3.19 or later. For a non-Homebrew
Alibaba Cloud CLI 3.3.5+ that supports self-upgrade, run after user approval:

```bash
aliyun upgrade
aliyun version
```

For Homebrew, a missing CLI, or an older distribution without that command,
use the official installer or package-manager update flow after user approval.

If Alibaba Cloud CLI is absent, use its official installer for the user's
platform. Do not invent an installer URL or make a system-wide change without
the user's authorization.

An install-only request does not require changing credentials. For a separately
requested connection, start with [the existing identity](bootstrap-flow.md).

## Standalone Python Entry

Use this only when the user explicitly wants the PyPI distribution or the
Alibaba Cloud CLI extension is unavailable.

Requirements:

- Python 3.9 or later;
- a working `python3 -m pip`;
- authorization before installing or upgrading Python itself.

```bash
python3 --version
python3 -m pip --version
python3 -m pip install --upgrade maxc-cli
maxc --version
```

For a user-local environment:

```bash
python3 -m pip install --user --upgrade maxc-cli
```

If `maxc` is not on `PATH` but the package is installed, use
`python3 -m maxc_cli` for the current task. Do not modify shell startup files
unless the user asks.

## Verification

After either installation path:

```bash
{{cli}} agent context --json
{{cli}} agent skill --json
```

These checks are local; they do not prove online readiness. For a requested
agent-platform install, inspect `{{cli}} agent skill install --help`, choose
the platform and matching `--invocation`, then verify with `agent skill diff`.
`agent skill update` preserves the installed invocation unless explicitly changed.

Managed environments may have a command usage worker. Users can opt out of
MaxC usage telemetry with `telemetry.enabled: false` in `~/.maxc/config.yaml`.
The worker is operated by the deployment administrator.

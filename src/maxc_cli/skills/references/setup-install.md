> Load only when the preferred CLI entry or its runtime is missing or too old.

# Install Or Upgrade The CLI

After the CLI is available, append the session User-Agent declared in SKILL.md
to each MaxCompute command: `--user-agent "$UA"`.

## Preferred Public-Cloud Entry

Use Alibaba Cloud CLI for the public-cloud Skill:

```bash
aliyun version
aliyun maxc --help
```

`aliyun maxc` requires Alibaba Cloud CLI 3.3.3 or later. If the installed
version is older:

```bash
aliyun upgrade
aliyun version
```

If Alibaba Cloud CLI is absent, use its official installer for the user's
platform. Do not invent an installer URL or make a system-wide change without
the user's authorization.

Start the MaxCompute OAuth flow through the selected CLI entry:

```bash
aliyun maxc auth login --oauth --json
aliyun maxc agent doctor --online --json
```

The callback is loopback-based on the CLI host. `--no-browser` only prevents
automatic browser opening; it is not a device-code/headless alternative.

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
maxc agent doctor --online --json
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
{{cli}} agent manifest --json
{{cli}} agent doctor --online --json
```

`agent context` is local-only; it cannot establish that the backend is
reachable. The online doctor is the readiness gate for remote operations.

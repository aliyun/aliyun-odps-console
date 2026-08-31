> Load only when preflight shows that the CLI or authentication is not ready.

# Bootstrap Flow

Append the session User-Agent declared in SKILL.md to each `{{cli}}` command
that calls a cloud API: `--user-agent "$UA"`. Local help, `agent context`,
`agent manifest`, `session show`, and `cache status` may omit it.

Keep installation, local configuration, and live verification as separate
gates:

```text
CLI available -> local context -> authentication -> online doctor
```

## 1. Verify The Entry

```bash
{{cli}} --version
{{cli}} agent context --json
```

For `aliyun maxc`, Alibaba Cloud CLI must be 3.3.19 or later. Read
[setup-install.md](setup-install.md) if the entry is missing or too old.

## 2. Inspect Existing Authentication

```bash
{{cli}} auth whoami --json
```

- If `data.identity.authenticated=true`, keep the current identity unless the
  user asked to change account or project.
- If an external or runtime-managed provider is configured, do not replace it.
- If authentication is missing, prefer OAuth for an interactive public-cloud
  session.

## 3. Configure Authentication

Preferred public-cloud path:

```bash
{{cli}} auth login --oauth --json
```

The callback server binds to `127.0.0.1` on the CLI host. `--no-browser` only
prints the sign-in URL instead of opening it; it does not change the callback
or provide device-code authentication. For an SSH-hosted CLI, arrange loopback
port forwarding or use a browser on that host before starting the flow.

If OAuth cannot be used, read [bootstrap-auth.md](bootstrap-auth.md) and select
AK/STS, environment, or external-process authentication based on the user's
runtime. Never request that a secret be pasted into chat.

## 4. Resolve Project Selection

Project names are opaque. Do not infer a development, production, or related
project from naming conventions. If login returns `status=pending`, use its
structured actions/project list and ask the user when more than one target is
plausible.

## 5. Verify Online

```bash
{{cli}} agent doctor --online --json
```

Continue only when `data.ready=true`. If a check fails, use its exact detail
and the envelope's recovery actions; do not repeat the same login blindly.

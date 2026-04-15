# maxc Command

Use the maxc CLI for MaxCompute/ODPS operations. **Read the SKILL.md first** to understand all commands and workflows.

## Prerequisites

```bash
pip install maxc-cli
maxc --version  # verify: 0.1.3+
```

## Authentication

Before any data operation, authenticate:

```bash
# Option A: from environment variables
maxc auth login --from-env --json

# Option B: interactive AK/SK
maxc auth login --json

# Verify
maxc auth whoami --json
```

## Core Commands

| What | Command |
|------|---------|
| Search tables | `maxc meta search <keyword> --json` |
| Describe table | `maxc meta describe <table> --json` |
| Run SQL | `maxc query <SQL> --json` |
| Estimate cost | `maxc query cost <SQL> --json` |
| Submit async job | `maxc job submit <SQL> --json` |
| Check job status | `maxc job status <job_id> --json` |
| Wait for job | `maxc job wait <job_id> --json` |
| Sample data | `maxc data sample <table> --json` |
| Profile data | `maxc data profile <table> --json` |
| Switch project | `maxc session set --project <name> --json` |
| Agent context | `maxc agent context --json` |
| Agent skill info | `maxc agent skill --json` |
| Full command list | `maxc agent commands --json` |

## Rules

1. **Always use `--json`** for structured output.
2. **Always check auth first**: run `maxc agent context --json` and verify `auth_status` is `authenticated`.
3. **Never run DML/DDL** — maxc-cli is read-only (SELECT only).
4. **Check cost before big queries**: `maxc query cost <SQL> --json`.
5. **Use cursor pagination** for large result sets: pass `--cursor <next_cursor>` from previous response.
6. **Read SKILL.md for full details**: the skill path is returned by `maxc agent skill --json`.

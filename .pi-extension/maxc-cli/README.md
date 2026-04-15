# MaxC CLI Extension for Cursor / Windsurf

## Overview

Agent-native CLI for MaxCompute/ODPS. Use `maxc` for auth, metadata discovery, SQL execution, job tracking, and data profiling.

## Installation

```bash
pip install maxc-cli
```

## Setup

```bash
# Verify installation
maxc --version

# Authenticate
maxc auth login --from-env --json
# or
maxc auth login --access-id "<id>" --secret-access-key "<secret>" --project "<project>" --endpoint "<endpoint>" --json

# Verify auth
maxc auth whoami --json
```

## Key Commands

```bash
maxc query "SELECT * FROM my_table LIMIT 10" --json
maxc meta search "keyword" --json
maxc meta describe table_name --json
maxc data sample table_name --json
maxc job submit "SELECT ..." --json
maxc agent context --json
```

All commands output structured JSON with `--json` flag.

"""Per-command sample text for ``--help``. Keys are dotted command names.

Style: 1-3 lines, concrete and copy-pasteable. Prefer realistic project names
('my_proj') over placeholders ('PROJECT'). Wrap user-facing commands in the
quoting style they'd actually type.
"""
from __future__ import annotations


SAMPLES: dict[str, str] = {
    "__top__": (
        "maxc auth login\n"
        'maxc query "SELECT 1"\n'
        "maxc meta list-tables --project my_proj"
    ),
    # ── query ──────────────────────────────────────────────────────────────
    "query": (
        'maxc query "SELECT 1"\n'
        'maxc query cost "SELECT * FROM big_table"\n'
        "maxc query explain \"SELECT * FROM t WHERE dt='20260101'\""
    ),
    # ── auth ───────────────────────────────────────────────────────────────
    "auth": "maxc auth login\nmaxc auth whoami --json",
    "auth.login": (
        "maxc auth login                         # interactive picker\n"
        "maxc auth login --access-id AK --secret-access-key SK --no-picker"
    ),
    "auth.login-external": (
        "maxc auth login-external --process-command 'curl -s https://my-sts/token'"
    ),
    "auth.whoami": "maxc auth whoami\nmaxc auth whoami --json",
    "auth.can-i": "maxc auth can-i --table my_proj.my_table --operation SELECT",
    # ── job ────────────────────────────────────────────────────────────────
    "job": (
        'maxc job submit "SELECT count(*) FROM my_table"\n'
        "maxc job list --limit 20\n"
        "maxc job status <job_id>"
    ),
    "job.submit": (
        'maxc job submit "SELECT count(*) FROM my_table"\n'
        'maxc job submit --file query.sql --project my_proj'
    ),
    "job.status": "maxc job status <job_id>\nmaxc job status <job_id> --json",
    "job.wait": (
        "maxc job wait <job_id>\n"
        "maxc job wait <job_id> --timeout 600 --stream"
    ),
    "job.diagnose": "maxc job diagnose <job_id>\nmaxc job diagnose <job_id> --json",
    "job.result": (
        "maxc job result <job_id>\n"
        "maxc job result <job_id> --max-rows 1000 --json"
    ),
    "job.cancel": "maxc job cancel <job_id>",
    "job.list": "maxc job list\nmaxc job list --limit 50 --json",
    # ── meta ───────────────────────────────────────────────────────────────
    "meta": (
        "maxc meta list-tables --project my_proj\n"
        "maxc meta describe my_table\n"
        "maxc meta search orders"
    ),
    "meta.list-tables": (
        "maxc meta list-tables --project my_proj\n"
        "maxc meta list-tables --schema default --limit 50 --json"
    ),
    "meta.describe": (
        "maxc meta describe my_table\n"
        "maxc meta describe my_proj.my_table --full --json"
    ),
    "meta.search": (
        "maxc meta search orders\n"
        "maxc meta search user --project my_proj --json"
    ),
    "meta.search-columns": (
        "maxc meta search-columns user_id\n"
        "maxc meta search-columns dt --project my_proj --json"
    ),
    "meta.latest-partition": (
        "maxc meta latest-partition my_table\n"
        "maxc meta latest-partition my_proj.my_table --json"
    ),
    "meta.freshness": (
        "maxc meta freshness my_table\n"
        "maxc meta freshness my_proj.my_table --json"
    ),
    "meta.partitions": (
        "maxc meta partitions my_table\n"
        "maxc meta partitions my_proj.my_table --limit 50 --json"
    ),
    "meta.list-projects": "maxc meta list-projects\nmaxc meta list-projects --json",
    "meta.list-schemas": (
        "maxc meta list-schemas\n"
        "maxc meta list-schemas --project my_proj --json"
    ),
    # ── meta semantic ──────────────────────────────────────────────────────
    "meta.semantic": (
        "maxc meta semantic set my_table --desc 'Order facts'\n"
        "maxc meta semantic get my_table\n"
        "maxc meta semantic list-missing"
    ),
    "meta.semantic.set": (
        "maxc meta semantic set my_table --desc 'Order facts'\n"
        "maxc meta semantic set my_table --use-cases reporting analytics --sample-questions 'top users by revenue'"
    ),
    "meta.semantic.get": (
        "maxc meta semantic get my_table\n"
        "maxc meta semantic get my_table --json"
    ),
    "meta.semantic.list-missing": (
        "maxc meta semantic list-missing\n"
        "maxc meta semantic list-missing --json"
    ),
    # ── session ────────────────────────────────────────────────────────────
    "session": (
        "maxc session set --project my_proj\n"
        "maxc session show\n"
        "maxc session unset"
    ),
    "session.set": (
        "maxc session set --project my_proj\n"
        "maxc session set --project my_proj --schema default"
    ),
    "session.show": "maxc session show\nmaxc session show --json",
    "session.unset": "maxc session unset",
    # ── data ───────────────────────────────────────────────────────────────
    "data": (
        "maxc data sample my_table --rows 10\n"
        "maxc data profile my_table\n"
        "maxc data download my_table --output rows.csv"
    ),
    "data.sample": (
        "maxc data sample my_table --rows 10\n"
        "maxc data sample my_table --partition \"dt='20260101'\" --columns id,name"
    ),
    "data.profile": (
        "maxc data profile my_table\n"
        "maxc data profile my_table --partition \"dt='20260101'\" --json"
    ),
    "data.upload": (
        "maxc data upload my_table --file rows.csv\n"
        "maxc data upload my_table --file rows.tsv --delimiter $'\\t' --partition \"dt='20260101'\" --overwrite"
    ),
    "data.download": (
        "maxc data download my_table --output rows.csv\n"
        "maxc data download my_table --output rows.csv --columns id,name --limit 1000"
    ),
    # ── agent ──────────────────────────────────────────────────────────────
    "agent": (
        "maxc agent context\n"
        "maxc agent skill\n"
        "maxc agent install-skill claude-code"
    ),
    "agent.context": "maxc agent context\nmaxc agent context --json",
    "agent.skill": "maxc agent skill\nmaxc agent skill --json",
    "agent.install-skill": (
        "maxc agent install-skill                  # defaults to claude-code\n"
        "maxc agent install-skill cursor"
    ),
    # ── cache ──────────────────────────────────────────────────────────────
    "cache": (
        "maxc cache build --project my_proj\n"
        "maxc cache status --project my_proj\n"
        "maxc cache clear --project my_proj"
    ),
    "cache.build": (
        "maxc cache build --project my_proj\n"
        "maxc cache build --project my_proj --schema default --async"
    ),
    "cache.build-status": (
        "maxc cache build-status --project my_proj\n"
        "maxc cache build-status --project my_proj --build-id <id> --json"
    ),
    "cache.status": (
        "maxc cache status --project my_proj\n"
        "maxc cache status --project my_proj --schema default --json"
    ),
    "cache.clear": (
        "maxc cache clear --project my_proj\n"
        "maxc cache clear --project my_proj --schema default"
    ),
}

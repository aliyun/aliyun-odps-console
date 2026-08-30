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
        "maxc query cost \"SELECT * FROM default.big_table WHERE ds='20260101'\"\n"
        "maxc query explain \"SELECT * FROM default.events WHERE dt='20260101'\""
    ),
    # ── auth ───────────────────────────────────────────────────────────────
    "auth": "maxc auth login --oauth\nmaxc auth whoami --json\nmaxc auth logout --json",
    "auth.login": (
        "maxc auth login --oauth\n"
        "maxc auth login --from-env --json"
    ),
    "auth.login-external": (
        "maxc auth login-external --process-command 'credential-helper --format json'"
    ),
    "auth.logout": "maxc auth logout --json",
    "auth.whoami": "maxc auth whoami\nmaxc auth whoami --json",
    "auth.can-i": (
        "maxc auth can-i --table default.orders --operation SELECT --project my_proj"
    ),
    # ── job ────────────────────────────────────────────────────────────────
    "job": (
        "maxc job submit \"SELECT count(*) FROM default.orders WHERE ds='20260101'\"\n"
        "maxc job list --limit 20\n"
        "maxc job status <job_id>"
    ),
    "job.submit": (
        "maxc job submit \"SELECT count(*) FROM default.orders WHERE ds='20260101'\"\n"
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
        "maxc meta describe default.orders --project my_proj\n"
        "maxc meta search orders"
    ),
    "meta.list-tables": (
        "maxc meta list-tables --project my_proj\n"
        "maxc meta list-tables --schema default --limit 50 --json"
    ),
    "meta.describe": (
        "maxc meta describe default.orders --project my_proj\n"
        "maxc meta describe default.orders --project my_proj --full --json"
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
        "maxc meta latest-partition default.events\n"
        "maxc meta latest-partition default.events --project my_proj --json"
    ),
    "meta.freshness": (
        "maxc meta freshness default.events\n"
        "maxc meta freshness default.events --project my_proj --json"
    ),
    "meta.partitions": (
        "maxc meta partitions default.events\n"
        "maxc meta partitions default.events --project my_proj --limit 50 --json"
    ),
    "meta.list-projects": "maxc meta list-projects\nmaxc meta list-projects --json",
    "meta.list-schemas": (
        "maxc meta list-schemas\n"
        "maxc meta list-schemas --project my_proj --json"
    ),
    # ── meta semantic ──────────────────────────────────────────────────────
    "meta.semantic": (
        "maxc meta semantic set default.orders --desc 'Order facts'\n"
        "maxc meta semantic get default.orders\n"
        "maxc meta semantic list-missing\n"
        "maxc meta semantic clear default.orders"
    ),
    "meta.semantic.set": (
        "maxc meta semantic set default.orders --desc 'Order facts'\n"
        "maxc meta semantic set default.orders --use-cases reporting analytics --sample-questions 'top users by revenue'"
    ),
    "meta.semantic.get": (
        "maxc meta semantic get default.orders\n"
        "maxc meta semantic get default.orders --json"
    ),
    "meta.semantic.list-missing": (
        "maxc meta semantic list-missing\n"
        "maxc meta semantic list-missing --json"
    ),
    "meta.semantic.clear": (
        "maxc meta semantic clear default.orders --json\n"
        "maxc meta semantic clear --all --project my_proj --force --json"
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
        "maxc data sample default.orders --rows 10\n"
        "maxc data profile default.orders\n"
        "maxc data download default.orders --output rows.csv"
    ),
    "data.sample": (
        "maxc data sample default.orders --rows 10\n"
        "maxc data sample default.events --partition \"dt='20260101'\" --columns id,name"
    ),
    "data.profile": (
        "maxc data profile default.orders\n"
        "maxc data profile default.events --partition \"dt='20260101'\" --json"
    ),
    "data.upload": (
        "maxc data upload default.orders --file rows.csv --dry-run\n"
        "maxc data upload default.events --file rows.tsv --delimiter $'\\t' --partition \"dt='20260101'\" --create-partition"
    ),
    "data.download": (
        "maxc data download default.orders --output rows.csv\n"
        "maxc data download default.orders --output rows.csv --columns id,name --limit 1000"
    ),
    # ── agent ──────────────────────────────────────────────────────────────
    "agent": (
        "maxc agent context\n"
        "maxc agent skill\n"
        "maxc agent skill install claude-code"
    ),
    "agent.context": "maxc agent context\nmaxc agent context --json",
    "agent.doctor": (
        "maxc agent doctor --json\n"
        "maxc agent doctor --online --json"
    ),
    "agent.manifest": "maxc agent manifest --json",
    "agent.skill": "maxc agent skill\nmaxc agent skill --json",
    "agent.skill.install": (
        "maxc agent skill install claude-code\n"
        "maxc agent skill install cursor --invocation aliyun-maxc\n"
        "maxc agent skill install others --dir /path/to/skills"
    ),
    "agent.skill.update": (
        "maxc agent skill update cursor\n"
        "maxc agent skill update --all"
    ),
    "agent.skill.uninstall": "maxc agent skill uninstall cursor",
    "agent.skill.list": "maxc agent skill list --json",
    "agent.skill.diff": "maxc agent skill diff cursor --unified",
    "agent.skill.path": "maxc agent skill path cursor\nmaxc agent skill path --source",
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
        "maxc cache clear --project my_proj --dry-run\n"
        "maxc cache clear --project my_proj --schema default --force"
    ),
}

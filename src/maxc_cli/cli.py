
from __future__ import annotations

import argparse
import csv
import difflib
import json
import os
import shlex
import sys
from collections.abc import Sequence
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, TextIO

from . import agent_platforms
from ._samples import SAMPLES
from .exceptions import ErrorPayload, MaxCError, ValidationError
from .help_format import AliyunRawTextFormatter, AliyunStyleFormatter
from .models import AgentHints, Envelope, action, suggested_action_is_safe
from .output import emit_json, emit_ndjson, render_error, render_key_values, render_table
from .utils import current_cli_entry_point, extract_table_names, now_utc_iso, read_sql_input

if TYPE_CHECKING:
    from .app import MaxCApp


class _OutputFormatError(ValidationError):
    """The requested presentation cannot represent this command's result."""

    error_code = "OUTPUT_FORMAT_ERROR"
    exit_code = 2
    recoverable = True


def __getattr__(name: str) -> Any:
    """Preserve historical CLI exports while loading their modules on demand."""
    if name in {"MaxCApp", "read_stdin"}:
        from . import app as app_module

        value = getattr(app_module, name)
    elif name == "classify_sql_error":
        from .helpers import classify_sql_error

        value = classify_sql_error
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def positive_int(value: str) -> int:
    """argparse type validator: int > 0.

    Rejects negatives and zero so callers don't silently accept
    e.g. `--limit -5` (PyODPS would either error obscurely or return
    garbage). Raising ArgumentTypeError makes argparse exit 2 with a
    clean message, matching `--limit foo` behavior.
    """
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            f"{value!r} is not a valid integer; must be a positive integer (>= 1)."
        )
    if parsed < 1:
        raise argparse.ArgumentTypeError(
            f"{value!r} must be a positive integer (>= 1); got {parsed}."
        )
    return parsed


def nonneg_int(value: str) -> int:
    """argparse type validator: int >= 0 (counters that allow zero)."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            f"{value!r} is not a valid integer; must be a non-negative integer (>= 0)."
        )
    if parsed < 0:
        raise argparse.ArgumentTypeError(
            f"{value!r} must be a non-negative integer (>= 0); got {parsed}."
        )
    return parsed


def bounded_int(min_value: int, max_value: int):
    """Build an argparse type validator that enforces an inclusive range."""
    def _check(value: str) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            raise argparse.ArgumentTypeError(
                f"{value!r} is not a valid integer; must be between {min_value} and {max_value}."
            )
        if parsed < min_value or parsed > max_value:
            raise argparse.ArgumentTypeError(
                f"{value!r} must be between {min_value} and {max_value}; got {parsed}."
            )
        return parsed
    return _check


def _user_agent_value(value: str) -> str:
    """Validate a caller-supplied User-Agent token before any network use."""
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError("--user-agent cannot be empty.")
    if len(normalized) > 256:
        raise argparse.ArgumentTypeError("--user-agent must be at most 256 characters.")
    if any(ord(char) < 0x20 or ord(char) > 0x7E for char in normalized):
        raise argparse.ArgumentTypeError(
            "--user-agent must contain visible ASCII characters only."
        )
    return normalized


def _epilog_for(command_path: str) -> str | None:
    sample = SAMPLES.get(command_path)
    if sample is None:
        return None
    cli_name = current_cli_entry_point()
    if cli_name != "maxc":
        sample = sample.replace("maxc ", f"{cli_name} ")
    return "Sample:\n  " + sample.replace("\n", "\n  ")


# Global flags that must work in any argv position. Subcommand-local flags
# (e.g., --project, --limit) are NOT hoisted — they belong to specific
# subparsers. arity = number of subsequent argv tokens consumed as a value
# when given as `--flag value`; `--flag=value` is always a single token.
#
# -h/--help is deliberately NOT hoisted: argparse auto-adds it to every
# subparser, and hoisting would turn `maxc query -h` into top-level help
# instead of the query subcommand's help.
_GLOBAL_FLAG_ARITY: dict[str, int] = {
    "--format":  1,
    "-f":        1,
    "--config":  1,
    "--user-agent": 1,
    "--json":    0,
    "-v":        0,
    "--version": 0,
}


def _hoist_global_flags(argv: list[str]) -> list[str]:
    """Move any global flag found after the subcommand to the front of argv.

    Lets agents write `maxc query "..." --json` interchangeably with
    `maxc --json query "..."`. Stops at the POSIX `--` terminator. Unknown
    flags are passed through untouched (they belong to subparsers).
    """
    hoisted: list[str] = []
    rest: list[str] = []
    i = 0
    n = len(argv)
    while i < n:
        token = argv[i]
        if token == "--":
            rest.extend(argv[i:])
            break
        if "=" in token and token.startswith("-"):
            flag = token.split("=", 1)[0]
            if flag in _GLOBAL_FLAG_ARITY:
                hoisted.append(token)
                i += 1
                continue
        if token in _GLOBAL_FLAG_ARITY:
            arity = _GLOBAL_FLAG_ARITY[token]
            hoisted.append(token)
            if arity == 1 and i + 1 < n:
                hoisted.append(argv[i + 1])
                i += 2
                continue
            i += 1
            continue
        rest.append(token)
        i += 1
    return hoisted + rest


def _make_parser(parent_subparsers, name, command_path, **kw):
    """Wrap add_parser so every parser gets aliyun-style formatting."""
    kw.setdefault("formatter_class", AliyunStyleFormatter)
    kw.setdefault("allow_abbrev", False)
    return parent_subparsers.add_parser(name, **kw)


def _add_required_subparsers(
    parser: argparse.ArgumentParser,
    *,
    dest: str,
):
    """Backport-required subparsers for Python 3.6."""
    subparsers = parser.add_subparsers(dest=dest)
    subparsers.required = True
    return subparsers


def build_parser() -> argparse.ArgumentParser:
    # Runtime code and reported version must come from the same source. Using
    # importlib.metadata here can pick up an unrelated older installation when
    # running from a checkout or an Alibaba Cloud CLI bundle.
    from maxc_cli import __version__ as cli_version
    cli_name = current_cli_entry_point()
    parser = argparse.ArgumentParser(
        prog=cli_name,
        formatter_class=AliyunStyleFormatter,
        allow_abbrev=False,
    )
    parser.add_argument("-v", "--version", action="version", version=f"{cli_name} {cli_version}")
    parser.add_argument("--config", help="Explicit path to a config file")
    parser.add_argument(
        "--user-agent",
        type=_user_agent_value,
        help="Append an Agent/Skill identity to MaxCompute request User-Agent",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=["json", "table", "csv", "ndjson", "markdown", "brief"],
        default=None,
        dest="format",
        help="Output format (overrides per-command defaults)",
    )
    # --json is also registered on subparsers for the post-subcommand form,
    # but it lives here too so _hoist_global_flags can safely move it to the
    # front. Both argparse passes write the same dest, so either form works.
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        dest="json",
        help="Shorthand for --format json",
    )

    # Top-level subparsers are NOT required: bare `maxc` is handled in run()
    # (prints help when auth is configured, redirects to `auth login` otherwise).
    # Explicit prog= prevents argparse from calling format_help() to derive it
    # (which would bake our version header into child parser prog strings).
    subparsers = parser.add_subparsers(dest="command_group", prog=cli_name)

    query_parser = _make_parser(
        subparsers,
        "query",
        "query",
        help="Run or inspect a MaxCompute SELECT query",
        description=(
            "Run, estimate, or explain a MaxCompute SELECT query.\n"
            "Usage:\n"
            f"  {cli_name} query \"SELECT 1\"             # default: run\n"
            f"  {cli_name} query run \"SELECT 1\"         # explicit run\n"
            f"  {cli_name} query cost \"SELECT 1\"        # estimate cost\n"
            f"  {cli_name} query explain \"SELECT 1\"     # show plan\n"
            "\n"
            "Legacy usage (--mode is deprecated):\n"
            f"  {cli_name} query \"SELECT 1\" --mode cost"
        ),
        formatter_class=AliyunRawTextFormatter,
    )
    query_parser.add_argument("sql_parts", nargs="*", help="MaxCompute SELECT statement")
    query_parser.add_argument("--file", help="Read SQL from file")
    query_parser.add_argument("--stdin", action="store_true", help="Read SQL from stdin")
    query_parser.add_argument("--project", help="Target MaxCompute project")
    query_parser.add_argument(
        "--mode",
        choices=["run", "cost", "explain"],
        default="run",
        help=argparse.SUPPRESS,  # Deprecated: use subcommand style instead (maxc query cost "SQL")
    )
    query_parser.add_argument("--json", action="store_true", help="Output as JSON envelope")
    query_parser.add_argument("--max-rows", type=positive_int, default=100, help="Maximum rows to return (default: 100)")
    query_parser.add_argument("--page-size", type=positive_int, help="Rows per page for pagination")
    query_parser.add_argument("--cursor", help="Pagination cursor from previous response")
    query_parser.add_argument("--output", help="Write output to file")
    query_parser.add_argument("--output-format", choices=["table", "json", "csv", "ndjson"], help="Output file format")
    query_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing --output file (default: fail without running the query)",
    )
    query_parser.add_argument("--wait", type=nonneg_int, default=10,
                              help="Seconds to poll before promoting to async (default: 10). --wait 0 returns job_id immediately.")
    query_parser.add_argument("--dry-run", action="store_true", help="Show query plan without executing")
    query_parser.add_argument("--cost-check", type=float, help="Abort if estimated cost exceeds threshold (CU)")
    query_parser.add_argument("--idempotency-key", help="Deduplication key for retries")
    # Kept parseable for compatibility, but hidden because resumable remote
    # execution rejects automatic retries to avoid duplicate submissions.
    query_parser.add_argument("--retry-on", default="", help=argparse.SUPPRESS)
    query_parser.add_argument("--max-retries", type=nonneg_int, default=0, help=argparse.SUPPRESS)
    query_parser.add_argument(
        "--retry-backoff",
        choices=["fixed", "exponential"],
        default="fixed",
        help=argparse.SUPPRESS,
    )
    query_parser.add_argument("--mcqa", action="store_true", default=None, help="Run query via MCQA v1")
    query_parser.add_argument("--maxqa", action="store_true", default=False, help="Run query via MCQA v2")
    query_parser.add_argument("--no-mcqa", action="store_true", default=False, help="Disable MCQA for this query")
    query_parser.add_argument("--mcqa-version", choices=["v1", "v2"], help="MCQA version to use")
    query_parser.add_argument("--quota", help="MCQA v2 quota name")
    query_parser.add_argument("--mcqa-fallback", dest="mcqa_fallback", action="store_true", default=None, help="Allow MCQA queries to fall back to offline mode")
    query_parser.add_argument("--no-mcqa-fallback", dest="mcqa_fallback", action="store_false", help="Do not fall back to offline mode when MCQA fails")
    query_parser.add_argument("--force", action="store_true", default=False, help=argparse.SUPPRESS)
    query_parser.set_defaults(handler=_handle_query)

    job_parser = _make_parser(subparsers, "job", "job", help="Manage async jobs")
    job_subparsers = _add_required_subparsers(job_parser, dest="job_command")

    job_submit = _make_parser(
        job_subparsers,
        "submit",
        "job.submit",
        help="Submit a MaxCompute SELECT query as an async job",
    )
    job_submit.add_argument("sql_parts", nargs="*", help="MaxCompute SELECT statement")
    job_submit.add_argument("--file", help="Read SQL from file")
    job_submit.add_argument("--stdin", action="store_true", help="Read SQL from stdin")
    job_submit.add_argument("--project", help="Target MaxCompute project")
    job_submit.add_argument("--json", action="store_true", help="Output as JSON envelope")
    job_submit.add_argument("--max-rows", type=positive_int, default=100, help="Maximum rows to return (default: 100)")
    job_submit.add_argument("--cost-check", type=float, help="Abort if estimated cost exceeds threshold (CU)")
    job_submit.add_argument("--idempotency-key", help="Deduplication key for retries")
    job_submit.add_argument("--dry-run", action="store_true", help="Estimate cost without submitting")
    job_submit.add_argument("--mcqa", action="store_true", default=None, help="Submit the job via MCQA v1")
    job_submit.add_argument("--maxqa", action="store_true", default=False, help="Submit the job via MCQA v2")
    job_submit.add_argument("--no-mcqa", action="store_true", default=False, help="Disable MCQA for this submission")
    job_submit.add_argument("--mcqa-version", choices=["v1", "v2"], help="MCQA version to use")
    job_submit.add_argument("--quota", help="MCQA v2 quota name")
    # Compatibility-only: resumable job submission never performs fallback.
    # Keep the old flags parseable so existing automation gets a typed runtime
    # error for the positive form and a strict no-op for the negative form.
    job_submit.add_argument(
        "--mcqa-fallback",
        dest="mcqa_fallback",
        action="store_true",
        default=None,
        help=argparse.SUPPRESS,
    )
    job_submit.add_argument(
        "--no-mcqa-fallback",
        dest="mcqa_fallback",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    job_submit.add_argument("--force", action="store_true", default=False, help=argparse.SUPPRESS)
    job_submit.set_defaults(handler=_handle_job_submit, mcqa_fallback=None)

    job_status = _make_parser(job_subparsers, "status", "job.status", help="Show job status")
    job_status.add_argument("job_id", help="Job ID returned by submit")
    job_status.add_argument("--project", help="Project that owns the job (uses stored submission context when omitted)")
    job_status.add_argument("--json", action="store_true", help="Output as JSON envelope")
    job_status.set_defaults(handler=_handle_job_status)

    job_wait = _make_parser(job_subparsers, "wait", "job.wait", help="Wait for a job to finish")
    job_wait.add_argument("job_id", help="Job ID returned by submit")
    job_wait.add_argument("--project", help="Project that owns the job (uses stored submission context when omitted)")
    job_wait.add_argument("--json", action="store_true", help="Output as JSON envelope")
    job_wait.add_argument("--stream", action="store_true", help="Emit buffered lifecycle events as NDJSON after the wait completes")
    job_wait.add_argument("--timeout", type=positive_int, default=None, help="Timeout in seconds (default: 300)")
    job_wait.set_defaults(handler=_handle_job_wait)

    job_diagnose = _make_parser(job_subparsers, "diagnose", "job.diagnose", help="Diagnose job status and failure reasons")
    job_diagnose.add_argument("job_id", help="Job ID returned by submit")
    job_diagnose.add_argument("--project", help="Project that owns the job (uses stored submission context when omitted)")
    job_diagnose.add_argument("--json", action="store_true", help="Output as JSON envelope")
    job_diagnose.set_defaults(handler=_handle_job_diagnose)

    job_result = _make_parser(job_subparsers, "result", "job.result", help="Fetch job results")
    job_result.add_argument("job_id", help="Job ID returned by submit")
    job_result.add_argument("--project", help="Project that owns the job (uses stored submission context when omitted)")
    job_result.add_argument("--json", action="store_true", help="Output as JSON envelope")
    job_result.add_argument("--max-rows", type=positive_int, default=100, dest="max_rows", help="Maximum rows to return (default: 100)")
    job_result.add_argument("--cursor", default=None, help="Pagination cursor from previous response")
    job_result.add_argument("--output", help="Write a successful result page to file")
    job_result.add_argument(
        "--output-format",
        choices=["table", "json", "csv", "ndjson"],
        help="Output file format",
    )
    job_result.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing --output file (default: fail before fetching the result)",
    )
    job_result.set_defaults(handler=_handle_job_result)

    job_cancel = _make_parser(job_subparsers, "cancel", "job.cancel", help="Cancel a job")
    job_cancel.add_argument("job_id", help="Job ID returned by submit")
    job_cancel.add_argument("--project", help="Project that owns the job (uses stored submission context when omitted)")
    job_cancel.add_argument("--json", action="store_true", help="Output as JSON envelope")
    job_cancel.set_defaults(handler=_handle_job_cancel)

    job_list = _make_parser(job_subparsers, "list", "job.list", help="List jobs")
    job_list.add_argument("--project", help="Target MaxCompute project")
    job_list.add_argument("--json", action="store_true", help="Output as JSON envelope")
    job_list.add_argument("--limit", type=positive_int, default=20, help="Maximum number of jobs to return (default: 20)")
    job_list.set_defaults(handler=_handle_job_list)

    meta_parser = _make_parser(subparsers, "meta", "meta", help="Metadata commands")
    meta_subparsers = _add_required_subparsers(meta_parser, dest="meta_command")

    meta_list = _make_parser(meta_subparsers, "list-tables", "meta.list-tables", help="List tables")
    meta_list.add_argument("--schema", help="Schema name (overrides session default)")
    meta_list.add_argument("--project", help="Target MaxCompute project")
    meta_list.add_argument(
        "--limit", type=positive_int, default=None,
        help="Maximum tables to return (paginated; default: no limit / cache full list)",
    )
    meta_list.add_argument(
        "--cursor",
        help="Pagination cursor returned by a previous call (offset token)",
    )
    meta_list.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_list.set_defaults(handler=_handle_meta_list_tables)

    meta_describe = _make_parser(meta_subparsers, "describe", "meta.describe", help="Describe a table")
    meta_describe.add_argument("table_name", help="Table name (schema.table or table)")
    meta_describe.add_argument("--schema", help="Schema name (overrides session default)")
    meta_describe.add_argument("--project", help="Target MaxCompute project")
    meta_describe.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_describe.add_argument("--full", action="store_true", help="Show full column list (default is summary mode)")
    meta_describe.set_defaults(handler=_handle_meta_describe)

    meta_search = _make_parser(meta_subparsers, "search", "meta.search", help="Search tables")
    meta_search.add_argument("keyword", help="Search keyword")
    meta_search.add_argument("--schema", help="Schema name (overrides session default)")
    meta_search.add_argument("--project", help="Target MaxCompute project")
    meta_search.add_argument(
        "--limit", type=positive_int, default=20,
        help="Maximum matches to return (default 20)",
    )
    meta_search.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_search.set_defaults(handler=_handle_meta_search)

    meta_search_columns = _make_parser(meta_subparsers, "search-columns", "meta.search-columns", help="Search columns")
    meta_search_columns.add_argument("keyword", help="Search keyword")
    meta_search_columns.add_argument("--schema", help="Schema name (overrides session default)")
    meta_search_columns.add_argument("--project", help="Target MaxCompute project")
    meta_search_columns.add_argument(
        "--limit", type=positive_int, default=20,
        help="Maximum matches to return (default 20)",
    )
    meta_search_columns.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_search_columns.set_defaults(handler=_handle_meta_search_columns)

    meta_latest_partition = _make_parser(meta_subparsers, "latest-partition", "meta.latest-partition", help="Show the latest partition")
    meta_latest_partition.add_argument("table_name", help="Table name (schema.table or table)")
    meta_latest_partition.add_argument("--schema", help="Schema name (overrides session default)")
    meta_latest_partition.add_argument("--project", help="Target MaxCompute project")
    meta_latest_partition.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_latest_partition.set_defaults(handler=_handle_meta_latest_partition)

    meta_freshness = _make_parser(meta_subparsers, "freshness", "meta.freshness", help="Show table freshness")
    meta_freshness.add_argument("table_name", help="Table name (schema.table or table)")
    meta_freshness.add_argument("--schema", help="Schema name (overrides session default)")
    meta_freshness.add_argument("--project", help="Target MaxCompute project")
    meta_freshness.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_freshness.set_defaults(handler=_handle_meta_freshness)

    meta_partitions = _make_parser(meta_subparsers, "partitions", "meta.partitions", help="List partitions")
    meta_partitions.add_argument("table_name", help="Table name (schema.table or table)")
    meta_partitions.add_argument("--schema", help="Schema name (overrides session default)")
    meta_partitions.add_argument("--project", help="Target MaxCompute project")
    meta_partitions.add_argument(
        "--limit", type=positive_int, default=100,
        help="Maximum partitions to return (default 100)",
    )
    meta_partitions.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_partitions.set_defaults(handler=_handle_meta_partitions)

    meta_list_projects = _make_parser(meta_subparsers, "list-projects", "meta.list-projects", help="List accessible projects")
    meta_list_projects.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_list_projects.set_defaults(handler=_handle_meta_list_projects)

    meta_list_schemas = _make_parser(meta_subparsers, "list-schemas", "meta.list-schemas", help="List schemas in a project")
    meta_list_schemas.add_argument("--project", help="Target MaxCompute project")
    meta_list_schemas.add_argument("--json", action="store_true", help="Output as JSON envelope")
    meta_list_schemas.set_defaults(handler=_handle_meta_list_schemas)

    # Semantic metadata subcommands
    meta_semantic = _make_parser(meta_subparsers, "semantic", "meta.semantic", help="Semantic metadata management")
    meta_semantic_subparsers = _add_required_subparsers(
        meta_semantic,
        dest="semantic_command",
    )

    # semantic set
    semantic_set = _make_parser(meta_semantic_subparsers, "set", "meta.semantic.set", help="Set semantic metadata for a table")
    semantic_set.add_argument("table_name", help="Table name")
    semantic_set.add_argument("--project", help="Project that owns the semantic metadata")
    semantic_set.add_argument("--schema", help="Schema that owns the table")
    semantic_set.add_argument("--desc", "--description", dest="semantic_desc", help="Table description")
    semantic_set.add_argument("--use-cases", nargs="*", help="Use cases (space-separated)")
    semantic_set.add_argument("--sample-questions", nargs="*", help="Sample questions (space-separated)")
    semantic_set.add_argument("--column-semantics", type=str, help="Column semantics as JSON string")
    semantic_set.add_argument("--relations", type=str, help="Relations as JSON string")
    semantic_set.add_argument("--stats", type=str, help="Stats as JSON string")
    semantic_set.add_argument("--json", action="store_true", help="Output as JSON envelope")
    semantic_set.set_defaults(handler=_handle_meta_semantic_set)

    # semantic get
    semantic_get = _make_parser(meta_semantic_subparsers, "get", "meta.semantic.get", help="Get semantic metadata for a table")
    semantic_get.add_argument("table_name", help="Table name")
    semantic_get.add_argument("--project", help="Project that owns the semantic metadata")
    semantic_get.add_argument("--schema", help="Schema that owns the table")
    semantic_get.add_argument("--json", action="store_true", help="Output as JSON envelope")
    semantic_get.set_defaults(handler=_handle_meta_semantic_get)

    # semantic list-missing
    semantic_list_missing = _make_parser(meta_semantic_subparsers, "list-missing", "meta.semantic.list-missing", help="List tables without semantic metadata")
    semantic_list_missing.add_argument("--project", help="Project to inspect")
    semantic_list_missing.add_argument("--schema", help="Limit the inspection to one schema")
    semantic_list_missing.add_argument("--json", action="store_true", help="Output as JSON envelope")
    semantic_list_missing.set_defaults(handler=_handle_meta_semantic_list_missing)

    semantic_clear = _make_parser(
        meta_semantic_subparsers,
        "clear",
        "meta.semantic.clear",
        help="Remove local semantic metadata for one table or an explicit scope",
    )
    semantic_clear.add_argument("table_name", nargs="?", help="Table name to clear")
    semantic_clear.add_argument(
        "--all",
        dest="all_semantics",
        action="store_true",
        help="Clear all semantic metadata in the current project (requires --force)",
    )
    semantic_clear.add_argument(
        "--schema",
        help="Limit --all to one schema, or select the table schema",
    )
    semantic_clear.add_argument("--project", help="Project that owns the semantic metadata")
    semantic_clear.add_argument(
        "--force",
        action="store_true",
        help="Confirm a bulk --all clear",
    )
    semantic_clear.add_argument("--json", action="store_true", help="Output as JSON envelope")
    semantic_clear.set_defaults(handler=_handle_meta_semantic_clear)

    session_parser = _make_parser(subparsers, "session", "session", help="Session management - switch project/schema")
    session_subparsers = _add_required_subparsers(
        session_parser,
        dest="session_command",
    )

    session_set = _make_parser(session_subparsers, "set", "session.set", help="Set current project and/or schema for this session")
    session_set.add_argument("--project", help="Project name")
    session_set.add_argument("--schema", help="Schema name")
    session_set.add_argument("--json", action="store_true", help="Output as JSON envelope")
    session_set.set_defaults(handler=_handle_session_set)

    session_show = _make_parser(session_subparsers, "show", "session.show", help="Show current session settings")
    session_show.add_argument("--json", action="store_true", help="Output as JSON envelope")
    session_show.set_defaults(handler=_handle_session_show)

    session_unset = _make_parser(session_subparsers, "unset", "session.unset", help="Clear session override, revert to env/config")
    session_unset.add_argument("--json", action="store_true", help="Output as JSON envelope")
    session_unset.set_defaults(handler=_handle_session_unset)

    data_parser = _make_parser(subparsers, "data", "data", help="Data exploration commands")
    data_subparsers = _add_required_subparsers(data_parser, dest="data_command")

    data_sample = _make_parser(data_subparsers, "sample", "data.sample", help="Sample rows")
    data_sample.add_argument("table_name", help="Table name (schema.table or table)")
    data_sample.add_argument("--rows", type=positive_int, default=5, help="Number of sample rows (default: 5)")
    data_sample.add_argument("--partition", help="Partition specification")
    data_sample.add_argument("--columns", help="Comma-separated column names")
    data_sample.add_argument("--project", help="Target MaxCompute project")
    data_sample.add_argument("--schema", help="Schema name (overrides session default)")
    data_sample.add_argument("--json", action="store_true", help="Output as JSON envelope")
    data_sample.set_defaults(handler=_handle_data_sample)

    data_profile = _make_parser(data_subparsers, "profile", "data.profile", help="Profile table data")
    data_profile.add_argument("table_name", help="Table name (schema.table or table)")
    data_profile.add_argument("--partition", help="Partition specification")
    data_profile.add_argument("--project", help="Target MaxCompute project")
    data_profile.add_argument("--schema", help="Schema name (overrides session default)")
    data_profile.add_argument("--json", action="store_true", help="Output as JSON envelope")
    data_profile.set_defaults(handler=_handle_data_profile)

    data_upload = _make_parser(data_subparsers, "upload", "data.upload", help="Upload a CSV/TSV file into a table")
    data_upload.add_argument("table_name", help="Table name (schema.table or table)")
    data_upload.add_argument("--file", required=True, help="Path to local CSV/TSV file")
    data_upload.add_argument("--partition", help="Partition spec, e.g. ds=20260508")
    data_upload.add_argument(
        "--create-partition",
        action="store_true",
        help="Explicitly allow Tunnel to create the partition when it is missing",
    )
    data_upload.add_argument("--overwrite", action="store_true",
                             help="Use INSERT OVERWRITE semantics for the partition/table")
    data_upload.add_argument("--delimiter", default=",", help="Field delimiter (default: ,)")
    data_upload.add_argument("--no-header", dest="has_header", action="store_false",
                             default=True, help="Treat the first row as data, not header")
    data_upload.add_argument("--null-marker", default=r"\N",
                             help=r"Token interpreted as SQL NULL (default: \N)")
    data_upload.add_argument("--schema", help="Schema name (overrides session default)")
    data_upload.add_argument("--block-size", type=positive_int, default=10000,
                             help="Rows per Tunnel block (default: 10000)")
    data_upload.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Validate table schema, CSV row widths, and mapped value types "
            "without creating an upload session"
        ),
    )
    data_upload.add_argument("--project", help="Target MaxCompute project")
    data_upload.add_argument("--json", action="store_true", help="Output as JSON envelope")
    data_upload.set_defaults(handler=_handle_data_upload)

    data_download = _make_parser(data_subparsers, "download", "data.download", help="Download a table/partition to a CSV/TSV file")
    data_download.add_argument("table_name", help="Table name (schema.table or table)")
    data_download.add_argument("--output", required=True, help="Path to local CSV/TSV file to write")
    data_download.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing output file (default: fail if it already exists)",
    )
    data_download.add_argument("--partition", help="Partition spec, e.g. ds=20260508")
    data_download.add_argument("--columns", help="Comma-separated subset of columns")
    data_download.add_argument("--limit", type=positive_int, help="Maximum rows to download")
    data_download.add_argument("--delimiter", default=",", help="Field delimiter (default: ,)")
    data_download.add_argument("--no-header", dest="write_header", action="store_false",
                               default=True, help="Suppress header row in output")
    data_download.add_argument("--null-marker", default="",
                               help='Token written for SQL NULL (default: empty string)')
    data_download.add_argument("--project", help="Target MaxCompute project")
    data_download.add_argument("--schema", help="Schema name (overrides session default)")
    data_download.add_argument("--json", action="store_true", help="Output as JSON envelope")
    data_download.set_defaults(handler=_handle_data_download)

    auth_parser = _make_parser(subparsers, "auth", "auth", help="Authentication and permission checks")
    auth_subparsers = _add_required_subparsers(auth_parser, dest="auth_command")

    auth_login = _make_parser(
        auth_subparsers,
        "login",
        "auth.login",
        help="Authenticate to MaxCompute with OAuth, an access key, or STS",
    )
    auth_login.add_argument("--access-id", "--access-key-id", dest="access_id", help="AccessKey ID")
    auth_login.add_argument(
        "--secret-access-key",
        "--access-key-secret",
        dest="secret_access_key",
        help="AccessKey Secret",
    )
    auth_login.add_argument("--security-token", help="STS security token")
    auth_login.add_argument(
        "--project",
        help="Target MaxCompute project (omit to pop an interactive picker over the catalog)",
    )
    auth_login.add_argument("--endpoint", help="MaxCompute endpoint URL")
    auth_login.add_argument("--region", dest="region_name", help="MaxCompute region name")
    auth_login.add_argument("--tunnel-endpoint", help="Tunnel endpoint URL for data transfer")
    auth_login.add_argument(
        "--catalog-endpoint",
        default=None,
        help="Catalog endpoint URL for the bootstrap ODPS (override for non-China regions)",
    )
    auth_login.add_argument("--from-env", action="store_true", help="Import credentials from environment variables")
    auth_login.add_argument(
        "--oauth",
        action="store_true",
        help=(
            "Authenticate in the browser via Alibaba Cloud OAuth (Authorization Code + PKCE), "
            "equivalent to `aliyun configure --mode OAuth`. Credentials refresh automatically."
        ),
    )
    auth_login.add_argument(
        "--oauth-continuation",
        help=argparse.SUPPRESS,
    )
    auth_login.add_argument(
        "--login-continuation",
        help=argparse.SUPPRESS,
    )
    auth_login.add_argument(
        "--site-type",
        choices=["CN", "INTL"],
        default=None,
        help="OAuth site type (default: CN)",
    )
    auth_login.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open the browser automatically; only print the sign-in URL",
    )
    auth_login.add_argument("--no-validate", action="store_true", help="Skip credential validation")
    auth_login.add_argument(
        "--no-picker",
        action="store_true",
        help="Disable the interactive catalog picker (CI escape hatch)",
    )
    auth_login.add_argument(
        "--reselect",
        action="store_true",
        help=(
            "Force the project picker even when a project is already saved in config "
            "(no effect with --project or --no-picker)."
        ),
    )
    auth_login.add_argument("--json", action="store_true", help="Output as JSON envelope")
    auth_login.set_defaults(handler=_handle_auth_login)

    auth_login_external = _make_parser(
        auth_subparsers,
        "login-external",
        "auth.login-external",
        help="Authenticate to MaxCompute with an external credential helper",
    )
    auth_login_external.add_argument("--process-command", required=True, help="Shell command that outputs credential JSON to stdout")
    auth_login_external.add_argument("--process-timeout", type=bounded_int(1, 600), default=60, help="Timeout in seconds for the external command (default: 60, max: 600)")
    auth_login_external.add_argument("--project", help="Target MaxCompute project")
    auth_login_external.add_argument("--endpoint", help="MaxCompute endpoint URL")
    auth_login_external.add_argument("--region", dest="region_name", help="MaxCompute region name")
    auth_login_external.add_argument("--tunnel-endpoint", help="Tunnel endpoint URL for data transfer")
    auth_login_external.add_argument("--no-validate", action="store_true", help="Skip credential validation")
    auth_login_external.add_argument("--json", action="store_true", help="Output as JSON envelope")
    auth_login_external.set_defaults(handler=_handle_auth_login_external)

    auth_logout = _make_parser(
        auth_subparsers,
        "logout",
        "auth.logout",
        help="Remove saved MaxCompute credentials and cached temporary credentials",
    )
    auth_logout.add_argument("--json", action="store_true", help="Output as JSON envelope")
    auth_logout.set_defaults(handler=_handle_auth_logout)

    auth_whoami = _make_parser(
        auth_subparsers,
        "whoami",
        "auth.whoami",
        help="Validate and show the current MaxCompute identity",
    )
    auth_whoami.add_argument("--json", action="store_true", help="Output as JSON envelope")
    auth_whoami.set_defaults(handler=_handle_auth_whoami)

    _CAN_I_ACTIONS = [
        "Select", "Describe", "Alter", "Update", "Drop", "Download", "All",
        "Read", "Write", "List",
        "CreateTable", "CreateInstance", "CreateFunction", "CreateResource",
        "Execute", "Delete",
    ]
    _CAN_I_ACTIONS_LOWER = {a.lower(): a for a in _CAN_I_ACTIONS}
    _CAN_I_OBJECT_TYPES = ["Table", "Project", "Schema", "Function", "Resource", "Instance"]
    _CAN_I_OBJECT_TYPES_LOWER = {t.lower(): t for t in _CAN_I_OBJECT_TYPES}

    auth_can_i = _make_parser(
        auth_subparsers, "can-i", "auth.can-i",
        help="Check whether current user has a specific permission on an object",
        description=(
            "Check whether the current user has a specific permission on an ODPS object.\n"
            "\n"
            "Examples:\n"
            "  maxc auth can-i --table default.orders --operation Select\n"
            "  maxc auth can-i --table sales.orders --operation Select --project my_proj\n"
            "  maxc auth can-i --object sales --type Schema --operation Describe --project my_proj\n"
            "  maxc auth can-i --table default.orders --operation Update --project other_proj\n"
            "  maxc auth can-i --object my_proj --type Project --operation CreateTable\n"
            "  maxc auth can-i --object my_proj --type Project --operation CreateInstance\n"
        ),
        formatter_class=AliyunRawTextFormatter,
    )
    auth_can_i.add_argument("--object", "--table", required=True, dest="object_name",
                            help="Object name to check (table name, or project name when --type=Project)")
    auth_can_i.add_argument(
        "--type",
        default="Table",
        type=lambda s: _CAN_I_OBJECT_TYPES_LOWER.get(s.lower(), s),
        choices=_CAN_I_OBJECT_TYPES,
        help=(
            "Object type (case-insensitive, default: Table). "
            "Table | Project | Function | Resource | Instance."
        ),
    )
    auth_can_i.add_argument(
        "--operation",
        required=True,
        type=lambda s: _CAN_I_ACTIONS_LOWER.get(s.lower(), s),
        choices=_CAN_I_ACTIONS,
        help=(
            "Permission to check (case-insensitive). "
            "Table: Select, Describe, Alter, Update, Drop, Download, All. "
            "Project: CreateTable, CreateInstance, CreateFunction, CreateResource, List, Read, Write, All. "
            "Function: Read, Write, Delete, Execute, All. "
            "Resource: Read, Write, Delete, All. "
            "Instance: Read, Write, All."
        ),
    )
    auth_can_i.add_argument("--project", help="Project where the object lives (default: current project)")
    auth_can_i.add_argument(
        "--schema",
        help="Schema containing the object (for three-tier projects; may also be supplied as schema.object)",
    )
    auth_can_i.add_argument("--json", action="store_true", help="Output as JSON envelope")
    auth_can_i.set_defaults(handler=_handle_auth_can_i)

    agent_parser = _make_parser(subparsers, "agent", "agent", help="Agent helper commands")
    agent_subparsers = _add_required_subparsers(agent_parser, dest="agent_command")

    agent_context = _make_parser(
        agent_subparsers,
        "context",
        "agent.context",
        help="Inspect local authentication and runtime readiness",
    )
    agent_context.add_argument("--json", action="store_true", help="Output as JSON envelope")
    agent_context.set_defaults(handler=_handle_agent_context)

    agent_doctor = _make_parser(
        agent_subparsers,
        "doctor",
        "agent.doctor",
        help="Check local readiness and optionally verify the live backend",
    )
    agent_doctor.add_argument(
        "--online",
        action="store_true",
        help="Verify credentials and backend reachability with a live identity request",
    )
    agent_doctor.add_argument("--json", action="store_true", help="Output as JSON envelope")
    agent_doctor.set_defaults(handler=_handle_agent_doctor)

    agent_manifest = _make_parser(
        agent_subparsers,
        "manifest",
        "agent.manifest",
        help="Describe the live command, argument, output, and side-effect surface",
    )
    agent_manifest.add_argument("--json", action="store_true", help="Output as JSON envelope")
    agent_manifest.set_defaults(handler=_handle_agent_manifest)

    agent_skill = _make_parser(agent_subparsers, "skill", "agent.skill", help="Show SKILL.md path and metadata")
    agent_skill.add_argument("--json", action="store_true", help="Output as JSON envelope")
    agent_skill.set_defaults(handler=_handle_agent_skill)

    # New six-verb `agent skill {install,update,uninstall,list,diff,path}` block.
    # NOT marked required=True — bare `agent skill --json` keeps falling through
    # to `_handle_agent_skill` above (legacy single-skill envelope). PR #2 will
    # remove the legacy form once SKILL-side migration is done.
    _platform_names = [p.name for p in agent_platforms.REGISTRY]
    _invocation_choices = list(agent_platforms.INVOCATIONS.keys())
    _platform_help = f"Target platform: {', '.join(_platform_names)}"
    _install_invocation_help = (
        f"CLI form in the Skill: {', '.join(_invocation_choices)} "
        "(default: auto-detect)"
    )
    _update_invocation_help = (
        f"Override the installed CLI form with: {', '.join(_invocation_choices)} "
        "(default: preserve each installed Skill's form)"
    )
    agent_skill_sub = agent_skill.add_subparsers(dest="agent_skill_command")

    _ask_install = _make_parser(
        agent_skill_sub, "install", "agent.skill.install",
        help="Install SKILL into the target agent platform's skills directory",
    )
    _ask_install.add_argument(
        "platform",
        nargs="?",
        default="claude-code",
        choices=_platform_names,
        help=f"{_platform_help} (default: claude-code)",
    )
    _ask_install.add_argument("--invocation", default=None, choices=_invocation_choices,
                              help=_install_invocation_help)
    _ask_install.add_argument("--dir", dest="dir_override", default=None,
                              help="Override install directory (skip standard platform path)")
    _ask_install.add_argument("--force", action="store_true",
                              help="Overwrite existing files even when version marker matches")
    _ask_install.add_argument("--json", action="store_true", help="Output as JSON envelope")
    _ask_install.set_defaults(handler=_handle_agent_skill_install)

    _ask_update = _make_parser(
        agent_skill_sub, "update", "agent.skill.update",
        help="Refresh an installed SKILL (re-copy files; bypasses version-marker idempotency)",
    )
    _ask_update.add_argument("platform", nargs="?", default=None, choices=_platform_names,
                             help=f"{_platform_help} (omit with --all)")
    _ask_update.add_argument("--all", dest="all_platforms", action="store_true",
                             help="Update every currently-installed platform")
    _ask_update.add_argument("--invocation", default=None, choices=_invocation_choices,
                             help=_update_invocation_help)
    _ask_update.add_argument("--dir", dest="dir_override", default=None,
                             help="Override install directory (single-platform mode only)")
    _ask_update.add_argument("--json", action="store_true", help="Output as JSON envelope")
    _ask_update.set_defaults(handler=_handle_agent_skill_update)

    _ask_uninstall = _make_parser(
        agent_skill_sub, "uninstall", "agent.skill.uninstall",
        help="Remove the installed SKILL directory for a platform",
    )
    _ask_uninstall.add_argument("platform", choices=_platform_names, help=_platform_help)
    _ask_uninstall.add_argument("--dir", dest="dir_override", default=None,
                                help="Override install directory (uninstall a non-standard location)")
    _ask_uninstall.add_argument("--json", action="store_true", help="Output as JSON envelope")
    _ask_uninstall.set_defaults(handler=_handle_agent_skill_uninstall)

    _ask_list = _make_parser(
        agent_skill_sub, "list", "agent.skill.list",
        help="List platforms that currently have a SKILL installed (standard paths only)",
    )
    _ask_list.add_argument("--json", action="store_true", help="Output as JSON envelope")
    _ask_list.set_defaults(handler=_handle_agent_skill_list)

    _ask_diff = _make_parser(
        agent_skill_sub, "diff", "agent.skill.diff",
        help="Compare an installed SKILL against the wheel-bundled source-of-truth",
    )
    _ask_diff.add_argument("platform", choices=_platform_names, help=_platform_help)
    _ask_diff.add_argument("--unified", action="store_true",
                           help="Include unified-diff text in each `modified` entry (default: kind-only)")
    _ask_diff.add_argument("--dir", dest="dir_override", default=None,
                           help="Override install directory (diff a non-standard install)")
    _ask_diff.add_argument("--json", action="store_true", help="Output as JSON envelope")
    _ask_diff.set_defaults(handler=_handle_agent_skill_diff)

    _ask_path = _make_parser(
        agent_skill_sub, "path", "agent.skill.path",
        help="Print SKILL install path (or --source for the wheel-bundled skills dir)",
    )
    _ask_path.add_argument("platform", nargs="?", default=None, choices=_platform_names,
                           help=f"{_platform_help} (required unless --source is given)")
    _ask_path.add_argument("--dir", dest="dir_override", default=None,
                           help="Override install directory for a non-standard install")
    _ask_path.add_argument("--source", action="store_true",
                           help="Print the wheel-bundled skills dir instead of an install path")
    _ask_path.add_argument("--json", action="store_true", help="Output as JSON envelope")
    _ask_path.set_defaults(handler=_handle_agent_skill_path)

    cache_parser = _make_parser(
        subparsers,
        "cache",
        "cache",
        help="Manage the local metadata cache",
    )
    cache_subparsers = _add_required_subparsers(cache_parser, dest="cache_command")

    cache_build = _make_parser(cache_subparsers, "build", "cache.build", help="Build the metadata cache")
    cache_build.add_argument("--project", help="Target MaxCompute project")
    cache_build.add_argument(
        "--schema",
        help="Target schema name (required when the project uses 3-tier namespaces)",
    )
    cache_build.add_argument(
        "--async",
        dest="async_mode",
        action="store_true",
        help="Deprecated compatibility flag; the build completes synchronously",
    )
    cache_build.add_argument("--json", action="store_true", help="Output as JSON envelope")
    cache_build.set_defaults(handler=_handle_cache_build)

    cache_build_status = _make_parser(cache_subparsers, "build-status", "cache.build-status", help="Show cache build status")
    cache_build_status.add_argument("--project", help="Target MaxCompute project")
    cache_build_status.add_argument("--build-id", help="Build ID")
    cache_build_status.add_argument("--json", action="store_true", help="Output as JSON envelope")
    cache_build_status.set_defaults(handler=_handle_cache_build_status)

    cache_status = _make_parser(cache_subparsers, "status", "cache.status", help="Show cache status")
    cache_status.add_argument("--project", help="Target MaxCompute project")
    cache_status.add_argument("--schema", help="Target schema name")
    cache_status.add_argument("--json", action="store_true", help="Output as JSON envelope")
    cache_status.set_defaults(handler=_handle_cache_status)

    cache_clear = _make_parser(cache_subparsers, "clear", "cache.clear", help="Clear cached metadata")
    cache_clear.add_argument("--project", help="Target MaxCompute project")
    cache_clear.add_argument("--schema", help="Target schema name")
    cache_clear.add_argument("--force", action="store_true", help="Confirm deletion (required to actually delete)")
    cache_clear.add_argument("--dry-run", dest="dry_run", action="store_true", help="Show what would be deleted without deleting")
    cache_clear.add_argument("--json", action="store_true", help="Output as JSON envelope")
    cache_clear.set_defaults(handler=_handle_cache_clear)

    return parser


def _build_error_schema_context(
    app: MaxCApp,
    exc: MaxCError,
    sql: str | None,
) -> dict[str, Any] | None:
    """Build schema context from cache to attach to error envelopes for self-correction."""
    classifier = getattr(sys.modules[__name__], "classify_sql_error")
    classification = classifier(exc.message)
    error_type = classification.get("error_type", "unknown")
    project = app.config.default_project
    schema_name = app.config.default_schema or "default"
    cache = app._read_only_cache()

    def _close_matches(name: str, pool: list[str]) -> list[str]:
        """Return high-confidence fuzzy matches.

        Skip very short queries (3+ chars to participate) and require a
        relatively tight similarity (0.6) so we don't suggest barely-related
        names that confuse rather than help.
        """
        if not name or len(name) < 3 or not pool:
            return []
        return difflib.get_close_matches(name, pool, n=5, cutoff=0.6)

    if error_type == "schema_not_found":
        requested_schema = classification.get("schema_name", "")
        all_schemas = cache.get_schemas(project)
        if all_schemas:
            similar = _close_matches(requested_schema, all_schemas)
            return {
                "context": {"requested_schema": requested_schema},
                "did_you_mean": similar if similar else None,
                "available_schemas": all_schemas[:20],
            }
        return {"context": {"requested_schema": requested_schema}}

    if error_type == "column_not_found":
        # Try to find the table from SQL or from the error message
        table_name = None
        if sql:
            tables = extract_table_names(sql)
            if tables:
                table_name = tables[0]
        if table_name:
            cached = cache.get_cached_table(
                project,
                table_name,
                schema_name=schema_name,
            )
            if cached:
                columns = [c.get("name") for c in cached.get("columns", []) if c.get("name")]
                if columns:
                    requested_column = classification.get("column_name", "")
                    return {
                        "context": {"requested_column": requested_column, "table": table_name},
                        "did_you_mean": _close_matches(requested_column, columns) or None,
                        "available_columns": columns,
                    }
        return None

    if error_type == "table_not_found":
        wrong_table = classification.get("table_name", "")
        all_tables = cache.get_all_cached_tables(
            project,
            schema_name=schema_name,
        )
        if all_tables:
            all_names = [t.get("table_name", "") for t in all_tables if t.get("table_name")]
            # Clean up qualified names (project.schema.table -> table)
            clean_wrong = wrong_table.rsplit(".", 1)[-1] if wrong_table else ""
            similar = _close_matches(clean_wrong, all_names)
            return {
                "context": {"requested_table": wrong_table},
                "did_you_mean": similar if similar else None,
                "available_tables": all_names[:20] if not similar else None,
            }
        return None

    if error_type == "generic_sql_error" and sql:
        tables = extract_table_names(sql)
        if tables:
            table_schemas: dict[str, list[str]] = {}
            for table_name in tables[:5]:
                cached = cache.get_cached_table(
                    project,
                    table_name,
                    schema_name=schema_name,
                )
                if cached:
                    columns = [c.get("name") for c in cached.get("columns", []) if c.get("name")]
                    if columns:
                        table_schemas[table_name] = columns
            if table_schemas:
                return {"table_schemas": table_schemas}
        return None

    return None


def _configure_stdio_encoding() -> None:
    if sys.platform != "win32":
        return
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            pass


def main(argv: Sequence[str] | None = None) -> int:
    _configure_stdio_encoding()
    return run(argv=argv)


def _is_json_mode(args: argparse.Namespace) -> bool:
    """True when the user asked for JSON output, via either --format json or --json."""
    explicit_format = getattr(args, "format", None)
    if explicit_format is not None:
        return explicit_format == "json"
    return bool(getattr(args, "json", False))


def _requested_record_format(args: argparse.Namespace) -> str | None:
    """Return the explicitly requested record-stream format, if any."""
    output_format = getattr(args, "format", None)
    return output_format if output_format in {"csv", "ndjson"} else None


def _argv_requested_output_format(argv: list[str]) -> str | None:
    """Best-effort output-format detection before argparse has a Namespace."""
    explicit_format: str | None = None
    json_requested = False
    for index, token in enumerate(argv):
        if token == "--":
            break
        if token == "--json":
            json_requested = True
        if token.startswith("--format=") or token.startswith("-f="):
            explicit_format = token.split("=", 1)[1]
        if token in {"-f", "--format"} and index + 1 < len(argv):
            explicit_format = argv[index + 1]
    return explicit_format or ("json" if json_requested else None)


def _argument_error_message(stderr_text: str) -> str:
    lines = [line.strip() for line in stderr_text.splitlines() if line.strip()]
    if not lines:
        return "Invalid command line arguments."
    last = lines[-1]
    return last.split(": error:", 1)[-1].strip() if ": error:" in last else last


_REDACTED_ARG_VALUE = "<redacted>"
_SENSITIVE_ARG_FLAGS = frozenset({
    "--access-id",
    "--access-key-id",
    "--access-key-secret",
    "--login-continuation",
    "--oauth-continuation",
    "--secret-access-key",
    "--security-token",
    "--process-command",
})
_SENSITIVE_ARG_NAME_FRAGMENTS = (
    "credential",
    "password",
    "passwd",
    "process-command",
    "process_command",
    "secret",
    "token",
)


def _is_sensitive_arg_flag(flag: str) -> bool:
    """Return whether an option name denotes a credential-bearing value.

    The explicit set covers MaxC's current auth flags. The conservative name
    heuristic also protects JSON argument errors for unknown future/provider
    flags such as ``--password`` or ``--refresh-token``.
    """
    normalized = flag.lower()
    return normalized in _SENSITIVE_ARG_FLAGS or any(
        fragment in normalized for fragment in _SENSITIVE_ARG_NAME_FRAGMENTS
    )


def _redact_sensitive_argv(argv: list[str]) -> tuple[list[str], list[str]]:
    """Redact separated and ``--flag=value`` secrets from argv metadata.

    Returns both the safe argv and the original values so argparse's error
    message can be scrubbed before it is placed in the same public envelope.
    """
    redacted: list[str] = []
    sensitive_values: list[str] = []
    index = 0
    while index < len(argv):
        token = argv[index]
        if token.startswith("--") and "=" in token:
            flag, value = token.split("=", 1)
            if _is_sensitive_arg_flag(flag):
                sensitive_values.append(value)
                redacted.append(f"{flag}={_REDACTED_ARG_VALUE}")
            else:
                redacted.append(token)
            index += 1
            continue
        if token.startswith("--") and _is_sensitive_arg_flag(token):
            redacted.append(token)
            if index + 1 < len(argv):
                sensitive_values.append(argv[index + 1])
                redacted.append(_REDACTED_ARG_VALUE)
                index += 2
            else:
                index += 1
            continue
        redacted.append(token)
        index += 1
    return redacted, sensitive_values


def _redact_sensitive_text(text: str, sensitive_values: list[str]) -> str:
    """Remove credential values that argparse may echo in its error text."""
    for value in sorted(filter(None, sensitive_values), key=len, reverse=True):
        text = text.replace(value, _REDACTED_ARG_VALUE)
    return text


def _emit_argument_error(
    *,
    argv: list[str],
    stderr_text: str,
    stdout: TextIO,
    output_format: str,
) -> None:
    safe_argv, sensitive_values = _redact_sensitive_argv(argv)
    message = _redact_sensitive_text(
        _argument_error_message(stderr_text),
        sensitive_values,
    )
    payload = Envelope(
        command="argument.parse",
        status="failure",
        data={},
        error=ErrorPayload(
            code="ARGUMENT_ERROR",
            message=message,
            suggestion="Run the command with --help to inspect the required arguments and supported flags.",
            recoverable=True,
            exit_code=2,
        ),
        metadata={"argv": safe_argv},
        agent_hints=AgentHints(
            actions=[action("agent.context")],
            warnings=["Argument parsing failed before command execution."],
        ),
    )
    if output_format == "json":
        emit_json(payload.to_dict(), stdout)
    else:
        _emit_record_format(payload, output_format, stdout)


def _build_permission_denied_hints(app: MaxCApp | None) -> AgentHints:
    """Build recovery hints without inventing project naming conventions."""
    project = app.config.default_project if app else None
    metadata = {"project": project} if project else {}
    return AgentHints(
        actions=[
            action("auth.whoami"),
            action("auth.can-i", metadata=metadata),
        ],
        warnings=[
            "Permission denial does not identify an alternative project. "
            "Verify the exact project, schema, object, and operation before changing context."
        ],
    )


def run(
    argv: Sequence[str] | None = None,
    *,
    cwd: Path | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    _auto_login_done: bool = False,
) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    parser = build_parser()
    argv_list = list(argv) if argv is not None else list(sys.argv[1:])
    argv_list = _hoist_global_flags(argv_list)
    preparse_format = _argv_requested_output_format(argv_list)
    if preparse_format in {"json", "csv", "ndjson"}:
        parse_stderr = StringIO()
        try:
            with redirect_stderr(parse_stderr):
                args = parser.parse_args(argv_list)
        except SystemExit as exc:
            code = int(exc.code) if isinstance(exc.code, int) else 1
            if code == 0:
                raise
            _emit_argument_error(
                argv=argv_list,
                stderr_text=parse_stderr.getvalue(),
                stdout=stdout,
                output_format=preparse_format,
            )
            return code
    else:
        args = parser.parse_args(argv_list)
    # argparse subparsers each redeclare --json with default=False, which
    # silently overwrites the value set by the top-level --json. Re-apply
    # post-parse so hoisted `--json` survives the subparser pass.
    if "--json" in argv_list:
        args.json = True
    from .odps_runtime import set_agent_user_agent
    set_agent_user_agent(getattr(args, "user_agent", None))
    working_dir = cwd or Path.cwd()
    requested_config_path = Path(args.config).resolve() if args.config else None
    args.requested_config_path = requested_config_path
    args.stderr = stderr
    command_name = _command_name(args)
    config_path = requested_config_path
    if (
        requested_config_path is not None
        and not requested_config_path.exists()
        and command_name in {"auth.login", "auth.login-external", ""}
    ):
        config_path = None

    app_type = getattr(sys.modules[__name__], "MaxCApp")

    # ── Auto-redirect to `auth login` when auth is missing ─────────────────
    # Bare `maxc` and any non-exempt subcommand both gate on this. Skip when
    # we've already redirected once in this run() chain (recursion guard).
    if not _auto_login_done and command_name not in _AUTO_LOGIN_EXEMPT_COMMANDS:
        try:
            _probe_app = app_type(
                cwd=working_dir,
                config_path=config_path,
                load_backend=False,
            )
        except Exception:
            _probe_app = None

        auth_ok = _probe_app is not None and _auth_seems_configured(_probe_app)

        if not command_name:
            # Bare `maxc`: print help if auth is set up or no TTY for picker.
            if auth_ok or not sys.stdin.isatty():
                parser.print_help(stdout)
                return 0
            # No auth + TTY → fall through to redirect block below.
        else:
            # Subcommand path: only redirect when auth is truly missing AND
            # we have a TTY to drive the picker. Non-TTY keeps the original
            # behavior (the command will fail with VALIDATION_ERROR as today).
            if auth_ok or not sys.stdin.isatty():
                pass  # continue normal execution below
            else:
                cli = current_cli_entry_point()
                stderr.write(
                    "未配置认证 (no auth configured). "
                    f"Running `{cli} auth login --oauth` first, then re-running your command...\n"
                )
                login_argv: list[str] = []
                if requested_config_path is not None:
                    login_argv += ["--config", str(requested_config_path)]
                if getattr(args, "user_agent", None):
                    login_argv += ["--user-agent", args.user_agent]
                login_argv += ["auth", "login", "--oauth"]
                # Capture the login envelope so only an explicit success may
                # trigger the original command. A project-picker ``pending``
                # response also exits zero, but authentication is not saved
                # yet and rerunning here would produce a second contradictory
                # failure envelope.
                login_stdout = StringIO()
                login_code = run(
                    login_argv,
                    cwd=working_dir,
                    stdout=login_stdout,
                    stderr=stderr,
                    _auto_login_done=True,
                )
                login_text = login_stdout.getvalue()
                if login_code != 0:
                    stderr.write(login_text)
                    return login_code
                try:
                    login_payload = json.loads(login_text)
                except (json.JSONDecodeError, TypeError):
                    stderr.write(login_text)
                    stderr.write(
                        "OAuth bootstrap did not return a verifiable success envelope; "
                        "the original command was not run.\n"
                    )
                    return 1
                login_status = login_payload.get("status")
                if login_status == "pending":
                    stdout.write(login_text)
                    return 0
                if login_status != "success":
                    stderr.write(login_text)
                    stderr.write(
                        "OAuth bootstrap did not complete successfully; the original command was not run.\n"
                    )
                    return 1
                # Successful login output is informational. Keep the eventual
                # command's stdout as one clean envelope.
                stderr.write(login_text)
                return run(
                    argv_list,
                    cwd=working_dir,
                    stdout=stdout,
                    stderr=stderr,
                    _auto_login_done=True,
                )

        if not command_name:
            # Bare maxc with no auth + TTY → redirect to auth login
            cli = current_cli_entry_point()
            stderr.write(
                f"未配置认证 (no auth configured). Launching `{cli} auth login --oauth`...\n"
            )
            login_argv = []
            if requested_config_path is not None:
                login_argv += ["--config", str(requested_config_path)]
            if getattr(args, "user_agent", None):
                login_argv += ["--user-agent", args.user_agent]
            login_argv += ["auth", "login", "--oauth"]
            return run(
                login_argv,
                cwd=working_dir,
                stdout=stdout,
                stderr=stderr,
                _auto_login_done=True,
            )

    # If we reach here with no command_group (e.g. exempt path or post-redirect
    # with auth still missing), print help instead of dispatching a None handler.
    if not command_name:
        parser.print_help(stdout)
        return 0

    app: MaxCApp | None = None
    try:
        _validate_output_request(args, command_name)
        app = app_type(
            cwd=working_dir,
            config_path=config_path,
            load_backend=_should_load_backend(command_name),
        )
        args.handler(app, args, stdout)
        return getattr(args, "_envelope_exit_code", 0)
    except MaxCError as exc:
        if app is not None and _should_audit_failure(args):
            app.log(
                _command_name(args),
                "failure",
                {},
                error=exc.to_payload().to_dict(),
            )
        # Derive contextual agent_hints from error code
        _AUTH_HINTS = AgentHints(
            actions=[action("auth.login"), action("auth.login-external")],
        )
        _error_hints: dict[str, AgentHints] = {
            "AUTHENTICATION_ERROR": _AUTH_HINTS,
            "BACKEND_CONNECTION_ERROR": AgentHints(
                actions=[
                    action("agent.doctor", data={"online": True}),
                    action("auth.whoami"),
                ],
                warnings=[
                    "Connectivity failure does not prove that saved credentials are invalid; diagnose before replacing authentication."
                ],
            ),
            "PERMISSION_DENIED": _build_permission_denied_hints(app),
            "NOT_FOUND": AgentHints(
                actions=[action("meta.search"), action("meta.list-tables")],
            ),
            "SCHEMA_NOT_FOUND": AgentHints(
                actions=[action("meta.list-schemas"), action("meta.search")],
            ),
            "TABLE_NOT_FOUND": AgentHints(
                actions=[action("meta.search"), action("meta.list-tables")],
            ),
            "COLUMN_NOT_FOUND": AgentHints(
                actions=[action("meta.describe")],
            ),
            "SQL_ERROR": AgentHints(
                actions=[action("query.cost"), action("query.explain")],
            ),
            "COST_LIMIT_EXCEEDED": AgentHints(
                actions=[action("query.cost")],
            ),
            "JOB_TIMEOUT": AgentHints(
                actions=[action("job.wait"), action("job.status")],
            ),
            "QUOTA_EXCEEDED": AgentHints(
                actions=[action("query.cost")],
            ),
            "READ_ONLY_VIOLATION": AgentHints(
                warnings=["Query rejected: server-side read-only mode blocks DDL/DML operations."],
                actions=[action("query")],
            ),
            "WRITE_OPERATION_REQUIRES_FORCE": AgentHints(
                warnings=[
                    "The public MaxCompute Agent Skill is SELECT-only; do not "
                    "bypass this SQL mutation gate."
                ],
            ),
            "UNSUPPORTED_SQL_OPERATION": AgentHints(
                warnings=[
                    "The public MaxCompute Agent Skill only submits SQL shapes "
                    "that the CLI can prove are read-only."
                ],
            ),
        }
        _hints = _error_hints.get(exc.error_code)
        # Build schema context for SQL errors to enable agent self-correction
        if app is not None and exc.error_code in (
            "SQL_ERROR", "NOT_FOUND", "SCHEMA_NOT_FOUND", "TABLE_NOT_FOUND", "COLUMN_NOT_FOUND",
            "WRITE_OPERATION_REQUIRES_FORCE",
            "UNSUPPORTED_SQL_OPERATION",
        ):
            sql_text = " ".join(getattr(args, "sql_parts", []) or []) or None
            try:
                schema_context = _build_error_schema_context(app, exc, sql_text)
            except Exception:
                schema_context = None  # graceful degradation
            # Promote schema_context onto the exception so it lands at
            # error.context in the envelope — the single canonical place
            # agents read structured failure context from.
            if schema_context and exc.context is None:
                exc.context = schema_context
        payload = Envelope(
            command=_command_name(args),
            status="failure",
            data={},
            error=exc.to_payload(),
            agent_hints=_hints,
        )
        if _is_job_command(args):
            _prepare_job_failure_envelope(payload, args)
        if _is_job_wait_stream(args):
            emit_ndjson([_job_wait_terminal_event(payload)], stdout)
            return exc.exit_code
        if _is_json_mode(args) or _job_uses_default_json(args):
            emit_json(payload.to_dict(), stdout)
        elif (record_format := _requested_record_format(args)) is not None:
            _emit_record_format(payload, record_format, stdout)
        else:
            stderr.write(render_error(exc.error_code, exc.message, exc.suggestion) + "\n")
            if getattr(exc, "instance_id", None):
                stderr.write(f"  Instance ID: {exc.instance_id}\n")
            if getattr(exc, "logview", None):
                stderr.write(f"  LogView: {exc.logview}\n")
        return exc.exit_code
    except Exception as exc:
        error_payload = ErrorPayload(
            code="INTERNAL_ERROR",
            message=str(exc) or type(exc).__name__,
            suggestion="This is an unexpected error. Please report it with the full message.",
            recoverable=False,
        )
        cmd = _command_name(args) if hasattr(args, "handler") else "unknown"
        if app is not None and _should_audit_failure(args):
            app.log(cmd, "failure", {}, error=error_payload.to_dict())
        envelope = Envelope(
            command=cmd,
            status="failure",
            error=error_payload,
        )
        if _is_job_command(args):
            _prepare_job_failure_envelope(envelope, args)
        if _is_job_wait_stream(args):
            emit_ndjson([_job_wait_terminal_event(envelope)], stdout)
            return 1
        if _is_json_mode(args) or _job_uses_default_json(args):
            emit_json(envelope.to_dict(), stdout)
        elif (record_format := _requested_record_format(args)) is not None:
            _emit_record_format(envelope, record_format, stdout)
        else:
            stderr.write(render_error(
                error_payload.code,
                error_payload.message,
                error_payload.suggestion,
            ) + "\n")
        return 1


def _handle_query(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    mode, sql_parts = _resolve_query_mode(args)
    args.resolved_command = "query" if mode == "run" else f"query.{mode}"
    sql = read_sql_input(
        sql_parts,
        file_path=args.file,
        use_stdin=args.stdin,
        stdin_text=getattr(sys.modules[__name__], "read_stdin")() if args.stdin else None,
    )
    output_path: Path | None = None
    if mode == "cost":
        _validate_query_analysis_args(args, mode)
        envelope = app.query_cost(sql=sql, project=args.project, force=args.force)
    elif mode == "explain":
        _validate_query_analysis_args(args, mode)
        envelope = app.query_explain(sql=sql, project=args.project, force=args.force)
    else:
        # Validate the publication target before the query can submit remote
        # work. Analysis modes reject --output and therefore must not create a
        # local directory or preflight file on their error path.
        if args.output:
            output_path = _prepare_output_path(
                args.output,
                overwrite=args.overwrite,
            )
        retry_on = [item.strip() for item in args.retry_on.split(",") if item.strip()]
        if retry_on or args.max_retries or args.retry_backoff != "fixed":
            raise ValidationError(
                "Automatic query retry flags are not supported by resumable remote execution.",
                suggestion=(
                    "Remove --retry-on, --max-retries, and --retry-backoff. "
                    "Use the returned job_id to inspect the original submission before deciding whether to retry."
                ),
            )
        envelope = app.query(
            command="query",
            sql=sql,
            project=args.project,
            max_rows=_query_page_size(args),
            cursor=args.cursor,
            dry_run=args.dry_run,
            wait=args.wait,
            cost_check=args.cost_check,
            idempotency_key=args.idempotency_key,
            retry_on=retry_on,
            max_retries=args.max_retries,
            force=args.force,
            mcqa=args.mcqa,
            maxqa=args.maxqa,
            no_mcqa=args.no_mcqa,
            mcqa_version=args.mcqa_version,
            quota=args.quota,
            mcqa_fallback=args.mcqa_fallback,
        )
    if output_path is not None:
        output_format = _query_output_format(args)
        publication_failed = _publish_result_output(
            envelope,
            output_path,
            output_format,
            overwrite=args.overwrite,
            max_rows=_query_page_size(args),
            operation="query",
        )
        if publication_failed:
            app.log(
                envelope.command,
                envelope.status,
                envelope.metadata,
                error=envelope.error.to_dict() if envelope.error else None,
            )
    _emit_envelope(
        envelope,
        args=args,
        stdout=stdout,
        default_format=_query_default_format(app, mode),
    )


def _handle_job_submit(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    sql = read_sql_input(
        args.sql_parts,
        file_path=args.file,
        use_stdin=args.stdin,
        stdin_text=getattr(sys.modules[__name__], "read_stdin")() if args.stdin else None,
    )
    envelope = app.submit_job(
        sql=sql,
        project=args.project,
        max_rows=args.max_rows,
        cost_check=args.cost_check,
        idempotency_key=args.idempotency_key,
        force=args.force,
        dry_run=args.dry_run,
        mcqa=args.mcqa,
        maxqa=args.maxqa,
        no_mcqa=args.no_mcqa,
        mcqa_version=args.mcqa_version,
        quota=args.quota,
        mcqa_fallback=args.mcqa_fallback,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_status(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.job_status(args.job_id, project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_wait(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope, events = app.job_wait(args.job_id, timeout=args.timeout, project=args.project)
    if args.stream:
        _set_envelope_exit_code(envelope, args)
        # Backend lifecycle events are advisory. Always end the stream with a
        # self-contained terminal record derived from the authoritative
        # Envelope, replacing any abbreviated terminal event returned by a
        # backend adapter.
        if events and events[-1].get("type") in {
            "completed",
            "failed",
            "cancelled",
            "pending",
            "unknown",
        }:
            events = events[:-1]
        events.append(_job_wait_terminal_event(envelope))
        emit_ndjson(events, stdout)
        return
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _job_wait_terminal_event(envelope: Envelope) -> dict[str, Any]:
    """Build a self-contained terminal NDJSON event for silent wait outcomes.

    Remote timeout and connection-error paths return no progress events. A
    stream consumer must still receive an unambiguous final record instead of
    interpreting empty stdout as success.
    """
    payload = envelope.to_dict()
    event_type = {
        "failure": "failed",
        "pending": "pending",
        "success": "completed",
    }.get(envelope.status, envelope.status)
    data = payload.get("data") or {}
    nested_job = data.get("job") if isinstance(data, dict) else None
    job_status = None
    if isinstance(nested_job, dict):
        job_status = str(nested_job.get("status") or "").lower()
    if job_status == "cancelled":
        event_type = "cancelled"
    elif job_status in {"failure", "failed"}:
        event_type = "failed"
    elif job_status and job_status not in {
        "pending",
        "queued",
        "running",
        "suspended",
        "cancel_requested",
        "success",
        "completed",
    }:
        event_type = "unknown"
    job_id = envelope.metadata.get("job_id")
    if job_id is None and isinstance(nested_job, dict):
        job_id = nested_job.get("job_id")
    if job_id is None and isinstance(envelope.data, dict):
        job_id = envelope.data.get("job_id")
    event: dict[str, Any] = {
        "type": event_type,
        "ts": now_utc_iso(),
        "status": envelope.status,
        "job_id": job_id,
        "data": data,
        "metadata": payload.get("metadata") or {},
        "error": payload.get("error"),
        "agent_hints": payload.get("agent_hints"),
    }
    return event


def _is_job_wait_stream(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "stream", False)
        and _command_name(args) == "job.wait"
    )


def _is_job_command(args: argparse.Namespace) -> bool:
    return _command_name(args).startswith("job.")


def _job_uses_default_json(args: argparse.Namespace) -> bool:
    return _is_job_command(args) and getattr(args, "format", None) is None


def _prepare_job_failure_envelope(
    envelope: Envelope,
    args: argparse.Namespace,
) -> None:
    """Attach exact job scope and command-family recovery to early failures."""
    job_id = getattr(args, "job_id", None)
    project = getattr(args, "project", None)
    envelope.data = {"job_id": job_id} if job_id else {}
    envelope.metadata = {
        key: value
        for key, value in {
            "job_id": job_id,
            "project": project,
            "logview": getattr(envelope.error, "logview", None),
        }.items()
        if value is not None
    }
    metadata = {"job_id": job_id, "project": project}
    error_code = getattr(envelope.error, "code", None)
    warnings = list(getattr(envelope.agent_hints, "warnings", []) or [])
    if error_code == "NOT_FOUND":
        actions = [action("job.list", metadata=metadata)]
        if envelope.error is not None:
            cli = current_cli_entry_point()
            project_flag = f" --project {shlex.quote(project)}" if project else ""
            envelope.error.recovery_steps = [
                f"List visible jobs in the same scope: {cli} job list{project_flag} --json",
                "Verify the job ID and project from the original submission response.",
            ]
    else:
        actions = [
            action(name, data={"job_id": job_id}, metadata=metadata)
            for name in ("job.status", "job.wait", "job.diagnose")
            if job_id
        ]
    envelope.agent_hints = AgentHints(actions=actions, warnings=warnings)


def _handle_job_diagnose(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.job_diagnose(args.job_id, project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_result(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    output_path = (
        _prepare_output_path(args.output, overwrite=args.overwrite)
        if args.output
        else None
    )
    envelope = app.job_result(
        args.job_id,
        max_rows=args.max_rows,
        cursor=args.cursor,
        project=args.project,
    )
    if output_path is not None:
        publication_failed = _publish_result_output(
            envelope,
            output_path,
            _query_output_format(args),
            overwrite=args.overwrite,
            max_rows=args.max_rows,
            operation="job result",
        )
        if publication_failed:
            app.log(
                envelope.command,
                envelope.status,
                envelope.metadata,
                error=envelope.error.to_dict() if envelope.error else None,
            )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_cancel(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cancel_job(args.job_id, project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_list(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.list_jobs(limit=args.limit, project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_list_tables(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    schema = getattr(args, "schema", None)
    envelope = app.meta_list_tables(
        schema=schema,
        project=args.project,
        limit=getattr(args, "limit", None),
        cursor=getattr(args, "cursor", None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_describe(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    # When --json is used, always return full schema (agents need all columns)
    full = args.full or _is_json_mode(args)
    envelope = app.meta_describe(
        args.table_name,
        full=full,
        project=args.project,
        schema=getattr(args, "schema", None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_search(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    schema = getattr(args, "schema", None)
    envelope = app.meta_search(
        args.keyword,
        schema=schema,
        project=args.project,
        limit=getattr(args, "limit", 20),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_search_columns(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    schema = getattr(args, "schema", None)
    envelope = app.meta_search_columns(
        args.keyword,
        schema=schema,
        project=args.project,
        limit=getattr(args, "limit", 20),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_latest_partition(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_latest_partition(
        args.table_name,
        project=args.project,
        schema=getattr(args, "schema", None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_freshness(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_freshness(
        args.table_name,
        project=args.project,
        schema=getattr(args, "schema", None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_partitions(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_partitions(
        args.table_name,
        project=args.project,
        limit=getattr(args, "limit", 100),
        schema=getattr(args, "schema", None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_list_projects(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_list_projects()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_list_schemas(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_list_schemas(project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_semantic_set(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    """Handle semantic set command."""
    import json
    
    # Parse JSON arguments if provided
    column_semantics = None
    if args.column_semantics:
        try:
            column_semantics = json.loads(args.column_semantics)
        except json.JSONDecodeError as e:
            envelope = Envelope(
                command="meta.semantic.set",
                status="failure",
                data=None,
                metadata={},
                error=ErrorPayload(
                    code="INVALID_JSON",
                    message=f"Invalid JSON for --column-semantics: {e}",
                    recoverable=True,
                    suggestion="Provide valid JSON for the --column-semantics argument.",
                ),
            )
            _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
            return

    relations = None
    if args.relations:
        try:
            relations = json.loads(args.relations)
        except json.JSONDecodeError as e:
            envelope = Envelope(
                command="meta.semantic.set",
                status="failure",
                data=None,
                metadata={},
                error=ErrorPayload(
                    code="INVALID_JSON",
                    message=f"Invalid JSON for --relations: {e}",
                    recoverable=True,
                    suggestion="Provide valid JSON for the --relations argument.",
                ),
            )
            _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
            return

    stats = None
    if args.stats:
        try:
            stats = json.loads(args.stats)
        except json.JSONDecodeError as e:
            envelope = Envelope(
                command="meta.semantic.set",
                status="failure",
                data=None,
                metadata={},
                error=ErrorPayload(
                    code="INVALID_JSON",
                    message=f"Invalid JSON for --stats: {e}",
                    recoverable=True,
                    suggestion="Provide valid JSON for the --stats argument.",
                ),
            )
            _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
            return

    envelope = app.semantic_set(
        table_name=args.table_name,
        project=args.project,
        schema=args.schema,
        semantic_desc=args.semantic_desc,
        use_cases=args.use_cases,
        sample_questions=args.sample_questions,
        column_semantics=column_semantics,
        relations=relations,
        stats=stats,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_semantic_get(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    """Handle semantic get command."""
    envelope = app.semantic_get(
        table_name=args.table_name,
        project=args.project,
        schema=args.schema,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_semantic_list_missing(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    """Handle semantic list-missing command."""
    envelope = app.semantic_list_missing(project=args.project, schema=args.schema)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_semantic_clear(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    if bool(args.table_name) == bool(args.all_semantics):
        raise ValidationError(
            "Provide exactly one table name or --all.",
            suggestion="Use `meta semantic clear <table>`, or `meta semantic clear --all --force`.",
        )
    if args.all_semantics and not args.force:
        raise ValidationError(
            "`meta semantic clear --all` requires --force.",
            suggestion="Inspect the active project/schema, then add --force only if the bulk clear is intended.",
        )
    envelope = app.semantic_clear(
        table_name=args.table_name,
        schema_name=args.schema,
        project=args.project,
        all_semantics=args.all_semantics,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_session_set(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    """Set current project and/or schema for the session."""
    project = args.project
    schema = args.schema

    if not project and not schema:
        raise ValidationError("At least one of `--project` or `--schema` must be specified.")

    envelope = app.session_set(
        project=project,
        schema=schema,
        target_config_path=args.requested_config_path,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_session_show(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    """Show current session settings."""
    envelope = app.session_show(target_config_path=args.requested_config_path)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_session_unset(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    """Clear session override."""
    envelope = app.session_unset(target_config_path=args.requested_config_path)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_data_sample(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    columns = _csv_arg_list(args.columns)
    envelope = app.data_sample(
        args.table_name,
        rows=args.rows,
        partition=args.partition,
        columns=columns or None,
        project=args.project,
        schema=getattr(args, "schema", None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_data_profile(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.data_profile(
        args.table_name,
        partition=args.partition,
        project=args.project,
        schema=getattr(args, "schema", None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_data_upload(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.data_upload(
        args.table_name,
        args.file,
        partition=args.partition,
        create_partition=args.create_partition,
        overwrite=args.overwrite,
        delimiter=args.delimiter,
        has_header=args.has_header,
        null_marker=args.null_marker,
        block_size=args.block_size,
        project=args.project,
        schema=getattr(args, "schema", None),
        dry_run=args.dry_run,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_data_download(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    columns = _csv_arg_list(args.columns)
    envelope = app.data_download(
        args.table_name,
        args.output,
        overwrite=args.overwrite,
        partition=args.partition,
        columns=columns or None,
        limit=args.limit,
        delimiter=args.delimiter,
        write_header=args.write_header,
        null_marker=args.null_marker,
        project=args.project,
        schema=getattr(args, "schema", None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_login(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    # Reject empty-string flags up front. Without this, `_resolve_login_value`
    # silently falls through to env/existing config, which masks user typos like
    # `--access-id ""` (e.g. unset shell variable in a wrapper script).
    for flag, value in (
        ("--access-id", args.access_id),
        ("--secret-access-key", args.secret_access_key),
        ("--security-token", args.security_token),
        ("--project", args.project),
        ("--endpoint", args.endpoint),
        ("--region", args.region_name),
        ("--tunnel-endpoint", args.tunnel_endpoint),
    ):
        if value is not None and value.strip() == "":
            raise ValidationError(
                f"`{flag}` cannot be empty.",
                suggestion=f"Either omit `{flag}` to fall back to environment/config, or pass a non-empty value.",
            )
    explicit_credentials = any(
        value is not None
        for value in (
            args.access_id,
            args.secret_access_key,
            args.security_token,
        )
    )
    if args.oauth:
        if args.from_env or explicit_credentials:
            raise ValidationError(
                "`--oauth` cannot be combined with AccessKey credential flags or `--from-env`.",
                suggestion=(
                    "Use OAuth by itself, or remove --oauth to import explicit/environment credentials."
                ),
            )
        if args.login_continuation:
            raise ValidationError(
                "`--login-continuation` cannot be combined with `--oauth`.",
                suggestion="Run the exact project-selection action returned by auth login.",
            )
        # Always publish the URL to stderr before waiting. This is required for
        # --no-browser and also provides a reliable fallback when opening the
        # browser fails. stdout remains a clean single Envelope.
        def url_sink(url: str) -> None:
            print(f"Sign-in URL: {url}", file=args.stderr)

        envelope = app.auth_login_oauth(
            site_type=args.site_type or "CN",
            no_browser=args.no_browser,
            on_url=url_sink,
            project=args.project,
            endpoint=args.endpoint,
            region_name=args.region_name,
            tunnel_endpoint=args.tunnel_endpoint,
            catalog_endpoint=args.catalog_endpoint,
            no_validate=args.no_validate,
            target_config_path=args.requested_config_path,
            no_picker=args.no_picker,
            reselect=args.reselect,
            continuation_id=args.oauth_continuation,
        )
        _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
        return
    if args.no_browser or args.site_type is not None:
        incompatible = "--no-browser" if args.no_browser else "--site-type"
        raise ValidationError(
            f"`{incompatible}` is only valid with `--oauth`.",
            suggestion=f"Add --oauth or remove {incompatible}.",
        )
    if args.oauth_continuation:
        raise ValidationError(
            "`--oauth-continuation` requires `--oauth`.",
            suggestion="Run the complete project-selection action returned by auth login.",
        )
    if args.login_continuation and (
        args.access_id
        or args.secret_access_key
        or args.security_token
        or args.from_env
    ):
        raise ValidationError(
            "Do not combine `--login-continuation` with credential flags or --from-env.",
            suggestion="Run the exact project-selection action returned by auth login.",
        )
    envelope = app.auth_login(
        access_id=args.access_id,
        secret_access_key=args.secret_access_key,
        security_token=args.security_token,
        project=args.project,
        endpoint=args.endpoint,
        region_name=args.region_name,
        tunnel_endpoint=args.tunnel_endpoint,
        from_env=args.from_env,
        no_validate=args.no_validate,
        target_config_path=args.requested_config_path,
        catalog_endpoint=args.catalog_endpoint,
        no_picker=args.no_picker,
        reselect=args.reselect,
        continuation_id=args.login_continuation,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_login_external(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.auth_login_external(
        process_command=args.process_command,
        process_timeout=args.process_timeout,
        project=args.project,
        endpoint=args.endpoint,
        region_name=args.region_name,
        tunnel_endpoint=args.tunnel_endpoint,
        no_validate=args.no_validate,
        target_config_path=args.requested_config_path,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_logout(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.auth_logout(target_config_path=args.requested_config_path)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_whoami(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.auth_whoami()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_can_i(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.auth_can_i(
        object_name=args.object_name,
        object_type=args.type,
        operation=args.operation,
        project=args.project,
        schema=args.schema,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_context(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.agent_context()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_doctor(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.agent_doctor(online=args.online)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_manifest(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    _ = app
    envelope = Envelope(
        command="agent.manifest",
        status="success",
        data=_command_manifest(build_parser()),
        agent_hints=AgentHints(
            insights=["This manifest is generated from the live argparse command tree."],
        ),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


_MANIFEST_LOCAL_NO_AUTH_COMMANDS = frozenset({
    "agent.context",
    "agent.manifest",
    "agent.skill",
    "agent.skill.diff",
    "agent.skill.install",
    "agent.skill.list",
    "agent.skill.path",
    "agent.skill.uninstall",
    "agent.skill.update",
    "auth.logout",
    "cache.build-status",
    "cache.clear",
    "cache.status",
    "meta.semantic.clear",
    "meta.semantic.get",
    "meta.semantic.list-missing",
    "meta.semantic.set",
    "session.set",
    "session.show",
    "session.unset",
})
_MANIFEST_CONDITIONAL_NETWORK_COMMANDS = frozenset({
    "agent.doctor",
    "auth.login",
    "auth.login-external",
    "auth.whoami",
})
_MANIFEST_JOB_FOLLOWUP_COMMANDS = frozenset({
    "job.cancel",
    "job.diagnose",
    "job.result",
    "job.status",
    "job.wait",
})


def _manifest_condition(
    arg: str,
    *,
    equals: Any | None = None,
    present: bool | None = None,
) -> dict[str, Any]:
    condition: dict[str, Any] = {"arg": arg}
    if present is not None:
        condition["present"] = present
    else:
        condition["equals"] = equals
    return condition


def _manifest_effect(
    scope: str,
    kind: str,
    target: str,
    *,
    when: dict[str, Any] | None = None,
    agent_allowed: bool = True,
    confirmation: str | None = None,
    best_effort: bool = False,
    note: str | None = None,
) -> dict[str, Any]:
    effect: dict[str, Any] = {
        "scope": scope,
        "kind": kind,
        "target": target,
        "agent_allowed": agent_allowed,
    }
    if when is not None:
        effect["when"] = when
    if confirmation is not None:
        effect["confirmation"] = confirmation
    if best_effort:
        effect["best_effort"] = True
    if note:
        effect["note"] = note
    return effect


_MANIFEST_CONFIRMED_NO_AUDIT_COMMANDS = frozenset({
    "agent.context",
    "agent.manifest",
})


def _with_manifest_audit_effect(
    command: str,
    effects: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Make best-effort audit writes visible at command granularity."""
    if command in _MANIFEST_CONFIRMED_NO_AUDIT_COMMANDS:
        return effects
    if any(effect.get("target") == "audit_log" for effect in effects):
        return effects
    return [
        *effects,
        _manifest_effect(
            "local",
            "append",
            "audit_log",
            when={
                "runtime": "handler_calls_log_or_post_construction_failure"
            },
            best_effort=True,
            note=(
                "The audit append is best-effort and sanitized; it may occur on "
                "the normal handler path or after application construction fails. "
                "If local result publication fails after a remote success record, "
                "a later failure record with the same invocation_id is authoritative."
            ),
        ),
    ]


def _manifest_requirements(command: str) -> dict[str, Any]:
    if command == "auth.login":
        return {
            "network": {
                "mode": "conditional",
                "rules": [
                    {
                        "when": {
                            "all": [
                                _manifest_condition("oauth", equals=True),
                                _manifest_condition(
                                    "oauth_continuation",
                                    present=False,
                                ),
                            ]
                        },
                        "mode": "required",
                        "reason": "A new OAuth authorization and STS exchange use network APIs.",
                    },
                    {
                        "when": _manifest_condition("no_validate", equals=False),
                        "mode": "required",
                        "reason": "The saved identity is remotely validated by default.",
                    },
                    {
                        "when": {"runtime": "project_picker_needs_catalog"},
                        "mode": "required",
                        "reason": "Project discovery uses the Catalog API.",
                    },
                ],
            },
            "credentials": {
                "mode": "candidate",
                "sources": [
                    "oauth",
                    "arguments",
                    "environment",
                    "existing_config",
                    "owner_only_continuation",
                ],
            },
        }
    if command == "auth.login-external":
        return {
            "network": {
                "mode": "conditional",
                "rules": [
                    {
                        "when": _manifest_condition("no_validate", equals=False),
                        "mode": "required",
                        "reason": "External credentials are remotely validated by default.",
                    }
                ],
            },
            "credentials": {
                "mode": "candidate",
                "sources": ["external_process"],
            },
        }
    if command == "auth.whoami":
        return {
            "network": {
                "mode": "conditional",
                "rules": [
                    {
                        "when": {"runtime": "complete_auth_candidate_exists"},
                        "mode": "required",
                        "reason": "Validates the configured identity and may refresh OAuth credentials.",
                    }
                ],
            },
            "credentials": {"mode": "optional"},
        }
    if command == "agent.doctor":
        return {
            "network": {
                "mode": "conditional",
                "rules": [
                    {
                        "when": {
                            "all": [
                                _manifest_condition("online", equals=True),
                                {"runtime": "complete_auth_candidate_exists"},
                            ]
                        },
                        "mode": "required",
                        "reason": "--online validates a complete configured identity against MaxCompute.",
                    }
                ],
            },
            "credentials": {
                "mode": "optional",
                "rules": [
                    {
                        "when": {
                            "all": [
                                _manifest_condition("online", equals=True),
                                {"runtime": "complete_auth_candidate_exists"},
                            ]
                        },
                        "mode": "required",
                    }
                ],
            },
        }
    if command in _MANIFEST_LOCAL_NO_AUTH_COMMANDS:
        return {
            "network": {"mode": "none", "rules": []},
            "credentials": {"mode": "none"},
        }
    return {
        "network": {"mode": "required", "rules": []},
        "credentials": {"mode": "required"},
    }


def _manifest_effects(command: str) -> list[dict[str, Any]]:
    output_preflight_effects = [
        _manifest_effect(
            "local",
            "create",
            "output_parent_directories",
            when=_manifest_condition("output", present=True),
            note=(
                "Missing parent directories are created before remote work so an "
                "unwritable publication target fails early. They can remain when "
                "the command is pending or fails after preflight."
            ),
        ),
        _manifest_effect(
            "local",
            "create",
            "output_preflight_probe",
            when=_manifest_condition("output", present=True),
            note="A temporary file verifies that the output directory is writable.",
        ),
        _manifest_effect(
            "local",
            "delete",
            "output_preflight_probe",
            when=_manifest_condition("output", present=True),
            note="The CLI removes its temporary output preflight file before remote work.",
        ),
    ]
    job_followup_effects = [
        _manifest_effect(
            "local",
            "create_or_open",
            "job_state_lock",
            note=(
                "Job follow-up resolution takes a local shared lock; the lock "
                "file may be created on its first read."
            ),
        ),
        _manifest_effect(
            "local",
            "read",
            "job_followup_context",
            note=(
                "An explicit --project lets remote follow-up continue when optional "
                "local routing context is unavailable."
            ),
        ),
    ]
    effects_by_command: dict[str, list[dict[str, Any]]] = {
        "agent.context": [
            _manifest_effect("local", "read", "config_and_runtime_readiness"),
        ],
        "agent.doctor": [
            _manifest_effect("local", "read", "runtime_readiness"),
            _manifest_effect(
                "remote",
                "read",
                "maxcompute_identity",
                when={
                    "all": [
                        _manifest_condition("online", equals=True),
                        {"runtime": "complete_auth_candidate_exists"},
                    ]
                },
            ),
            _manifest_effect(
                "local",
                "append",
                "audit_log",
                when=_manifest_condition("online", equals=True),
                best_effort=True,
            ),
        ],
        "agent.manifest": [
            _manifest_effect("local", "read", "live_parser_tree"),
        ],
        "agent.skill": [
            _manifest_effect("local", "read", "bundled_skill"),
        ],
        "agent.skill.path": [
            _manifest_effect("local", "read", "installed_skill_path"),
        ],
        "agent.skill.list": [
            _manifest_effect("local", "read", "installed_skills"),
        ],
        "agent.skill.diff": [
            _manifest_effect("local", "read", "installed_and_bundled_skill"),
        ],
        "agent.skill.install": [
            _manifest_effect("local", "create_or_replace", "agent_skill_files"),
        ],
        "agent.skill.update": [
            _manifest_effect("local", "replace", "agent_skill_files"),
        ],
        "agent.skill.uninstall": [
            _manifest_effect(
                "local",
                "delete",
                "agent_skill_files",
                confirmation="explicit_command",
            ),
        ],
        "auth.login": [
            _manifest_effect(
                "remote",
                "authenticate",
                "oauth_catalog_and_maxcompute",
                when={"runtime": "oauth_or_catalog_or_remote_validation"},
            ),
            _manifest_effect(
                "local",
                "create_or_replace",
                "auth_config",
                when={"runtime": "login_succeeds"},
            ),
            _manifest_effect(
                "local",
                "create",
                "owner_only_access_key_continuation",
                when={"runtime": "access_key_project_selection_pending"},
            ),
            _manifest_effect(
                "local",
                "create",
                "owner_only_oauth_continuation",
                when={"runtime": "oauth_project_selection_pending"},
            ),
            _manifest_effect(
                "local",
                "delete",
                "owner_only_auth_continuation",
                when={"runtime": "continuation_resume_claimed"},
                note=(
                    "AccessKey and OAuth continuation bearer state is single-use; "
                    "a resume atomically claims it before completing login."
                ),
            ),
        ],
        "auth.login-external": [
            _manifest_effect(
                "local",
                "execute_process",
                "credential_helper",
                when=_manifest_condition("no_validate", equals=False),
            ),
            _manifest_effect(
                "remote",
                "authenticate",
                "maxcompute_identity",
                when=_manifest_condition("no_validate", equals=False),
            ),
            _manifest_effect(
                "local",
                "create_or_replace",
                "auth_config",
                when={"runtime": "login_succeeds"},
            ),
            _manifest_effect(
                "local",
                "create_or_open",
                "metadata_cache",
                when={"runtime": "login_succeeds"},
                best_effort=True,
                note="Cache-readiness metadata is best-effort enrichment after config commit.",
            ),
        ],
        "auth.logout": [
            _manifest_effect(
                "local",
                "delete_fields",
                "auth_config",
                when={"runtime": "target_config_contains_auth"},
                confirmation="explicit_command",
            ),
            _manifest_effect(
                "local",
                "delete",
                "external_credential_cache_entries",
                best_effort=True,
                confirmation="explicit_command",
            ),
            _manifest_effect(
                "local",
                "delete",
                "auth_continuations_for_target_and_expired_entries",
                best_effort=True,
                confirmation="explicit_command",
            ),
            _manifest_effect(
                "local",
                "create_or_open",
                "metadata_cache",
                best_effort=True,
                note="Opening the credential cache may create its owner-only SQLite file.",
            ),
        ],
        "auth.whoami": [
            _manifest_effect(
                "remote",
                "read",
                "maxcompute_identity",
                when={"runtime": "complete_auth_candidate_exists"},
            ),
            _manifest_effect(
                "local",
                "create_or_replace",
                "external_credential_cache",
                when={
                    "all": [
                        {"runtime": "active_provider_is_external_or_ncs"},
                        {"runtime": "helper_returns_expiring_credentials"},
                    ]
                },
                best_effort=True,
            ),
            _manifest_effect(
                "local",
                "replace",
                "oauth_auth_config",
                when={
                    "all": [
                        {"runtime": "active_provider_is_oauth"},
                        {"runtime": "cached_sts_missing_or_expiring"},
                        {"runtime": "refresh_binding_is_still_current"},
                    ]
                },
            ),
        ],
        "cache.build": [
            _manifest_effect("remote", "read", "maxcompute_metadata"),
            _manifest_effect("local", "replace", "metadata_cache"),
        ],
        "cache.build-status": [
            _manifest_effect("local", "read", "metadata_cache_build_state"),
        ],
        "cache.status": [
            _manifest_effect("local", "read", "metadata_cache"),
        ],
        "cache.clear": [
            _manifest_effect("local", "read", "metadata_cache_scope"),
            _manifest_effect(
                "local",
                "delete",
                "metadata_cache",
                when={
                    "all": [
                        _manifest_condition("force", equals=True),
                        _manifest_condition("dry_run", equals=False),
                    ]
                },
                confirmation="--force",
            ),
        ],
        "data.download": [
            _manifest_effect("remote", "read", "maxcompute_table_data"),
            _manifest_effect(
                "local",
                "create",
                "output_file",
                when=_manifest_condition("overwrite", equals=False),
            ),
            _manifest_effect(
                "local",
                "replace",
                "output_file",
                when=_manifest_condition("overwrite", equals=True),
                confirmation="--overwrite",
            ),
        ],
        "data.upload": [
            _manifest_effect("local", "read", "input_file"),
            _manifest_effect(
                "local",
                "create",
                "owner_private_upload_snapshot",
                when=_manifest_condition("dry_run", equals=False),
                note=(
                    "Validation and Tunnel replay use the same temporary "
                    "owner-private snapshot."
                ),
            ),
            _manifest_effect(
                "local",
                "delete",
                "owner_private_upload_snapshot",
                when=_manifest_condition("dry_run", equals=False),
                best_effort=True,
                note=(
                    "Cleanup failure after a remote commit is reported only as "
                    "a warning and never makes the upload look retryable."
                ),
            ),
            _manifest_effect("remote", "read", "target_table_schema"),
            _manifest_effect(
                "remote",
                "create",
                "maxcompute_tunnel_upload_session",
                when=_manifest_condition("dry_run", equals=False),
                note=(
                    "PyODPS exposes no upload-session abort API. Before commit, "
                    "uncommitted blocks stay invisible and expire server-side; "
                    "a failed commit request has an unknown remote outcome."
                ),
            ),
            _manifest_effect(
                "remote",
                "create",
                "maxcompute_partition",
                when={
                    "all": [
                        _manifest_condition("create_partition", equals=True),
                        _manifest_condition("dry_run", equals=False),
                    ]
                },
                confirmation="--create-partition",
            ),
            _manifest_effect(
                "remote",
                "append",
                "maxcompute_table_data",
                when={
                    "all": [
                        _manifest_condition("dry_run", equals=False),
                        _manifest_condition("overwrite", equals=False),
                    ]
                },
            ),
            _manifest_effect(
                "remote",
                "replace",
                "maxcompute_table_or_partition_data",
                when={
                    "all": [
                        _manifest_condition("dry_run", equals=False),
                        _manifest_condition("overwrite", equals=True),
                    ]
                },
                confirmation="--overwrite",
            ),
        ],
        "job.cancel": [
            _manifest_effect(
                "remote",
                "cancel",
                "maxcompute_job",
                confirmation="explicit_user_intent",
            ),
        ],
        "job.result": [
            *output_preflight_effects,
            _manifest_effect("remote", "read", "maxcompute_job_result"),
            _manifest_effect(
                "local",
                "read",
                "pagination_context",
                when=_manifest_condition("cursor", present=True),
            ),
            _manifest_effect(
                "local",
                "create_or_replace",
                "pagination_context",
                when={"runtime": "successful_result_has_more_pages"},
                best_effort=True,
            ),
            _manifest_effect(
                "local",
                "create",
                "job_result_output_file",
                when={
                    "all": [
                        _manifest_condition("output", present=True),
                        _manifest_condition("overwrite", equals=False),
                        {"runtime": "successful_result_available"},
                    ]
                },
            ),
            _manifest_effect(
                "local",
                "replace",
                "job_result_output_file",
                when={
                    "all": [
                        _manifest_condition("output", present=True),
                        _manifest_condition("overwrite", equals=True),
                        {"runtime": "successful_result_available"},
                    ]
                },
                confirmation="--overwrite",
            ),
        ],
        "job.submit": [
            _manifest_effect(
                "remote",
                "compute_estimate",
                "maxcompute_sql_cost",
                when=_manifest_condition("dry_run", equals=True),
            ),
            _manifest_effect(
                "remote",
                "job_submit",
                "maxcompute_select_job",
                when=_manifest_condition("dry_run", equals=False),
            ),
            _manifest_effect(
                "remote",
                "data_mutation",
                "maxcompute",
                when={"runtime": "hidden_force_with_mutating_sql"},
                agent_allowed=False,
                note="Compatibility escape hatch; public Agent Skill is SELECT-only.",
            ),
            _manifest_effect(
                "local",
                "create_or_replace",
                "job_followup_context",
                when=_manifest_condition("dry_run", equals=False),
                best_effort=True,
            ),
            _manifest_effect(
                "local",
                "create_or_open",
                "job_state_lock",
                when=_manifest_condition("dry_run", equals=False),
                best_effort=True,
            ),
        ],
        "query": [
            *output_preflight_effects,
            _manifest_effect(
                "remote",
                "compute_estimate",
                "maxcompute_sql_cost",
                when={"runtime": "cost_explain_or_dry_run"},
            ),
            _manifest_effect(
                "remote",
                "job_submit",
                "maxcompute_select_job",
                when={
                    "all": [
                        {"runtime": "run_mode_and_not_dry_run"},
                        _manifest_condition("cursor", present=False),
                    ]
                },
            ),
            _manifest_effect(
                "remote",
                "read",
                "maxcompute_existing_job_result",
                when=_manifest_condition("cursor", present=True),
            ),
            _manifest_effect(
                "local",
                "read",
                "pagination_context",
                when=_manifest_condition("cursor", present=True),
            ),
            _manifest_effect(
                "remote",
                "data_mutation",
                "maxcompute",
                when={"runtime": "hidden_force_with_mutating_sql"},
                agent_allowed=False,
                note="Compatibility escape hatch; public Agent Skill is SELECT-only.",
            ),
            _manifest_effect(
                "local",
                "create",
                "query_output_file",
                when={
                    "all": [
                        _manifest_condition("output", present=True),
                        _manifest_condition("overwrite", equals=False),
                        {"runtime": "successful_result_available"},
                    ]
                },
            ),
            _manifest_effect(
                "local",
                "replace",
                "query_output_file",
                when={
                    "all": [
                        _manifest_condition("output", present=True),
                        _manifest_condition("overwrite", equals=True),
                        {"runtime": "successful_result_available"},
                    ]
                },
                confirmation="--overwrite",
            ),
            _manifest_effect(
                "local",
                "replace",
                "job_and_pagination_context",
                when={"runtime": "remote_job_or_paginated_result"},
                best_effort=True,
            ),
        ],
        "session.set": [
            _manifest_effect("local", "create_or_replace", "session_config"),
        ],
        "session.show": [
            _manifest_effect("local", "read", "session_config"),
        ],
        "session.unset": [
            _manifest_effect("local", "delete_fields", "session_config"),
        ],
        "meta.semantic.get": [
            _manifest_effect("local", "read", "semantic_metadata"),
        ],
        "meta.semantic.list-missing": [
            _manifest_effect("local", "read", "semantic_metadata_cache"),
        ],
        "meta.semantic.set": [
            _manifest_effect("local", "create_or_replace", "semantic_metadata"),
        ],
        "meta.semantic.clear": [
            _manifest_effect(
                "local",
                "delete",
                "semantic_metadata",
                confirmation="--force for project-wide clear",
            ),
        ],
    }
    if command in effects_by_command:
        effects = list(effects_by_command[command])
        if command in _MANIFEST_JOB_FOLLOWUP_COMMANDS:
            effects.extend(job_followup_effects)
        return _with_manifest_audit_effect(command, effects)
    if command.startswith("job."):
        effects = [_manifest_effect("remote", "read", "maxcompute_job")]
        if command in _MANIFEST_JOB_FOLLOWUP_COMMANDS:
            effects.extend(job_followup_effects)
        return _with_manifest_audit_effect(command, effects)
    if command.startswith("meta."):
        return _with_manifest_audit_effect(command, [
            _manifest_effect("remote", "read", "maxcompute_metadata"),
            _manifest_effect(
                "local",
                "replace",
                "metadata_cache",
                best_effort=True,
            ),
        ])
    if command.startswith("data."):
        return _with_manifest_audit_effect(
            command,
            [_manifest_effect("remote", "read", "maxcompute_table_data")],
        )
    if command == "auth.can-i":
        return _with_manifest_audit_effect(
            command,
            [_manifest_effect("remote", "read", "maxcompute_permissions")],
        )
    return _with_manifest_audit_effect(command, [])


def _manifest_output_shape_contracts() -> dict[str, Any]:
    structured_rules = [
        {
            "id": "json",
            "when": {"output_format": "json"},
            "shape": "envelope",
            "statuses": ["success", "pending", "failure"],
            "version": "2.0",
        },
        {
            "id": "human",
            "when": {"output_format_in": ["table", "markdown", "brief"]},
            "shape": "human_readable",
            "statuses": ["success", "pending", "failure"],
        },
    ]
    return {
        "structured": {
            "shape_rules": structured_rules,
        },
        "record_stream": {
            "extends": "structured",
            "shape_rules": [
                {
                    "id": "csv_records",
                    "when": {
                        "all": [
                            {"output_format": "csv"},
                            {"status": "success"},
                            {"runtime": "record_collection_available"},
                        ]
                    },
                    "shape": "records",
                    "framing": "csv_header_and_rows",
                },
                {
                    "id": "csv_control",
                    "when": {
                        "all": [
                            {"output_format": "csv"},
                            {"runtime": "no_successful_record_collection"},
                        ]
                    },
                    "shape": "control_record",
                    "framing": "csv_header_and_one_row",
                    "statuses": ["success", "pending", "failure"],
                },
                {
                    "id": "ndjson_records",
                    "when": {
                        "all": [
                            {"output_format": "ndjson"},
                            {"status": "success"},
                            {"runtime": "record_collection_available"},
                        ]
                    },
                    "shape": "records",
                    "framing": "one_record_per_line",
                },
                {
                    "id": "ndjson_control",
                    "when": {
                        "all": [
                            {"output_format": "ndjson"},
                            {"runtime": "no_successful_record_collection"},
                        ]
                    },
                    "shape": "envelope",
                    "framing": "one_envelope_per_line",
                    "statuses": ["success", "pending", "failure"],
                    "version": "2.0",
                },
            ],
        },
        "job_wait_streamable": {
            "extends": "record_stream",
            "parent_applies_when": _manifest_condition("stream", equals=False),
            "shape_rules": [
                {
                    "id": "job_wait_stream",
                    "when": _manifest_condition("stream", equals=True),
                    "format": "ndjson",
                    "shape": "lifecycle_event",
                    "statuses": ["success", "pending", "failure"],
                    "terminal_event": "authoritative",
                    "reason": (
                        "Each line is a lifecycle event. The final event carries the "
                        "authoritative status, data, metadata, error, and agent_hints."
                    ),
                }
            ],
        },
        "query_result_file": {
            "shape_rules": [
                {
                    "id": "file_pending_deferred",
                    "when": {"status": "pending"},
                    "shape": "no_file",
                    "metadata": {
                        "output_written": False,
                        "output_deferred": True,
                    },
                    "resume": "exact job.result action in agent_hints",
                    "reason": (
                        "A pending job has no result rows to publish; stdout carries "
                        "the pending Envelope and exact deferred fetch action."
                    ),
                },
                {
                    "id": "file_json_success",
                    "when": {
                        "all": [
                            {"file_format": "json"},
                            {"status": "success"},
                        ]
                    },
                    "shape": "envelope",
                    "version": "2.0",
                },
                {
                    "id": "file_table_success",
                    "when": {
                        "all": [
                            {"file_format": "table"},
                            {"status": "success"},
                        ]
                    },
                    "shape": "human_readable",
                },
                {
                    "id": "file_csv_records_success",
                    "when": {
                        "all": [
                            {"file_format": "csv"},
                            {"status": "success"},
                            {"runtime": "record_collection_available"},
                        ]
                    },
                    "shape": "records",
                },
                {
                    "id": "file_csv_control_success",
                    "when": {
                        "all": [
                            {"file_format": "csv"},
                            {"runtime": "no_successful_record_collection"},
                            {"status": "success"},
                        ]
                    },
                    "shape": "control_record",
                },
                {
                    "id": "file_ndjson_records_success",
                    "when": {
                        "all": [
                            {"file_format": "ndjson"},
                            {"status": "success"},
                            {"runtime": "record_collection_available"},
                        ]
                    },
                    "shape": "records",
                },
                {
                    "id": "file_ndjson_control_success",
                    "when": {
                        "all": [
                            {"file_format": "ndjson"},
                            {"runtime": "no_successful_record_collection"},
                            {"status": "success"},
                        ]
                    },
                    "shape": "envelope",
                    "version": "2.0",
                },
                {
                    "id": "file_failure",
                    "when": {"status": "failure"},
                    "shape": "no_file",
                    "reason": (
                        "A failed envelope is emitted to stdout; no result file "
                        "is published."
                    ),
                },
            ],
        },
    }


def _manifest_output_contract(command: str) -> dict[str, Any]:
    formats = ["json", "table", "markdown", "brief"]
    record_command = command in _RECORD_FORMAT_COMMANDS
    if record_command:
        formats.extend(["csv", "ndjson"])
    shape_contract = (
        "job_wait_streamable"
        if command == "job.wait"
        else "record_stream"
        if record_command
        else "structured"
    )
    contract: dict[str, Any] = {
        "formats": formats,
        "envelope_version": "2.0",
        "shape_contract": shape_contract,
    }
    rules: list[dict[str, Any]] = []
    if command == "query":
        rules.extend(
            [
                {
                    "when": {"runtime": "cost_explain_or_dry_run"},
                    "formats": ["json", "table", "markdown", "brief"],
                    "reason": "Analysis results are structured objects, not record streams.",
                },
                {
                    "when": _manifest_condition("output", present=True),
                    "file_formats": ["json", "table", "csv", "ndjson"],
                    "file_shape_contract": "query_result_file",
                },
            ]
        )
    if command == "job.result":
        rules.append(
            {
                "when": _manifest_condition("output", present=True),
                "file_formats": ["json", "table", "csv", "ndjson"],
                "file_shape_contract": "query_result_file",
                "reason": (
                    "Only a successful job result is published; pending jobs "
                    "return an exact deferred job.result action instead."
                ),
            }
        )
    if command == "job.wait":
        rules.append(
            {
                "when": _manifest_condition("stream", equals=True),
                "formats": ["ndjson"],
                "reason": "Buffered lifecycle events and one authoritative terminal event are emitted as NDJSON.",
            }
        )
    if rules:
        contract["rules"] = rules
    return contract


def _manifest_default(action_obj: argparse.Action) -> Any:
    default = action_obj.default
    if default is argparse.SUPPRESS or callable(default):
        return None
    sensitive = _is_sensitive_arg_flag(f"--{action_obj.dest.replace('_', '-')}")
    if sensitive and default not in {None, ""}:
        return "<redacted>"
    if isinstance(default, (str, int, float, bool, list, dict)) or default is None:
        return default
    return str(default)


def _manifest_argument(action_obj: argparse.Action) -> dict[str, Any]:
    positional = not action_obj.option_strings
    nargs = action_obj.nargs
    required = bool(action_obj.required)
    if positional:
        required = nargs not in {"?", "*"}
    payload: dict[str, Any] = {
        "name": action_obj.dest,
        "kind": "argument" if positional else "option",
        "flags": list(action_obj.option_strings),
        "required": required,
        "takes_value": nargs != 0,
        "multiple": nargs in {"*", "+"} or isinstance(nargs, int) and nargs > 1,
        "help": "" if action_obj.help is argparse.SUPPRESS else action_obj.help or "",
    }
    if action_obj.help is argparse.SUPPRESS:
        if action_obj.dest == "force":
            payload.update(
                {
                    "visibility": "hidden_compatibility",
                    "agent_allowed": False,
                    "safety": "Public Agent Skill is SELECT-only; do not use this escape hatch.",
                }
            )
        elif action_obj.dest in {"login_continuation", "oauth_continuation"}:
            payload.update(
                {
                    "visibility": "internal_continuation",
                    "agent_allowed": False,
                    "safety": "Use only an exact action returned by auth.login.",
                }
            )
        else:
            payload.update(
                {
                    "visibility": "deprecated_or_internal",
                    "agent_allowed": False,
                }
            )
    else:
        payload["visibility"] = "public"
        payload["agent_allowed"] = True
    if action_obj.choices is not None:
        payload["choices"] = list(action_obj.choices)
    default = _manifest_default(action_obj)
    if default is not None:
        payload["default"] = default
    return payload


def _manifest_arguments(parser: argparse.ArgumentParser) -> list[dict[str, Any]]:
    """Collapse multiple argparse actions that set one logical destination."""
    grouped: dict[str, list[argparse.Action]] = {}
    for action_obj in parser._actions:
        if isinstance(action_obj, argparse._SubParsersAction) or action_obj.dest == "help":
            continue
        grouped.setdefault(action_obj.dest, []).append(action_obj)

    arguments: list[dict[str, Any]] = []
    for actions in grouped.values():
        payload = _manifest_argument(actions[0])
        if len(actions) > 1:
            payload["flags"] = [
                flag for action_obj in actions for flag in action_obj.option_strings
            ]
            public_help = [
                str(action_obj.help)
                for action_obj in actions
                if action_obj.help not in {None, argparse.SUPPRESS}
            ]
            payload["help"] = " / ".join(dict.fromkeys(public_help))
            payload["variants"] = [
                {
                    "flags": list(action_obj.option_strings),
                    "sets": getattr(action_obj, "const", None),
                    "help": (
                        ""
                        if action_obj.help is argparse.SUPPRESS
                        else action_obj.help or ""
                    ),
                }
                for action_obj in actions
            ]
        arguments.append(payload)
    return arguments


def _command_effect(effects: list[dict[str, Any]]) -> str:
    remote_kinds = {
        effect["kind"]
        for effect in effects
        if effect["scope"] == "remote" and effect.get("agent_allowed", True)
    }
    local_kinds = {
        effect["kind"]
        for effect in effects
        if effect["scope"] == "local" and effect.get("agent_allowed", True)
    }
    if remote_kinds & {"append", "create", "replace", "data_mutation", "cancel"}:
        return "remote_write"
    if "job_submit" in remote_kinds:
        return "remote_compute"
    if local_kinds & {
        "append",
        "create",
        "create_or_open",
        "create_or_replace",
        "delete",
        "delete_fields",
        "replace",
    }:
        return "local_write"
    return "read_only"


def _command_manifest(parser: argparse.ArgumentParser) -> dict[str, Any]:
    """Generate an Agent-readable manifest from the live parser tree."""
    from maxc_cli import __version__

    commands: list[dict[str, Any]] = []

    def _walk(
        current: argparse.ArgumentParser,
        path: tuple[str, ...],
        summary: str = "",
    ) -> None:
        if path and "handler" in current._defaults:
            command = ".".join(path)
            requirements = _manifest_requirements(command)
            effects = _manifest_effects(command)
            output_contract = _manifest_output_contract(command)
            arguments = _manifest_arguments(current)
            entry: dict[str, Any] = {
                "command": command,
                "invocation": f"{current_cli_entry_point()} {' '.join(path)}",
                "summary": summary or current.description or "",
                # Legacy summaries remain for 1.0 consumers.  Structured
                # requirements/effects below are authoritative for branches.
                "network": requirements["network"]["mode"],
                "auth": requirements["credentials"]["mode"],
                "effect": _command_effect(effects),
                "requirements": requirements,
                "effects": effects,
                "output": output_contract,
                "arguments": arguments,
                "supports_json": any(item["name"] == "json" for item in arguments),
            }
            if command == "query":
                entry["modes"] = ["run", "cost", "explain"]
            commands.append(entry)

        for action_obj in current._actions:
            if not isinstance(action_obj, argparse._SubParsersAction):
                continue
            help_by_name = {
                choice.dest: choice.help
                for choice in action_obj._get_subactions()
            }
            for name, child in action_obj.choices.items():
                _walk(child, (*path, name), help_by_name.get(name, ""))

    _walk(parser, ())
    commands.sort(key=lambda item: item["command"])
    global_arguments = [
        item
        for item in _manifest_arguments(parser)
        if item["visibility"] != "deprecated_or_internal"
    ]
    audited_commands = [
        command["command"]
        for command in commands
        if any(
            effect.get("target") == "audit_log"
            for effect in command["effects"]
        )
    ]
    return {
        "schema_version": "1.1",
        "cli_version": __version__,
        "envelope_version": "2.0",
        "entry_point": current_cli_entry_point(),
        "global_arguments": global_arguments,
        "output_formats": ["json", "table", "csv", "ndjson", "markdown", "brief"],
        "status_values": ["success", "pending", "failure"],
        "output_shapes": {
            "envelope": "One Envelope 2.0 object, optionally framed as one NDJSON line.",
            "records": "A collection stream containing data rows only.",
            "control_record": "One CSV row carrying status/error/control fields instead of data rows.",
            "lifecycle_event": "One job lifecycle event per NDJSON line; the final event is authoritative.",
            "human_readable": "Presentation text for a person; not a stable machine contract.",
            "no_file": "No result file is published for this branch.",
        },
        "output_shape_contracts": _manifest_output_shape_contracts(),
        "contract_semantics": {
            "requirements": "Preconditions, including conditional network and credential rules.",
            "effects": "Potential local and remote effects; every `when` clause narrows an effect.",
            "agent_allowed": "False means compatibility/internal surface that Agents must not invoke.",
            "output": "Machine shape depends on format, status, and whether a record collection exists.",
        },
        "implicit_flows": [
            {
                "id": "interactive_oauth_bootstrap",
                "when": {
                    "all": [
                        {"runtime": "command_requires_credentials"},
                        {"runtime": "credentials_missing"},
                        {"runtime": "stdin_is_tty"},
                        {"runtime": "command_not_auto_login_exempt"},
                    ]
                },
                "excluded_commands": sorted(_AUTO_LOGIN_EXEMPT_COMMANDS),
                "action": "auth.login --oauth",
                "network": "required",
                "effects": [
                    _manifest_effect("remote", "authenticate", "oauth_and_maxcompute"),
                    _manifest_effect(
                        "local",
                        "create_or_replace",
                        "auth_config",
                        when={"runtime": "login_succeeds"},
                    ),
                    _manifest_effect(
                        "local",
                        "create",
                        "owner_only_oauth_continuation",
                        when={"runtime": "project_selection_pending"},
                    ),
                ],
            },
            {
                "id": "external_or_ncs_credential_resolution",
                "when": {
                    "all": [
                        {"runtime": "command_initializes_authenticated_backend"},
                        {"runtime": "command_uses_saved_auth_provider"},
                        {"runtime": "active_provider_is_external_or_ncs"},
                    ]
                },
                "effects": [
                    _manifest_effect(
                        "local",
                        "create_or_open",
                        "external_credential_cache_store",
                        best_effort=True,
                    ),
                    _manifest_effect(
                        "local",
                        "read",
                        "external_credential_cache",
                    ),
                    _manifest_effect(
                        "local",
                        "execute_process",
                        "credential_helper",
                        when={"runtime": "credential_cache_miss_or_expiring"},
                    ),
                    _manifest_effect(
                        "local",
                        "create_or_replace",
                        "external_credential_cache",
                        when={
                            "all": [
                                {"runtime": "credential_cache_miss_or_expiring"},
                                {"runtime": "helper_returns_expiring_credentials"},
                            ]
                        },
                        best_effort=True,
                    ),
                ],
                "note": (
                    "This provider flow can occur before an ordinary metadata, query, "
                    "data, job, permission, whoami, or online-doctor operation."
                ),
            },
            {
                "id": "oauth_sts_refresh",
                "when": {
                    "all": [
                        {"runtime": "command_initializes_authenticated_backend"},
                        {"runtime": "active_provider_is_oauth"},
                        {"runtime": "cached_sts_missing_or_expiring"},
                    ]
                },
                "effects": [
                    _manifest_effect(
                        "remote",
                        "authenticate",
                        "oauth_and_sts_services",
                    ),
                    _manifest_effect(
                        "local",
                        "replace",
                        "oauth_auth_config",
                        when={"runtime": "refresh_binding_is_still_current"},
                    ),
                ],
                "note": (
                    "An expired OAuth access token is refreshed before STS exchange; "
                    "a still-valid access token needs only the STS exchange."
                ),
            },
            {
                "id": "default_config_permission_repair",
                "when": {
                    "all": [
                        {"runtime": "platform_is_posix"},
                        {"runtime": "default_global_config_exists"},
                        {"runtime": "default_global_config_mode_is_broader_than_0600"},
                    ]
                },
                "effects": [
                    _manifest_effect(
                        "local",
                        "restrict_permissions",
                        "default_global_config",
                    )
                ],
                "note": (
                    "Reading an existing default config pins it without following "
                    "links and repairs overly broad POSIX permissions; it never "
                    "creates a config file on a read-only path."
                ),
            },
            {
                "id": "legacy_session_migration",
                "when": {
                    "all": [
                        {"runtime": "command_writes_default_global_config"},
                        {"runtime": "unmigrated_legacy_session_override_exists"},
                    ]
                },
                "effects": [
                    _manifest_effect(
                        "local",
                        "create_or_replace",
                        "default_global_config",
                    ),
                    _manifest_effect(
                        "local",
                        "delete",
                        "legacy_session_override",
                    ),
                    _manifest_effect(
                        "local",
                        "create",
                        "legacy_session_migration_marker",
                    ),
                ],
                "note": (
                    "Read-only commands consume legacy project/schema values "
                    "without migration. Migration is deferred until an explicit "
                    "auth or session config write."
                ),
            },
            {
                "id": "stale_legacy_session_override_cleanup",
                "when": {
                    "all": [
                        {"runtime": "command_writes_default_global_config"},
                        {"runtime": "legacy_session_migration_marker_exists"},
                        {"runtime": "stale_legacy_session_override_exists"},
                    ]
                },
                "effects": [
                    _manifest_effect(
                        "local",
                        "delete",
                        "legacy_session_override",
                    ),
                ],
                "note": (
                    "The durable marker is authoritative: a later stale override is "
                    "deleted without folding its values into the current config."
                ),
            },
        ],
        "observability": {
            "audit_log": {
                "authoritative": True,
                "effect": "local_append",
                "best_effort": True,
                "commands": audited_commands,
                "when": {
                    "runtime": "per_command_effect_condition_matches"
                },
                "confirmed_no_append_paths": [
                    "agent.context",
                    "agent.manifest",
                    {
                        "command": "agent.doctor",
                        "when": _manifest_condition("online", equals=False),
                    },
                ],
                "content": "command/status/sanitized metadata; credentials are excluded",
                "correlation_field": "invocation_id",
                "ordering": (
                    "Append order is authoritative within one invocation_id; a later "
                    "output-publication failure supersedes the earlier remote success status."
                ),
                "note": (
                    "This command list and each command's audit_log effect are "
                    "machine-authoritative; confirmed no-append branches are excluded."
                ),
            }
        },
        "command_count": len(commands),
        "commands": commands,
    }


def _handle_agent_skill(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.agent_skill()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _detect_cli_name() -> str:
    """Return the current standalone or Alibaba Cloud CLI invocation.

    The value is used directly as {{cli}} in SKILL templates.
    Example: MAXC_CLI_NAME='aliyun maxc' renders all commands as `aliyun maxc ...`.
    """
    return current_cli_entry_point()


def _resolve_dir_override(args: argparse.Namespace) -> Path | None:
    raw = getattr(args, "dir_override", None)
    return Path(raw).expanduser() if raw else None


def _handle_agent_skill_install(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    invocation = args.invocation or _detect_cli_name()
    envelope = app.skill_install(
        platform=args.platform,
        invocation=invocation,
        dir_override=_resolve_dir_override(args),
        force=getattr(args, "force", False),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_update(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    dir_override = _resolve_dir_override(args)
    envelope = app.skill_update(
        platform=args.platform,
        all_platforms=getattr(args, "all_platforms", False),
        # Keep None distinct from an explicit override. MaxCApp resolves an
        # omitted value from each target's installed marker independently.
        invocation=args.invocation,
        dir_override=dir_override,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_uninstall(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.skill_uninstall(
        platform=args.platform,
        dir_override=_resolve_dir_override(args),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_list(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.skill_list()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_diff(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.skill_diff(
        platform=args.platform,
        unified=getattr(args, "unified", False),
        dir_override=_resolve_dir_override(args),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_skill_path(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.skill_path(
        platform=args.platform,
        source=getattr(args, "source", False),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_build(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    """Build metadata through the same application path in every output mode."""
    import time

    is_json_mode = _is_json_mode(args)
    is_async_mode = getattr(args, "async_mode", False)
    schema_name = getattr(args, "schema", None)
    machine_output = is_json_mode or is_async_mode
    progress_stream = (
        (getattr(args, "stderr", None) or sys.stderr)
        if machine_output
        else stdout
    )
    last_progress_emit = 0.0

    def emit_progress(event: dict[str, Any]) -> None:
        nonlocal last_progress_emit
        event_type = str(event.get("type", ""))
        now = time.monotonic()
        if event_type == "listing_start":
            progress_stream.write("Fetching table list...\n")
            progress_stream.flush()
            return
        if event_type == "listing_complete":
            total = int(event.get("total_tables", 0))
            progress_stream.write(
                f"Discovered {total} table(s), starting cache build...\n"
            )
            progress_stream.flush()
            return
        if event_type == "progress":
            if now - last_progress_emit < 0.5:
                return
            last_progress_emit = now
            progress_stream.write(
                "\rProgress: {processed}/{total} tables processed "
                "(cached: {cached}, failed: {failed})".format(
                    processed=event.get("processed_tables", 0),
                    cached=event.get("cached_tables", 0),
                    total=event.get("total_tables", 0),
                    failed=event.get("failed_tables", 0),
                )
            )
            progress_stream.flush()
            return
        if event_type == "completed":
            progress_stream.write(
                "\rProgress: {processed}/{total} tables processed "
                "(cached: {cached}, failed: {failed})\n".format(
                    processed=event.get("processed_tables", 0),
                    cached=event.get("cached_tables", 0),
                    total=event.get("total_tables", 0),
                    failed=event.get("failed_tables", 0),
                )
            )
            progress_stream.flush()

    envelope = app.cache_build(
        project=args.project,
        schema_name=schema_name,
        async_mode=is_async_mode,
        progress_callback=emit_progress,
    )
    _emit_envelope(
        envelope,
        args=args,
        stdout=stdout,
        default_format="json" if machine_output else "table",
    )



def _handle_cache_build_status(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cache_build_status(
        project=args.project,
        build_id=getattr(args, 'build_id', None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_status(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cache_status(
        project=args.project,
        schema_name=getattr(args, 'schema', None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_clear(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cache_clear(
        project=args.project,
        schema_name=getattr(args, 'schema', None),
        force=getattr(args, 'force', False),
        dry_run=getattr(args, 'dry_run', False),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _emit_envelope(
    envelope: Envelope,
    *,
    args: argparse.Namespace,
    stdout: TextIO,
    default_format: str,
) -> None:
    _set_envelope_exit_code(envelope, args)

    fmt = getattr(args, "format", None)

    # --json flag is shorthand for --format json
    if not fmt and getattr(args, "json", False):
        fmt = "json"

    fmt = fmt or default_format

    if fmt == "json":
        emit_json(envelope.to_dict(), stdout)
        return
    if fmt == "markdown":
        from .output import render_markdown
        stdout.write(render_markdown(envelope) + "\n")
        return
    if fmt == "brief":
        from .output import render_brief
        stdout.write(render_brief(envelope) + "\n")
        return
    if fmt in {"csv", "ndjson"}:
        _emit_record_format(envelope, fmt, stdout)
        return

    # Default: human-readable table/text
    stdout.write(_render_human(envelope) + "\n")


def _set_envelope_exit_code(envelope: Envelope, args: argparse.Namespace) -> None:
    """Propagate failure-envelope exit status for every output mode."""
    # Stash exit code for run() to surface — failure envelopes built directly
    # in a handler (e.g., JSON parse error in `meta semantic set`) used to
    # silently exit 0 because run() only inspected raised exceptions. Now
    # any failure status propagates as a non-zero exit.
    if envelope.status == "failure":
        # Pull exit code from the originating exception via ErrorPayload.exit_code.
        # Default 1 when payload is None or hand-built without an override.
        args._envelope_exit_code = getattr(envelope.error, "exit_code", 1) or 1


def _render_human(envelope: Envelope) -> str:
    command = envelope.command
    data = envelope.data if isinstance(envelope.data, dict) else {}
    metadata = envelope.metadata if isinstance(envelope.metadata, dict) else {}

    # A failure envelope is authoritative regardless of command-specific data
    # shape. Render it before touching result rows so a lost connection or
    # remote job failure cannot be rewritten as an INTERNAL_ERROR or a
    # misleading "(no rows)" response.
    if envelope.error is not None:
        from .output import render_error

        error = envelope.error
        sections = [render_error(error.code, error.message, error.suggestion)]
        details: dict[str, Any] = {"status": envelope.status}
        job_id = data.get("job_id") or metadata.get("job_id") or error.instance_id
        logview = metadata.get("logview") or error.logview
        if job_id:
            details["job_id"] = job_id
        if metadata.get("project"):
            details["project"] = metadata["project"]
        if logview:
            details["logview"] = logview
        if error.context:
            details["context"] = error.context
        sections.append(render_key_values(details))
        if error.recovery_steps:
            sections.append(
                "Recovery steps:\n"
                + "\n".join(f"- {step}" for step in error.recovery_steps)
            )
        hints = envelope.agent_hints
        if hints is not None and hints.warnings:
            sections.append(
                "Warnings:\n" + "\n".join(f"- {item}" for item in hints.warnings)
            )
        safe_actions = (
            [item for item in hints.actions if suggested_action_is_safe(item)]
            if hints is not None
            else []
        )
        if safe_actions:
            sections.append(
                "Next actions:\n"
                + "\n".join(
                    f"- {item.to_dict()['command']}" for item in safe_actions
                )
            )
        return "\n\n".join(section for section in sections if section)

    if command == "query":
        rows = data.get("rows", [])
        summary = render_key_values(
            {
                "status": envelope.status,
                "project": metadata.get("project"),
                "elapsed_ms": metadata.get("elapsed_ms"),
                "returned_rows": data.get("returned_rows"),
                "total_rows": data.get("total_rows"),
                "has_more": data.get("has_more"),
                "next_cursor": data.get("next_cursor"),
                "current_offset": metadata.get("current_offset"),
                "bytes_scanned": metadata.get("bytes_scanned"),
                "task_cost_cpu": metadata.get("task_cost_cpu"),
                "task_cost_memory": metadata.get("task_cost_memory"),
                "tables": metadata.get("tables_used", []),
            }
        )
        body = render_table(rows)
        return f"{summary}\n\n{body}"

    if command in {"query.cost", "query.explain"}:
        return render_key_values(data)

    if command == "meta.list-tables":
        return render_table(data.get("tables", []))

    if command == "meta.describe":
        # Render schema/partition_columns as nested sub-tables instead of
        # JSON-stringifying them into a single cell.
        scalar_kv: dict[str, Any] = {}
        nested_sections: list[tuple[str, list[dict[str, Any]]]] = []
        nested_keys = ("columns", "schema", "partition_columns", "partitions", "sample_rows")
        for k, v in data.items():
            if v is None:
                continue
            if k in nested_keys and isinstance(v, list) and v and all(isinstance(item, dict) for item in v):
                nested_sections.append((k, v))
            else:
                scalar_kv[k] = v
        sections = [render_key_values(scalar_kv)] if scalar_kv else []
        for label, rows in nested_sections:
            sections.append(f"\n### {label}\n")
            sections.append(render_table(rows))
        return "\n".join(sections)

    if command in {"meta.search", "meta.search-columns"}:
        return render_table(data.get("matches", []))

    if command == "data.sample":
        return render_table(data.get("rows", []))

    if command == "skill.list":
        return render_table(data.get("skills", []))

    return render_key_values(data if isinstance(data, dict) else {"value": data})


_RECORD_COLLECTION_KEYS: dict[str, tuple[str, str]] = {
    "agent.skill.list": ("installed", "installation"),
    "data.sample": ("rows", "value"),
    "job.list": ("jobs", "job"),
    "job.result": ("rows", "value"),
    "job.wait": ("rows", "value"),
    "meta.list-projects": ("projects", "project"),
    "meta.list-schemas": ("schemas", "schema"),
    "meta.list-tables": ("tables", "table"),
    "meta.partitions": ("partitions", "partition"),
    "meta.search": ("matches", "match"),
    "meta.search-columns": ("matches", "match"),
    "meta.semantic.list-missing": ("tables", "table"),
    "query": ("rows", "value"),
}

_RECORD_DEFAULT_COLUMNS: dict[str, list[str]] = {
    "agent.skill.list": ["platform", "install_path", "installed_version_marker"],
    "job.list": ["job_id", "status", "progress", "project", "submitted_at"],
    "meta.list-projects": ["name"],
    "meta.list-schemas": ["name"],
    "meta.list-tables": [
        "table_name",
        "schema_name",
        "qualified_name",
        "table_type",
        "description",
        "partition_columns",
    ],
    "meta.search": ["table_name", "description", "score", "matched_columns"],
    "meta.search-columns": [
        "table_name",
        "column_name",
        "column_type",
        "column_comment",
        "score",
    ],
    "meta.partitions": ["partition"],
    "meta.semantic.list-missing": [
        "table_name",
        "schema_name",
        "description",
        "column_count",
    ],
}

_CONTROL_RECORD_COLUMNS = [
    "version",
    "command",
    "status",
    "error_code",
    "error_message",
    "suggestion",
    "recoverable",
    "data",
    "metadata",
    "agent_hints",
]


def _record_collection(
    envelope: Envelope,
) -> tuple[list[dict[str, Any]], list[str]] | None:
    """Extract a command's record collection without confusing objects for rows."""
    if envelope.status != "success":
        return None
    if envelope.command in {"query", "job.wait", "job.result"}:
        if envelope.metadata.get("result_kind") == "statement":
            return None
    spec = _RECORD_COLLECTION_KEYS.get(envelope.command)
    if spec is None or not isinstance(envelope.data, dict):
        return None
    key, scalar_name = spec
    if key not in envelope.data:
        return None
    values = envelope.data.get(key)
    if not isinstance(values, list):
        return None

    rows: list[dict[str, Any]] = []
    for value in values:
        if isinstance(value, dict):
            rows.append(dict(value))
        else:
            rows.append({scalar_name: value})

    columns: list[str] = []
    if envelope.command in {"query", "job.wait", "job.result", "data.sample"}:
        schema = envelope.data.get("schema")
        if isinstance(schema, list):
            columns = [
                str(column["name"])
                for column in schema
                if isinstance(column, dict) and column.get("name") is not None
            ]
    if not columns:
        columns = list(_RECORD_DEFAULT_COLUMNS.get(envelope.command, []))
    return rows, columns


def _control_record(envelope: Envelope) -> dict[str, Any]:
    payload = envelope.to_dict()
    error = payload.get("error") or {}
    return {
        "version": payload.get("version"),
        "command": payload.get("command"),
        "status": payload.get("status"),
        "error_code": error.get("code"),
        "error_message": error.get("message"),
        "suggestion": error.get("suggestion"),
        "recoverable": error.get("recoverable"),
        "data": payload.get("data"),
        "metadata": payload.get("metadata"),
        "agent_hints": payload.get("agent_hints"),
    }


def _emit_record_format(envelope: Envelope, output_format: str, stdout: TextIO) -> None:
    """Emit row records or one self-contained control/error record."""
    collection = _record_collection(envelope)
    if collection is None:
        if output_format == "ndjson":
            emit_ndjson([envelope.to_dict()], stdout)
        else:
            _emit_csv([_control_record(envelope)], stdout, columns=_CONTROL_RECORD_COLUMNS)
        return
    rows, columns = collection
    if output_format == "ndjson":
        emit_ndjson(rows, stdout)
    else:
        _emit_csv(rows, stdout, columns=columns)


def _csv_cell(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def _emit_csv(
    rows: list[dict[str, Any]],
    stdout: TextIO,
    *,
    columns: list[str] | None = None,
) -> None:
    resolved_columns = list(columns or [])
    for row in rows:
        for column in row:
            if column not in resolved_columns:
                resolved_columns.append(column)
    if not resolved_columns:
        stdout.write("\n")
        return
    writer = csv.writer(stdout, lineterminator="\n")
    writer.writerow(resolved_columns)
    for row in rows:
        writer.writerow([_csv_cell(row.get(column, "")) for column in resolved_columns])


def _publish_result_output(
    envelope: Envelope,
    output_path: Path,
    output_format: str,
    *,
    overwrite: bool,
    max_rows: int,
    operation: str,
) -> bool:
    """Publish only completed result data and preserve resumability otherwise."""
    original_status = envelope.status
    envelope.metadata.update(
        {
            "output_path": str(output_path),
            "output_format": output_format,
            "output_overwrite": overwrite,
            "output_written": False,
            "output_deferred": original_status == "pending",
        }
    )

    if original_status == "pending":
        envelope.metadata["remote_submission_succeeded"] = True
        _attach_deferred_output_action(
            envelope,
            output_path=output_path,
            output_format=output_format,
            overwrite=overwrite,
            max_rows=max_rows,
        )
        return False
    if original_status != "success":
        return False

    # Set this before serialization so a JSON result file describes its own
    # successful publication. The flag is reverted if atomic publication fails.
    envelope.metadata["output_written"] = True
    try:
        _write_output_file(
            envelope,
            str(output_path),
            output_format,
            overwrite=overwrite,
        )
    except OSError as exc:
        envelope.status = "failure"
        envelope.metadata.update(
            {
                "output_written": False,
                "remote_execution_status": original_status,
                "remote_execution_succeeded": True,
                "remote_submission_succeeded": True,
            }
        )
        envelope.error = ErrorPayload(
            code="OUTPUT_WRITE_FAILED",
            message=(
                f"The {operation} returned `{original_status}`, but the local "
                f"output file `{output_path}` could not be published: {exc}"
            ),
            suggestion=(
                "Do not rerun or resubmit the SQL solely for this file error. The remote "
                "result remains authoritative; fetch that same job result to "
                "another path or deliberately use --overwrite."
            ),
            recoverable=True,
        )
        envelope.agent_hints = envelope.agent_hints or AgentHints()
        envelope.agent_hints.warnings.append(
            "Remote execution already succeeded; resubmitting the SQL may duplicate work."
        )
        return True
    return False


def _attach_deferred_output_action(
    envelope: Envelope,
    *,
    output_path: Path,
    output_format: str,
    overwrite: bool,
    max_rows: int,
) -> None:
    """Carry explicit output intent into the exact resumable job-result action."""
    data = envelope.data if isinstance(envelope.data, dict) else {}
    metadata = envelope.metadata if isinstance(envelope.metadata, dict) else {}
    job_id = data.get("job_id") or metadata.get("job_id")
    envelope.agent_hints = envelope.agent_hints or AgentHints()
    envelope.agent_hints.warnings.append(
        "The job is still pending; no result file was created. Fetch the same "
        "job when it completes instead of resubmitting the SQL."
    )
    if not isinstance(job_id, str) or not job_id.strip():
        return
    resume_metadata = {
        "job_id": job_id,
        "project": metadata.get("project"),
        "output_path": str(output_path),
        "output_format": output_format,
        "output_overwrite": overwrite,
    }
    resume_action = action(
        "job.result",
        data={"job_id": job_id, "max_rows": max_rows},
        metadata=resume_metadata,
        effect="local_write",
    )
    envelope.agent_hints.actions = [
        resume_action,
        *(
            candidate
            for candidate in envelope.agent_hints.actions
            if candidate.id != "job.result"
        ),
    ]


def _prepare_output_path(raw_path: str, *, overwrite: bool) -> Path:
    """Validate local publication before a query can cause remote effects."""
    path = Path(os.path.abspath(Path(raw_path).expanduser()))
    if path.is_dir():
        raise ValidationError(
            f"`--output` must name a file, but `{path}` is a directory."
        )
    if os.path.lexists(path) and not overwrite:
        raise ValidationError(
            f"Output file `{path}` already exists.",
            suggestion="Choose another path or add --overwrite deliberately.",
        )
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.preflight.",
            suffix=".tmp",
        ):
            pass
    except OSError as exc:
        raise ValidationError(
            f"Output directory `{path.parent}` is not writable: {exc}",
            suggestion="Choose a writable --output path before running the query.",
        ) from exc
    return path


def _write_output_file(
    envelope: Envelope,
    raw_path: str,
    output_format: str,
    *,
    overwrite: bool = False,
) -> Path:
    path = Path(os.path.abspath(Path(raw_path).expanduser()))
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            if output_format in {"csv", "ndjson"}:
                _emit_record_format(envelope, output_format, handle)
            elif output_format == "table":
                handle.write(_render_human(envelope) + "\n")
            else:
                emit_json(envelope.to_dict(), handle)
            handle.flush()
            os.fsync(handle.fileno())
        if overwrite:
            os.replace(temporary_path, path)
            temporary_path = None
        else:
            # A hard-link publication is atomic and fails if another process
            # creates the destination after preflight. The temporary file is
            # in the same directory, so cross-device links are impossible.
            os.link(temporary_path, path)
            temporary_path.unlink()
            temporary_path = None
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
    return path


def _command_name(args: argparse.Namespace) -> str:
    resolved = getattr(args, "resolved_command", None)
    if resolved:
        return resolved
    if not getattr(args, "command_group", None):
        return ""
    parts = [args.command_group]
    for attr in (
        "job_command",
        "meta_command",
        "semantic_command",
        "session_command",
        "data_command",
        "auth_command",
        "agent_command",
        "agent_skill_command",
        "cache_command",
        "skill_command",
    ):
        value = getattr(args, attr, None)
        if value:
            parts.append(value)
    return ".".join(parts)


def _should_audit_failure(args: argparse.Namespace) -> bool:
    """Preserve the manifest's zero-write discovery guarantees on failures."""
    command = _command_name(args)
    if command in _MANIFEST_CONFIRMED_NO_AUDIT_COMMANDS:
        return False
    if command == "agent.doctor" and not getattr(args, "online", False):
        return False
    return True


# Commands whose handler operates without an ODPS connection — they read
# the local config / SQLite cache only. Skipping backend construction lets
# them run on a fresh machine before `auth login`, and avoids paying the
# pyodps client construction cost.
_LOCAL_ONLY_COMMANDS = frozenset({
    "auth.login",
    "auth.login-external",
    "auth.logout",
    "auth.whoami",
    "session.set",
    "session.show",
    "session.unset",
    "agent.context",
    "agent.doctor",
    "agent.manifest",
    "agent.skill",
    "agent.skill.install",
    "agent.skill.update",
    "agent.skill.uninstall",
    "agent.skill.list",
    "agent.skill.diff",
    "agent.skill.path",
    "meta.semantic.set",
    "meta.semantic.get",
    "meta.semantic.list-missing",
    "meta.semantic.clear",
    "cache.build-status",
    "cache.status",
    "cache.clear",
})


def _should_load_backend(command_name: str) -> bool:
    return command_name not in _LOCAL_ONLY_COMMANDS


# Commands that must NOT trigger the auto-redirect to `auth login` — either
# because they don't need auth at all, or because they're the redirect target
# (recursing would loop forever). Kept in sync with CLAUDE.md's promise that
# `auth.*`, `session.*`, `agent.*`, and `cache.*` never redirect.
_AUTO_LOGIN_EXEMPT_COMMANDS = frozenset(_LOCAL_ONLY_COMMANDS | {
    "auth.can-i",
    "cache.build",
    "cache.build-status",
})


def _auth_seems_configured(app: MaxCApp) -> bool:
    """Cheap heuristic: do we have AK/SK from any source the backend will see?

    Checks the loaded config first, then env-var aliases. Avoids constructing
    OdpsBackend (which would trigger pyodps and fail loudly on missing creds).
    """
    import os

    from .helpers import ODPS_ENV_ALIASES
    auth = app.config.auth
    if auth.access_id and auth.secret_access_key:
        return True
    if auth.external.is_configured() or auth.ncs.is_configured():
        return True
    if auth.oauth.is_configured():
        return True
    has_ak = any(os.environ.get(a) for a in ODPS_ENV_ALIASES["access_id"])
    has_sk = any(os.environ.get(a) for a in ODPS_ENV_ALIASES["secret_access_key"])
    return has_ak and has_sk


def _resolve_query_mode(args: argparse.Namespace) -> tuple[str, list[str]]:
    mode = args.mode
    sql_parts = list(args.sql_parts)
    alias = sql_parts[0].lower() if sql_parts else ""
    if mode != "run":
        if alias in {"run", "cost", "explain"} and (len(sql_parts) > 1 or args.file or args.stdin):
            raise ValidationError("Do not combine query subcommands with `--mode`; use `maxc query cost \"SQL\"` instead.")
        import warnings
        warnings.warn(
            "`--mode` is deprecated. Use subcommand style: `maxc query cost \"SQL\"` instead of `maxc query \"SQL\" --mode cost`.",
            DeprecationWarning,
            stacklevel=3,
        )
        return mode, sql_parts

    if alias in {"run", "cost", "explain"}:
        if len(sql_parts) > 1 or args.file or args.stdin:
            return alias, sql_parts[1:]
        # Alias with no SQL anywhere — caller meant the subcommand, not literal
        # SQL text. Surface a clean missing-SQL error instead of silently
        # treating the alias word as SQL.
        raise ValidationError(
            f"`query {alias}` requires SQL.",
            suggestion=(
                f"Provide SQL inline (`maxc query {alias} \"SELECT 1\"`), "
                f"via `--file`, or via `--stdin`."
            ),
        )
    return mode, sql_parts


def _validate_query_analysis_args(args: argparse.Namespace, mode: str) -> None:
    _ = mode
    unsupported = []
    if args.dry_run:
        unsupported.append("--dry-run")
    if args.cursor:
        unsupported.append("--cursor")
    if args.output:
        unsupported.append("--output")
    if args.output_format:
        unsupported.append("--output-format")
    if getattr(args, "wait", 10) != 10:
        unsupported.append("--wait")
    if getattr(args, "max_rows", 100) != 100:
        unsupported.append("--max-rows")
    if getattr(args, "page_size", None) is not None:
        unsupported.append("--page-size")
    if getattr(args, "cost_check", None) is not None:
        unsupported.append("--cost-check")
    if getattr(args, "idempotency_key", None):
        unsupported.append("--idempotency-key")
    if getattr(args, "retry_on", "") != "":
        unsupported.append("--retry-on")
    if getattr(args, "max_retries", 0) != 0:
        unsupported.append("--max-retries")
    if getattr(args, "retry_backoff", "fixed") != "fixed":
        unsupported.append("--retry-backoff")
    if getattr(args, "mcqa", None) is True:
        unsupported.append("--mcqa")
    if getattr(args, "maxqa", False):
        unsupported.append("--maxqa")
    if getattr(args, "no_mcqa", False):
        unsupported.append("--no-mcqa")
    if getattr(args, "mcqa_version", None):
        unsupported.append("--mcqa-version")
    if getattr(args, "quota", None):
        unsupported.append("--quota")
    if getattr(args, "mcqa_fallback", None) is not None:
        unsupported.append("--mcqa-fallback/--no-mcqa-fallback")
    if unsupported:
        raise ValidationError(
            f"{', '.join(unsupported)} cannot be combined with `query cost` or `query explain`."
        )
    if args.format in {"csv", "ndjson"}:
        raise _OutputFormatError(
            "`query cost` and `query explain` return analysis objects and cannot use "
            f"`--format {args.format}`.",
            suggestion="Use --format json or --format table for query analysis.",
        )


def _query_page_size(args: argparse.Namespace) -> int:
    return args.page_size if args.page_size is not None else args.max_rows


def _csv_arg_list(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _query_output_format(args: argparse.Namespace) -> str:
    if args.output_format:
        return args.output_format
    if args.output:
        suffix = Path(args.output).suffix.lower()
        if suffix == ".csv":
            return "csv"
        if suffix == ".ndjson":
            return "ndjson"
        if suffix == ".table":
            return "table"
    if args.format in {"json", "csv", "ndjson", "table"}:
        return args.format
    return "json"


def _query_default_format(app: MaxCApp, mode: str) -> str:
    if mode == "run":
        return app.config.default_format
    return "table"


_RECORD_FORMAT_COMMANDS = frozenset(_RECORD_COLLECTION_KEYS)


def _requested_query_mode(args: argparse.Namespace) -> str:
    """Resolve only the query mode needed for pre-backend output validation."""
    mode = getattr(args, "mode", "run")
    if mode != "run":
        return mode
    sql_parts = list(getattr(args, "sql_parts", []) or [])
    alias = str(sql_parts[0]).lower() if sql_parts else ""
    return alias if alias in {"cost", "explain"} else "run"


def _validate_output_request(args: argparse.Namespace, command_name: str) -> None:
    """Reject output combinations that cannot produce the requested shape.

    This runs before constructing the application/backend so a presentation
    error cannot occur after a mutating command has already executed.
    """
    if (
        command_name in {"query", "job.result"}
        and getattr(args, "output_format", None)
        and not getattr(args, "output", None)
    ):
        raise _OutputFormatError(
            "`--output-format` requires --output <path>.",
            suggestion="Add --output <path>, or remove --output-format.",
        )

    record_format = _requested_record_format(args)
    if record_format is None:
        return
    if command_name not in _RECORD_FORMAT_COMMANDS:
        supported = ", ".join(sorted(_RECORD_FORMAT_COMMANDS))
        raise _OutputFormatError(
            f"`--format {record_format}` requires a record-producing command; "
            f"`{command_name}` returns a structured object instead. "
            f"Supported commands: {supported}.",
            suggestion="Use --format json for structured non-record results.",
        )
    if command_name == "query" and _requested_query_mode(args) in {"cost", "explain"}:
        raise _OutputFormatError(
            "`query cost` and `query explain` return analysis objects and cannot use "
            f"`--format {record_format}`.",
            suggestion="Use --format json or --format table for query analysis.",
        )
    if command_name == "query" and getattr(args, "dry_run", False):
        raise _OutputFormatError(
            f"`query --dry-run` returns an analysis object, not records, and cannot use "
            f"`--format {record_format}`.",
            suggestion="Use --format json or --format table for dry-run output.",
        )
    if (
        command_name == "job.wait"
        and getattr(args, "stream", False)
        and record_format != "ndjson"
    ):
        raise _OutputFormatError(
            "`job wait --stream` emits NDJSON and cannot use --format csv.",
            suggestion="Remove --format csv, or use --format ndjson.",
        )

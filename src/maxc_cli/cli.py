
import argparse
import sys
from pathlib import Path
from typing import Any, Sequence, TextIO

from .app import MaxCApp, read_stdin
from .exceptions import MaxCError, ValidationError
from .models import Envelope
from .output import emit_json, emit_ndjson, render_key_values, render_table
from .utils import read_sql_input


def _add_required_subparsers(
    parser: 'argparse.ArgumentParser',
    *,
    dest: 'str',
):
    """Backport-required subparsers for Python 3.6."""
    subparsers = parser.add_subparsers(dest=dest)
    subparsers.required = True
    return subparsers


def build_parser() -> 'argparse.ArgumentParser':
    parser = argparse.ArgumentParser(prog="maxc", description="Agent-first MaxCompute CLI MVP")
    parser.add_argument("--config", help="Explicit path to a config file")

    subparsers = _add_required_subparsers(parser, dest="command_group")

    query_parser = subparsers.add_parser(
        "query",
        help="Run a SQL query (supports run/cost/explain aliases)",
        description=(
            "Run a SQL query.\n"
            "Recommended usage:\n"
            "  maxc query \"SELECT 1\"\n"
            "  maxc query cost \"SELECT 1\"\n"
            "  maxc query explain \"SELECT 1\"\n"
            "Legacy-compatible usage:\n"
            "  maxc query \"SELECT 1\" --mode cost"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    query_parser.add_argument("sql_parts", nargs="*", help="SQL text; `@natural` placeholders are reserved for future support")
    query_parser.add_argument("--file")
    query_parser.add_argument("--stdin", action="store_true")
    query_parser.add_argument("--project")
    query_parser.add_argument(
        "--mode",
        choices=["run", "cost", "explain"],
        default="run",
        help="Query mode. Legacy-compatible, but `query run/cost/explain <SQL>` is preferred.",
    )
    query_parser.add_argument("--format", choices=["table", "json", "csv", "ndjson"])
    query_parser.add_argument("--json", action="store_true")
    query_parser.add_argument("--max-rows", type=int, default=100)
    query_parser.add_argument("--page-size", type=int)
    query_parser.add_argument("--cursor")
    query_parser.add_argument("--output")
    query_parser.add_argument("--output-format", choices=["table", "json", "csv", "ndjson"])
    query_parser.add_argument("--timeout", type=int)
    query_parser.add_argument("--async", dest="async_mode", action="store_true")
    query_parser.add_argument("--dry-run", action="store_true")
    query_parser.add_argument("--cost-check", type=float)
    query_parser.add_argument("--idempotency-key")
    query_parser.add_argument("--retry-on", default="")
    query_parser.add_argument("--max-retries", type=int, default=0)
    query_parser.add_argument("--retry-backoff", choices=["fixed", "exponential"], default="fixed")
    query_parser.set_defaults(handler=_handle_query)

    job_parser = subparsers.add_parser("job", help="Manage async jobs")
    job_subparsers = _add_required_subparsers(job_parser, dest="job_command")

    job_submit = job_subparsers.add_parser("submit", help="Submit an async job")
    job_submit.add_argument("sql_parts", nargs="*")
    job_submit.add_argument("--file")
    job_submit.add_argument("--stdin", action="store_true")
    job_submit.add_argument("--project")
    job_submit.add_argument("--json", action="store_true")
    job_submit.add_argument("--max-rows", type=int, default=100)
    job_submit.add_argument("--cost-check", type=float)
    job_submit.add_argument("--idempotency-key")
    job_submit.set_defaults(handler=_handle_job_submit)

    job_status = job_subparsers.add_parser("status", help="Show job status")
    job_status.add_argument("job_id")
    job_status.add_argument("--json", action="store_true")
    job_status.set_defaults(handler=_handle_job_status)

    job_wait = job_subparsers.add_parser("wait", help="Wait for a job to finish")
    job_wait.add_argument("job_id")
    job_wait.add_argument("--json", action="store_true")
    job_wait.add_argument("--stream", action="store_true")
    job_wait.add_argument("--timeout", type=int, default=None, help="Timeout in seconds (default: 300)")
    job_wait.set_defaults(handler=_handle_job_wait)

    job_diagnose = job_subparsers.add_parser("diagnose", help="Diagnose job status and failure reasons")
    job_diagnose.add_argument("job_id")
    job_diagnose.add_argument("--json", action="store_true")
    job_diagnose.set_defaults(handler=_handle_job_diagnose)

    job_result = job_subparsers.add_parser("result", help="Fetch job results")
    job_result.add_argument("job_id")
    job_result.add_argument("--json", action="store_true")
    job_result.add_argument("--max-rows", type=int, default=100, dest="max_rows", help="Maximum rows to return (default: 100)")
    job_result.add_argument("--cursor", default=None, help="Pagination cursor from previous response")
    job_result.set_defaults(handler=_handle_job_result)

    job_cancel = job_subparsers.add_parser("cancel", help="Cancel a job")
    job_cancel.add_argument("job_id")
    job_cancel.add_argument("--json", action="store_true")
    job_cancel.set_defaults(handler=_handle_job_cancel)

    job_list = job_subparsers.add_parser("list", help="List jobs")
    job_list.add_argument("--json", action="store_true")
    job_list.add_argument("--limit", type=int, default=20, help="Maximum number of jobs to return (default: 20)")
    job_list.set_defaults(handler=_handle_job_list)

    meta_parser = subparsers.add_parser("meta", help="Metadata commands")
    meta_subparsers = _add_required_subparsers(meta_parser, dest="meta_command")

    meta_list = meta_subparsers.add_parser("list-tables", help="List tables")
    meta_list.add_argument("--json", action="store_true")
    meta_list.set_defaults(handler=_handle_meta_list_tables)

    meta_describe = meta_subparsers.add_parser("describe", help="Describe a table")
    meta_describe.add_argument("table_name")
    meta_describe.add_argument("--json", action="store_true")
    meta_describe.add_argument("--full", action="store_true", help="Show full column list (default is summary mode)")
    meta_describe.set_defaults(handler=_handle_meta_describe)

    meta_search = meta_subparsers.add_parser("search", help="Search tables")
    meta_search.add_argument("keyword")
    meta_search.add_argument("--json", action="store_true")
    meta_search.set_defaults(handler=_handle_meta_search)

    meta_search_columns = meta_subparsers.add_parser("search-columns", help="Search columns")
    meta_search_columns.add_argument("keyword")
    meta_search_columns.add_argument("--json", action="store_true")
    meta_search_columns.set_defaults(handler=_handle_meta_search_columns)

    meta_latest_partition = meta_subparsers.add_parser("latest-partition", help="Show the latest partition")
    meta_latest_partition.add_argument("table_name")
    meta_latest_partition.add_argument("--json", action="store_true")
    meta_latest_partition.set_defaults(handler=_handle_meta_latest_partition)

    meta_freshness = meta_subparsers.add_parser("freshness", help="Show table freshness")
    meta_freshness.add_argument("table_name")
    meta_freshness.add_argument("--json", action="store_true")
    meta_freshness.set_defaults(handler=_handle_meta_freshness)

    meta_lineage = meta_subparsers.add_parser("lineage", help="Show lineage")
    meta_lineage.add_argument("table_name")
    meta_lineage.add_argument("--json", action="store_true")
    meta_lineage.set_defaults(handler=_handle_meta_lineage)

    meta_partitions = meta_subparsers.add_parser("partitions", help="List partitions")
    meta_partitions.add_argument("table_name")
    meta_partitions.add_argument("--json", action="store_true")
    meta_partitions.set_defaults(handler=_handle_meta_partitions)

    meta_list_projects = meta_subparsers.add_parser("list-projects", help="List accessible projects")
    meta_list_projects.add_argument("--json", action="store_true")
    meta_list_projects.set_defaults(handler=_handle_meta_list_projects)

    meta_list_schemas = meta_subparsers.add_parser("list-schemas", help="List schemas in a project")
    meta_list_schemas.add_argument("--project")
    meta_list_schemas.add_argument("--json", action="store_true")
    meta_list_schemas.set_defaults(handler=_handle_meta_list_schemas)

    # Semantic metadata subcommands
    meta_semantic = meta_subparsers.add_parser("semantic", help="Semantic metadata management")
    meta_semantic_subparsers = _add_required_subparsers(
        meta_semantic,
        dest="semantic_command",
    )

    # semantic set
    semantic_set = meta_semantic_subparsers.add_parser("set", help="Set semantic metadata for a table")
    semantic_set.add_argument("table_name", help="Table name")
    semantic_set.add_argument("--desc", "--description", dest="semantic_desc", help="Table description")
    semantic_set.add_argument("--use-cases", nargs="*", help="Use cases (space-separated)")
    semantic_set.add_argument("--sample-questions", nargs="*", help="Sample questions (space-separated)")
    semantic_set.add_argument("--column-semantics", type=str, help="Column semantics as JSON string")
    semantic_set.add_argument("--relations", type=str, help="Relations as JSON string")
    semantic_set.add_argument("--stats", type=str, help="Stats as JSON string")
    semantic_set.add_argument("--json", action="store_true")
    semantic_set.set_defaults(handler=_handle_meta_semantic_set)

    # semantic get
    semantic_get = meta_semantic_subparsers.add_parser("get", help="Get semantic metadata for a table")
    semantic_get.add_argument("table_name", help="Table name")
    semantic_get.add_argument("--json", action="store_true")
    semantic_get.set_defaults(handler=_handle_meta_semantic_get)

    # semantic list-missing
    semantic_list_missing = meta_semantic_subparsers.add_parser("list-missing", help="List tables without semantic metadata")
    semantic_list_missing.add_argument("--json", action="store_true")
    semantic_list_missing.set_defaults(handler=_handle_meta_semantic_list_missing)

    session_parser = subparsers.add_parser("session", help="Session management - switch project/schema")
    session_subparsers = _add_required_subparsers(
        session_parser,
        dest="session_command",
    )

    session_set = session_subparsers.add_parser("set", help="Set current project and/or schema for this session")
    session_set.add_argument("--project", help="Project name")
    session_set.add_argument("--schema", help="Schema name")
    session_set.add_argument("--json", action="store_true")
    session_set.set_defaults(handler=_handle_session_set)

    session_show = session_subparsers.add_parser("show", help="Show current session settings")
    session_show.add_argument("--json", action="store_true")
    session_show.set_defaults(handler=_handle_session_show)

    session_unset = session_subparsers.add_parser("unset", help="Clear session override, revert to env/config")
    session_unset.add_argument("--json", action="store_true")
    session_unset.set_defaults(handler=_handle_session_unset)

    data_parser = subparsers.add_parser("data", help="Data exploration commands")
    data_subparsers = _add_required_subparsers(data_parser, dest="data_command")

    data_sample = data_subparsers.add_parser("sample", help="Sample rows")
    data_sample.add_argument("table_name")
    data_sample.add_argument("--rows", type=int, default=5)
    data_sample.add_argument("--partition")
    data_sample.add_argument("--columns")
    data_sample.add_argument("--json", action="store_true")
    data_sample.set_defaults(handler=_handle_data_sample)

    data_profile = data_subparsers.add_parser("profile", help="Profile table data")
    data_profile.add_argument("table_name")
    data_profile.add_argument("--partition")
    data_profile.add_argument("--json", action="store_true")
    data_profile.set_defaults(handler=_handle_data_profile)

    auth_parser = subparsers.add_parser("auth", help="Authentication and permission checks")
    auth_subparsers = _add_required_subparsers(auth_parser, dest="auth_command")

    auth_login = auth_subparsers.add_parser("login", help="Save MaxCompute login configuration")
    auth_login.add_argument("--access-id", "--access-key-id", dest="access_id")
    auth_login.add_argument(
        "--secret-access-key",
        "--access-key-secret",
        dest="secret_access_key",
    )
    auth_login.add_argument("--security-token")
    auth_login.add_argument("--project")
    auth_login.add_argument("--endpoint")
    auth_login.add_argument("--region", dest="region_name")
    auth_login.add_argument("--tunnel-endpoint")
    auth_login.add_argument("--from-env", action="store_true")
    auth_login.add_argument("--no-validate", action="store_true")
    auth_login.add_argument("--json", action="store_true")
    auth_login.set_defaults(handler=_handle_auth_login)

    auth_login_ncs = auth_subparsers.add_parser("login-ncs", help="Save ncs-based MaxCompute login configuration")
    auth_login_ncs.add_argument("--account-type", choices=["user", "account", "app"])
    auth_login_ncs.add_argument("--employee-id")
    auth_login_ncs.add_argument("--account-name")
    auth_login_ncs.add_argument("--app-name")
    auth_login_ncs.add_argument("--project")
    auth_login_ncs.add_argument("--endpoint")
    auth_login_ncs.add_argument("--region", dest="region_name")
    auth_login_ncs.add_argument("--tunnel-endpoint")
    auth_login_ncs.add_argument("--interactive", action="store_true")
    auth_login_ncs.add_argument("--list-accounts", action="store_true")
    auth_login_ncs.add_argument("--no-validate", action="store_true")
    auth_login_ncs.add_argument("--json", action="store_true")
    auth_login_ncs.set_defaults(handler=_handle_auth_login_ncs)

    auth_whoami = auth_subparsers.add_parser("whoami", help="Show the current identity")
    auth_whoami.add_argument("--json", action="store_true")
    auth_whoami.set_defaults(handler=_handle_auth_whoami)

    auth_can_i = auth_subparsers.add_parser("can-i", help="Check whether an operation is allowed")
    auth_can_i.add_argument("--table", required=True)
    auth_can_i.add_argument("--operation", required=True)
    auth_can_i.add_argument("--project")
    auth_can_i.add_argument("--brief", action="store_true")
    auth_can_i.add_argument("--json", action="store_true")
    auth_can_i.set_defaults(handler=_handle_auth_can_i)

    diff_parser = subparsers.add_parser("diff", help="Diff commands")
    diff_subparsers = _add_required_subparsers(diff_parser, dest="diff_command")

    diff_schema = diff_subparsers.add_parser("schema", help="Compare two table schemas")
    diff_schema.add_argument("left_table")
    diff_schema.add_argument("right_table")
    diff_schema.add_argument("--json", action="store_true")
    diff_schema.set_defaults(handler=_handle_diff_schema)

    diff_partition = diff_subparsers.add_parser("partition", help="Compare partition lists")
    diff_partition.add_argument("left_table")
    diff_partition.add_argument("right_table")
    diff_partition.add_argument("--json", action="store_true")
    diff_partition.set_defaults(handler=_handle_diff_partition)

    diff_data = diff_subparsers.add_parser("data", help="Compare read-only table snapshots by key")
    diff_data.add_argument("left_table")
    diff_data.add_argument("right_table")
    diff_data.add_argument("--keys", required=True, help="Comma-separated alignment key columns")
    diff_data.add_argument("--columns", help="Comma-separated non-key comparison columns; defaults to shared columns")
    diff_data.add_argument("--rows", type=int, default=100, help="Maximum rows to sample from each side")
    diff_data.add_argument("--partition", help="Partition applied to both tables")
    diff_data.add_argument("--left-partition")
    diff_data.add_argument("--right-partition")
    diff_data.add_argument("--json", action="store_true")
    diff_data.set_defaults(handler=_handle_diff_data)

    agent_parser = subparsers.add_parser("agent", help="Agent helper commands")
    agent_subparsers = _add_required_subparsers(agent_parser, dest="agent_command")

    agent_context = agent_subparsers.add_parser("context", help="Show agent context")
    agent_context.add_argument("--json", action="store_true")
    agent_context.set_defaults(handler=_handle_agent_context)

    cache_parser = subparsers.add_parser("cache", help="Metadata cache management")
    cache_subparsers = _add_required_subparsers(cache_parser, dest="cache_command")

    cache_build = cache_subparsers.add_parser("build", help="Build the metadata cache")
    cache_build.add_argument("--project")
    cache_build.add_argument("--schema", help="Target schema name")
    cache_build.add_argument("--async", dest="async_mode", action="store_true", help="Run the cache build asynchronously")
    cache_build.add_argument("--json", action="store_true")
    cache_build.set_defaults(handler=_handle_cache_build)

    cache_build_status = cache_subparsers.add_parser("build-status", help="Show cache build status")
    cache_build_status.add_argument("--project")
    cache_build_status.add_argument("--build-id", help="Build ID")
    cache_build_status.add_argument("--json", action="store_true")
    cache_build_status.set_defaults(handler=_handle_cache_build_status)

    cache_status = cache_subparsers.add_parser("status", help="Show cache status")
    cache_status.add_argument("--project")
    cache_status.add_argument("--schema", help="Target schema name")
    cache_status.add_argument("--json", action="store_true")
    cache_status.set_defaults(handler=_handle_cache_status)

    cache_clear = cache_subparsers.add_parser("clear", help="Clear cached metadata")
    cache_clear.add_argument("--project")
    cache_clear.add_argument("--schema", help="Target schema name")
    cache_clear.add_argument("--json", action="store_true")
    cache_clear.set_defaults(handler=_handle_cache_clear)

    cache_save_semantic = cache_subparsers.add_parser("save-semantic", help="Save semantic metadata")
    cache_save_semantic.add_argument("--table", required=True, help="Table name")
    cache_save_semantic.add_argument("--schema", default="default", help="Schema name (default: default)")
    cache_save_semantic.add_argument("--semantic-desc", required=True, help="One-sentence business description for the table")
    cache_save_semantic.add_argument("--use-cases", default="[]", help="Use cases as a JSON array")
    cache_save_semantic.add_argument("--sample-questions", default="[]", help="Sample questions as a JSON array")
    cache_save_semantic.add_argument("--column-semantics", default="[]", help="Column semantics as a JSON array")
    cache_save_semantic.add_argument("--project")
    cache_save_semantic.add_argument("--json", action="store_true")
    cache_save_semantic.set_defaults(handler=_handle_cache_save_semantic)

    cache_get_semantic = cache_subparsers.add_parser("get-semantic", help="Get semantic metadata")
    cache_get_semantic.add_argument("--table", required=True, help="Table name")
    cache_get_semantic.add_argument("--schema", default="default", help="Schema name (default: default)")
    cache_get_semantic.add_argument("--project")
    cache_get_semantic.add_argument("--json", action="store_true")
    cache_get_semantic.set_defaults(handler=_handle_cache_get_semantic)

    return parser


def main(argv: 'Sequence[str] | None' = None) -> 'int':
    return run(argv=argv)


def run(
    argv: 'Sequence[str] | None' = None,
    *,
    cwd: 'Path | None' = None,
    stdout: 'TextIO | None' = None,
    stderr: 'TextIO | None' = None,
) -> 'int':
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    parser = build_parser()
    args = parser.parse_args(argv)
    working_dir = cwd or Path.cwd()
    requested_config_path = Path(args.config).resolve() if args.config else None
    args.requested_config_path = requested_config_path
    args.stderr = stderr
    config_path = requested_config_path
    if (
        requested_config_path is not None
        and not requested_config_path.exists()
        and _command_name(args) in {"auth.login", "auth.login-ncs"}
    ):
        config_path = None

    app: 'MaxCApp | None' = None
    try:
        command_name = _command_name(args)
        app = MaxCApp(
            cwd=working_dir,
            config_path=config_path,
            load_backend=_should_load_backend(command_name),
        )
        args.handler(app, args, stdout)
        return 0
    except MaxCError as exc:
        if app is not None:
            app.log(
                _command_name(args),
                "failure",
                {},
                error=exc.to_payload().to_dict(),
            )
        if getattr(args, "json", False):
            payload = Envelope(
                command=_command_name(args),
                status="failure",
                error=exc.to_payload(),
            )
            emit_json(payload.to_dict(), stdout)
        else:
            stderr.write(f"[{exc.error_code}] {exc.message}\n")
            if exc.suggestion:
                stderr.write(f"Suggestion: {exc.suggestion}\n")
        return exc.exit_code


def _handle_query(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    mode, sql_parts = _resolve_query_mode(args)
    args.resolved_command = "query" if mode == "run" else f"query.{mode}"
    sql = read_sql_input(
        sql_parts,
        file_path=args.file,
        use_stdin=args.stdin,
        stdin_text=read_stdin() if args.stdin else None,
    )
    if mode == "cost":
        _validate_query_analysis_args(args, mode)
        envelope = app.query_cost(sql=sql, project=args.project)
    elif mode == "explain":
        _validate_query_analysis_args(args, mode)
        envelope = app.query_explain(sql=sql, project=args.project)
    else:
        retry_on = [item.strip() for item in args.retry_on.split(",") if item.strip()]
        envelope = app.query(
            command="query",
            sql=sql,
            project=args.project,
            max_rows=_query_page_size(args),
            cursor=args.cursor,
            dry_run=args.dry_run,
            async_mode=args.async_mode,
            cost_check=args.cost_check,
            idempotency_key=args.idempotency_key,
            retry_on=retry_on,
            max_retries=args.max_retries,
        )
    if args.output:
        output_format = _query_output_format(args)
        output_path = _write_output_file(envelope, args.output, output_format)
        envelope.metadata["output_path"] = str(output_path)
        envelope.metadata["output_format"] = output_format
    _emit_envelope(
        envelope,
        args=args,
        stdout=stdout,
        default_format=args.format or _query_default_format(app, mode),
    )


def _handle_job_submit(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    sql = read_sql_input(
        args.sql_parts,
        file_path=args.file,
        use_stdin=args.stdin,
        stdin_text=read_stdin() if args.stdin else None,
    )
    envelope = app.submit_job(
        sql=sql,
        project=args.project,
        max_rows=args.max_rows,
        cost_check=args.cost_check,
        idempotency_key=args.idempotency_key,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_status(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.job_status(args.job_id)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_wait(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope, events = app.job_wait(args.job_id, timeout=args.timeout)
    if args.stream:
        emit_ndjson(events, stdout)
        return
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_diagnose(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.job_diagnose(args.job_id)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_result(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.job_result(args.job_id, max_rows=args.max_rows, cursor=args.cursor)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_cancel(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.cancel_job(args.job_id)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_list(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.list_jobs(limit=args.limit)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_list_tables(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_list_tables()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_describe(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_describe(args.table_name, full=args.full)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_search(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_search(args.keyword)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_search_columns(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_search_columns(args.keyword)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_latest_partition(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_latest_partition(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_freshness(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_freshness(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_lineage(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_lineage(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_partitions(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_partitions(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_list_projects(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_list_projects()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_list_schemas(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.meta_list_schemas(project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_semantic_set(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
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
                error={
                    "code": "INVALID_JSON",
                    "message": f"Invalid JSON for --column-semantics: {e}",
                    "recoverable": True,
                    "suggestion": "Provide valid JSON for the --column-semantics argument.",
                },
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
                error={
                    "code": "INVALID_JSON",
                    "message": f"Invalid JSON for --relations: {e}",
                    "recoverable": True,
                    "suggestion": "Provide valid JSON for the --relations argument.",
                },
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
                error={
                    "code": "INVALID_JSON",
                    "message": f"Invalid JSON for --stats: {e}",
                    "recoverable": True,
                    "suggestion": "Provide valid JSON for the --stats argument.",
                },
            )
            _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
            return

    envelope = app.semantic_set(
        table_name=args.table_name,
        semantic_desc=args.semantic_desc,
        use_cases=args.use_cases,
        sample_questions=args.sample_questions,
        column_semantics=column_semantics,
        relations=relations,
        stats=stats,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_semantic_get(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    """Handle semantic get command."""
    envelope = app.semantic_get(table_name=args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_semantic_list_missing(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    """Handle semantic list-missing command."""
    envelope = app.semantic_list_missing()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_session_set(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    """Set current project and/or schema for the session."""
    project = args.project
    schema = args.schema
    
    if not project and not schema:
        raise ValidationError("At least one of `--project` or `--schema` must be specified.")
    
    envelope = app.session_set(project=project, schema=schema)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_session_show(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    """Show current session settings."""
    envelope = app.session_show()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_session_unset(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    """Clear session override."""
    envelope = app.session_unset()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_data_sample(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    columns = _csv_arg_list(args.columns)
    envelope = app.data_sample(
        args.table_name,
        rows=args.rows,
        partition=args.partition,
        columns=columns or None,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_data_profile(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.data_profile(args.table_name, partition=args.partition)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_login(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
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
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_login_ncs(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.auth_login_ncs(
        account_type=args.account_type,
        employee_id=args.employee_id,
        account_name=args.account_name,
        app_name=args.app_name,
        project=args.project,
        endpoint=args.endpoint,
        region_name=args.region_name,
        tunnel_endpoint=args.tunnel_endpoint,
        interactive=args.interactive,
        list_accounts_mode=args.list_accounts,
        no_validate=args.no_validate,
        target_config_path=args.requested_config_path,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_whoami(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.auth_whoami()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_can_i(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.auth_can_i(
        table_name=args.table,
        operation=args.operation,
        project=args.project,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_diff_schema(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.schema_diff(args.left_table, args.right_table)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_diff_partition(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.partition_diff(args.left_table, args.right_table)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_diff_data(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.data_diff(
        args.left_table,
        args.right_table,
        keys=_csv_arg_list(args.keys),
        columns=_csv_arg_list(args.columns) or None,
        rows=args.rows,
        partition=args.partition,
        left_partition=args.left_partition,
        right_partition=args.right_partition,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_agent_context(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.agent_context()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_build(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    """Build the metadata cache.

    JSON or async invocations return the standard envelope contract.
    Human-mode synchronous builds keep the incremental terminal progress output.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import time
    import uuid

    is_json_mode = getattr(args, "json", False)
    is_async_mode = getattr(args, "async_mode", False)
    target_project = args.project or app.config.default_project
    schema_name = getattr(args, "schema", None)
    max_workers = 8

    if is_async_mode:
        envelope = app.cache_build(
            project=args.project,
            schema_name=schema_name,
            async_mode=is_async_mode,
        )
        _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
        return

    if is_json_mode:
        progress_stream = getattr(args, "stderr", None) or sys.stderr
        last_progress_emit = 0.0

        def emit_progress(event: 'dict[str, Any]') -> 'None':
            nonlocal last_progress_emit
            event_type = str(event.get("type", ""))
            now = time.monotonic()
            if event_type == "listing_start":
                progress_stream.write("Fetching table list...\n")
                progress_stream.flush()
                return
            if event_type == "listing_complete":
                total = int(event.get("total_tables", 0))
                progress_stream.write(f"Discovered {total} table(s), starting cache build...\n")
                progress_stream.flush()
                return
            if event_type == "progress":
                if now - last_progress_emit < 0.5:
                    return
                last_progress_emit = now
                progress_stream.write(
                    "\rProgress: {cached}/{total} tables cached (failed: {failed})".format(
                        cached=event.get("cached_tables", 0),
                        total=event.get("total_tables", 0),
                        failed=event.get("failed_tables", 0),
                    )
                )
                progress_stream.flush()
                return
            if event_type == "completed":
                progress_stream.write(
                    "\rProgress: {cached}/{total} tables cached (failed: {failed})\n".format(
                        cached=event.get("cached_tables", 0),
                        total=event.get("total_tables", 0),
                        failed=event.get("failed_tables", 0),
                    )
                )
                progress_stream.flush()

        envelope = app.cache_build(
            project=args.project,
            schema_name=schema_name,
            async_mode=False,
            progress_callback=emit_progress,
        )
        _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")
        return

    # Phase 1: Single-threaded list all tables
    stdout.write("Fetching table list...\n")
    stdout.flush()
    
    tables = app.backend.list_tables()
    total = len(tables)
    
    if total == 0:
        stdout.write("No tables found, cache build completed.\n")
        return

    build_id = str(uuid.uuid4())[:8]
    app.cache.start_build(target_project, build_id, total)

    # Output initial state
    stdout.write(f"Target project: {target_project}\n")
    if schema_name:
        stdout.write(f"Target schema: {schema_name}\n")
    stdout.write(f"Discovered {total} table(s), starting cache build...\n\n")
    stdout.flush()

    # Phase 2: Multi-threaded fetch schema for each table
    cached_count = 0
    errors: 'list[str]' = []
    lock = threading.Lock()
    last_progress_time = time.monotonic()
    progress_interval = 3.0  # seconds

    def fetch_and_cache(table_name: 'str') -> 'tuple[str, str | None]':
        """Fetch and cache a table. Returns (table_name, error_or_none)."""
        import concurrent.futures
        
        def _do_fetch():
            # Use a simpler approach: only get table metadata without sample rows
            # to avoid potential hangs on table.head() or iterate_partitions()
            table = app.backend._get_table(table_name)
            # Force reload to get full schema info
            if hasattr(table, 'reload'):
                table.reload()
            
            columns = [
                {"name": c.name, "type": str(c.type), "comment": getattr(c, 'comment', '') or ''}
                for c in getattr(table.table_schema, 'columns', [])
            ]
            
            # Get partition columns but don't fetch actual partitions (can be slow)
            partitions = [
                c.name for c in getattr(table.table_schema, 'partitions', [])
            ]
            
            row_count = int(getattr(table, 'record_num', -1) or -1)
            size_bytes = int(getattr(table, 'size', 0)) if getattr(table, 'size', None) else None
            
            app.cache.cache_table(
                project=target_project,
                table_name=table.name,
                description=getattr(table, 'comment', '') or '',
                columns=columns,
                partitions=partitions,
                row_count=row_count if row_count >= 0 else None,
                size_bytes=size_bytes,
                owner=getattr(table, 'owner', None),
                schema_name=schema_name or "default",
            )
            return table_name, None
        
        # Run with timeout to prevent hanging
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_do_fetch)
            try:
                return future.result(timeout=30)  # 30 second timeout per table
            except concurrent.futures.TimeoutError:
                return table_name, f"{table_name}: timeout after 30s"
            except Exception as exc:
                return table_name, f"{table_name}: {exc}"

    def emit_progress(force: 'bool' = False) -> 'None':
        nonlocal last_progress_time
        current_time = time.monotonic()
        
        _ = force
        if current_time - last_progress_time >= progress_interval:
            last_progress_time = current_time
        stdout.write(f"\rProgress: {cached_count}/{total} tables cached (failed: {len(errors)})")
        stdout.flush()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_and_cache, t.name): t.name for t in tables}
        
        for future in as_completed(futures):
            table_name, error = future.result()

            with lock:
                if error:
                    errors.append(error)
                else:
                    cached_count += 1

            # Move emit_progress outside the lock to avoid holding lock during IO
            emit_progress()

            # Update build progress in DB (can be done outside lock)
            app.cache.update_build_progress(
                target_project, build_id, cached_count, len(errors)
            )

    # Complete build
    if errors:
        app.cache.complete_build(target_project, build_id, error_message=f"{len(errors)} errors")
    else:
        app.cache.complete_build(target_project, build_id)

    # Final output
    stdout.write("\n\n")
    
    if not errors:
        stdout.write("Cache build completed successfully!\n")
        stdout.write(f"  Tables cached: {cached_count}/{total}\n")
    else:
        stdout.write("Cache build completed with errors.\n")
        stdout.write(f"  Succeeded: {cached_count}\n")
        stdout.write(f"  Failed: {len(errors)}\n")
        error_list = errors[:5]
        if error_list:
            stdout.write("\nError details:\n")
            for error in error_list:
                stdout.write(f"  - {error}\n")
        if len(errors) > 5:
            stdout.write(f"  ... and {len(errors) - 5} more error(s)\n")

    stats = app.cache.get_cache_stats(target_project)
    if stats:
        stdout.write("\nCache stats:\n")
        stdout.write(f"  Total tables: {stats.get('table_count', 0)}\n")
        if stats.get("oldest"):
            stdout.write(f"  Oldest update: {stats.get('oldest')}\n")
        if stats.get("newest"):
            stdout.write(f"  Newest update: {stats.get('newest')}\n")

    schemas = app.cache.get_schemas(target_project)
    if schemas:
        stdout.write("\nCached schemas:\n")
        for schema in schemas:
            stdout.write(f"  - {schema}\n")

    stdout.write("\n")
    stdout.flush()

    app.log("cache.build", "success" if not errors else "partial", {"project": target_project})



def _handle_cache_build_status(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.cache_build_status(
        project=args.project,
        build_id=getattr(args, 'build_id', None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_status(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.cache_status(
        project=args.project,
        schema_name=getattr(args, 'schema', None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_clear(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.cache_clear(
        project=args.project,
        schema_name=getattr(args, 'schema', None),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_save_semantic(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    import json as json_module
    envelope = app.cache_save_semantic(
        table_name=args.table,
        semantic_desc=args.semantic_desc,
        use_cases=json_module.loads(args.use_cases),
        sample_questions=json_module.loads(args.sample_questions),
        column_semantics=json_module.loads(args.column_semantics),
        project=args.project,
        schema_name=getattr(args, 'schema', 'default'),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_get_semantic(app: 'MaxCApp', args: 'argparse.Namespace', stdout: 'TextIO') -> 'None':
    envelope = app.cache_get_semantic(
        table_name=args.table,
        project=args.project,
        schema_name=getattr(args, 'schema', 'default'),
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _emit_envelope(
    envelope: 'Envelope',
    *,
    args: 'argparse.Namespace',
    stdout: 'TextIO',
    default_format: 'str',
) -> 'None':
    if getattr(args, "brief", False):
        stdout.write(_render_brief(envelope) + "\n")
        return
    if getattr(args, "json", False):
        emit_json(envelope.to_dict(), stdout)
        return

    if default_format == "ndjson":
        rows = envelope.data.get("rows", [])
        emit_ndjson(rows, stdout)
        return
    if default_format == "csv":
        _emit_csv(envelope.data.get("rows", []), stdout)
        return
    if default_format == "json":
        emit_json(envelope.data, stdout)
        return

    stdout.write(_render_human(envelope) + "\n")


def _render_brief(envelope: 'Envelope') -> 'str':
    if envelope.command == "auth.can-i":
        return "ALLOWED" if envelope.data.get("allowed") else "DENIED"
    return envelope.status.upper()


def _render_human(envelope: 'Envelope') -> 'str':
    command = envelope.command
    data = envelope.data
    metadata = envelope.metadata

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

    if command in {"meta.search", "meta.search-columns"}:
        return render_table(data.get("matches", []))

    if command == "data.sample":
        return render_table(data.get("rows", []))

    if command == "skill.list":
        return render_table(data.get("skills", []))

    return render_key_values(data if isinstance(data, dict) else {"value": data})


def _emit_csv(rows: 'list[dict[str, Any]]', stdout: 'TextIO') -> 'None':
    if not rows:
        stdout.write("\n")
        return
    columns = list(rows[0])
    stdout.write(",".join(columns) + "\n")
    for row in rows:
        stdout.write(",".join(str(row.get(column, "")) for column in columns) + "\n")


def _write_output_file(envelope: 'Envelope', raw_path: 'str', output_format: 'str') -> 'Path':
    path = Path(raw_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        if output_format == "ndjson":
            emit_ndjson(envelope.data.get("rows", []), handle)
        elif output_format == "csv":
            _emit_csv(envelope.data.get("rows", []), handle)
        elif output_format == "table":
            handle.write(render_table(envelope.data.get("rows", [])) + "\n")
        else:
            emit_json(envelope.data, handle)
    return path


def _command_name(args: 'argparse.Namespace') -> 'str':
    resolved = getattr(args, "resolved_command", None)
    if resolved:
        return resolved
    parts = [args.command_group]
    for attr in (
        "job_command",
        "meta_command",
        "semantic_command",
        "session_command",
        "data_command",
        "auth_command",
        "diff_command",
        "agent_command",
        "cache_command",
        "skill_command",
    ):
        value = getattr(args, attr, None)
        if value:
            parts.append(value)
    return ".".join(parts)


def _should_load_backend(command_name: 'str') -> 'bool':
    return command_name not in {
        "auth.login",
        "auth.login-ncs",
        "auth.whoami",
        "session.set",
        "session.show",
        "session.unset",
        "agent.context",
    }


def _resolve_query_mode(args: 'argparse.Namespace') -> 'tuple[str, list[str]]':
    mode = args.mode
    sql_parts = list(args.sql_parts)
    alias = sql_parts[0].lower() if sql_parts else ""
    if mode != "run":
        if alias in {"run", "cost", "explain"} and (len(sql_parts) > 1 or args.file or args.stdin):
            raise ValidationError("Do not combine query mode aliases with `--mode`; pick one style.")
        return mode, sql_parts

    if alias in {"run", "cost", "explain"} and (len(sql_parts) > 1 or args.file or args.stdin):
        return alias, sql_parts[1:]
    return mode, sql_parts


def _validate_query_analysis_args(args: 'argparse.Namespace', mode: 'str') -> 'None':
    _ = mode
    unsupported = []
    if args.async_mode:
        unsupported.append("--async")
    if args.dry_run:
        unsupported.append("--dry-run")
    if args.cursor:
        unsupported.append("--cursor")
    if args.output:
        unsupported.append("--output")
    if args.output_format:
        unsupported.append("--output-format")
    if unsupported:
        raise ValidationError(
            f"{', '.join(unsupported)} cannot be combined with `query cost` or `query explain`."
        )
    if args.format in {"csv", "ndjson"}:
        raise ValidationError("`query cost` and `query explain` support only `table` or `json` output.")


def _query_page_size(args: 'argparse.Namespace') -> 'int':
    return args.page_size if args.page_size is not None else args.max_rows


def _csv_arg_list(value: 'str | None') -> 'list[str]':
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _query_output_format(args: 'argparse.Namespace') -> 'str':
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


def _query_default_format(app: 'MaxCApp', mode: 'str') -> 'str':
    if mode == "run":
        return app.config.default_format
    return "table"

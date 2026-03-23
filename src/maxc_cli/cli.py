from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Sequence, TextIO

from .app import MaxCApp, read_stdin
from .exceptions import MaxCError, ValidationError
from .models import Envelope
from .output import emit_json, emit_ndjson, render_key_values, render_table
from .utils import read_sql_input


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="maxc", description="Agent-first MaxCompute CLI MVP")
    parser.add_argument("--config", help="显式指定配置文件路径")

    subparsers = parser.add_subparsers(dest="command_group", required=True)

    query_parser = subparsers.add_parser("query", help="执行 SQL 查询")
    query_parser.add_argument("sql_parts", nargs="*", help="SQL 文本，支持 @natural 前缀占位")
    query_parser.add_argument("--file")
    query_parser.add_argument("--stdin", action="store_true")
    query_parser.add_argument("--project")
    query_parser.add_argument("--mode", choices=["run", "cost", "explain"], default="run")
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

    job_parser = subparsers.add_parser("job", help="异步任务管理")
    job_subparsers = job_parser.add_subparsers(dest="job_command", required=True)

    job_submit = job_subparsers.add_parser("submit", help="提交异步任务")
    job_submit.add_argument("sql_parts", nargs="*")
    job_submit.add_argument("--file")
    job_submit.add_argument("--stdin", action="store_true")
    job_submit.add_argument("--project")
    job_submit.add_argument("--json", action="store_true")
    job_submit.add_argument("--max-rows", type=int, default=100)
    job_submit.add_argument("--cost-check", type=float)
    job_submit.add_argument("--idempotency-key")
    job_submit.set_defaults(handler=_handle_job_submit)

    job_status = job_subparsers.add_parser("status", help="查看任务状态")
    job_status.add_argument("job_id")
    job_status.add_argument("--json", action="store_true")
    job_status.set_defaults(handler=_handle_job_status)

    job_wait = job_subparsers.add_parser("wait", help="等待任务完成")
    job_wait.add_argument("job_id")
    job_wait.add_argument("--json", action="store_true")
    job_wait.add_argument("--stream", action="store_true")
    job_wait.set_defaults(handler=_handle_job_wait)

    job_diagnose = job_subparsers.add_parser("diagnose", help="诊断任务状态与失败原因")
    job_diagnose.add_argument("job_id")
    job_diagnose.add_argument("--json", action="store_true")
    job_diagnose.set_defaults(handler=_handle_job_diagnose)

    job_result = job_subparsers.add_parser("result", help="获取任务结果")
    job_result.add_argument("job_id")
    job_result.add_argument("--json", action="store_true")
    job_result.set_defaults(handler=_handle_job_result)

    job_cancel = job_subparsers.add_parser("cancel", help="取消任务")
    job_cancel.add_argument("job_id")
    job_cancel.add_argument("--json", action="store_true")
    job_cancel.set_defaults(handler=_handle_job_cancel)

    job_list = job_subparsers.add_parser("list", help="列出任务")
    job_list.add_argument("--json", action="store_true")
    job_list.set_defaults(handler=_handle_job_list)

    meta_parser = subparsers.add_parser("meta", help="元数据命令")
    meta_subparsers = meta_parser.add_subparsers(dest="meta_command", required=True)

    meta_list = meta_subparsers.add_parser("list-tables", help="列出表")
    meta_list.add_argument("--json", action="store_true")
    meta_list.set_defaults(handler=_handle_meta_list_tables)

    meta_describe = meta_subparsers.add_parser("describe", help="查看表结构")
    meta_describe.add_argument("table_name")
    meta_describe.add_argument("--json", action="store_true")
    meta_describe.set_defaults(handler=_handle_meta_describe)

    meta_search = meta_subparsers.add_parser("search", help="搜索表")
    meta_search.add_argument("keyword")
    meta_search.add_argument("--json", action="store_true")
    meta_search.set_defaults(handler=_handle_meta_search)

    meta_search_columns = meta_subparsers.add_parser("search-columns", help="搜索列")
    meta_search_columns.add_argument("keyword")
    meta_search_columns.add_argument("--json", action="store_true")
    meta_search_columns.set_defaults(handler=_handle_meta_search_columns)

    meta_latest_partition = meta_subparsers.add_parser("latest-partition", help="查看最新分区")
    meta_latest_partition.add_argument("table_name")
    meta_latest_partition.add_argument("--json", action="store_true")
    meta_latest_partition.set_defaults(handler=_handle_meta_latest_partition)

    meta_freshness = meta_subparsers.add_parser("freshness", help="查看表新鲜度")
    meta_freshness.add_argument("table_name")
    meta_freshness.add_argument("--json", action="store_true")
    meta_freshness.set_defaults(handler=_handle_meta_freshness)

    meta_lineage = meta_subparsers.add_parser("lineage", help="查看血缘")
    meta_lineage.add_argument("table_name")
    meta_lineage.add_argument("--json", action="store_true")
    meta_lineage.set_defaults(handler=_handle_meta_lineage)

    meta_partitions = meta_subparsers.add_parser("partitions", help="查看分区")
    meta_partitions.add_argument("table_name")
    meta_partitions.add_argument("--json", action="store_true")
    meta_partitions.set_defaults(handler=_handle_meta_partitions)

    meta_list_projects = meta_subparsers.add_parser("list-projects", help="列出可访问的项目")
    meta_list_projects.add_argument("--json", action="store_true")
    meta_list_projects.set_defaults(handler=_handle_meta_list_projects)

    meta_list_schemas = meta_subparsers.add_parser("list-schemas", help="列出项目中的 schemas")
    meta_list_schemas.add_argument("--project")
    meta_list_schemas.add_argument("--json", action="store_true")
    meta_list_schemas.set_defaults(handler=_handle_meta_list_schemas)

    project_parser = subparsers.add_parser("project", help="项目相关命令")
    project_subparsers = project_parser.add_subparsers(dest="project_command", required=True)

    project_use = project_subparsers.add_parser("use", help="切换默认项目")
    project_use.add_argument("project_name", help="项目名称")
    project_use.add_argument("--schema", help="默认 schema（可选）")
    project_use.add_argument("--json", action="store_true")
    project_use.set_defaults(handler=_handle_project_use)

    project_info = project_subparsers.add_parser("info", help="查看项目详情")
    project_info.add_argument("project_name", nargs="?", help="项目名称，默认为当前项目")
    project_info.add_argument("--json", action="store_true")
    project_info.set_defaults(handler=_handle_project_info)

    data_parser = subparsers.add_parser("data", help="数据探查命令")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)

    data_sample = data_subparsers.add_parser("sample", help="采样数据")
    data_sample.add_argument("table_name")
    data_sample.add_argument("--rows", type=int, default=5)
    data_sample.add_argument("--partition")
    data_sample.add_argument("--columns")
    data_sample.add_argument("--json", action="store_true")
    data_sample.set_defaults(handler=_handle_data_sample)

    data_profile = data_subparsers.add_parser("profile", help="剖析数据")
    data_profile.add_argument("table_name")
    data_profile.add_argument("--partition")
    data_profile.add_argument("--json", action="store_true")
    data_profile.set_defaults(handler=_handle_data_profile)

    auth_parser = subparsers.add_parser("auth", help="认证与权限检查")
    auth_subparsers = auth_parser.add_subparsers(dest="auth_command", required=True)

    auth_login = auth_subparsers.add_parser("login", help="保存 MaxCompute 登录配置")
    auth_login.add_argument("--access-id", "--access-key-id", dest="access_id")
    auth_login.add_argument(
        "--secret-access-key",
        "--access-key-secret",
        dest="secret_access_key",
    )
    auth_login.add_argument("--project")
    auth_login.add_argument("--endpoint")
    auth_login.add_argument("--region", dest="region_name")
    auth_login.add_argument("--tunnel-endpoint")
    auth_login.add_argument("--from-env", action="store_true")
    auth_login.add_argument("--no-validate", action="store_true")
    auth_login.add_argument("--json", action="store_true")
    auth_login.set_defaults(handler=_handle_auth_login)

    auth_whoami = auth_subparsers.add_parser("whoami", help="查看当前身份")
    auth_whoami.add_argument("--json", action="store_true")
    auth_whoami.set_defaults(handler=_handle_auth_whoami)

    auth_can_i = auth_subparsers.add_parser("can-i", help="检查指定操作是否可执行")
    auth_can_i.add_argument("--table", required=True)
    auth_can_i.add_argument("--operation", required=True)
    auth_can_i.add_argument("--project")
    auth_can_i.add_argument("--json", action="store_true")
    auth_can_i.set_defaults(handler=_handle_auth_can_i)

    diff_parser = subparsers.add_parser("diff", help="差异对比")
    diff_subparsers = diff_parser.add_subparsers(dest="diff_command", required=True)

    diff_schema = diff_subparsers.add_parser("schema", help="对比两张表的 schema")
    diff_schema.add_argument("left_table")
    diff_schema.add_argument("right_table")
    diff_schema.add_argument("--json", action="store_true")
    diff_schema.set_defaults(handler=_handle_diff_schema)

    diff_partition = diff_subparsers.add_parser("partition", help="对比分区列表")
    diff_partition.add_argument("left_table")
    diff_partition.add_argument("right_table")
    diff_partition.add_argument("--json", action="store_true")
    diff_partition.set_defaults(handler=_handle_diff_partition)

    diff_data = diff_subparsers.add_parser("data", help="按 key 对比两张表的只读快照")
    diff_data.add_argument("left_table")
    diff_data.add_argument("right_table")
    diff_data.add_argument("--keys", required=True, help="逗号分隔的对齐 key 列")
    diff_data.add_argument("--columns", help="逗号分隔的非 key 对比列，默认取两侧共有列")
    diff_data.add_argument("--rows", type=int, default=100, help="每侧最多读取多少行做对比")
    diff_data.add_argument("--partition", help="同时应用到两侧表的分区")
    diff_data.add_argument("--left-partition")
    diff_data.add_argument("--right-partition")
    diff_data.add_argument("--json", action="store_true")
    diff_data.set_defaults(handler=_handle_diff_data)

    agent_parser = subparsers.add_parser("agent", help="Agent 相关命令")
    agent_subparsers = agent_parser.add_subparsers(dest="agent_command", required=True)

    agent_context = agent_subparsers.add_parser("context", help="输出 Agent 上下文")
    agent_context.add_argument("--json", action="store_true")
    agent_context.set_defaults(handler=_handle_agent_context)

    cache_parser = subparsers.add_parser("cache", help="元数据缓存管理")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", required=True)

    cache_build = cache_subparsers.add_parser("build", help="构建元数据缓存")
    cache_build.add_argument("--project")
    cache_build.add_argument("--json", action="store_true")
    cache_build.set_defaults(handler=_handle_cache_build)

    cache_status = cache_subparsers.add_parser("status", help="查看缓存状态")
    cache_status.add_argument("--project")
    cache_status.add_argument("--json", action="store_true")
    cache_status.set_defaults(handler=_handle_cache_status)

    cache_clear = cache_subparsers.add_parser("clear", help="清除缓存")
    cache_clear.add_argument("--project")
    cache_clear.add_argument("--json", action="store_true")
    cache_clear.set_defaults(handler=_handle_cache_clear)

    cache_save_semantic = cache_subparsers.add_parser("save-semantic", help="保存语义元数据")
    cache_save_semantic.add_argument("--table", required=True, help="表名")
    cache_save_semantic.add_argument("--semantic-desc", required=True, help="表的一句话业务描述")
    cache_save_semantic.add_argument("--use-cases", default="[]", help="使用场景 JSON 数组")
    cache_save_semantic.add_argument("--sample-questions", default="[]", help="示例问题 JSON 数组")
    cache_save_semantic.add_argument("--column-semantics", default="[]", help="列语义 JSON 数组")
    cache_save_semantic.add_argument("--project")
    cache_save_semantic.add_argument("--json", action="store_true")
    cache_save_semantic.set_defaults(handler=_handle_cache_save_semantic)

    cache_get_semantic = cache_subparsers.add_parser("get-semantic", help="获取语义元数据")
    cache_get_semantic.add_argument("--table", required=True, help="表名")
    cache_get_semantic.add_argument("--project")
    cache_get_semantic.add_argument("--json", action="store_true")
    cache_get_semantic.set_defaults(handler=_handle_cache_get_semantic)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv=argv)


def run(
    argv: Sequence[str] | None = None,
    *,
    cwd: Path | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    parser = build_parser()
    args = parser.parse_args(argv)
    working_dir = cwd or Path.cwd()
    requested_config_path = Path(args.config).resolve() if args.config else None
    args.requested_config_path = requested_config_path
    config_path = requested_config_path
    if (
        requested_config_path is not None
        and not requested_config_path.exists()
        and _command_name(args) == "auth.login"
    ):
        config_path = None

    app: MaxCApp | None = None
    try:
        app = MaxCApp(
            cwd=working_dir,
            config_path=config_path,
            load_backend=_command_name(args) != "auth.login",
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
                stderr.write(f"建议: {exc.suggestion}\n")
        return exc.exit_code


def _handle_query(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
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


def _handle_job_submit(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
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


def _handle_job_status(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.job_status(args.job_id)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_wait(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope, events = app.job_wait(args.job_id)
    if args.stream:
        emit_ndjson(events, stdout)
        return
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_diagnose(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.job_diagnose(args.job_id)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_result(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.job_result(args.job_id)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_cancel(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cancel_job(args.job_id)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_job_list(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.list_jobs()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_list_tables(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_list_tables()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_describe(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_describe(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_search(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_search(args.keyword)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_search_columns(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_search_columns(args.keyword)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_latest_partition(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_latest_partition(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_freshness(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_freshness(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_lineage(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_lineage(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_partitions(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_partitions(args.table_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_meta_list_projects(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_list_projects()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_meta_list_schemas(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.meta_list_schemas(project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_project_use(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.project_use(args.project_name, schema=args.schema)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_project_info(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.project_info(project_name=args.project_name)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_data_sample(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    columns = _csv_arg_list(args.columns)
    envelope = app.data_sample(
        args.table_name,
        rows=args.rows,
        partition=args.partition,
        columns=columns or None,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="table")


def _handle_data_profile(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.data_profile(args.table_name, partition=args.partition)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_login(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.auth_login(
        access_id=args.access_id,
        secret_access_key=args.secret_access_key,
        project=args.project,
        endpoint=args.endpoint,
        region_name=args.region_name,
        tunnel_endpoint=args.tunnel_endpoint,
        from_env=args.from_env,
        no_validate=args.no_validate,
        target_config_path=args.requested_config_path,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_whoami(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.auth_whoami()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_auth_can_i(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.auth_can_i(
        table_name=args.table,
        operation=args.operation,
        project=args.project,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_diff_schema(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.schema_diff(args.left_table, args.right_table)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_diff_partition(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.partition_diff(args.left_table, args.right_table)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_diff_data(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
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


def _handle_agent_context(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.agent_context()
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_build(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cache_build(project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_status(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cache_status(project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_clear(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cache_clear(project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_save_semantic(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    import json as json_module
    envelope = app.cache_save_semantic(
        table_name=args.table,
        semantic_desc=args.semantic_desc,
        use_cases=json_module.loads(args.use_cases),
        sample_questions=json_module.loads(args.sample_questions),
        column_semantics=json_module.loads(args.column_semantics),
        project=args.project,
    )
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _handle_cache_get_semantic(app: MaxCApp, args: argparse.Namespace, stdout: TextIO) -> None:
    envelope = app.cache_get_semantic(table_name=args.table, project=args.project)
    _emit_envelope(envelope, args=args, stdout=stdout, default_format="json")


def _emit_envelope(
    envelope: Envelope,
    *,
    args: argparse.Namespace,
    stdout: TextIO,
    default_format: str,
) -> None:
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


def _render_human(envelope: Envelope) -> str:
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


def _emit_csv(rows: list[dict[str, Any]], stdout: TextIO) -> None:
    if not rows:
        stdout.write("\n")
        return
    columns = list(rows[0])
    stdout.write(",".join(columns) + "\n")
    for row in rows:
        stdout.write(",".join(str(row.get(column, "")) for column in columns) + "\n")


def _write_output_file(envelope: Envelope, raw_path: str, output_format: str) -> Path:
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


def _command_name(args: argparse.Namespace) -> str:
    resolved = getattr(args, "resolved_command", None)
    if resolved:
        return resolved
    parts = [args.command_group]
    for attr in (
        "job_command",
        "meta_command",
        "data_command",
        "auth_command",
        "diff_command",
        "agent_command",
        "skill_command",
    ):
        value = getattr(args, attr, None)
        if value:
            parts.append(value)
    return ".".join(parts)


def _resolve_query_mode(args: argparse.Namespace) -> tuple[str, list[str]]:
    mode = args.mode
    sql_parts = list(args.sql_parts)
    if mode != "run" or not sql_parts:
        return mode, sql_parts

    alias = sql_parts[0].lower()
    if alias in {"run", "cost", "explain"} and (len(sql_parts) > 1 or args.file or args.stdin):
        return alias, sql_parts[1:]
    return mode, sql_parts


def _validate_query_analysis_args(args: argparse.Namespace, mode: str) -> None:
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
            f"{', '.join(unsupported)} 不能和 query cost/explain 一起使用。"
        )
    if args.format in {"csv", "ndjson"}:
        raise ValidationError("query cost/explain 只支持 table 或 json 输出。")


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

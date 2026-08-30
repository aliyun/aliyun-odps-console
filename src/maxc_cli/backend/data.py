"""Data-related mixin for OdpsBackend."""

import shlex
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from ..config import TableDefinition
from ..exceptions import (
    CsvParseError,
    UploadCommitOutcomeUnknownError,
    ValidationError,
)
from ..helpers import (
    build_profile,
    csv_format_value,
    csv_parse_value,
    csv_supported_type,
    resolve_sample_request,
    translate_odps_error,
)
from ..utils import (
    current_cli_entry_point,
    validate_csv_delimiter,
    validate_download_output_path,
    validate_upload_input_path,
)


def _scoped_table_name(name: 'str', schema: 'str | None') -> 'str':
    if schema and "." not in name:
        return f"{schema}.{name}"
    return name


def _agent_cli_command(
    *command_tokens: 'str',
    project: 'str | None' = None,
    schema: 'str | None' = None,
) -> 'str':
    """Render a shell-safe recovery command with the active scope and UA."""
    from ..odps_runtime import current_agent_user_agent

    tokens = shlex.split(current_cli_entry_point())
    user_agent = current_agent_user_agent()
    if user_agent:
        tokens.extend(["--user-agent", user_agent])
    tokens.extend(command_tokens)
    if project:
        tokens.extend(["--project", project])
    if schema:
        tokens.extend(["--schema", schema])
    tokens.append("--json")
    return shlex.join(tokens)


def _serialize_value(value: 'Any') -> 'Any':
    """Convert an ODPS read_table cell to a JSON-safe value.

    PyODPS hands us native Python objects whose types depend on the column
    type. The stdlib json encoder rejects Decimal and bytes; we string-ify
    both to keep precision (Decimal) and round-trip safety (bytes via
    latin-1, which is a total mapping over [0, 255]).
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("latin-1")
    return value


class DataMixin:
    """Mixin providing data sampling and profiling methods."""

    def _table_tunnel(self, project: 'str | None' = None):
        """Return a TableTunnel bound to ``project`` (or the client default).

        Real PyODPS `ODPS` instances do not expose a `.tunnel` attribute,
        so we construct `odps.tunnel.TableTunnel(odps=self.client)` lazily.
        The tunnel resolves tables against its own project, so a cross-project
        download/upload (e.g. `--project other_proj`) MUST construct the tunnel
        with that project — `create_*_session()` takes no project argument.

        Test doubles (FakeODPS) DO expose `.tunnel` directly — honor that so
        existing FakeTunnel infrastructure keeps working, and surface the
        requested project on the instance for assertions.
        """
        existing = getattr(self.client, "tunnel", None)
        if existing is not None:
            if project is not None:
                existing.requested_project = project
            return existing
        from odps.tunnel import TableTunnel
        if project:
            return TableTunnel(odps=self.client, project=project)
        return TableTunnel(odps=self.client)

    def _resolve_partition_for_sample(
        self,
        definition: 'TableDefinition',
        partition: 'str | None',
        *,
        project: 'str | None',
        schema: 'str | None' = None,
    ) -> 'tuple[str | None, list[str]]':
        """Resolve the partition spec to use, auto-detecting latest if needed.

        Returns (partition_spec, warnings).

        Raises ValidationError if the table is partitioned and no partition
        can be determined.
        """
        warnings: list[str] = []
        if partition or not definition.partition_columns:
            return partition, warnings

        # Partitioned table without partition spec — try latest-partition.
        try:
            latest_payload, _latest_warnings = self.latest_partition_info(
                definition.name, project=project, schema=schema,
            )
            latest_spec = latest_payload.get("latest_partition")
        except Exception:
            latest_spec = None

        if latest_spec:
            warnings.append(
                f"No --partition specified; auto-selected latest partition "
                f"`{latest_spec}`. Pass --partition explicitly to pin a value."
            )
            return latest_spec, warnings

        partition_keys = ", ".join(c.name for c in definition.partition_columns)
        qualified_name = _scoped_table_name(definition.name, schema)
        raise ValidationError(
            (
                f"Table `{definition.name}` is partitioned ({partition_keys}) "
                f"but no --partition was specified, and no latest partition "
                f"could be determined."
            ),
            suggestion=(
                f"Run `{_agent_cli_command('meta', 'latest-partition', qualified_name, project=project, schema=schema)}` "
                "to find a valid partition, then re-run with --partition <spec>."
            ),
        )

    def sample_table(
        self,
        table_name: 'str',
        rows: 'int',
        *,
        partition: 'str | None' = None,
        columns: 'list[str] | None' = None,
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'tuple[TableDefinition, list[dict[str, Any]], dict[str, Any]]':
        """Sample data from a table.

        Uses ``client.read_table()`` for efficient row-level access with
        optional partition pruning and column selection. When the table is
        partitioned and *partition* is not provided, automatically selects
        the latest partition (and adds a warning to ``sample_info``).

        Args:
            table_name: Table name.
            rows: Maximum number of rows to return.
            partition: Optional partition spec (e.g. ``"ds=20260101"``).
            columns: Optional list of column names to select.

        Returns:
            Tuple of (table definition, sample rows as list of dicts,
            sample metadata with applied_partition, selected_columns,
            and warnings).
        """
        definition = self.describe_table(table_name, project=project, schema=schema)
        if definition.table_type == "VIRTUAL_VIEW":
            qualified_name = _scoped_table_name(definition.name, schema)
            query_command = _agent_cli_command(
                "query",
                f"SELECT * FROM {qualified_name} LIMIT {rows}",
                project=project,
            )
            raise ValidationError(
                f"`{definition.name}` is a view; the tunnel-based sampler cannot "
                f"read views.",
                suggestion=(
                    f"Run `{query_command}` to sample a view via SQL."
                ),
            )
        partition, auto_partition_warnings = self._resolve_partition_for_sample(
            definition, partition, project=project, schema=schema,
        )

        selected_columns, applied_partition, partition_values = resolve_sample_request(
            definition,
            partition=partition,
            columns=columns,
            strict_partition_check=False,
        )

        # Build column selection
        column_names = selected_columns if selected_columns else [c.name for c in definition.columns]

        # Build partition spec if needed
        partition_spec = None
        if applied_partition and partition_values:
            partition_spec = ",".join(
                f"{k}={v}" for k, v in partition_values.items()
            )

        try:
            read_kwargs: dict[str, Any] = {
                "limit": rows,
                "partition": partition_spec,
                "project": project or self.project,
            }
            if schema:
                read_kwargs["schema"] = schema
            records = self.client.read_table(table_name, **read_kwargs)
            sample_rows = [
                {column: _serialize_value(record[column]) for column in column_names}
                for record in records
            ]
        except Exception as exc:
            raise translate_odps_error(exc) from exc

        return definition, sample_rows, {
            "schema": [{"name": c.name, "type": c.type, "comment": c.comment} for c in definition.columns if c.name in column_names],
            "applied_partition": applied_partition,
            "selected_columns": selected_columns,
            "warnings": auto_partition_warnings,
        }

    def profile_table(self, table_name: 'str', *, partition: 'str | None' = None, project: 'str | None' = None, schema: 'str | None' = None) -> 'dict[str, Any]':
        """Profile data from a table by sampling and computing statistics.

        Samples up to 20 rows and computes per-column statistics (null count,
        distinct count, min/max, etc.) using heuristic analysis. Not a native
        ODPS profile feature — results are approximate.

        Limitations:
            - Based on a 20-row sample; not statistically representative.
            - No native ODPS ``PROFILE`` command is used.
            - For accurate statistics, run explicit aggregation SQL.

        Args:
            table_name: Table name.
            partition: Optional partition spec for partition pruning.

        Returns:
            Dict with table name, column profiles, and sample info.
        """
        definition, sample_rows, sample_info = self.sample_table(
            table_name,
            rows=20,
            partition=partition,
            columns=None,
            project=project,
            schema=schema,
        )
        return build_profile(
            definition,
            sample_rows,
            applied_partition=sample_info["applied_partition"],
        )

    def upload_table(
        self,
        table_name: 'str',
        file_path: 'str',
        *,
        partition: 'str | None' = None,
        create_partition: 'bool' = False,
        overwrite: 'bool' = False,
        delimiter: 'str' = ",",
        has_header: 'bool' = True,
        null_marker: 'str' = r"\N",
        block_size: 'int' = 10000,
        project: 'str | None' = None,
        schema: 'str | None' = None,
        dry_run: 'bool' = False,
    ) -> 'dict[str, Any]':
        """Upload a CSV/TSV file into an existing table or partition via Tunnel.

        Args:
            table_name: Target table name (schema.table or table).
            file_path: Path to the local CSV/TSV file to upload.
            partition: Optional partition spec (e.g. ``"ds=20260508"``); required
                for partitioned tables, forbidden for non-partitioned tables.
            overwrite: If True, use INSERT OVERWRITE semantics for the target.
            delimiter: Field delimiter (default ``","``).
            has_header: If True, the first row is treated as a header and
                columns are mapped by name; otherwise mapped by ordinal.
            null_marker: Token interpreted as SQL NULL (default ``"\\N"``).
            block_size: Rows per Tunnel block (default 10000).
            project: Optional MaxCompute project override.

        Returns:
            Dict with ``table``, ``applied_partition``, ``rows_written``,
            ``bytes_read``, ``blocks``, ``overwrite``, and ``warnings``.

        Raises:
            ValidationError: For invalid partitioning, unsupported column
                types, or invalid block sizes.
            CsvParseError: When a CSV row cannot be parsed; carries
                ``line`` / ``column`` context. PyODPS upload sessions do not
                expose an abort API, so uncommitted blocks remain invisible
                and the server-side session expires.
        """
        import csv
        import os

        if block_size < 1:
            raise ValidationError("`block_size` must be >= 1.")
        if create_partition and not partition:
            raise ValidationError("`create_partition` requires a partition spec.")
        validate_csv_delimiter(delimiter)
        validated_file_path = validate_upload_input_path(file_path)

        definition = self.describe_table(table_name, project=project, schema=schema)
        if definition.table_type == "VIRTUAL_VIEW":
            raise ValidationError(
                f"`{definition.name}` is a view; views are read-only and cannot be loaded via Tunnel.",
                suggestion=(
                    "Choose a physical table that supports Tunnel upload, or use "
                    "an approved data-write workflow outside the public Agent Skill."
                ),
            )
        partition_columns = {c.name for c in definition.partition_columns}
        data_columns = [c for c in definition.columns if c.name not in partition_columns]
        name_to_type = {c.name: c.type for c in data_columns}

        if definition.partition_columns and not partition:
            keys = ", ".join(c.name for c in definition.partition_columns)
            raise ValidationError(
                f"Table `{definition.name}` is partitioned ({keys}); --partition is required.",
                suggestion=f"Pass --partition <{keys}=...>.",
            )
        if partition and not definition.partition_columns:
            raise ValidationError(
                f"Table `{definition.name}` is not partitioned; --partition is not allowed.",
            )
        if partition:
            _validate_partition_keys(partition, definition.partition_columns)

        unsupported = [c.name for c in data_columns if not csv_supported_type(c.type)]
        if unsupported:
            raise ValidationError(
                f"Columns {unsupported} have complex types not supported by CSV upload.",
                suggestion=(
                    "Convert the input through an approved data-write workflow "
                    "outside the public Agent Skill."
                ),
            )

        snapshot_dir = None
        upload_file_path = validated_file_path
        if not dry_run:
            snapshot_dir, upload_file_path = _create_upload_snapshot(
                validated_file_path
            )
        try:
            (
                column_mapping,
                expected_row_width,
                rows_found,
                warnings,
                source_fingerprint,
            ) = _validate_upload_csv_file(
                upload_file_path,
                delimiter=delimiter,
                has_header=has_header,
                data_columns=data_columns,
                name_to_type=name_to_type,
                null_marker=null_marker,
            )
        except BaseException:
            if snapshot_dir is not None:
                _cleanup_upload_snapshot(snapshot_dir)
            raise
        bytes_read = source_fingerprint[2]

        if dry_run:
            warnings.append(
                "Dry-run: table schema, CSV row widths, and mapped value types "
                "validated; no upload session was created."
            )
            return {
                "table": definition.name,
                "applied_partition": partition,
                "rows_written": 0,
                "rows_found": rows_found,
                "bytes_read": bytes_read,
                "column_mapping": [name for _, name in column_mapping],
                "blocks": 0,
                "overwrite": overwrite,
                "create_partition": create_partition,
                "dry_run": True,
                "validation": {
                    "table_schema": True,
                    "csv_structure": True,
                    "row_widths": True,
                    "mapped_value_types": True,
                    "upload_session_created": False,
                },
                "warnings": warnings,
            }

        block_ids: list[int] = []
        rows_written = 0
        upload_session = None
        writer = None
        reader = None
        upload_committed = False
        commit_attempted = False
        try:
            with open(upload_file_path, encoding="utf-8", newline="") as fh:
                upload_fingerprint = _file_fingerprint(os.fstat(fh.fileno()))
                if upload_fingerprint != source_fingerprint:
                    raise ValidationError(
                        "Upload input changed after local validation.",
                        suggestion="Retry with a stable input file.",
                    )
                reader = csv.reader(fh, delimiter=delimiter, strict=True)
                replay_warnings: list[str] = []
                replay_mapping, replay_row_width = _resolve_upload_mapping(
                    reader,
                    has_header=has_header,
                    data_columns=data_columns,
                    warnings=replay_warnings,
                )
                if (
                    replay_mapping != column_mapping
                    or replay_row_width != expected_row_width
                ):
                    raise ValidationError(
                        "Upload input mapping changed after local validation.",
                        suggestion="Retry with a stable input file.",
                    )

                create_session_kwargs: dict[str, Any] = {
                    "partition_spec": partition,
                    "overwrite": overwrite,
                }
                if partition and create_partition:
                    # This can create a missing remote partition, so it occurs
                    # only after the caller explicitly opts in and the entire
                    # local file has passed validation.
                    create_session_kwargs["create_partition"] = True
                if schema:
                    create_session_kwargs["schema"] = schema
                upload_session = self._table_tunnel(
                    project=project or self.project
                ).create_upload_session(
                    definition.name,
                    **create_session_kwargs,
                )

                current_block = 0
                in_block = 0
                writer = upload_session.open_record_writer(current_block)
                block_ids.append(current_block)

                try:
                    for row in reader:
                        parsed_values = _parse_upload_row(
                            row,
                            column_mapping=column_mapping,
                            expected_row_width=expected_row_width,
                            name_to_type=name_to_type,
                            null_marker=null_marker,
                            line_no=reader.line_num,
                        )
                        if writer is None:
                            writer = upload_session.open_record_writer(current_block)
                            block_ids.append(current_block)
                        record = upload_session.new_record()
                        for col_name, value in parsed_values.items():
                            record[col_name] = value
                        writer.write(record)
                        rows_written += 1
                        in_block += 1
                        if in_block >= block_size:
                            writer.close()
                            writer = None
                            current_block += 1
                            in_block = 0
                except csv.Error as exc:
                    raise CsvParseError(
                        f"invalid CSV syntax: {exc}",
                        line=reader.line_num,
                    ) from exc

                if writer is not None:
                    writer.close()
                    writer = None

            # PyODPS validates that every server-side block has been closed
            # before commit. Keep commit in the protected region as it performs
            # network I/O and may fail after all local parsing has succeeded.
            commit_attempted = True
            upload_session.commit(block_ids)
            upload_committed = True
        except UnicodeError as exc:
            _safe_close_writer(writer)
            error = CsvParseError(
                "CSV file is not valid UTF-8.",
                line=getattr(reader, "line_num", None),
            )
            _annotate_failed_upload(
                error,
                upload_session,
                commit_attempted=commit_attempted,
                create_partition=create_partition,
            )
            raise error from exc
        except (CsvParseError, ValidationError) as exc:
            _safe_close_writer(writer)
            _annotate_failed_upload(
                exc,
                upload_session,
                commit_attempted=commit_attempted,
                create_partition=create_partition,
            )
            raise
        except Exception as exc:
            _safe_close_writer(writer)
            translated = translate_odps_error(exc)
            _annotate_failed_upload(
                translated,
                upload_session,
                commit_attempted=commit_attempted,
                create_partition=create_partition,
            )
            raise translated from exc
        except KeyboardInterrupt as exc:
            _safe_close_writer(writer)
            if commit_attempted:
                error = UploadCommitOutcomeUnknownError(
                    "Upload was interrupted after the Tunnel commit request began."
                )
                _annotate_failed_upload(
                    error,
                    upload_session,
                    commit_attempted=True,
                    create_partition=create_partition,
                )
                raise error from exc
            _try_abort_upload_session(upload_session)
            raise
        except BaseException:
            # Before commit, close the writer and use an optional abort API if
            # the concrete session has one. Standard PyODPS sessions have no
            # abort method; their uncommitted blocks expire server-side.
            _safe_close_writer(writer)
            if not commit_attempted:
                _try_abort_upload_session(upload_session)
            raise
        finally:
            if snapshot_dir is not None:
                cleanup_error = _cleanup_upload_snapshot(snapshot_dir)
                if cleanup_error and upload_committed:
                    warnings.append(
                        "Upload committed successfully, but its private local "
                        f"snapshot could not be removed ({cleanup_error}). The "
                        "remote data must not be uploaded again solely for this "
                        "local cleanup warning."
                    )

        return {
            "table": definition.name,
            "applied_partition": partition,
            "rows_written": rows_written,
            "bytes_read": bytes_read,
            "blocks": len(block_ids),
            "overwrite": overwrite,
            "create_partition": create_partition,
            "warnings": warnings,
        }

    def download_table(
        self,
        table_name: 'str',
        output_path: 'str',
        *,
        overwrite: 'bool' = False,
        partition: 'str | None' = None,
        columns: 'list[str] | None' = None,
        limit: 'int | None' = None,
        delimiter: 'str' = ",",
        write_header: 'bool' = True,
        null_marker: 'str' = "",
        project: 'str | None' = None,
        schema: 'str | None' = None,
    ) -> 'dict[str, Any]':
        """Download a table or partition to a local CSV/TSV file via Tunnel.

        Args:
            table_name: Table name (schema.table or table).
            output_path: Local file path to write.
            overwrite: Replace an existing output file when True.
            partition: Required when table is partitioned.
            columns: Optional column subset; default = all columns in schema order.
            limit: Optional max rows; default = full partition / table.
            delimiter: Field delimiter (default ",").
            write_header: When False, suppress header row.
            null_marker: Token written for SQL NULL (default empty string).
            project: Target project; default = backend's default project.

        Returns:
            Dict with table, applied_partition, output_path, rows_written,
            bytes_written, columns, truncated, warnings.
        """
        import csv
        import os
        import tempfile
        from pathlib import Path

        if limit is not None and limit < 1:
            raise ValidationError("`limit` must be >= 1.")
        validate_csv_delimiter(delimiter)
        target_path = validate_download_output_path(
            output_path,
            overwrite=overwrite,
        )

        definition = self.describe_table(table_name, project=project, schema=schema)
        if definition.table_type == "VIRTUAL_VIEW":
            qualified_name = _scoped_table_name(definition.name, schema)
            query_command = _agent_cli_command(
                "query",
                f"SELECT * FROM {qualified_name}",
                "--output",
                str(target_path),
                "--output-format",
                "csv",
                project=project,
                schema=schema,
            )
            raise ValidationError(
                f"`{definition.name}` is a view; the tunnel-based downloader cannot "
                f"read views.",
                suggestion=(
                    f"Run `{query_command}` to materialize the view via SQL."
                ),
            )
        partition_columns = {c.name for c in definition.partition_columns}
        data_columns = [c for c in definition.columns if c.name not in partition_columns]
        name_to_type = {c.name: c.type for c in data_columns}

        if definition.partition_columns and not partition:
            keys = ", ".join(c.name for c in definition.partition_columns)
            raise ValidationError(
                f"Table `{definition.name}` is partitioned ({keys}); --partition is required.",
                suggestion=f"Pass --partition <{keys}=...>.",
            )
        if partition and not definition.partition_columns:
            raise ValidationError(
                f"Table `{definition.name}` is not partitioned; --partition is not allowed.",
            )
        if partition:
            _validate_partition_keys(partition, definition.partition_columns)

        if columns:
            unknown = [c for c in columns if c not in name_to_type]
            if unknown:
                raise ValidationError(f"Unknown columns: {unknown}")
            selected = list(columns)
        else:
            selected = [c.name for c in data_columns]

        temp_path: Path | None = None
        bytes_written = 0
        try:
            download_kwargs: dict[str, Any] = {"partition_spec": partition}
            if schema:
                download_kwargs["schema"] = schema
            session = self._table_tunnel(project=project or self.project).create_download_session(
                definition.name, **download_kwargs,
            )
            total = session.count
            count = min(total, limit) if limit is not None else total

            rows_written = 0
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="",
                dir=str(target_path.parent),
                prefix=f".{target_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as fh:
                temp_path = Path(fh.name)
                writer = csv.writer(fh, delimiter=delimiter)
                if write_header:
                    writer.writerow(selected)
                for record in session.open_record_reader(0, count):
                    writer.writerow([
                        csv_format_value(
                            record[col], name_to_type[col],
                            null_marker=null_marker,
                        )
                        for col in selected
                    ])
                    rows_written += 1
                fh.flush()
                os.fsync(fh.fileno())
                bytes_written = os.fstat(fh.fileno()).st_size

            if overwrite:
                # Same-directory replace is atomic on supported platforms. The
                # previous target remains untouched until the complete download
                # is durable and ready to publish.
                os.replace(temp_path, target_path)
            else:
                # A hard-link publishes the completed same-filesystem temp file
                # atomically and fails with EEXIST if another process created the
                # destination during the remote download. Unlike an existence
                # check followed by os.replace(), this cannot clobber a race
                # winner.
                try:
                    os.link(temp_path, target_path)
                except FileExistsError as exc:
                    raise ValidationError(
                        f"Download output already exists: {target_path}",
                        suggestion=(
                            "Choose a new path or pass --overwrite to replace the file."
                        ),
                    ) from exc
                _safe_unlink(temp_path)
            temp_path = None
        except ValidationError:
            _safe_unlink(temp_path)
            raise
        except OSError as exc:
            _safe_unlink(temp_path)
            raise ValidationError(
                f"Could not write download output `{target_path}`: {exc}",
                suggestion="Check that the output directory exists and is writable.",
            ) from exc
        except Exception as exc:
            _safe_unlink(temp_path)
            raise translate_odps_error(exc) from exc

        truncated = limit is not None and limit < total
        warnings: list[str] = []
        if truncated:
            warnings.append(
                f"--limit reached; output may be partial (session has {total} rows)."
            )

        return {
            "table": definition.name,
            "applied_partition": partition,
            "output_path": str(target_path),
            "rows_written": rows_written,
            "bytes_written": bytes_written,
            "columns": selected,
            "truncated": truncated,
            "warnings": warnings,
        }


def _file_fingerprint(file_stat: 'Any') -> 'tuple[int, int, int, int]':
    return (
        int(file_stat.st_dev),
        int(file_stat.st_ino),
        int(file_stat.st_size),
        int(file_stat.st_mtime_ns),
    )


def _create_upload_snapshot(file_path: 'Any') -> 'tuple[Any, Any]':
    """Copy an upload source into an owner-private immutable-by-convention snapshot.

    Validation and Tunnel replay both read this snapshot. The user-selected
    source may therefore be replaced or edited after the copy without changing
    the bytes committed remotely. A descriptor-pinned source open also avoids
    pathname substitution during the copy itself.
    """
    import os
    import shutil
    import stat
    import tempfile
    from pathlib import Path

    snapshot_dir = tempfile.TemporaryDirectory(prefix="maxc-upload-")
    snapshot_path = Path(snapshot_dir.name) / "input.csv"
    source_descriptor = None
    snapshot_descriptor = None
    completed = False
    try:
        source_flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        source_flags |= getattr(os, "O_NOFOLLOW", 0)
        source_descriptor = os.open(file_path, source_flags)
        source_stat = os.fstat(source_descriptor)
        if not stat.S_ISREG(source_stat.st_mode):
            raise ValidationError(
                f"Upload input is not a regular file: {file_path}",
                suggestion="Choose a regular CSV/TSV file.",
            )

        snapshot_flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_BINARY", 0)
        )
        snapshot_descriptor = os.open(snapshot_path, snapshot_flags, 0o600)
        with os.fdopen(source_descriptor, "rb") as source, os.fdopen(
            snapshot_descriptor,
            "wb",
        ) as snapshot:
            source_descriptor = None
            snapshot_descriptor = None
            shutil.copyfileobj(source, snapshot, length=1024 * 1024)
            snapshot.flush()
            os.fsync(snapshot.fileno())
        if os.name != "nt":
            os.chmod(snapshot_dir.name, 0o700)
            os.chmod(snapshot_path, 0o600)
        completed = True
        return snapshot_dir, snapshot_path
    except ValidationError:
        raise
    except OSError as exc:
        raise ValidationError(
            f"Could not snapshot upload input `{file_path}`: {exc}",
            suggestion="Check that the file is a stable, readable regular file.",
        ) from exc
    finally:
        if source_descriptor is not None:
            os.close(source_descriptor)
        if snapshot_descriptor is not None:
            os.close(snapshot_descriptor)
        if not completed:
            # Close Windows file handles before asking TemporaryDirectory to
            # remove the failed snapshot.  Cleanup must not mask the original
            # validation/read error.
            try:
                snapshot_dir.cleanup()
            except OSError:
                pass


def _validate_upload_csv_file(
    file_path: 'Any',
    *,
    delimiter: 'str',
    has_header: 'bool',
    data_columns: 'list[Any]',
    name_to_type: 'dict[str, str]',
    null_marker: 'str',
) -> 'tuple[list[tuple[int, str]], int, int, list[str], tuple[int, int, int, int]]':
    """Fully validate upload contents before any Tunnel write session exists."""
    import csv
    import os

    warnings: list[str] = []
    rows_found = 0
    reader = None
    try:
        with open(file_path, encoding="utf-8", newline="") as stream:
            fingerprint = _file_fingerprint(os.fstat(stream.fileno()))
            reader = csv.reader(stream, delimiter=delimiter, strict=True)
            column_mapping, expected_row_width = _resolve_upload_mapping(
                reader,
                has_header=has_header,
                data_columns=data_columns,
                warnings=warnings,
            )
            for row in reader:
                _parse_upload_row(
                    row,
                    column_mapping=column_mapping,
                    expected_row_width=expected_row_width,
                    name_to_type=name_to_type,
                    null_marker=null_marker,
                    line_no=reader.line_num,
                )
                rows_found += 1
    except csv.Error as exc:
        raise CsvParseError(
            f"invalid CSV syntax: {exc}",
            line=getattr(reader, "line_num", None),
        ) from exc
    except UnicodeError as exc:
        raise CsvParseError(
            "CSV file is not valid UTF-8.",
            line=getattr(reader, "line_num", None),
        ) from exc
    except OSError as exc:
        raise ValidationError(
            f"Could not read upload input `{file_path}`: {exc}",
            suggestion="Check that the file exists and is readable.",
        ) from exc
    return (
        column_mapping,
        expected_row_width,
        rows_found,
        warnings,
        fingerprint,
    )


def _try_abort_upload_session(session) -> 'bool':
    """Use an optional client abort API without pretending PyODPS has one."""
    abort = getattr(session, "abort", None)
    if not callable(abort):
        return False
    try:
        abort()
    except Exception:
        return False
    return True


def _annotate_failed_upload(
    error: 'Any',
    session: 'Any',
    *,
    commit_attempted: bool,
    create_partition: bool,
) -> 'None':
    """Expose retry safety for an upload that created a Tunnel session."""
    if session is None:
        return
    context = dict(getattr(error, "context", None) or {})
    context["upload_session_created"] = True
    context["partition_may_remain"] = bool(create_partition)
    if commit_attempted:
        context.update(
            {
                "remote_commit_state": "unknown",
                "duplicate_write_risk": True,
                "upload_session_cleanup": "not_attempted_after_commit_request",
            }
        )
        error.recoverable = False
        safety_note = (
            "The Tunnel commit outcome is unknown. Do not retry this upload until "
            "you verify the target table or partition; the commit may have succeeded."
        )
    else:
        aborted = _try_abort_upload_session(session)
        context.update(
            {
                "remote_commit_state": "not_attempted",
                "duplicate_write_risk": False,
                "uncommitted_rows_visible": False,
                "upload_session_cleanup": (
                    "client_abort" if aborted else "server_expiry_expected"
                ),
            }
        )
        safety_note = (
            "No Tunnel commit was attempted, so uploaded blocks are not visible. "
            + (
                "The optional client abort completed."
                if aborted
                else "PyODPS exposes no abort API; the uncommitted session will expire server-side."
            )
        )
    error.context = context
    previous_suggestion = getattr(error, "suggestion", None)
    error.suggestion = (
        f"{previous_suggestion} {safety_note}"
        if previous_suggestion
        else safety_note
    )


def _cleanup_upload_snapshot(snapshot_dir: 'Any') -> 'str | None':
    """Best-effort cleanup that never masks a primary or committed outcome."""
    try:
        snapshot_dir.cleanup()
    except BaseException as exc:
        return str(exc) or type(exc).__name__
    return None


def _safe_close_writer(writer) -> 'None':
    """Best-effort close for a partially written Tunnel block."""
    if writer is None:
        return
    try:
        writer.close()
    except Exception:
        pass


def _safe_unlink(path: 'Any | None') -> 'None':
    """Remove a temporary output file without masking the primary failure."""
    if path is None:
        return
    try:
        path.unlink()
    except OSError:
        pass


def _resolve_upload_mapping(
    reader: 'Any',
    *,
    has_header: 'bool',
    data_columns: 'list',
    warnings: 'list[str]',
) -> 'tuple[list[tuple[int, str]], int]':
    """Resolve CSV-to-table columns and the exact expected row width."""
    if has_header:
        try:
            header = next(reader)
        except StopIteration:
            header = []
        return _resolve_header_mapping(header, data_columns, warnings), len(header)

    mapping = [
        (index, column.name)
        for index, column in enumerate(data_columns)
    ]
    return mapping, len(mapping)


def _parse_upload_row(
    row: 'list[str]',
    *,
    column_mapping: 'list[tuple[int, str]]',
    expected_row_width: 'int',
    name_to_type: 'dict[str, str]',
    null_marker: 'str',
    line_no: 'int',
) -> 'dict[str, Any]':
    """Validate and type-convert one CSV row for dry-run and real upload.

    Keeping this path shared makes a successful dry-run a truthful local
    preflight for every row the uploader will later hand to Tunnel.
    """
    if len(row) != expected_row_width:
        raise CsvParseError(
            f"expected {expected_row_width} columns, got {len(row)}",
            line=line_no,
        )

    parsed: dict[str, Any] = {}
    for source_index, col_name in column_mapping:
        try:
            parsed[col_name] = csv_parse_value(
                row[source_index],
                name_to_type[col_name],
                null_marker=null_marker,
            )
        except CsvParseError as exc:
            exc.line = line_no
            exc.column = col_name
            raise
    return parsed


def _resolve_header_mapping(
    header: 'list[str]',
    data_columns: 'list',
    warnings: 'list[str]',
) -> 'list[tuple[int, str]]':
    expected = {c.name for c in data_columns}
    seen = set(header)
    missing = expected - seen
    if missing:
        raise ValidationError(
            f"CSV header missing required columns: {sorted(missing)}",
        )
    extras = [name for name in header if name not in expected]
    if extras:
        warnings.append(
            f"CSV header has extra columns ignored: {extras}"
        )
    duplicate_targets = sorted(
        name for name in expected if header.count(name) > 1
    )
    if duplicate_targets:
        raise ValidationError(
            f"CSV header contains duplicate target columns: {duplicate_targets}",
        )
    return [
        (source_index, name)
        for source_index, name in enumerate(header)
        if name in expected
    ]


def _validate_partition_keys(
    partition: 'str',
    partition_columns: 'list',
) -> 'None':
    """Raise ValidationError if `partition` doesn't match the table's keys."""
    from ..helpers import parse_partition_spec

    expected_keys = [c.name for c in partition_columns]
    parsed = parse_partition_spec(partition)
    if not parsed:
        raise ValidationError(
            f"Could not parse --partition {partition!r}.",
            suggestion=f"Use the form {','.join(f'{k}=...' for k in expected_keys)}.",
        )
    given = set(parsed.keys())
    expected = set(expected_keys)
    missing = expected - given
    extra = given - expected
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"missing keys {sorted(missing)}")
        if extra:
            parts.append(f"unknown keys {sorted(extra)}")
        raise ValidationError(
            f"--partition {partition!r} {' and '.join(parts)}; "
            f"table keys are {expected_keys}.",
        )


from dataclasses import dataclass, field
from typing import Any


@dataclass
class ErrorPayload:
    code: 'str'
    message: 'str'
    suggestion: 'str | None'
    recoverable: 'bool'
    recovery_steps: 'list[str]' = field(default_factory=list)
    instance_id: 'str | None' = None
    logview: 'str | None' = None
    context: 'dict[str, Any] | None' = None
    # CLI-internal: tells _emit_envelope which exit code to surface for
    # failure envelopes. NOT serialized to the envelope JSON (schema 2.0
    # stays intact — agents reading the envelope never see it).
    exit_code: 'int' = 1

    def to_dict(self) -> 'dict[str, Any]':
        from .utils import distribution_cli_text

        payload: dict[str, Any] = {
            "code": self.code,
            "message": distribution_cli_text(self.message),
            "recoverable": self.recoverable,
        }
        if self.suggestion:
            payload["suggestion"] = distribution_cli_text(self.suggestion)
        if self.recovery_steps:
            payload["recovery_steps"] = [
                distribution_cli_text(step) for step in self.recovery_steps
            ]
        if self.instance_id:
            payload["instance_id"] = self.instance_id
        if self.logview:
            payload["logview"] = self.logview
        if self.context:
            payload["context"] = self.context
        return payload


class MaxCError(Exception):
    exit_code = 1
    error_code = "EXECUTION_FAILED"
    recoverable = True

    def __init__(
        self,
        message: 'str',
        *,
        suggestion: 'str | None' = None,
        recoverable: 'bool | None' = None,
        instance_id: 'str | None' = None,
        logview: 'str | None' = None,
        context: 'dict[str, Any] | None' = None,
    ) -> 'None':
        super().__init__(message)
        self.message = message
        self.suggestion = suggestion
        self.instance_id = instance_id
        self.logview = logview
        self.context = context
        if recoverable is None:
            self.recoverable = self.__class__.recoverable
        else:
            self.recoverable = recoverable

    def to_payload(self) -> 'ErrorPayload':
        return ErrorPayload(
            code=self.error_code,
            message=self.message,
            suggestion=self.suggestion,
            recoverable=self.recoverable,
            recovery_steps=self._default_recovery_steps(),
            instance_id=self.instance_id,
            logview=self.logview,
            context=self.context,
            exit_code=self.__class__.exit_code,
        )

    def _default_recovery_steps(self) -> 'list[str]':
        """Return command-oriented recovery steps for stable error codes."""
        # Lazy import avoids exceptions.py <-> utils.py import initialization.
        from .utils import current_cli_entry_point

        cli = current_cli_entry_point()
        steps: dict[str, list[str]] = {
            "PERMISSION_DENIED": [
                f"Check the exact object and operation: {cli} auth can-i --table <table> --operation SELECT --json",
                f"Verify the active identity and project: {cli} auth whoami --json",
                "Request the required permission from the MaxCompute project administrator.",
            ],
            "BACKEND_CONNECTION_ERROR": [
                "Check network connectivity to the configured MaxCompute endpoint.",
                f"Verify credentials and endpoint: {cli} auth whoami --json",
                f"Inspect local configuration without a network call: {cli} agent context --json",
            ],
            "JOB_TIMEOUT": [
                f"Continue waiting with a longer timeout: {cli} job wait <job_id> --timeout 600 --json",
                f"Inspect the current job state: {cli} job status <job_id> --json",
                f"Diagnose the job if it failed: {cli} job diagnose <job_id> --json",
            ],
            "COST_LIMIT_EXCEEDED": [
                f"Review the estimate: {cli} query cost <sql> --json",
                "Reduce scanned partitions or projected columns before retrying.",
            ],
            "VALIDATION_ERROR": [
                "Correct the field named in the error message.",
                f"Inspect command syntax: {cli} <command> --help",
                f"Inspect local context: {cli} agent context --json",
            ],
            "NOT_FOUND": [
                f"Search for the object: {cli} meta search <keyword> --json",
                f"Verify the active project and schema: {cli} session show --json",
                f"List available tables: {cli} meta list-tables --json",
            ],
            "SQL_ERROR": [
                f"Validate the plan without running the query: {cli} query explain <sql> --json",
                f"Inspect referenced table schemas: {cli} meta describe <table> --json",
            ],
            "READ_ONLY_VIOLATION": [
                "The public MaxCompute Agent Skill supports SELECT SQL only.",
                "Use an approved table or data change workflow outside this Skill for DDL/DML.",
            ],
            "WRITE_OPERATION_REQUIRES_FORCE": [
                "The public MaxCompute Agent Skill supports SELECT SQL only.",
                "Do not bypass the SQL gate from this Skill.",
                "Use an approved table or data change workflow outside this Skill for DDL/DML.",
            ],
            "UNSUPPORTED_SQL_OPERATION": [
                "Use SELECT, SHOW, DESC, DESCRIBE, EXPLAIN, or a WITH query whose outer statement is read-only.",
                "Use an approved workflow outside the public MaxCompute Agent Skill for other SQL operations.",
            ],
        }
        return steps.get(self.error_code, [])


class PermissionDeniedError(MaxCError):
    exit_code = 2
    error_code = "PERMISSION_DENIED"
    recoverable = False


class QuotaExceededError(MaxCError):
    exit_code = 3
    error_code = "QUOTA_EXCEEDED"
    recoverable = True


class SqlError(MaxCError):
    exit_code = 4
    error_code = "SQL_ERROR"
    recoverable = False


class CostLimitExceededError(MaxCError):
    exit_code = 5
    error_code = "COST_LIMIT_EXCEEDED"
    recoverable = False


class NotFoundError(MaxCError):
    error_code = "NOT_FOUND"
    recoverable = False


class ValidationError(MaxCError):
    error_code = "VALIDATION_ERROR"
    recoverable = False


class TwoTierNamespaceError(ValidationError):
    """The service explicitly confirmed that a project is not 3-tier."""


class FeatureUnavailableError(MaxCError):
    error_code = "FEATURE_UNAVAILABLE"
    recoverable = False


class BackendConnectionError(MaxCError):
    error_code = "BACKEND_CONNECTION_ERROR"
    recoverable = True


class UploadCommitOutcomeUnknownError(MaxCError):
    """A client interruption occurred after a Tunnel commit request began."""

    exit_code = 130
    error_code = "UPLOAD_COMMIT_OUTCOME_UNKNOWN"
    recoverable = False


class JobTimeoutError(MaxCError):
    error_code = "JOB_TIMEOUT"
    recoverable = True


class ReadOnlyError(SqlError):
    error_code = "READ_ONLY_VIOLATION"
    recoverable = False


class SchemaNotFoundError(NotFoundError):
    error_code = "SCHEMA_NOT_FOUND"
    recoverable = False


class TableNotFoundError(NotFoundError):
    error_code = "TABLE_NOT_FOUND"
    recoverable = False


class ColumnNotFoundError(NotFoundError):
    error_code = "COLUMN_NOT_FOUND"
    recoverable = False


class WriteOperationRequiresForceError(MaxCError):
    error_code = "WRITE_OPERATION_REQUIRES_FORCE"
    recoverable = False


class UnsupportedSqlOperationError(MaxCError):
    error_code = "UNSUPPORTED_SQL_OPERATION"
    recoverable = False


class CsvParseError(ValidationError):
    error_code = "CSV_PARSE_ERROR"
    recoverable = False

    def __init__(
        self,
        message: 'str',
        *,
        line: 'int | None' = None,
        column: 'str | None' = None,
        suggestion: 'str | None' = None,
    ) -> 'None':
        super().__init__(message, suggestion=suggestion)
        self.line = line
        self.column = column

    def to_payload(self) -> 'ErrorPayload':
        payload = super().to_payload()
        context: dict[str, Any] = dict(payload.context or {})
        if self.line is not None:
            context["line"] = self.line
        if self.column is not None:
            context["column"] = self.column
        if context:
            payload.context = context
        return payload

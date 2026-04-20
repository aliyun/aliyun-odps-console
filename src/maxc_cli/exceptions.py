
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ErrorPayload:
    code: 'str'
    message: 'str'
    suggestion: 'str | None'
    recoverable: 'bool'
    recovery_steps: 'list[str]' = field(default_factory=list)

    def to_dict(self) -> 'dict[str, Any]':
        payload: 'dict[str, Any]' = {
            "code": self.code,
            "message": self.message,
            "recoverable": self.recoverable,
        }
        if self.suggestion:
            payload["suggestion"] = self.suggestion
        if self.recovery_steps:
            payload["recovery_steps"] = self.recovery_steps
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
    ) -> 'None':
        super().__init__(message)
        self.message = message
        self.suggestion = suggestion
        if recoverable is None:
            self.recoverable = self.__class__.recoverable
        else:
            self.recoverable = recoverable

    def to_payload(self) -> 'ErrorPayload':
        steps = self._default_recovery_steps()
        return ErrorPayload(
            code=self.error_code,
            message=self.message,
            suggestion=self.suggestion,
            recoverable=self.recoverable,
            recovery_steps=steps,
        )

    def _default_recovery_steps(self) -> 'list[str]':
        """Return default recovery steps based on error code."""
        _STEPS: 'dict[str, list[str]]' = {
            "PERMISSION_DENIED": [
                "Check the table and operation with: maxc auth can-i --table <table> --operation SELECT --json",
                "Verify your project access with: maxc auth whoami --json",
                "Contact your project administrator for access.",
            ],
            "BACKEND_CONNECTION_ERROR": [
                "Check your network connection to the ODPS endpoint.",
                "Verify credentials with: maxc auth whoami --json",
                "If using AK/SK, re-authenticate with: maxc auth login --from-env --json",
            ],
            "JOB_TIMEOUT": [
                "Retry with a longer timeout: maxc job wait <job_id> --timeout 600 --json",
                "Optimize the query to reduce execution time.",
                "Check query cost first: maxc query cost <SQL> --json",
            ],
            "COST_LIMIT_EXCEEDED": [
                "Review the cost estimate: maxc query cost <SQL> --json",
                "Adjust cost_threshold_cu in your config if appropriate.",
                "Optimize the query to scan less data.",
            ],
            "VALIDATION_ERROR": [
                "If the error mentions missing connection settings, run: maxc auth login --from-env --json",
                "Otherwise check the error message for the specific field that failed validation.",
                "Use --help for command syntax: maxc <command> --help",
            ],
            "NOT_FOUND": [
                "Verify the resource name with: maxc meta search <keyword> --json",
                "Check your current project: maxc session show --json",
                "List available tables: maxc meta list-tables --json",
            ],
            "SCHEMA_NOT_FOUND": [
                "List available schemas: maxc meta list-schemas --json",
                "Search for the correct schema: maxc meta search <keyword> --json",
            ],
            "TABLE_NOT_FOUND": [
                "Search for similar tables: maxc meta search <keyword> --json",
                "List all tables: maxc meta list-tables --json",
            ],
            "COLUMN_NOT_FOUND": [
                "Describe the table to see available columns: maxc meta describe <table_name> --json",
            ],
            "WRITE_OPERATION_REQUIRES_FORCE": [
                "Re-run the command with --force to bypass read-only mode.",
                "Verify you have write permission: maxc auth can-i --table <table> --operation INSERT --json",
            ],
        }
        return _STEPS.get(self.error_code, [])


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


class FeatureUnavailableError(MaxCError):
    error_code = "FEATURE_UNAVAILABLE"
    recoverable = False


class BackendConnectionError(MaxCError):
    error_code = "BACKEND_CONNECTION_ERROR"
    recoverable = True


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
    recoverable = True

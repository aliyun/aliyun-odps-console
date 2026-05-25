
from dataclasses import dataclass
from typing import Any


@dataclass
class ErrorPayload:
    code: 'str'
    message: 'str'
    suggestion: 'str | None'
    recoverable: 'bool'
    instance_id: 'str | None' = None
    logview: 'str | None' = None
    context: 'dict[str, Any] | None' = None
    # CLI-internal: tells _emit_envelope which exit code to surface for
    # failure envelopes. NOT serialized to the envelope JSON (schema 2.0
    # stays intact — agents reading the envelope never see it).
    exit_code: 'int' = 1

    def to_dict(self) -> 'dict[str, Any]':
        payload: 'dict[str, Any]' = {
            "code": self.code,
            "message": self.message,
            "recoverable": self.recoverable,
        }
        if self.suggestion:
            payload["suggestion"] = self.suggestion
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
            instance_id=self.instance_id,
            logview=self.logview,
            context=self.context,
            exit_code=self.__class__.exit_code,
        )


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
        context: 'dict[str, Any]' = dict(payload.context or {})
        if self.line is not None:
            context["line"] = self.line
        if self.column is not None:
            context["column"] = self.column
        if context:
            payload.context = context
        return payload

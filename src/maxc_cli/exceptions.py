
from dataclasses import dataclass
from typing import Any


@dataclass
class ErrorPayload:
    code: 'str'
    message: 'str'
    suggestion: 'str | None'
    recoverable: 'bool'

    def to_dict(self) -> 'dict[str, Any]':
        payload = {
            "code": self.code,
            "message": self.message,
            "recoverable": self.recoverable,
        }
        if self.suggestion:
            payload["suggestion"] = self.suggestion
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
        return ErrorPayload(
            code=self.error_code,
            message=self.message,
            suggestion=self.suggestion,
            recoverable=self.recoverable,
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

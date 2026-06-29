from __future__ import annotations

from dataclasses import dataclass

from .exceptions import ValidationError

_ASCII_WHITESPACE = " \t\n\r\f\v"
_COMPOSITE_FORMAT_MESSAGE = "MCQA job IDs must use the format `<instance-id>@<subquery-id>`."
COMPOSITE_METADATA_MESSAGE = "MCQA async SQLRT jobs require subquery metadata to build composite job IDs."


@dataclass(frozen=True)
class ParsedJobId:
    instance_id: str
    subquery_id: int | None


def format_job_id(instance_id: str, subquery_id: int | None) -> str:
    return instance_id if subquery_id is None else f"{instance_id}@{subquery_id}"


def parse_job_id(raw_job_id: str) -> ParsedJobId:
    token = raw_job_id.strip(_ASCII_WHITESPACE)
    if not token or _contains_ascii_whitespace(token):
        raise ValidationError(_COMPOSITE_FORMAT_MESSAGE)

    if "@" not in token:
        return ParsedJobId(instance_id=token, subquery_id=None)

    if token.count("@") != 1:
        raise ValidationError(_COMPOSITE_FORMAT_MESSAGE)

    instance_id, subquery_token = token.split("@", 1)
    if not instance_id or not subquery_token:
        raise ValidationError(_COMPOSITE_FORMAT_MESSAGE)
    if _contains_ascii_whitespace(instance_id) or _contains_ascii_whitespace(subquery_token):
        raise ValidationError(_COMPOSITE_FORMAT_MESSAGE)

    try:
        subquery_id = int(subquery_token)
    except ValueError as exc:
        raise ValidationError(_COMPOSITE_FORMAT_MESSAGE) from exc

    return ParsedJobId(instance_id=instance_id, subquery_id=subquery_id)


def _contains_ascii_whitespace(value: str) -> bool:
    return any(ch in _ASCII_WHITESPACE for ch in value)

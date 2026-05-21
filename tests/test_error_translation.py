"""Tests for helpers.translate_odps_error typed-exception mapping (C3)."""
from odps import errors as odps_errors

from maxc_cli.exceptions import (
    BackendConnectionError,
    NotFoundError,
    PermissionDeniedError,
    ReadOnlyError,
    SchemaNotFoundError,
    SqlError,
    TableNotFoundError,
)
from maxc_cli.helpers import translate_odps_error


def test_no_such_object_table_maps_to_table_not_found():
    # PyODPS raises NoSuchObject when the server says "table not found".
    # translate_odps_error must classify into TableNotFoundError so the
    # CLI surfaces a recoverable, user-actionable error.
    exc = odps_errors.NoSuchObject("Table not found: foo.bar")
    out = translate_odps_error(exc)
    assert isinstance(out, TableNotFoundError)


def test_no_permission_maps_to_permission_denied():
    # The message is deliberately devoid of "permission"/"access denied"/etc.
    # keywords - that way, deleting the typed isinstance(exc, OdpsNoPermission)
    # branch would actually break this test instead of silently passing via
    # the substring-matching fallback.
    exc = odps_errors.NoPermission("Forbidden")
    out = translate_odps_error(exc)
    assert isinstance(out, PermissionDeniedError)


def test_readonly_keyword_maps_to_readonly_error():
    exc = odps_errors.ODPSError("server in readonly mode")
    out = translate_odps_error(exc)
    assert isinstance(out, ReadOnlyError)


def test_generic_odps_error_preserves_message():
    # Plain ODPSError with no keyword text falls through to SqlError;
    # the original server message must be preserved verbatim so users
    # can grep for the ODPS error code.
    exc = odps_errors.ODPSError("ODPS-0010000: weird server message")
    out = translate_odps_error(exc)
    assert isinstance(out, SqlError)
    assert "ODPS-0010000" in out.message


def test_generic_odps_error_surfaces_request_id_in_suggestion():
    # When PyODPS attaches request_id, the suggestion field of the
    # returned SqlError must include it so users can file actionable
    # bug reports to MaxCompute support.
    exc = odps_errors.ODPSError("some opaque server error")
    exc.request_id = "20260521abc123"
    out = translate_odps_error(exc)
    assert isinstance(out, SqlError)
    assert out.suggestion is not None
    assert "20260521abc123" in out.suggestion


def test_plain_exception_falls_through_to_connection_error():
    out = translate_odps_error(RuntimeError("socket reset"))
    assert isinstance(out, BackendConnectionError)


def test_no_such_object_schema_maps_to_schema_not_found():
    # Schema-not-found is classified via classify_sql_error from the
    # message text; lock the behaviour in.
    exc = odps_errors.NoSuchObject("Schema 'meta_schema' not found in project foo")
    out = translate_odps_error(exc)
    # Either SchemaNotFoundError (preferred) or NotFoundError (fallback).
    assert isinstance(out, (SchemaNotFoundError, NotFoundError))


def test_generic_odps_error_without_request_id_has_no_suggestion():
    # When PyODPS does not attach a request_id, the SqlError suggestion
    # must remain None - we never want to invent a "request_id=None" tag.
    exc = odps_errors.ODPSError("opaque error with no request_id")
    out = translate_odps_error(exc)
    assert isinstance(out, SqlError)
    assert out.suggestion is None


def test_readonly_error_surfaces_request_id():
    exc = odps_errors.ODPSError("server in readonly mode")
    exc.request_id = "ro123"
    out = translate_odps_error(exc)
    assert isinstance(out, ReadOnlyError)
    assert out.suggestion is not None
    assert "ro123" in out.suggestion


def test_table_not_found_surfaces_request_id():
    exc = odps_errors.NoSuchObject("Table not found: foo.bar")
    exc.request_id = "tnf456"
    out = translate_odps_error(exc)
    assert isinstance(out, TableNotFoundError)
    assert out.suggestion is not None
    assert "tnf456" in out.suggestion

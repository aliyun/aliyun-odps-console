"""Tests for src/maxc_cli/backend/data.py private helpers."""



def test_try_abort_upload_session_reports_unsupported_session():
    from maxc_cli.backend.data import _try_abort_upload_session

    assert _try_abort_upload_session(object()) is False


def test_try_abort_upload_session_swallows_optional_abort_exception():
    from maxc_cli.backend.data import _try_abort_upload_session

    class _FlakySession:
        def abort(self):
            raise RuntimeError("network blip mid-abort")

    assert _try_abort_upload_session(_FlakySession()) is False


def test_try_abort_upload_session_calls_optional_abort_when_supported():
    from maxc_cli.backend.data import _try_abort_upload_session

    calls = []

    class _CleanSession:
        def abort(self):
            calls.append("aborted")

    assert _try_abort_upload_session(_CleanSession()) is True
    assert calls == ["aborted"]


def test_installed_pyodps_upload_session_has_no_abort_contract():
    from odps.tunnel.tabletunnel import TableUploadSession

    assert not callable(getattr(TableUploadSession, "abort", None))


def test_failed_realistic_session_reports_server_expiry_instead_of_abort():
    from maxc_cli.backend.data import _annotate_failed_upload
    from maxc_cli.exceptions import ValidationError

    error = ValidationError("row rejected")
    _annotate_failed_upload(
        error,
        object(),
        commit_attempted=False,
        create_partition=True,
    )

    assert error.context == {
        "upload_session_created": True,
        "partition_may_remain": True,
        "remote_commit_state": "not_attempted",
        "duplicate_write_risk": False,
        "uncommitted_rows_visible": False,
        "upload_session_cleanup": "server_expiry_expected",
    }
    assert "no abort API" in str(error.suggestion)

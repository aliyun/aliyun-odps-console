"""Platform-neutral coverage for local state I/O fallbacks."""

import json
from pathlib import Path

import pytest

import maxc_cli.state_permissions as state_permissions
from maxc_cli.audit import AuditLogger
from maxc_cli.cache import LocalCache
from maxc_cli.config import load_config_mapping, save_config_mapping
from maxc_cli.store import JobStore

pytestmark = pytest.mark.unit


def test_path_based_state_fallback_supports_all_local_state_consumers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Exercise the branch used when Windows cannot provide directory FDs."""
    monkeypatch.setattr(state_permissions, "_USE_DIRECTORY_FDS", False)

    config_path = tmp_path / "config" / "config.yaml"
    save_config_mapping(config_path, {"default_project": "project-a"})
    assert load_config_mapping(config_path) == {"default_project": "project-a"}

    audit_path = tmp_path / "state" / "audit.log"
    logger = AuditLogger(audit_path)
    logger.log({"command": "agent.context", "status": "success"})
    assert json.loads(audit_path.read_text(encoding="utf-8"))["status"] == "success"

    cache = LocalCache(tmp_path / "cache")
    session_id = cache.create_session("job-1", "project-a", "SELECT 1")
    assert cache.get_session(session_id)["job_id"] == "job-1"

    store = JobStore(tmp_path / "jobs")
    job = store.create_job(
        sql="SELECT 1",
        project="project-a",
        result={"data": {"rows": [{"value": 1}]}},
    )
    assert store.get_job(job["job_id"])["project"] == "project-a"


def test_path_based_atomic_replace_cleans_up_failed_temporary_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(state_permissions, "_USE_DIRECTORY_FDS", False)
    target = tmp_path / "config.yaml"
    save_config_mapping(target, {"version": "old"})

    def fail_replace(source: object, destination: object) -> None:
        raise OSError("simulated path replace failure")

    monkeypatch.setattr(state_permissions.os, "replace", fail_replace)

    with pytest.raises(OSError, match="simulated path replace failure"):
        save_config_mapping(target, {"version": "new"})

    assert load_config_mapping(target) == {"version": "old"}
    assert list(tmp_path.glob(".config.yaml.*.tmp")) == []


def test_path_based_directory_fallback_rejects_linked_leaf(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(state_permissions, "_USE_DIRECTORY_FDS", False)
    target = tmp_path / "target"
    target.mkdir()
    linked = tmp_path / "linked"
    try:
        linked.symlink_to(target, target_is_directory=True)
    except OSError as exc:  # pragma: no cover - Windows policy dependent
        pytest.skip(f"Directory symlinks are unavailable: {exc}")

    with pytest.raises(OSError, match="linked path"):
        state_permissions.open_private_directory(linked, create=False)

    fallback_handle = state_permissions.PrivateDirectoryHandle(path=target)
    assert not state_permissions.descriptor_matches_path(
        linked,
        fallback_handle,
        directory=True,
    )


def test_windows_reparse_attribute_is_treated_as_a_linked_leaf() -> None:
    class ReparseStat:
        st_mode = 0o040755
        st_file_attributes = 0x400

    assert state_permissions._is_link_or_reparse(ReparseStat())

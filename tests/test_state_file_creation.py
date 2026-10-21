"""Deterministic contracts for the concurrent create-or-open kernel boundary."""
import errno
import os

import pytest

from maxc_cli import state_permissions as state


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("pinned", [False, True])
def test_create_avoids_nonexclusive_kernel_path(tmp_path, monkeypatch, existing, pinned):
    directory = state.open_private_directory(tmp_path)
    target = tmp_path / "jobs.lock"
    if existing:
        target.write_bytes(b"existing lock")
    handle = directory if pinned else state.PrivateDirectoryHandle(tmp_path)
    real_open = os.open
    calls = []

    def guarded_open(path, flags, mode=0o777, **kwargs):
        calls.append(flags)
        # Model the observed macOS failure independently of scheduler/platform.
        if flags & os.O_CREAT and not flags & os.O_EXCL:
            raise FileNotFoundError(errno.ENOENT, "concurrent create-or-open", path)
        return real_open(path, flags, mode, **kwargs)

    monkeypatch.setattr(state.os, "open", guarded_open)
    try:
        fd = state.open_private_file_at(handle, "jobs.lock", os.O_RDWR, create=True)
        try:
            assert os.fstat(fd).st_ino == target.stat().st_ino
            assert os.read(fd, 100) == (b"existing lock" if existing else b"")
        finally:
            os.close(fd)
        assert len(calls) == (2 if existing else 1)
        if existing:
            assert not calls[-1] & os.O_CREAT
    finally:
        state.close_private_directory(directory)


@pytest.mark.skipif(not hasattr(os, "O_NOFOLLOW"), reason="requires O_NOFOLLOW")
@pytest.mark.parametrize("replacement", ["symlink", "removed"])
def test_replaced_winner_fails_closed(tmp_path, monkeypatch, replacement):
    directory = state.open_private_directory(tmp_path)
    target = tmp_path / "jobs.lock"
    victim = tmp_path / "victim"
    victim.write_bytes(b"unchanged")
    target.touch()
    real_open = os.open

    def replace_after_collision(path, flags, mode=0o777, **kwargs):
        try:
            return real_open(path, flags, mode, **kwargs)
        except FileExistsError:
            target.unlink()
            if replacement == "symlink":
                target.symlink_to(victim)
            raise

    monkeypatch.setattr(state.os, "open", replace_after_collision)
    try:
        with pytest.raises(OSError):
            state.open_private_file_at(directory, "jobs.lock", os.O_RDWR, create=True)
        assert victim.read_bytes() == b"unchanged"
        assert target.is_symlink() if replacement == "symlink" else not target.exists()
    finally:
        state.close_private_directory(directory)


def test_exclusive_create_still_rejects_existing_file(tmp_path):
    directory = state.open_private_directory(tmp_path)
    (tmp_path / "jobs.lock").touch()
    try:
        with pytest.raises(FileExistsError):
            state.open_private_file_at(directory, "jobs.lock", os.O_RDWR | os.O_EXCL, create=True)
    finally:
        state.close_private_directory(directory)

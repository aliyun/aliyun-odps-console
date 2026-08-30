"""Permission helpers for local state that may contain sensitive data.

State paths are security boundaries: opening a safe leaf and then reopening it
by pathname is not sufficient when a parent directory can be replaced. The
helpers in this module therefore pin the containing directory with a file
descriptor and use ``openat``-style operations wherever the platform supports
them.
"""

import os
import stat
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

try:
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - Windows
    _fcntl = None  # type: ignore[assignment]

try:
    import msvcrt as _msvcrt
except ImportError:  # pragma: no cover - POSIX
    _msvcrt = None  # type: ignore[assignment]

PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600

_USE_DIRECTORY_FDS = (
    os.name == "posix"
    and hasattr(os, "O_DIRECTORY")
    and os.open in getattr(os, "supports_dir_fd", ())
    and os.mkdir in getattr(os, "supports_dir_fd", ())
    and os.unlink in getattr(os, "supports_dir_fd", ())
)


@dataclass(frozen=True)
class PrivateDirectoryHandle:
    """A state directory pinned by descriptor when the platform supports it.

    Windows cannot portably open a directory through :func:`os.open`, and its
    ``os`` functions do not accept POSIX ``dir_fd`` arguments.  Keeping the
    canonical path alongside the optional descriptor lets callers use the
    same API on both platforms while preserving descriptor-relative I/O on
    POSIX.
    """

    path: 'Path'
    descriptor: 'int | None' = None


def _is_link_or_reparse(path_stat: 'os.stat_result') -> 'bool':
    """Return whether an ``lstat`` result identifies a linked path leaf."""
    if stat.S_ISLNK(path_stat.st_mode):
        return True
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    file_attributes = getattr(path_stat, "st_file_attributes", 0)
    return bool(file_attributes & reparse_flag)


def _directory_open_flags() -> 'int':
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    return flags


def _canonical_parent_path(path: 'Path') -> 'Path':
    """Resolve existing parent links without ever accepting a linked leaf.

    macOS exposes legitimate system paths such as ``/tmp`` through a symlink,
    so rejecting every linked ancestor would make otherwise safe custom state
    locations unusable. Resolving the parent first gives us a canonical path
    whose components can then be opened with ``O_NOFOLLOW``. The leaf itself
    is deliberately kept verbatim and is validated separately.
    """
    absolute = Path(os.path.abspath(os.fspath(path)))
    parent = Path(os.path.realpath(os.fspath(absolute.parent)))
    return parent / absolute.name


def open_private_directory(
    path: 'Path',
    *,
    create: 'bool' = True,
    secure_existing: 'bool' = False,
) -> 'PrivateDirectoryHandle':
    """Open and pin a CLI state directory without following its leaf.

    Missing components are created when *create* is true. On POSIX the final
    directory must be owned by the effective user. Existing custom directories
    retain their mode unless *secure_existing* is true; newly created
    directories are always mode 0700.

    The caller owns the returned handle and must close it with
    :func:`close_private_directory`.
    """
    canonical = _canonical_parent_path(path)

    if not _USE_DIRECTORY_FDS:
        existed = os.path.lexists(canonical)
        if existed:
            initial_stat = os.lstat(canonical)
            if _is_link_or_reparse(initial_stat):
                raise OSError(f"CLI state directory is a linked path: {path}")
        elif create:
            canonical.mkdir(mode=PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
        else:
            raise FileNotFoundError(os.fspath(canonical))

        directory_stat = os.lstat(canonical)
        if _is_link_or_reparse(directory_stat):
            raise OSError(f"CLI state directory is a linked path: {path}")
        if not stat.S_ISDIR(directory_stat.st_mode):
            raise NotADirectoryError(os.fspath(canonical))
        if os.name == "posix" and (secure_existing or not existed):
            canonical.chmod(PRIVATE_DIRECTORY_MODE)
        return PrivateDirectoryHandle(path=canonical)

    flags = _directory_open_flags()
    descriptor = os.open(os.path.sep, flags)
    created_leaf = False
    try:
        components = canonical.parts[1:]
        for index, component in enumerate(components):
            is_leaf = index == len(components) - 1
            try:
                next_descriptor = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, PRIVATE_DIRECTORY_MODE, dir_fd=descriptor)
                next_descriptor = os.open(component, flags, dir_fd=descriptor)
                if is_leaf:
                    created_leaf = True
            os.close(descriptor)
            descriptor = next_descriptor

        directory_stat = os.fstat(descriptor)
        if not stat.S_ISDIR(directory_stat.st_mode):
            raise OSError(f"CLI state path is not a directory: {path}")
        effective_uid = os.geteuid()
        if directory_stat.st_uid != effective_uid:
            raise OSError(
                f"CLI state directory is not owned by uid {effective_uid}: {path}"
            )
        if created_leaf or secure_existing:
            os.fchmod(descriptor, PRIVATE_DIRECTORY_MODE)
        return PrivateDirectoryHandle(path=canonical, descriptor=descriptor)
    except Exception:
        os.close(descriptor)
        raise


def close_private_directory(directory: 'PrivateDirectoryHandle') -> 'None':
    """Release a handle returned by :func:`open_private_directory`."""
    if directory.descriptor is not None:
        os.close(directory.descriptor)


def ensure_private_directory(
    path: 'Path',
    *,
    secure_existing: 'bool' = False,
) -> 'None':
    """Create and validate a state directory, then release its descriptor.

    Existing custom directories may be shared mounts. Their mode is preserved
    unless the caller positively identifies the directory as CLI-owned through
    *secure_existing*.
    """
    directory = open_private_directory(
        path,
        create=True,
        secure_existing=secure_existing,
    )
    close_private_directory(directory)


def _private_file_flags(flags: 'int', *, create: 'bool') -> 'int':
    if create:
        flags |= os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_NONBLOCK"):
        # Prevent a malicious FIFO from blocking before fstat can reject it.
        flags |= os.O_NONBLOCK
    return flags


def _validate_private_file_descriptor(
    descriptor: 'int',
    display_path: 'object',
) -> 'None':
    file_stat = os.fstat(descriptor)
    if not stat.S_ISREG(file_stat.st_mode):
        raise OSError(f"CLI state path is not a regular file: {display_path}")
    if os.name == "posix":
        effective_uid = os.geteuid()
        if file_stat.st_uid != effective_uid:
            raise OSError(
                f"CLI state file is not owned by uid {effective_uid}: {display_path}"
            )
        os.fchmod(descriptor, PRIVATE_FILE_MODE)


def open_private_file_at(
    directory: 'PrivateDirectoryHandle',
    name: 'str',
    flags: 'int',
    *,
    create: 'bool' = False,
    display_path: 'object | None' = None,
) -> 'int':
    """Open a private regular file relative to a pinned directory."""
    if not name or name in {".", ".."} or os.path.basename(name) != name:
        raise ValueError(f"State file name must be a single path component: {name!r}")
    resolved_flags = _private_file_flags(flags, create=create)
    if directory.descriptor is not None:
        descriptor = os.open(
            name,
            resolved_flags,
            PRIVATE_FILE_MODE,
            dir_fd=directory.descriptor,
        )
    else:
        descriptor = os.open(
            os.fspath(directory.path / name),
            resolved_flags,
            PRIVATE_FILE_MODE,
        )
    try:
        _validate_private_file_descriptor(
            descriptor,
            display_path if display_path is not None else name,
        )
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def descriptor_matches_path(
    path: 'Path',
    descriptor: 'int | PrivateDirectoryHandle',
    *,
    directory: 'bool' = False,
) -> 'bool':
    """Return whether *path* still identifies the object pinned by *descriptor*."""
    try:
        path_stat = os.stat(os.fspath(path), follow_symlinks=False)
        if _is_link_or_reparse(path_stat):
            return False
        expected_type = stat.S_ISDIR if directory else stat.S_ISREG
        if not expected_type(path_stat.st_mode):
            return False
        if isinstance(descriptor, PrivateDirectoryHandle):
            if descriptor.descriptor is None:
                canonical = _canonical_parent_path(Path(path))
                return directory and os.path.normcase(
                    os.path.abspath(os.fspath(canonical))
                ) == os.path.normcase(os.path.abspath(os.fspath(descriptor.path)))
            descriptor_stat = os.fstat(descriptor.descriptor)
        else:
            descriptor_stat = os.fstat(descriptor)
    except OSError:
        return False
    return (
        expected_type(descriptor_stat.st_mode)
        and path_stat.st_dev == descriptor_stat.st_dev
        and path_stat.st_ino == descriptor_stat.st_ino
    )


def ensure_private_file(path: 'Path') -> 'None':
    """Create *path* without truncating it and enforce a safe regular file."""
    directory = open_private_directory(path.parent, create=False)
    try:
        descriptor = open_private_file_at(
            directory,
            path.name,
            os.O_RDONLY,
            create=True,
            display_path=path,
        )
        os.close(descriptor)
    finally:
        close_private_directory(directory)


def open_private_file(
    path: 'Path',
    flags: 'int',
    *,
    create: 'bool' = False,
) -> 'int':
    """Open a CLI state file without following links or special files.

    This wrapper pins the containing directory for the duration of the open.
    Callers performing multiple related operations should prefer
    :func:`open_private_directory` plus :func:`open_private_file_at` so the
    same directory remains pinned for their entire transaction.
    """
    directory = open_private_directory(path.parent, create=False)
    try:
        return open_private_file_at(
            directory,
            path.name,
            flags,
            create=create,
            display_path=path,
        )
    finally:
        close_private_directory(directory)


def replace_private_file_at(
    directory: 'PrivateDirectoryHandle',
    source_name: 'str',
    destination_name: 'str',
) -> 'None':
    """Atomically replace one file with another in the same state directory."""
    for name in (source_name, destination_name):
        if not name or name in {".", ".."} or os.path.basename(name) != name:
            raise ValueError(
                f"State file name must be a single path component: {name!r}"
            )
    if directory.descriptor is not None:
        os.replace(
            source_name,
            destination_name,
            src_dir_fd=directory.descriptor,
            dst_dir_fd=directory.descriptor,
        )
    else:
        os.replace(
            os.fspath(directory.path / source_name),
            os.fspath(directory.path / destination_name),
        )


def unlink_private_file_at(
    directory: 'PrivateDirectoryHandle',
    name: 'str',
) -> 'None':
    """Remove a file relative to a state directory on every platform."""
    if not name or name in {".", ".."} or os.path.basename(name) != name:
        raise ValueError(f"State file name must be a single path component: {name!r}")
    if directory.descriptor is not None:
        os.unlink(name, dir_fd=directory.descriptor)
    else:
        os.unlink(os.fspath(directory.path / name))


def fsync_private_directory(directory: 'PrivateDirectoryHandle') -> 'None':
    """Best-effort directory-entry durability barrier where supported."""
    if os.name != "posix" or directory.descriptor is None:
        return
    try:
        os.fsync(directory.descriptor)
    except OSError:
        pass


@contextmanager
def lock_file_descriptor(
    descriptor: 'int',
    *,
    exclusive: 'bool' = True,
) -> 'Generator[None, None, None]':
    """Lock an already-open state file on POSIX or Windows."""
    if _fcntl is not None:
        operation = _fcntl.LOCK_EX if exclusive else _fcntl.LOCK_SH
        _fcntl.flock(descriptor, operation)
        try:
            yield
        finally:
            _fcntl.flock(descriptor, _fcntl.LOCK_UN)
        return

    if _msvcrt is not None:  # pragma: no cover - exercised on Windows CI
        # ``msvcrt.locking`` has no shared-lock equivalent. Serialize reads and
        # writes on a single durable byte instead.
        if os.fstat(descriptor).st_size == 0:
            os.write(descriptor, b"\0")
        os.lseek(descriptor, 0, os.SEEK_SET)
        _msvcrt.locking(descriptor, _msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            os.lseek(descriptor, 0, os.SEEK_SET)
            _msvcrt.locking(descriptor, _msvcrt.LK_UNLCK, 1)
        return

    raise RuntimeError("File locking requires fcntl or msvcrt")

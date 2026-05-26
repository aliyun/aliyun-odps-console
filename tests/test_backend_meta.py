"""Tests for src/maxc_cli/backend/meta.py best-effort helpers.

These helpers (_table_head, _list_partitions, _max_partition_spec) are
deliberately tolerant of ODPS errors — sample rows and partition lists
are optional enrichment for describe_table. But they MUST NOT swallow
unrelated Python bugs (TypeError, AttributeError, etc.), or a real
implementation defect will silently degrade describe output instead
of surfacing.
"""

import pytest
from odps.errors import ODPSError

from maxc_cli.backend.meta import MetaMixin


class _StubMeta(MetaMixin):
    """Bind the private helpers without invoking OdpsBackend.__init__."""


def _stub():
    return _StubMeta.__new__(_StubMeta)


class _Schema:
    columns = []


class _HeadOdpsErrorTable:
    table_schema = _Schema()

    def head(self, limit):
        raise ODPSError("simulated ODPS read failure")


class _HeadPythonBugTable:
    table_schema = _Schema()

    def head(self, limit):
        raise TypeError("real python bug — accidental misuse")


class _PartitionsOdpsErrorTable:
    def iterate_partitions(self):
        raise ODPSError("simulated ODPS metadata failure")


class _PartitionsPythonBugTable:
    def iterate_partitions(self):
        raise TypeError("real python bug — accidental misuse")


def test_table_head_swallows_odps_error():
    result = _stub()._table_head(_HeadOdpsErrorTable(), limit=2)
    assert result == []


def test_table_head_propagates_python_bug():
    with pytest.raises(TypeError, match="real python bug"):
        _stub()._table_head(_HeadPythonBugTable(), limit=2)


def test_list_partitions_swallows_odps_error():
    result = _stub()._list_partitions(_PartitionsOdpsErrorTable(), limit=20)
    assert result == []


def test_list_partitions_propagates_python_bug():
    with pytest.raises(TypeError, match="real python bug"):
        _stub()._list_partitions(_PartitionsPythonBugTable(), limit=20)


class _MaxPartitionOdpsErrorTable:
    def get_max_partition(self, **kwargs):
        raise ODPSError("simulated max-partition failure")


class _MaxPartitionPythonBugTable:
    # AttributeError is a realistic "Python bug" shape — TypeError can't be
    # used here because it has special meaning ("kwarg unsupported, try
    # next signature") in _max_partition_spec's loop.
    def get_max_partition(self, **kwargs):
        raise AttributeError("real python bug — accidental misuse")


def test_max_partition_spec_swallows_odps_error():
    assert _stub()._max_partition_spec(_MaxPartitionOdpsErrorTable()) is None


def test_max_partition_spec_propagates_python_bug():
    with pytest.raises(AttributeError, match="real python bug"):
        _stub()._max_partition_spec(_MaxPartitionPythonBugTable())

"""Timestamp metadata and actual PyODPS value decoding must both work."""
import pandas as pd
import pytest
from odps.models import Record
from odps.types import OdpsSchema

from maxc_cli.backend.meta import MetaMixin
from maxc_cli.helpers import record_to_dict

pytestmark = pytest.mark.unit


class TimestampTable:
    name = 'events'
    table_schema = OdpsSchema.from_lists(['event_time'], ['timestamp_ntz'], [], [])

    def iterate_partitions(self):
        return iter(())

    def head(self, limit):
        return iter([Record(schema=self.table_schema, values=[
            pd.Timestamp('2026-10-22 09:35:30.123456789')
        ])])


def backend(table):
    obj = MetaMixin()
    obj._get_table = lambda *a, **kw: table
    # Isolate network metadata properties, retaining real SDK schema/record types.
    table.comment = ''
    table.owner = None
    table.creation_time = None
    table.last_data_modified_time = None
    table.is_virtual_view = False
    table.size = 0
    table.lifecycle = None
    return obj


def test_describe_timestamp_ntz_includes_decoded_sample():
    result = backend(TimestampTable()).describe_table('events')
    assert result.columns[0].type.lower() == 'timestamp_ntz'
    assert result.sample_rows[0]['event_time'].startswith('2026-10-22T09:35:30.123456789')
    assert result.sample_warnings == []


@pytest.mark.parametrize('type_name', ['timestamp_ntz', 'timestamp'])
def test_actual_timestamp_record_retains_nanoseconds_and_null(type_name):
    schema = OdpsSchema.from_lists(['event_time', 'missing'], [type_name, type_name])
    record = Record(schema=schema, values=[pd.Timestamp('2026-10-22 09:35:30.123456789'), None])
    result = record_to_dict(['event_time', 'missing'], record.values)
    assert result['event_time'].startswith('2026-10-22T09:35:30.123456789')
    assert result['missing'] is None


def test_describe_retains_schema_and_reports_missing_decoder():
    table = TimestampTable()
    def missing(limit):
        raise ImportError('To use %s in pyodps, you need to install pandas.', 'TIMESTAMP_NTZ')
    table.head = missing
    result = backend(table).describe_table('events')
    assert result.columns[0].type.lower() == 'timestamp_ntz'
    assert result.sample_rows == []
    assert len(result.sample_warnings) == 1
    assert 'missing pandas' in result.sample_warnings[0]


def test_describe_does_not_hide_unrelated_import_errors():
    table = TimestampTable()
    def broken(limit):
        raise ImportError('application bug')
    table.head = broken
    with pytest.raises(ImportError, match='application bug'):
        backend(table).describe_table('events')


def test_sampling_warning_reaches_cli_envelope():
    from tests.test_meta_schema_and_partition_cols import _make_app, _SchemaRecordingBackend
    table = TimestampTable()
    def missing(limit):
        raise ImportError('To use TIMESTAMP in pyodps, you need to install pandas.')
    table.head = missing
    definition = backend(table).describe_table('events')
    fake = _SchemaRecordingBackend()
    fake.describe_table = lambda *a, **kw: definition
    payload = _make_app(fake).meta_describe('events').to_dict()
    assert any('missing pandas' in w for w in payload['agent_hints']['warnings'])

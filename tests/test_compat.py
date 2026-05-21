from datetime import timezone

import pytest

pytestmark = pytest.mark.unit

from maxc_cli.cli import build_parser
from maxc_cli.helpers import parse_time_value


def test_build_parser_allows_bare_invocation() -> 'None':
    """Bare `maxc` is allowed at the parser level; run() handles the no-command
    case by printing help or redirecting to `auth login`."""
    parser = build_parser()

    args = parser.parse_args([])
    assert args.command_group is None


def test_build_parser_requires_nested_subcommand() -> 'None':
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["auth"])


def test_parse_time_value_accepts_iso_z_suffix() -> 'None':
    parsed = parse_time_value("2026-03-26T12:34:56Z")

    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.astimezone(timezone.utc).isoformat() == "2026-03-26T12:34:56+00:00"


def test_parse_time_value_accepts_iso_offset_with_colon() -> 'None':
    parsed = parse_time_value("2026-03-26T20:34:56+08:00")

    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.astimezone(timezone.utc).isoformat() == "2026-03-26T12:34:56+00:00"

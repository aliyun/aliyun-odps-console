"""Tests for query auto-promote feature (--wait flag, removal of --async/--timeout)."""
import pytest
from maxc_cli.cli import build_parser


def test_query_wait_default_is_10():
    parser = build_parser()
    args = parser.parse_args(["query", "SELECT 1"])
    assert args.wait == 10


def test_query_wait_flag_accepted():
    parser = build_parser()
    args30 = parser.parse_args(["query", "--wait", "30", "SELECT 1"])
    assert args30.wait == 30
    args0 = parser.parse_args(["query", "--wait", "0", "SELECT 1"])
    assert args0.wait == 0


def test_query_no_longer_has_async_flag():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["query", "--async", "SELECT 1"])


def test_query_no_longer_has_timeout_flag():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["query", "--timeout", "60", "SELECT 1"])

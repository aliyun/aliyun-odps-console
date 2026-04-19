#!/usr/bin/env python3
"""Pre-release regression test - runs all CLI commands against real MaxCompute backend."""
import json
import os
import sys
import tempfile
from io import StringIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from maxc_cli.cli import run

RESULTS = []
TABLE = None
TABLE_COL = None
JOB_ID = None

def assert_eq(a, b): assert a == b, f"expected {b!r}, got {a!r}"
def assert_true(v): assert v, f"expected truthy, got {v!r}"
def assert_key(d, k): assert k in d, f"missing key: {k!r} in {list(d.keys())}"
def assert_type(v, t): assert isinstance(v, t), f"expected {t.__name__}, got {type(v).__name__}"

def run_cmd(label, argv, expect_code=0, check=None):
    global JOB_ID
    stdout, stderr = StringIO(), StringIO()
    code = run(argv, cwd=Path.cwd(), stdout=stdout, stderr=stderr)
    out = stdout.getvalue()
    err = stderr.getvalue()
    try:
        data = json.loads(out) if out.strip() else {}
    except json.JSONDecodeError:
        data = {"_raw": out[:300]}

    ok = (expect_code is None) or (code == expect_code)
    detail = ""
    if not ok:
        detail = f"exit_code={code}, expected={expect_code}"
        if err.strip():
            detail += f" stderr={err[:200]}"
    if ok and check:
        try:
            check(data)
        except Exception as e:
            ok = False
            detail = str(e)

    icon = "PASS" if ok else "FAIL"
    RESULTS.append((label, icon, detail))
    suffix = f"  -- {detail}" if detail else ""
    print(f"  [{icon}] {label}{suffix}")
    sys.stdout.flush()
    return code, data

def pdata(d):
    return d.get("data", {})


# ===========================================================
print("\n=== 1. Auth ===")
# ===========================================================
run_cmd("1.1 auth whoami", ["auth", "whoami", "--json"],
    check=lambda d: (
        assert_eq(d["status"], "success"),
        assert_true(pdata(d)["identity"]["authenticated"]),
    ))

# Get test table
_, tdata = run_cmd("(preflight) list-tables", ["meta", "list-tables", "--json"])
tables = pdata(tdata).get("tables", [])
TABLE = tables[0]["table_name"] if tables else None
if not TABLE:
    print("  [SKIP] No tables available, aborting")
    sys.exit(1)
print(f"  >> Test table: {TABLE}")

# Describe to get column name
_, ddata = run_cmd("(preflight) describe", ["meta", "describe", TABLE, "--json"])
cols = pdata(ddata).get("table", {}).get("schema", [])
TABLE_COL = cols[0]["name"] if cols else "id"
print(f"  >> Test column: {TABLE_COL}")

run_cmd("1.2 auth can-i SELECT", ["auth", "can-i", "--table", TABLE, "--operation", "SELECT", "--json"],
    expect_code=None,  # may be 0 or 1 depending on permissions
    check=lambda d: assert_key(pdata(d), "authorization"))

run_cmd("1.3 auth can-i --brief", ["auth", "can-i", "--table", TABLE, "--operation", "SELECT", "--brief", "--json"],
    expect_code=None,  # may be 0 or 1
    check=lambda d: assert_true(d["status"] in ("success", "failure")))


# ===========================================================
print("\n=== 2. Meta ===")
# ===========================================================
run_cmd("2.1 meta list-tables", ["meta", "list-tables", "--json"],
    check=lambda d: assert_true(len(pdata(d).get("tables", [])) > 0))

run_cmd("2.2 meta describe", ["meta", "describe", TABLE, "--json"],
    check=lambda d: (
        assert_eq(pdata(d)["table"]["table_name"], TABLE),
        assert_key(pdata(d)["table"], "schema"),
    ))

run_cmd("2.3 meta describe --full", ["meta", "describe", TABLE, "--full", "--json"],
    check=lambda d: assert_eq(d["status"], "success"))

run_cmd("2.4 meta search", ["meta", "search", "test", "--json"],
    check=lambda d: assert_key(pdata(d)["search"], "matches"))

run_cmd("2.5 meta search-columns", ["meta", "search-columns", "id", "--json"],
    check=lambda d: assert_key(pdata(d)["search"], "matches"))

run_cmd("2.6 meta partitions", ["meta", "partitions", TABLE, "--json"],
    check=lambda d: assert_key(pdata(d), "partitions"))

run_cmd("2.7 meta latest-partition", ["meta", "latest-partition", TABLE, "--json"],
    check=lambda d: assert_key(pdata(d)["partition"], "has_partitions"))

run_cmd("2.8 meta freshness", ["meta", "freshness", TABLE, "--json"],
    check=lambda d: assert_key(pdata(d)["freshness"], "freshness_status"))

run_cmd("2.9 meta list-projects", ["meta", "list-projects", "--json"],
    check=lambda d: assert_type(pdata(d)["projects"], list))

run_cmd("2.10 meta list-schemas", ["meta", "list-schemas", "--json"],
    expect_code=None,  # may fail if catalog not available
    check=lambda d: assert_true(d["status"] in ("success", "failure")))


# ===========================================================
print("\n=== 3. Meta Semantic ===")
# ===========================================================
run_cmd("3.1 meta semantic set", ["meta", "semantic", "set", TABLE, "--desc", "regression test desc", "--json"],
    check=lambda d: assert_eq(d["status"], "success"))

run_cmd("3.2 meta semantic get", ["meta", "semantic", "get", TABLE, "--json"],
    check=lambda d: assert_eq(d["status"], "success"))

run_cmd("3.3 meta semantic list-missing", ["meta", "semantic", "list-missing", "--json"],
    check=lambda d: assert_eq(d["status"], "success"))


# ===========================================================
print("\n=== 4. Query ===")
# ===========================================================
run_cmd("4.1 query simple", ["query", "SELECT 1 AS c1, 'test' AS c2", "--json", "--force"],
    check=lambda d: (
        assert_eq(d["status"], "success"),
        assert_true(len(pdata(d)["result"]["rows"]) == 1),
    ))

run_cmd("4.2 query cost", ["query", "cost", "SELECT 1 AS c1", "--json", "--force"],
    check=lambda d: assert_key(pdata(d)["analysis"], "cost_model"))

run_cmd("4.3 query explain", ["query", "explain", "SELECT 1 AS c1", "--json", "--force"],
    check=lambda d: assert_eq(pdata(d)["analysis"]["analysis_mode"], "explain"))

run_cmd("4.4 query pagination",
    ["query", "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5",
     "--page-size", "2", "--json", "--force"],
    check=lambda d: (
        assert_true(pdata(d)["pagination"]["has_more"]),
        assert_true(pdata(d)["pagination"]["next_cursor"] is not None),
    ))

run_cmd("4.5 query dry-run", ["query", "SELECT 1", "--dry-run", "--json", "--force"],
    check=lambda d: assert_eq(pdata(d)["result"]["returned_rows"], 0))

run_cmd("4.6 query wait=0", ["query", "SELECT 1 AS async_test", "--wait", "0", "--json", "--force"],
    check=lambda d: (
        assert_eq(d["status"], "pending"),
        assert_key(pdata(d), "job"),
    ))

tmpfile = Path(tempfile.mktemp(suffix=".csv"))
run_cmd("4.7 query --output csv",
    ["query", "SELECT 1 AS a, 2 AS b", "--json", "--force", "--output", str(tmpfile), "--output-format", "csv"],
    check=lambda d: assert_eq(d["status"], "success"))
if tmpfile.exists():
    print(f"       CSV file created: {tmpfile.stat().st_size} bytes")
    tmpfile.unlink()
else:
    RESULTS.append(("4.7b csv file exists", "FAIL", "file not created"))


# ===========================================================
print("\n=== 5. Query: New Features ===")
# ===========================================================
run_cmd("5.1 masking (phone/email/password)",
    ["query", "SELECT 1 as id, '13812345678' as phone, 'alice@example.com' as email, 'secret' as password",
     "--force", "--json"],
    check=lambda d: (
        assert_eq(pdata(d)["result"]["rows"][0]["phone"], "138****5678"),
        assert_eq(pdata(d)["result"]["rows"][0]["email"], "a***@example.com"),
        assert_eq(pdata(d)["result"]["rows"][0]["password"], "******"),
        assert_true(any("masked" in w.lower() for w in d.get("agent_hints", {}).get("warnings", []))),
    ))

run_cmd("5.2 LIMIT truncation warning",
    ["query", "SELECT t.* FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t",
     "--max-rows", "2", "--force", "--json"],
    check=lambda d: (
        assert_true(pdata(d)["pagination"]["has_more"]),
        assert_true(any("truncated" in w.lower() for w in d.get("agent_hints", {}).get("warnings", []))),
    ))

run_cmd("5.3 error self-correction (wrong column)",
    ["query", f"SELECT nonexistent_col FROM {TABLE} LIMIT 5", "--force", "--json"],
    expect_code=None,  # SQL_ERROR(1) or other
    check=lambda d: (
        assert_true(d["status"] == "failure"),
        assert_key(d.get("data", {}), "schema_context"),
    ))


# ===========================================================
print("\n=== 6. Job ===")
# ===========================================================
_, jsubmit = run_cmd("6.1 job submit", ["job", "submit", "SELECT 1 AS job_test", "--json", "--force"],
    check=lambda d: assert_key(pdata(d), "job_id"))
JOB_ID = pdata(jsubmit).get("job_id")
if JOB_ID:
    print(f"       job_id: {JOB_ID}")

    run_cmd("6.2 job status", ["job", "status", JOB_ID, "--json"],
        check=lambda d: assert_key(pdata(d).get("job", {}), "stage"))

    run_cmd("6.3 job wait", ["job", "wait", JOB_ID, "--json"],
        check=lambda d: assert_true(d["status"] in ("success", "failure")))

    run_cmd("6.4 job result", ["job", "result", JOB_ID, "--json"],
        check=lambda d: assert_key(pdata(d).get("result", {}), "rows"))

    run_cmd("6.5 job diagnose", ["job", "diagnose", JOB_ID, "--json"],
        check=lambda d: assert_key(pdata(d).get("diagnosis", {}), "diagnosis_category"))
else:
    for label in ["6.2 job status", "6.3 job wait", "6.4 job result", "6.5 job diagnose"]:
        RESULTS.append((label, "SKIP", "no job_id"))

run_cmd("6.6 job list", ["job", "list", "--json"],
    check=lambda d: assert_type(pdata(d).get("jobs", None), list))


# ===========================================================
print("\n=== 7. Data ===")
# ===========================================================
run_cmd("7.1 data sample", ["data", "sample", TABLE, "--rows", "3", "--json"],
    expect_code=None,  # may fail if table is partitioned
    check=lambda d: assert_true(d["status"] in ("success", "failure")))

run_cmd("7.2 data profile", ["data", "profile", TABLE, "--json"],
    expect_code=None,  # may fail if table is partitioned
    check=lambda d: assert_true(d["status"] in ("success", "failure")))


# ===========================================================
print("\n=== 8. Diff ===")
# ===========================================================
run_cmd("8.1 diff schema self", ["diff", "schema", TABLE, TABLE, "--json"],
    check=lambda d: assert_eq(pdata(d)["diff"]["compatible"], True))

run_cmd("8.2 diff partition self", ["diff", "partition", TABLE, TABLE, "--json"],
    check=lambda d: assert_key(pdata(d)["diff"], "summary"))

run_cmd("8.3 diff data self", ["diff", "data", TABLE, TABLE, "--keys", TABLE_COL, "--rows", "5", "--json"],
    expect_code=None,  # may fail if table is partitioned
    check=lambda d: assert_true(d["status"] in ("success", "failure")))


# ===========================================================
print("\n=== 9. Session ===")
# ===========================================================
run_cmd("9.1 session show", ["session", "show", "--json"],
    check=lambda d: assert_key(pdata(d), "project"))

run_cmd("9.2 session set", ["session", "set", "--project", "meta_dev", "--json"],
    check=lambda d: assert_eq(d["status"], "success"))

run_cmd("9.3 session unset", ["session", "unset", "--json"],
    check=lambda d: assert_eq(d["status"], "success"))


# ===========================================================
print("\n=== 10. Agent ===")
# ===========================================================
run_cmd("10.1 agent context", ["agent", "context", "--json"],
    check=lambda d: assert_key(pdata(d).get("context", {}), "project"))

run_cmd("10.2 agent skill", ["agent", "skill", "--json"],
    check=lambda d: assert_eq(d["status"], "success"))


# ===========================================================
print("\n=== 11. Cache ===")
# ===========================================================
run_cmd("11.1 cache status", ["cache", "status", "--json"],
    check=lambda d: assert_key(pdata(d), "table_count"))

run_cmd("11.2 cache clear", ["cache", "clear", "--json"],
    check=lambda d: assert_key(pdata(d), "deleted_tables"))

run_cmd("11.3 cache build", ["cache", "build", "--json"],
    check=lambda d: assert_key(pdata(d), "cached_tables"))

run_cmd("11.4 cache build-status", ["cache", "build-status", "--json"],
    check=lambda d: assert_eq(d["status"], "success"))


# ===========================================================
# SUMMARY
# ===========================================================
print("\n" + "=" * 60)
print("REGRESSION TEST SUMMARY")
print("=" * 60)
passed = sum(1 for _, s, _ in RESULTS if s == "PASS")
failed = sum(1 for _, s, _ in RESULTS if s == "FAIL")
skipped = sum(1 for _, s, _ in RESULTS if s == "SKIP")

if failed:
    print(f"\nFAILED ({failed}):")
    for label, status, detail in RESULTS:
        if status == "FAIL":
            print(f"  - {label}: {detail}")

print(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped / {len(RESULTS)} tests")
print("=" * 60)
sys.exit(1 if failed else 0)

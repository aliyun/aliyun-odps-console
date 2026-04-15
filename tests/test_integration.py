"""Integration tests for maxc-cli with real MaxCompute backend.

These tests require:
1. Valid MaxCompute credentials in environment variables:
   - MAXCOMPUTE_PROJECT (or ODPS_PROJECT)
   - MAXCOMPUTE_ENDPOINT (or ODPS_ENDPOINT)
   - MAXCOMPUTE_ACCESS_ID (or ODPS_ACCESS_ID)
   - MAXCOMPUTE_ACCESS_KEY (or ODPS_ACCESS_KEY)
2. A test project with sample tables

Skip these tests in CI by not setting the environment variables.
"""

import os
import pytest

pytestmark = pytest.mark.integration
from pathlib import Path
from io import StringIO
from maxc_cli.cli import run


def has_real_credentials() -> 'bool':
    """Check if real MaxCompute credentials are available."""
    required_vars = [
        "MAXCOMPUTE_PROJECT",
        "MAXCOMPUTE_ENDPOINT", 
        "MAXCOMPUTE_ACCESS_ID",
        "MAXCOMPUTE_ACCESS_KEY",
    ]
    # Also check legacy ODPS_* names
    for var in required_vars:
        if not (os.environ.get(var) or os.environ.get(var.replace("MAXCOMPUTE", "ODPS"))):
            return False
    return True


@pytest.fixture
def real_config(tmp_path: 'Path') -> 'Path':
    """Create a minimal config that uses real MaxCompute credentials from env."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
backend:
  type: auto
default_format: json
""".strip(),
        encoding="utf-8",
    )
    return config_path


@pytest.mark.skipif(not has_real_credentials(), reason="Requires real MaxCompute credentials")
class TestRealMaxComputeBackend:
    """Integration tests with real MaxCompute backend."""

    def test_auth_whoami_returns_identity(self, real_config: 'Path') -> 'None':
        """Test that auth whoami returns current user identity."""
        stdout = StringIO()
        code = run(["--config", str(real_config), "auth", "whoami", "--json"], stdout=stdout, stderr=StringIO())
        
        assert code == 0
        # Parse output to verify structure
        import json
        result = json.loads(stdout.getvalue())
        assert result["command"] == "auth whoami"
        assert result["status"] == "success"
        assert "identity" in result["data"]
        assert result["data"]["identity"]["authenticated"] is True
        # Should have project_owner from security API or fallback to access_id
        assert "project_owner" in result["data"]["identity"]
        owner = result["data"]["identity"]["project_owner"]
        if owner:
            print(f"✓ Auth whoami returned owner: {owner}")
        else:
            print("⚠ Warning: project_owner is null (security API may be unavailable)")

    def test_meta_list_tables(self, real_config: 'Path') -> 'None':
        """Test listing tables in real project."""
        stdout = StringIO()
        code = run(["--config", str(real_config), "meta", "list-tables", "--json"], stdout=stdout, stderr=StringIO())
        
        assert code == 0
        import json
        result = json.loads(stdout.getvalue())
        assert result["command"] == "meta list-tables"
        assert result["status"] == "success"
        assert "tables" in result["data"]
        print(f"✓ Found {len(result['data']['tables'])} tables")

    def test_agent_context_fast(self, real_config: 'Path') -> 'None':
        """Test that agent context doesn't call list_tables (should be fast)."""
        import time
        stdout = StringIO()
        
        start = time.time()
        code = run(["--config", str(real_config), "agent", "context", "--json"], stdout=stdout, stderr=StringIO())
        elapsed = time.time() - start
        
        assert code == 0
        import json
        result = json.loads(stdout.getvalue())
        assert result["command"] == "agent context"
        assert result["status"] == "success"
        # Should NOT have catalog.tables anymore
        assert "catalog" not in result["data"]["context"]
        print(f"✓ Agent context completed in {elapsed:.3f}s (fast!)")

    def test_query_basic_select(self, real_config: 'Path') -> 'None':
        """Test basic SELECT query on real backend."""
        stdout = StringIO()
        # Use a simple query that should work on any project
        code = run([
            "--config", str(real_config),
            "query", "SELECT 1 AS test",
            "--json"
        ], stdout=stdout, stderr=StringIO())
        
        assert code == 0
        import json
        result = json.loads(stdout.getvalue())
        assert result["command"] == "query"
        assert result["status"] == "success"
        assert "result" in result["data"]
        assert len(result["data"]["result"]["rows"]) > 0
        print(f"✓ Query executed successfully")

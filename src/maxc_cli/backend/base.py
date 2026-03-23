"""Base backend abstract class."""

from abc import ABC, abstractmethod
from typing import Any

from ..config import TableDefinition
from ..models import JobInfo, QueryResult


class BaseBackend(ABC):
    """Abstract base class for all backends."""

    @abstractmethod
    def execute_query(
        self,
        sql: str,
        *,
        project: str,
        max_rows: int,
        dry_run: bool,
        offset: int = 0,
    ) -> QueryResult:
        """Execute a SQL query and return results."""
        ...

    @abstractmethod
    def estimate_query_cost(self, sql: str, *, project: str) -> dict[str, Any]:
        """Estimate the cost of a query."""
        ...

    @abstractmethod
    def explain_query(self, sql: str, *, project: str) -> dict[str, Any]:
        """Explain a query execution plan."""
        ...

    @abstractmethod
    def submit_query(
        self,
        sql: str,
        *,
        project: str,
        max_rows: int = 100,
    ) -> JobInfo:
        """Submit a query for async execution."""
        ...

    @abstractmethod
    def get_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        """Get job status."""
        ...

    @abstractmethod
    def wait_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        """Wait for job completion."""
        ...

    @abstractmethod
    def fetch_job_result(
        self,
        job_id: str,
        *,
        project: str | None = None,
        max_rows: int = 100,
        offset: int = 0,
    ) -> QueryResult:
        """Fetch job results."""
        ...

    @abstractmethod
    def cancel_job(self, job_id: str, *, project: str | None = None) -> JobInfo:
        """Cancel a job."""
        ...

    @abstractmethod
    def diagnose_job(self, job_id: str, *, project: str | None = None) -> dict[str, Any]:
        """Diagnose a job failure."""
        ...

    @abstractmethod
    def list_jobs(self, *, project: str | None = None, limit: int = 20) -> list[JobInfo]:
        """List jobs."""
        ...

    @abstractmethod
    def list_tables(self) -> list[TableDefinition]:
        """List tables."""
        ...

    @abstractmethod
    def describe_table(self, table_name: str) -> TableDefinition:
        """Describe a table."""
        ...

    @abstractmethod
    def search_tables(self, keyword: str) -> list[dict[str, Any]]:
        """Search tables by keyword."""
        ...

    @abstractmethod
    def search_columns(self, keyword: str) -> list[dict[str, Any]]:
        """Search columns by keyword."""
        ...

    @abstractmethod
    def latest_partition_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        """Get latest partition info."""
        ...

    @abstractmethod
    def freshness_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        """Get data freshness info."""
        ...

    @abstractmethod
    def lineage_info(self, table_name: str) -> tuple[dict[str, Any], list[str]]:
        """Get table lineage info."""
        ...

    @abstractmethod
    def sample_table(
        self,
        table_name: str,
        *,
        partition: str | None = None,
        columns: list[str] | None = None,
        rows: int = 5,
    ) -> dict[str, Any]:
        """Sample table data."""
        ...

    @abstractmethod
    def profile_table(self, table_name: str, *, partition: str | None = None) -> dict[str, Any]:
        """Profile table data."""
        ...

    @abstractmethod
    def list_projects(self) -> list[dict[str, Any]]:
        """List accessible projects."""
        ...

    @abstractmethod
    def list_schemas(self, *, project: str | None = None) -> list[dict[str, Any]]:
        """List schemas in a project."""
        ...

    @abstractmethod
    def get_project_info(self, project_name: str | None = None) -> dict[str, Any]:
        """Get project info."""
        ...

    @abstractmethod
    def whoami_info(self, *, project: str | None = None) -> tuple[dict[str, Any], list[str]]:
        """Get current identity info."""
        ...

    @abstractmethod
    def can_i_info(
        self,
        *,
        table_name: str,
        operation: str,
        project: str | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        """Check if operation is allowed on table."""
        ...

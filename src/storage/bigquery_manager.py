"""BigQuery Lakehouse Persistence & Partition-Targeted Storage Manager.

Manages dataset provisioning, free-tier zero-cost batch load jobs,
partition-filtered SQL executions, and partition pruning.
"""

from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, QueryJobConfig, WriteDisposition
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="bigquery-storage-manager"
)


class BigQueryManager:
    """Manages cloud lakehouse operations adhering strictly to GCP Always-Free constraints."""

    def __init__(
        self,
        settings: Optional[AppSettings] = None,
        client: Optional[bigquery.Client] = None,
    ) -> None:
        self.settings: AppSettings = settings or load_settings()
        self.project_id: str = self.settings.gcp.project_id
        self.dataset_id: str = self.settings.gcp.dataset_id
        self.location: str = self.settings.gcp.location
        self.silver_table_name: str = self.settings.bigquery_tables.silver_table
        self.gold_table_name: str = self.settings.bigquery_tables.gold_kpi_table
        self.audit_table_name: str = self.settings.bigquery_tables.audit_table

        # Client injection for testing or default GCP ADC client
        self.client: bigquery.Client = client or bigquery.Client(project=self.project_id)
        self.dataset_ref: bigquery.DatasetReference = self.client.dataset(self.dataset_id)

    @retry(
        retry=retry_if_exception_type((GoogleAPICallError, RetryError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def ensure_dataset(self) -> None:
        """Provisions BigQuery dataset if not exists in specified geographic region."""
        dataset = bigquery.Dataset(self.dataset_ref)
        dataset.location = self.location
        dataset.description = "Flight and weather analytical lakehouse dataset (Always-Free Tier)"
        dataset = self.client.create_dataset(dataset, exists_ok=True)
        logger.info(
            "Verified BigQuery dataset existence",
            extra={"project": self.project_id, "dataset": self.dataset_id, "location": self.location},
        )

    def initialize_tables(self, ddl_dir: Optional[Path] = None) -> None:
        """Executes DDL scripts to create silver, gold, and audit tables if not present."""
        base_ddl_dir = ddl_dir or (
            Path(__file__).resolve().parent.parent.parent / "sql" / "ddl"
        )
        ddl_files = [
            base_ddl_dir / "create_silver_tables.sql",
            base_ddl_dir / "create_gold_tables.sql",
            base_ddl_dir / "create_audit_tables.sql",
        ]

        for ddl_path in ddl_files:
            if not ddl_path.exists():
                logger.error("DDL file missing", extra={"path": str(ddl_path)})
                raise FileNotFoundError(f"Missing DDL file at {ddl_path}")

            ddl_sql = ddl_path.read_text(encoding="utf-8")
            # Enforce project and dataset targeting
            resolved_sql = ddl_sql.replace(
                "flight_weather_analytics", f"{self.project_id}.{self.dataset_id}"
            )
            logger.info("Executing DDL provisioning", extra={"file": ddl_path.name})
            query_job = self.client.query(resolved_sql)
            query_job.result()  # Wait for execution

    @retry(
        retry=retry_if_exception_type((GoogleAPICallError, RetryError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def load_silver_records(self, records: List[Dict[str, Any]]) -> int:
        """Loads enriched flight-weather telemetry into BigQuery Silver table.

        Crucial Architecture: Utilizes BigQuery batch load jobs instead of streaming inserts.
        Batch load jobs are 100% free of charge under GCP Always-Free Tier limits.
        """
        if not records:
            logger.warning("Empty records list provided to BigQuery Silver loader")
            return 0

        table_ref = self.dataset_ref.table(self.silver_table_name)
        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ignore_unknown_values=True,
        )

        logger.info(
            "Submitting batch load job to Silver table",
            extra={
                "table": f"{self.dataset_id}.{self.silver_table_name}",
                "records_count": len(records),
            },
        )

        load_job = self.client.load_table_from_json(
            json_rows=records,
            destination=table_ref,
            job_config=job_config,
        )
        load_job.result()  # Wait for completion

        if load_job.errors:
            logger.error("Errors encountered during Silver batch load", extra={"errors": load_job.errors})
            raise RuntimeError(f"BigQuery load failed with errors: {load_job.errors}")

        logger.info(
            "Silver load job completed successfully",
            extra={"output_rows": load_job.output_rows, "job_id": load_job.job_id},
        )
        return load_job.output_rows or len(records)

    @retry(
        retry=retry_if_exception_type((GoogleAPICallError, RetryError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def refresh_gold_kpis(
        self,
        sql_path: Optional[Path] = None,
        target_date: Optional[str] = None,
    ) -> int:
        """Executes Gold analytical SQL rollup for the current partition date."""
        target_sql_path = sql_path or (
            Path(__file__).resolve().parent.parent.parent
            / "sql"
            / "gold_aggregations"
            / "weather_impact_matrix.sql"
        )
        if not target_sql_path.exists():
            raise FileNotFoundError(f"Missing Gold SQL script at {target_sql_path}")

        raw_sql = target_sql_path.read_text(encoding="utf-8")
        partition_date = (
            target_date
            if target_date
            else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )

        # Substitute dataset namespace and date partition
        rendered_sql = raw_sql.replace(
            "flight_weather_analytics", f"{self.project_id}.{self.dataset_id}"
        ).replace("@target_date", f"'{partition_date}'")

        logger.info(
            "Executing Gold KPI rollup aggregation",
            extra={"partition_date": partition_date, "script": target_sql_path.name},
        )

        query_job = self.client.query(rendered_sql)
        query_job.result()

        logger.info("Gold KPI aggregation executed successfully", extra={"job_id": query_job.job_id})
        return query_job.num_dml_affected_rows or 0

    @retry(
        retry=retry_if_exception_type((GoogleAPICallError, RetryError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def delete_partition(self, table_name: str, partition_date: str) -> int:
        """Prunes an expired partition date from a hot table after archival verification.

        Args:
            table_name: Name of table (e.g. silver_flight_weather_telemetry).
            partition_date: Date string formatted as 'YYYY-MM-DD'.

        Returns:
            int: Number of deleted rows.
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE recorded_date = DATE('{partition_date}');
        """

        logger.info(
            "Pruning expired partition from hot table",
            extra={"table": table_id, "partition_date": partition_date},
        )

        job = self.client.query(delete_query)
        job.result()
        affected = job.num_dml_affected_rows or 0
        logger.info(
            "Partition pruned successfully",
            extra={"table": table_id, "rows_evicted": affected},
        )
        return affected

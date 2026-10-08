"""Autonomous Eviction Sentinel & Multi-Cloud Cold Tier Archival Engine.

Enforces BigQuery 10 GB Always-Free storage caps by pruning aged partitions,
serializing datasets to ZSTD Parquet, streaming to Cloudflare R2,
and recording immutable SHA-256 cryptographic audit logs.
"""

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Optional
import uuid
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings
from src.storage.bigquery_manager import BigQueryManager
from src.storage.parquet_exporter import ExportArtifact, ParquetExporter

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="eviction-sentinel"
)


class EvictionSentinel:
    """Zero-Dollar cloud storage custodian and archival coordinator."""

    def __init__(
        self,
        settings: Optional[AppSettings] = None,
        bq_manager: Optional[BigQueryManager] = None,
        s3_client: Optional[Any] = None,
        parquet_exporter: Optional[ParquetExporter] = None,
    ) -> None:
        self.settings: AppSettings = settings or load_settings()
        self.bq_manager: BigQueryManager = bq_manager or BigQueryManager(settings=self.settings)
        self.exporter: ParquetExporter = parquet_exporter or ParquetExporter(settings=self.settings)
        self.r2_bucket: str = self.settings.archival.r2_bucket_name
        self.r2_endpoint: str = self.settings.archival.r2_endpoint_url
        self.retention_days: int = self.settings.archival.retention_days_cloud
        self.audit_table_name: str = self.settings.bigquery_tables.audit_table

        # Cloudflare R2 / S3 client initialization
        if s3_client is not None:
            self.s3_client = s3_client
        else:
            r2_access_key = os.environ.get("R2_ACCESS_KEY_ID")
            r2_secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
            if r2_access_key and r2_secret_key:
                self.s3_client = boto3.client(
                    "s3",
                    endpoint_url=self.r2_endpoint,
                    aws_access_key_id=r2_access_key,
                    aws_secret_access_key=r2_secret_key,
                )
            else:
                logger.info("R2 credentials not detected; operating in local archival fallback mode")
                self.s3_client = None

    @retry(
        retry=retry_if_exception_type((BotoCoreError, ClientError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def upload_to_cold_storage(self, local_file_path: Path, remote_key: str) -> str:
        """Transfers local Parquet archive to Cloudflare R2 object storage.

        Returns:
            str: Destination URI (e.g. r2://bucket/key or local://path).
        """
        if self.s3_client is not None:
            logger.info(
                "Uploading Parquet archive to Cloudflare R2",
                extra={"bucket": self.r2_bucket, "key": remote_key},
            )
            self.s3_client.upload_file(
                Filename=str(local_file_path),
                Bucket=self.r2_bucket,
                Key=remote_key,
            )
            return f"r2://{self.r2_bucket}/{remote_key}"

        # Local fallback mode
        return f"local://{local_file_path.as_posix()}"

    def write_audit_log(self, audit_row: Dict[str, Any]) -> None:
        """Persists immutable 1 KB audit record into BigQuery audit log table."""
        audit_table_ref = self.bq_manager.dataset_ref.table(self.audit_table_name)
        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        load_job = self.bq_manager.client.load_table_from_json(
            json_rows=[audit_row],
            destination=audit_table_ref,
            job_config=job_config,
        )
        load_job.result()
        logger.info(
            "Persisted immutable archival audit record to BigQuery",
            extra={"audit_id": audit_row["audit_id"], "sha256": audit_row["sha256_checksum"]},
        )

    def evict_partition(self, table_name: str, partition_date: str) -> Dict[str, Any]:
        """Executes full custody transfer and eviction cycle for a single partition date.

        Steps:
        1. Query partition records from BigQuery.
        2. Serialize to compressed ZSTD Parquet.
        3. Upload Parquet to Cloudflare R2.
        4. Write immutable SHA-256 audit entry to BigQuery.
        5. Truncate/delete source partition from hot BigQuery table.

        Returns:
            Dict[str, Any]: Audit summary of the eviction.
        """
        start_time = time.time()
        audit_id = str(uuid.uuid4())
        dataset = f"{self.bq_manager.project_id}.{self.bq_manager.dataset_id}"
        source_table = f"{dataset}.{table_name}"

        logger.info(
            "Beginning partition eviction protocol",
            extra={"table": source_table, "partition_date": partition_date, "audit_id": audit_id},
        )

        # Step 1: Query partition records
        select_query = f"""
        SELECT *
        FROM `{source_table}`
        WHERE recorded_date = DATE('{partition_date}')
        """
        query_job = self.bq_manager.client.query(select_query)
        rows = [dict(row) for row in query_job]

        if not rows:
            logger.warning("Partition contains 0 rows; nothing to evict", extra={"partition": partition_date})
            return {"status": "SKIPPED_EMPTY_PARTITION", "record_count": 0}

        # Step 2: Serialize to ZSTD Parquet
        parquet_file_name = f"{table_name}_{partition_date}.parquet"
        artifact: ExportArtifact = self.exporter.export_records_to_parquet(
            records=rows,
            output_file_name=parquet_file_name,
        )

        # Step 3: Upload to Cloudflare R2
        remote_key = f"archives/{table_name}/{partition_date}/{parquet_file_name}"
        destination_uri = self.upload_to_cold_storage(artifact.file_path, remote_key)

        elapsed = round(time.time() - start_time, 2)

        # Step 4: Write Audit Entry to BigQuery
        audit_row = {
            "audit_id": audit_id,
            "archival_timestamp": datetime.now(timezone.utc).isoformat(),
            "target_partition_date": partition_date,
            "record_count_evicted": artifact.record_count,
            "raw_uncompressed_bytes": artifact.uncompressed_bytes,
            "compressed_parquet_bytes": artifact.compressed_bytes,
            "compression_ratio": artifact.compression_ratio,
            "sha256_checksum": artifact.sha256_checksum,
            "storage_destination_uri": destination_uri,
            "eviction_status": "SUCCESS",
            "truncated_table_name": table_name,
            "execution_duration_seconds": elapsed,
            "metadata_json": json.dumps({
                "compression": self.settings.archival.compression_algorithm,
                "compression_level": self.settings.archival.compression_level,
                "storage_provider": "Cloudflare R2" if self.s3_client else "Local",
            }),
        }
        self.write_audit_log(audit_row)

        # Step 5: Safely wipe partition from BigQuery hot storage
        deleted_rows = self.bq_manager.delete_partition(table_name, partition_date)

        logger.info(
            "Partition eviction lifecycle completed successfully",
            extra={
                "audit_id": audit_id,
                "rows_evicted": deleted_rows,
                "destination": destination_uri,
                "sha256": artifact.sha256_checksum,
            },
        )

        return audit_row

"""Laptop-Side Zero-Egress Local Archive Sync & Integrity Verification Agent.

Pulls cold Parquet archives from Cloudflare R2 (zero egress fees) to local storage
and verifies cryptographic SHA-256 checksums against BigQuery audit logs.
"""

import argparse
import hashlib
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# Ensure repository root is on sys.path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings
from src.storage.bigquery_manager import BigQueryManager

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="local-archive-sync"
)


def compute_local_sha256(file_path: Path) -> str:
    """Calculates SHA-256 checksum for a local binary file."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


class LocalArchiveSyncAgent:
    """Syncs cloud Parquet archives to local laptop disk with zero egress cost."""

    def __init__(
        self,
        settings: Optional[AppSettings] = None,
        bq_manager: Optional[BigQueryManager] = None,
        s3_client: Optional[Any] = None,
        destination_dir: Optional[Path] = None,
    ) -> None:
        self.settings: AppSettings = settings or load_settings()
        self.bq_manager: BigQueryManager = bq_manager or BigQueryManager(settings=self.settings)
        self.dest_dir: Path = destination_dir or (
            _REPO_ROOT / self.settings.archival.local_archive_dir
        )
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        self.r2_bucket: str = self.settings.archival.r2_bucket_name

        if s3_client is not None:
            self.s3_client = s3_client
        else:
            r2_access_key = os.environ.get("R2_ACCESS_KEY_ID")
            r2_secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
            if r2_access_key and r2_secret_key:
                self.s3_client = boto3.client(
                    "s3",
                    endpoint_url=self.settings.archival.r2_endpoint_url,
                    aws_access_key_id=r2_access_key,
                    aws_secret_access_key=r2_secret_key,
                )
            else:
                self.s3_client = None

    def fetch_audit_manifests(self) -> List[Dict[str, Any]]:
        """Queries BigQuery audit table to retrieve all successful eviction manifests."""
        dataset = f"{self.bq_manager.project_id}.{self.bq_manager.dataset_id}"
        audit_table = f"{dataset}.{self.settings.bigquery_tables.audit_table}"

        manifest_query = f"""
        SELECT
            audit_id,
            archival_timestamp,
            target_partition_date,
            record_count_evicted,
            compressed_parquet_bytes,
            sha256_checksum,
            storage_destination_uri,
            truncated_table_name
        FROM `{audit_table}`
        WHERE eviction_status = 'SUCCESS'
        ORDER BY target_partition_date DESC;
        """

        try:
            logger.info("Querying BigQuery archival audit manifests", extra={"table": audit_table})
            job = self.bq_manager.client.query(manifest_query)
            return [dict(row) for row in job]
        except Exception as exc:
            logger.error("Failed to query audit manifests from BigQuery", exc_info=True)
            return []

    @retry(
        retry=retry_if_exception_type((BotoCoreError, ClientError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def download_archive(self, remote_uri: str, local_path: Path) -> None:
        """Downloads Parquet archive from remote cloud storage to local disk."""
        if remote_uri.startswith("r2://"):
            if not self.s3_client:
                raise RuntimeError("Cloudflare R2 credentials not configured for remote download")

            # Extract S3 bucket and key from URI
            parts = remote_uri.replace("r2://", "").split("/", 1)
            bucket_name = parts[0]
            key_name = parts[1]

            logger.info(
                "Streaming Parquet archive from Cloudflare R2",
                extra={"bucket": bucket_name, "key": key_name, "local_target": str(local_path)},
            )
            self.s3_client.download_file(
                Bucket=bucket_name,
                Key=key_name,
                Filename=str(local_path),
            )
        elif remote_uri.startswith("local://"):
            source_path = Path(remote_uri.replace("local://", ""))
            if source_path.resolve() != local_path.resolve():
                local_path.write_bytes(source_path.read_bytes())
        else:
            raise ValueError(f"Unsupported storage protocol: {remote_uri}")

    def sync_all_archives(
        self, verify_only: bool = False, dry_run: bool = False
    ) -> Dict[str, Any]:
        """Synchronizes remote archives locally and verifies cryptographic checksums.

        Returns:
            Dict[str, Any]: Sync metrics report.
        """
        manifests = self.fetch_audit_manifests()
        logger.info("Discovered archival records", extra={"total_manifests": len(manifests)})

        downloaded_count = 0
        verified_count = 0
        corrupted_count = 0

        for m in manifests:
            partition_date = str(m["target_partition_date"])
            table_name = m["truncated_table_name"]
            expected_hash = m["sha256_checksum"]
            remote_uri = m["storage_destination_uri"]

            filename = f"{table_name}_{partition_date}.parquet"
            local_file = self.dest_dir / filename

            # Download if missing
            if not local_file.exists():
                if verify_only:
                    logger.warning("Archive missing locally in verify-only mode", extra={"file": filename})
                    continue
                if dry_run:
                    logger.info("Dry-run: would download archive", extra={"file": filename, "uri": remote_uri})
                    continue

                self.download_archive(remote_uri=remote_uri, local_path=local_file)
                downloaded_count += 1

            # Validate cryptographic SHA-256 checksum
            if local_file.exists():
                actual_hash = compute_local_sha256(local_file)
                if actual_hash == expected_hash:
                    verified_count += 1
                    logger.info("Archive cryptographic integrity verified", extra={"file": filename, "sha256": actual_hash})
                else:
                    corrupted_count += 1
                    logger.error(
                        "CRITICAL: Cryptographic hash mismatch!",
                        extra={
                            "file": filename,
                            "expected_sha256": expected_hash,
                            "actual_sha256": actual_hash,
                        },
                    )

        summary = {
            "total_manifests_evaluated": len(manifests),
            "newly_downloaded": downloaded_count,
            "verified_valid": verified_count,
            "corrupted_detected": corrupted_count,
        }
        logger.info("Local archive synchronization completed", extra=summary)
        return summary


def main() -> None:
    """CLI Entrypoint for the laptop archive sync agent."""
    parser = argparse.ArgumentParser(
        description="Zero-Egress Laptop Archive Sync & SHA-256 Verification Utility"
    )
    parser.add_argument(
        "--dest-dir",
        type=str,
        default=None,
        help="Custom destination directory for Parquet archives",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing local files without downloading missing ones",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate downloads and print planned operations",
    )

    args = parser.parse_args()
    custom_dest = Path(args.dest_dir) if args.dest_dir else None
    agent = LocalArchiveSyncAgent(destination_dir=custom_dest)
    agent.sync_all_archives(verify_only=args.verify_only, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

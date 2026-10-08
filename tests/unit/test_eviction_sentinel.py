"""Unit tests for Autonomous Eviction Sentinel and custody transfer."""

from pathlib import Path
from unittest.mock import MagicMock
import pytest

from config.settings import AppSettings
from src.archival.eviction_sentinel import EvictionSentinel
from src.storage.bigquery_manager import BigQueryManager
from src.storage.parquet_exporter import ExportArtifact, ParquetExporter


@pytest.fixture
def mock_bq_manager() -> MagicMock:
    manager = MagicMock(spec=BigQueryManager)
    manager.project_id = "test-project"
    manager.dataset_id = "test-dataset"
    manager.dataset_ref = MagicMock()
    manager.client = MagicMock()

    # Mock query returning 2 rows
    row1 = {"flight_record_id": "r1", "icao24": "a1", "recorded_date": "2026-10-01"}
    row2 = {"flight_record_id": "r2", "icao24": "a2", "recorded_date": "2026-10-01"}
    manager.client.query.return_value = [row1, row2]

    # Mock partition deletion
    manager.delete_partition.return_value = 2
    return manager


@pytest.fixture
def mock_s3_client() -> MagicMock:
    client = MagicMock()
    client.upload_file.return_value = None
    return client


def test_eviction_lifecycle_success(
    tmp_path: Path, mock_bq_manager: MagicMock, mock_s3_client: MagicMock
):
    """Verify complete sequence: query -> serialize -> upload -> audit log -> delete partition."""
    settings = AppSettings()
    exporter = ParquetExporter(settings=settings)

    sentinel = EvictionSentinel(
        settings=settings,
        bq_manager=mock_bq_manager,
        s3_client=mock_s3_client,
        parquet_exporter=exporter,
    )

    result = sentinel.evict_partition(
        table_name="silver_flight_weather_telemetry",
        partition_date="2026-10-01",
    )

    assert result["eviction_status"] == "SUCCESS"
    assert result["record_count_evicted"] == 2
    assert "r2://" in result["storage_destination_uri"]
    assert len(result["sha256_checksum"]) == 64

    # Verify S3 upload and partition delete were called
    mock_s3_client.upload_file.assert_called_once()
    mock_bq_manager.delete_partition.assert_called_once_with(
        "silver_flight_weather_telemetry", "2026-10-01"
    )


def test_eviction_aborts_if_upload_fails(
    tmp_path: Path, mock_bq_manager: MagicMock, mock_s3_client: MagicMock
):
    """Verify that if cold storage upload fails, partition is NEVER deleted."""
    from botocore.exceptions import ClientError

    mock_s3_client.upload_file.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "R2 Service Outage"}}, "upload_file"
    )

    sentinel = EvictionSentinel(
        bq_manager=mock_bq_manager,
        s3_client=mock_s3_client,
    )

    with pytest.raises(Exception):
        sentinel.evict_partition(
            table_name="silver_flight_weather_telemetry",
            partition_date="2026-10-01",
        )

    # Invariant: delete_partition MUST NOT be called!
    mock_bq_manager.delete_partition.assert_not_called()


def test_eviction_skips_empty_partition(mock_bq_manager: MagicMock):
    """Verify empty partition returns skipped status and does not delete."""
    mock_bq_manager.client.query.return_value = []  # No rows found

    sentinel = EvictionSentinel(bq_manager=mock_bq_manager)
    res = sentinel.evict_partition("silver_flight_weather_telemetry", "2026-10-01")

    assert res["status"] == "SKIPPED_EMPTY_PARTITION"
    mock_bq_manager.delete_partition.assert_not_called()

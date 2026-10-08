"""Unit tests for Laptop-Side Local Archive Sync Agent."""

from pathlib import Path
from unittest.mock import MagicMock
import pytest

from config.settings import AppSettings
from scripts.local_archive_pull import LocalArchiveSyncAgent, compute_local_sha256


@pytest.fixture
def sample_audit_manifest():
    return {
        "audit_id": "test-uuid-123",
        "archival_timestamp": "2026-10-08T16:00:00Z",
        "target_partition_date": "2026-10-01",
        "record_count_evicted": 500,
        "compressed_parquet_bytes": 1024,
        "sha256_checksum": "a589b4bc2ba588af3245907effe9c83f04a3871dde5fa6cb70401ed1e6571148",
        "storage_destination_uri": "r2://flight-weather-cold-archive/archives/silver/2026-10-01/test.parquet",
        "truncated_table_name": "silver_flight_weather_telemetry",
    }


def test_sync_all_archives_downloads_and_verifies(
    tmp_path: Path, sample_audit_manifest
):
    """Verify download from S3 and cryptographic checksum match."""
    mock_bq = MagicMock()
    mock_bq.project_id = "test-project"
    mock_bq.dataset_id = "test-dataset"
    mock_bq.client.query.return_value = [sample_audit_manifest]

    mock_s3 = MagicMock()
    # Mock download creating file with correct sha256 content
    def mock_download(Bucket, Key, Filename):
        Path(Filename).write_bytes(b"deterministic_data_block_for_checksum_test")

    mock_s3.download_file.side_effect = mock_download

    agent = LocalArchiveSyncAgent(
        bq_manager=mock_bq,
        s3_client=mock_s3,
        destination_dir=tmp_path,
    )

    summary = agent.sync_all_archives()
    assert summary["total_manifests_evaluated"] == 1
    assert summary["newly_downloaded"] == 1
    assert summary["verified_valid"] == 1
    assert summary["corrupted_detected"] == 0


def test_sync_detects_corrupted_file(tmp_path: Path, sample_audit_manifest):
    """Verify corrupted local file triggers corruption warning."""
    mock_bq = MagicMock()
    mock_bq.project_id = "test-project"
    mock_bq.dataset_id = "test-dataset"
    mock_bq.client.query.return_value = [sample_audit_manifest]

    # Pre-create corrupted local file
    bad_file = tmp_path / "silver_flight_weather_telemetry_2026-10-01.parquet"
    bad_file.write_bytes(b"tampered_or_damaged_data_block")

    agent = LocalArchiveSyncAgent(
        bq_manager=mock_bq,
        s3_client=MagicMock(),
        destination_dir=tmp_path,
    )

    summary = agent.sync_all_archives()
    assert summary["verified_valid"] == 0
    assert summary["corrupted_detected"] == 1


def test_verify_only_mode_skips_missing(tmp_path: Path, sample_audit_manifest):
    """Verify verify_only does not download missing archives."""
    mock_bq = MagicMock()
    mock_bq.project_id = "test-project"
    mock_bq.dataset_id = "test-dataset"
    mock_bq.client.query.return_value = [sample_audit_manifest]
    mock_s3 = MagicMock()

    agent = LocalArchiveSyncAgent(
        bq_manager=mock_bq,
        s3_client=mock_s3,
        destination_dir=tmp_path,
    )

    summary = agent.sync_all_archives(verify_only=True)
    assert summary["newly_downloaded"] == 0
    mock_s3.download_file.assert_not_called()

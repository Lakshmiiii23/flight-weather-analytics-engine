"""Unit tests for BigQuery storage manager using mocked google-cloud-bigquery client."""

from pathlib import Path
from unittest.mock import MagicMock
import pytest
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery

from config.settings import AppSettings
from src.storage.bigquery_manager import BigQueryManager


@pytest.fixture
def mock_bigquery_client() -> MagicMock:
    client = MagicMock(spec=bigquery.Client)
    client.project = "test-flight-project"

    # Mock dataset reference
    dataset_ref = MagicMock(spec=bigquery.DatasetReference)
    client.dataset.return_value = dataset_ref

    # Mock query job
    query_job = MagicMock()
    query_job.result.return_value = None
    query_job.job_id = "test-job-uuid-123"
    query_job.num_dml_affected_rows = 150
    client.query.return_value = query_job

    # Mock load job
    load_job = MagicMock()
    load_job.result.return_value = None
    load_job.errors = None
    load_job.output_rows = 50
    load_job.job_id = "load-job-uuid-456"
    client.load_table_from_json.return_value = load_job

    return client


def test_ensure_dataset(mock_bigquery_client: MagicMock):
    """Verify dataset is created in specified cloud location."""
    settings = AppSettings()
    manager = BigQueryManager(settings=settings, client=mock_bigquery_client)

    manager.ensure_dataset()
    mock_bigquery_client.create_dataset.assert_called_once()


def test_load_silver_records_batch_job(mock_bigquery_client: MagicMock):
    """Verify records are dispatched via zero-cost batch load job."""
    settings = AppSettings()
    manager = BigQueryManager(settings=settings, client=mock_bigquery_client)

    records = [
        {"flight_record_id": "r1", "icao24": "a1", "velocity_mps": 240.0},
        {"flight_record_id": "r2", "icao24": "a2", "velocity_mps": 250.0},
    ]

    loaded_count = manager.load_silver_records(records)
    assert loaded_count == 50  # From mocked load_job.output_rows
    mock_bigquery_client.load_table_from_json.assert_called_once()


def test_load_silver_records_empty_noop(mock_bigquery_client: MagicMock):
    """Verify empty list returns 0 and does not submit job."""
    manager = BigQueryManager(client=mock_bigquery_client)
    loaded = manager.load_silver_records([])
    assert loaded == 0
    mock_bigquery_client.load_table_from_json.assert_not_called()


def test_delete_partition(mock_bigquery_client: MagicMock):
    """Verify partition deletion query execution and affected row reporting."""
    manager = BigQueryManager(client=mock_bigquery_client)
    affected = manager.delete_partition("silver_flight_weather_telemetry", "2026-10-01")

    assert affected == 150
    mock_bigquery_client.query.assert_called_once()
    sql_executed = mock_bigquery_client.query.call_args[0][0]
    assert "DELETE FROM `flight-weather-free-tier.flight_weather_analytics.silver_flight_weather_telemetry`" in sql_executed
    assert "WHERE recorded_date = DATE('2026-10-01')" in sql_executed

"""Integration test suite verifying end-to-end telemetry flow across modules."""

from unittest.mock import MagicMock
import pytest

from config.settings import AppSettings
from scripts.run_pipeline import run_pipeline_iteration
from src.ingestion.opensky_client import OpenSkyClient
from src.ingestion.weather_client import OpenMeteoClient
from src.processing.spatial_join import SpatialCorrelationEngine
from src.storage.bigquery_manager import BigQueryManager


@pytest.fixture
def mock_opensky_client() -> MagicMock:
    client = MagicMock(spec=OpenSkyClient)
    client.fetch_live_states.return_value = [
        {
            "icao24": "a1b2c3",
            "callsign": "AAL100",
            "origin_country": "United States",
            "time_position": 1718000000,
            "last_contact": 1718000000,
            "longitude": -74.0060,
            "latitude": 40.7128,
            "baro_altitude_m": 11000.0,
            "on_ground": False,
            "velocity_mps": 245.0,
            "true_track_deg": 85.0,
            "vertical_rate_mps": 0.0,
            "geo_altitude_m": 11200.0,
            "squawk": "1200",
            "spi": False,
            "position_source": 0,
            "ingested_at": "2026-10-08T16:00:00Z",
        },
        {
            "icao24": "d4e5f6",
            "callsign": "DAL200",
            "origin_country": "United States",
            "time_position": 1718000000,
            "last_contact": 1718000000,
            "longitude": -73.9851,
            "latitude": 40.7488,
            "baro_altitude_m": 9500.0,
            "on_ground": False,
            "velocity_mps": 235.0,
            "true_track_deg": 90.0,
            "vertical_rate_mps": -1.5,
            "geo_altitude_m": 9650.0,
            "squawk": "1200",
            "spi": False,
            "position_source": 0,
            "ingested_at": "2026-10-08T16:00:00Z",
        },
    ]
    return client


@pytest.fixture
def mock_weather_client() -> MagicMock:
    client = MagicMock(spec=OpenMeteoClient)
    # Both NYC flights fall into geohash 'dr5r'
    client.fetch_batch_weather.return_value = {
        "dr5r": {
            "geohash": "dr5r",
            "weather_recorded_at": "2026-10-08T16:00:00Z",
            "temperature_c": 19.5,
            "relative_humidity_pct": 72.0,
            "precipitation_mm": 1.8,
            "wind_speed_mps": 9.2,
            "wind_direction_deg": 250.0,
            "wind_gusts_mps": 15.5,
            "cloud_cover_pct": 85.0,
            "visibility_m": 9000.0,
        }
    }
    return client


def test_end_to_end_pipeline_iteration_dry_run(
    mock_opensky_client: MagicMock, mock_weather_client: MagicMock
):
    """Verify execution of full pipeline flow in dry-run mode."""
    settings = AppSettings()
    spatial_engine = SpatialCorrelationEngine(settings=settings)

    metrics = run_pipeline_iteration(
        settings=settings,
        opensky_client=mock_opensky_client,
        weather_client=mock_weather_client,
        spatial_engine=spatial_engine,
        bq_manager=None,
        dry_run=True,
    )

    assert metrics["status"] == "SUCCESS"
    assert metrics["raw_flights"] == 2
    assert metrics["valid_flights"] == 2
    assert metrics["unique_weather_grids"] == 1
    assert metrics["silver_records_produced"] == 2
    assert metrics["duration_seconds"] >= 0.0


def test_end_to_end_pipeline_with_mock_bq(
    mock_opensky_client: MagicMock, mock_weather_client: MagicMock
):
    """Verify persistence calls into BigQueryManager when dry_run is False."""
    settings = AppSettings()
    spatial_engine = SpatialCorrelationEngine(settings=settings)
    mock_bq = MagicMock(spec=BigQueryManager)
    mock_bq.load_silver_records.return_value = 2

    metrics = run_pipeline_iteration(
        settings=settings,
        opensky_client=mock_opensky_client,
        weather_client=mock_weather_client,
        spatial_engine=spatial_engine,
        bq_manager=mock_bq,
        dry_run=False,
    )

    assert metrics["status"] == "SUCCESS"
    assert metrics["records_committed"] == 2
    mock_bq.ensure_dataset.assert_called_once()
    mock_bq.initialize_tables.assert_called_once()
    mock_bq.load_silver_records.assert_called_once()
    mock_bq.refresh_gold_kpis.assert_called_once()

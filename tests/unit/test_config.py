"""Unit tests for pipeline configuration loader and schema validations."""

import os
from pathlib import Path
import pytest
from config.settings import AppSettings, load_settings, BoundingBoxCoordinates


def test_load_default_settings():
    """Verify default settings.yaml loads cleanly and populates all sub-schemas."""
    settings: AppSettings = load_settings()

    assert settings.pipeline.service_name == "flight-weather-engine"
    assert settings.gcp.dataset_id == "flight_weather_analytics"
    assert settings.duckdb.max_memory == "350MB"
    assert settings.opensky.max_retries >= 3
    assert settings.weather.batch_chunk_size > 0
    assert "precipitation" in settings.weather.required_parameters
    assert settings.archival.compression_algorithm == "zstd"


def test_bounding_box_active_preset():
    """Verify active bounding box preset resolution."""
    settings: AppSettings = load_settings()
    box: BoundingBoxCoordinates = settings.opensky.get_active_box()

    assert box.lamin is not None
    assert box.lamax is not None
    assert box.lomin is not None
    assert box.lomax is not None
    assert box.lamin < box.lamax
    assert box.lomin < box.lomax


def test_environment_variable_override(monkeypatch: pytest.MonkeyPatch):
    """Verify environment variables take precedence over YAML defaults."""
    custom_project = "override-prod-project-99"
    custom_webhook = "https://discord.com/api/webhooks/mocked_hook"
    
    monkeypatch.setenv("GCP_PROJECT_ID", custom_project)
    monkeypatch.setenv("WEBHOOK_URL", custom_webhook)

    settings: AppSettings = load_settings()
    assert settings.gcp.project_id == custom_project
    assert settings.monitoring.webhook_url == custom_webhook


def test_missing_config_fallback():
    """Verify loader gracefully returns default AppSettings when file is missing."""
    settings: AppSettings = load_settings(config_path="/nonexistent/path/settings.yaml")
    assert isinstance(settings, AppSettings)
    assert settings.duckdb.max_memory == "350MB"

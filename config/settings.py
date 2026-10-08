"""Centralized Typed Settings Loader with YAML and Environment Override Support.

Provides typed access to pipeline configurations, thresholds, and cloud parameters.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import yaml


class PipelineConfig(BaseModel):
    environment: str = "production"
    service_name: str = "flight-weather-engine"
    run_interval_seconds: int = 600
    log_level: str = "INFO"


class GCPConfig(BaseModel):
    project_id: str = "flight-weather-free-tier"
    dataset_id: str = "flight_weather_analytics"
    gcs_bucket_landing: str = "flights-bronze-landing"
    location: str = "US"


class DuckDBConfig(BaseModel):
    max_memory: str = "350MB"
    threads: int = 2
    temp_directory: str = ".duckdb_temp"
    geohash_precision: int = 4


class BoundingBoxCoordinates(BaseModel):
    lamin: Optional[float] = None
    lamax: Optional[float] = None
    lomin: Optional[float] = None
    lomax: Optional[float] = None


class OpenSkyConfig(BaseModel):
    base_url: str = "https://opensky-network.org/api"
    states_endpoint: str = "/states/all"
    request_timeout_seconds: int = 15
    max_retries: int = 5
    backoff_multiplier: int = 2
    min_backoff_seconds: int = 2
    max_backoff_seconds: int = 60
    rate_limit_cooldown_seconds: int = 15
    bounding_box: Dict[str, Any] = Field(default_factory=dict)

    def get_active_box(self) -> BoundingBoxCoordinates:
        """Retrieves coordinates for the active bounding box preset."""
        preset_name = self.bounding_box.get("active_preset", "us_northeast_corridor")
        presets = self.bounding_box.get("presets", {})
        box_dict = presets.get(preset_name, {})
        return BoundingBoxCoordinates(**box_dict)


class WeatherConfig(BaseModel):
    base_url: str = "https://api.open-meteo.com/v1/forecast"
    request_timeout_seconds: int = 15
    max_retries: int = 5
    backoff_multiplier: int = 2
    min_backoff_seconds: int = 2
    max_backoff_seconds: int = 30
    batch_chunk_size: int = 50
    required_parameters: List[str] = Field(
        default_factory=lambda: [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "cloud_cover",
            "visibility",
        ]
    )


class BigQueryTablesConfig(BaseModel):
    silver_table: str = "silver_flight_weather_telemetry"
    gold_kpi_table: str = "gold_delay_weather_matrix"
    audit_table: str = "cloud_archive_audit_logs"
    partition_expiration_days: int = 14


class ArchivalConfig(BaseModel):
    retention_days_cloud: int = 7
    compression_algorithm: str = "zstd"
    compression_level: int = 9
    r2_bucket_name: str = "flight-weather-cold-archive"
    r2_endpoint_url: str = "https://r2.cloudflarestorage.com"
    local_archive_dir: str = "archive/parquet"


class MonitoringConfig(BaseModel):
    webhook_url: str = ""
    alert_on_failure: bool = True
    alert_on_archival: bool = True
    heartbeat_enabled: bool = True


class AppSettings(BaseModel):
    """Root Application Settings schema."""
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    gcp: GCPConfig = Field(default_factory=GCPConfig)
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    opensky: OpenSkyConfig = Field(default_factory=OpenSkyConfig)
    weather: WeatherConfig = Field(default_factory=WeatherConfig)
    bigquery_tables: BigQueryTablesConfig = Field(default_factory=BigQueryTablesConfig)
    archival: ArchivalConfig = Field(default_factory=ArchivalConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)


def load_settings(config_path: Optional[str] = None) -> AppSettings:
    """Loads configuration from YAML with environmental overrides.

    Args:
        config_path: Optional path to settings.yaml. If None, resolves from default location.

    Returns:
        AppSettings: Strongly typed Pydantic configuration model.
    """
    if config_path is None:
        base_dir = Path(__file__).resolve().parent
        config_path = str(base_dir / "settings.yaml")

    path_obj = Path(config_path)
    if not path_obj.exists():
        return AppSettings()

    with open(path_obj, "r", encoding="utf-8") as f:
        raw_dict = yaml.safe_load(f) or {}

    # Environment variable overrides
    if "GCP_PROJECT_ID" in os.environ:
        raw_dict.setdefault("gcp", {})["project_id"] = os.environ["GCP_PROJECT_ID"]
    if "WEBHOOK_URL" in os.environ:
        raw_dict.setdefault("monitoring", {})["webhook_url"] = os.environ["WEBHOOK_URL"]
    if "OPENSKY_ACTIVE_PRESET" in os.environ:
        raw_dict.setdefault("opensky", {}).setdefault("bounding_box", {})["active_preset"] = os.environ["OPENSKY_ACTIVE_PRESET"]

    return AppSettings(**raw_dict)

"""Unit tests for BigQuery DDL schema definitions and partition/clustering rules."""

from pathlib import Path
import pytest


@pytest.fixture
def ddl_directory() -> Path:
    base_dir = Path(__file__).resolve().parent.parent.parent
    return base_dir / "sql" / "ddl"


def test_silver_ddl_structure(ddl_directory: Path):
    """Verify Silver DDL includes necessary partitions, clustering, and columns."""
    silver_sql_path = ddl_directory / "create_silver_tables.sql"
    assert silver_sql_path.exists(), "create_silver_tables.sql must exist"

    sql_content = silver_sql_path.read_text(encoding="utf-8")
    sql_upper = sql_content.upper()

    # Table identification
    assert "CREATE TABLE IF NOT EXISTS `FLIGHT_WEATHER_ANALYTICS.SILVER_FLIGHT_WEATHER_TELEMETRY`" in sql_upper

    # Partitioning & Clustering verification (Crucial for GCP Free Tier cost control)
    assert "PARTITION BY RECORDED_DATE" in sql_upper
    assert "CLUSTER BY GEOHASH, CALLSIGN" in sql_upper

    # Key Columns
    required_cols = [
        "flight_record_id",
        "icao24",
        "callsign",
        "latitude",
        "longitude",
        "velocity_mps",
        "geohash",
        "temperature_c",
        "precipitation_mm",
        "wind_speed_mps",
        "convective_risk_score",
        "recorded_date",
    ]
    for col in required_cols:
        assert col in sql_content, f"Missing required column: {col}"


def test_gold_ddl_structure(ddl_directory: Path):
    """Verify Gold DDL includes aggregation metrics, partitioning, and clustering."""
    gold_sql_path = ddl_directory / "create_gold_tables.sql"
    assert gold_sql_path.exists(), "create_gold_tables.sql must exist"

    sql_content = gold_sql_path.read_text(encoding="utf-8")
    sql_upper = sql_content.upper()

    assert "CREATE TABLE IF NOT EXISTS `FLIGHT_WEATHER_ANALYTICS.GOLD_DELAY_WEATHER_MATRIX`" in sql_upper
    assert "PARTITION BY RECORDED_DATE" in sql_upper
    assert "CLUSTER BY GEOHASH, WEATHER_CONDITION_CATEGORY" in sql_upper

    required_metrics = [
        "window_start",
        "window_end",
        "total_active_flights",
        "avg_velocity_mps",
        "velocity_deficit_pct",
        "avg_precipitation_mm",
        "peak_wind_gust_mps",
        "mean_convective_risk",
    ]
    for metric in required_metrics:
        assert metric in sql_content, f"Missing required metric: {metric}"


def test_audit_ddl_structure(ddl_directory: Path):
    """Verify Audit DDL includes cryptographic tracking and partition controls."""
    audit_sql_path = ddl_directory / "create_audit_tables.sql"
    assert audit_sql_path.exists(), "create_audit_tables.sql must exist"

    sql_content = audit_sql_path.read_text(encoding="utf-8")
    sql_upper = sql_content.upper()

    assert "CREATE TABLE IF NOT EXISTS `FLIGHT_WEATHER_ANALYTICS.CLOUD_ARCHIVE_AUDIT_LOGS`" in sql_upper
    assert "PARTITION BY DATE(ARCHIVAL_TIMESTAMP)" in sql_upper
    assert "CLUSTER BY TARGET_PARTITION_DATE, EVICTION_STATUS" in sql_upper

    required_fields = [
        "audit_id",
        "archival_timestamp",
        "target_partition_date",
        "record_count_evicted",
        "compressed_parquet_bytes",
        "sha256_checksum",
        "storage_destination_uri",
        "eviction_status",
    ]
    for field in required_fields:
        assert field in sql_content, f"Missing required audit field: {field}"

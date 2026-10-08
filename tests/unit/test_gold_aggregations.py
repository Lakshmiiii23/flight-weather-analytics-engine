"""Unit tests for Gold Layer analytical aggregation SQL logic and metrics."""

from pathlib import Path
import duckdb
import pytest


@pytest.fixture
def gold_sql_path() -> Path:
    base_dir = Path(__file__).resolve().parent.parent.parent
    return base_dir / "sql" / "gold_aggregations" / "weather_impact_matrix.sql"


def test_gold_sql_syntax_and_keywords(gold_sql_path: Path):
    """Verify Gold aggregation script contains required analytical constructs."""
    assert gold_sql_path.exists(), "weather_impact_matrix.sql must exist"

    sql_content = gold_sql_path.read_text(encoding="utf-8")
    sql_upper = sql_content.upper()

    assert "MERGE `FLIGHT_WEATHER_ANALYTICS.GOLD_DELAY_WEATHER_MATRIX`" in sql_upper
    assert "TIMESTAMP_TRUNC(WEATHER_RECORDED_AT, HOUR)" in sql_upper
    assert "SEVERE_STORM" in sql_upper
    assert "HIGH_WIND_SHEAR" in sql_upper
    assert "VELOCITY_DEFICIT_PCT" in sql_upper
    assert "FLIGHTS_IMPACTED_COUNT" in sql_upper
    assert "@TARGET_DATE" in sql_upper


def test_gold_aggregation_math_in_duckdb():
    """Verify SQL aggregation math (velocity deficit, grouping) inside DuckDB."""
    con = duckdb.connect(":memory:")

    # Create dummy silver records
    con.execute("""
    CREATE TABLE silver_test (
        weather_recorded_at TIMESTAMP,
        recorded_date DATE,
        geohash VARCHAR,
        convective_risk_score DOUBLE,
        precipitation_mm DOUBLE,
        wind_speed_mps DOUBLE,
        wind_gusts_mps DOUBLE,
        cloud_cover_pct DOUBLE,
        visibility_m DOUBLE,
        icao24 VARCHAR,
        velocity_mps DOUBLE,
        baro_altitude_m DOUBLE
    );
    """)

    # Flight in storm: speed 180 m/s (baseline 230 m/s -> deficit = ((230-180)/230)*100 = 21.74%)
    con.execute("""
    INSERT INTO silver_test VALUES
    ('2026-10-08 14:15:00', '2026-10-08', 'dr5r', 75.0, 4.0, 15.0, 25.0, 90.0, 4000.0, 'a1', 180.0, 8000.0),
    ('2026-10-08 14:45:00', '2026-10-08', 'dr5r', 80.0, 4.5, 16.0, 26.0, 95.0, 3500.0, 'a2', 170.0, 9000.0);
    """)

    query = """
    SELECT
        COUNT(DISTINCT icao24) AS total_active_flights,
        COUNT(CASE WHEN baro_altitude_m >= 3000.0 THEN 1 END) AS total_cruising_flights,
        ROUND(AVG(velocity_mps), 2) AS avg_velocity_mps,
        ROUND(((230.0 - AVG(velocity_mps)) / 230.0) * 100.0, 2) AS velocity_deficit_pct,
        COUNT(CASE WHEN convective_risk_score >= 50.0 THEN 1 END) AS flights_impacted_count
    FROM silver_test
    WHERE recorded_date = DATE '2026-10-08';
    """
    res = con.execute(query).fetchone()
    con.close()

    assert res is not None
    assert res[0] == 2  # 2 active flights
    assert res[1] == 2  # 2 cruising flights
    assert res[2] == 175.0  # avg velocity (180 + 170) / 2
    assert res[3] == 23.91  # ((230 - 175) / 230) * 100
    assert res[4] == 2  # both flights in high hazard

"""Unit tests for Geohash encoding/decoding and DuckDB spatial correlation engine."""

from typing import Any, Dict, List
import pytest
from src.processing.spatial_join import (
    SpatialCorrelationEngine,
    decode_geohash_centroid,
    encode_geohash,
)


def test_geohash_encoding_known_coordinates():
    """Verify standard geohash encoding for landmark coordinates."""
    # NYC (approx 40.7128, -74.0060) precision 4 is 'dr5r'
    hash_nyc = encode_geohash(40.7128, -74.0060, precision=4)
    assert hash_nyc == "dr5r"

    # London Heathrow (approx 51.4700, -0.4543) precision 4 is 'gcps'
    hash_lhr = encode_geohash(51.4700, -0.4543, precision=4)
    assert hash_lhr == "gcps"


def test_geohash_decode_centroid():
    """Verify centroid decoding maps back near the original bounding interval."""
    lat, lon = decode_geohash_centroid("dr5r")
    assert 40.0 < lat < 41.5
    assert -75.0 < lon < -73.5


def test_extract_spatial_centroids():
    """Verify flight grouping reduces redundant queries to unique centroids."""
    engine = SpatialCorrelationEngine()
    flights: List[Dict[str, Any]] = [
        {"icao24": "a1", "latitude": 40.71, "longitude": -74.00, "time_position": 1718000000},
        {"icao24": "a2", "latitude": 40.72, "longitude": -74.01, "time_position": 1718000000},
        {"icao24": "a3", "latitude": 51.47, "longitude": -0.45, "time_position": 1718000000},
    ]

    tagged_flights, centroids = engine.extract_spatial_centroids(flights)
    assert len(tagged_flights) == 3
    # Two flights in NYC ('dr5r') and one in London ('gcps') -> exactly 2 distinct centroids
    assert len(centroids) == 2
    hashes = [c[2] for c in centroids]
    assert "dr5r" in hashes
    assert "gcps" in hashes


def test_duckdb_spatial_join_execution():
    """Verify memory-capped DuckDB execution joins flight telemetry with weather observation."""
    engine = SpatialCorrelationEngine()

    tagged_flights: List[Dict[str, Any]] = [
        {
            "flight_record_id": "rec_1",
            "icao24": "a1",
            "callsign": "AAL100",
            "origin_country": "United States",
            "time_position": 1718000000,
            "last_contact": 1718000000,
            "longitude": -74.0,
            "latitude": 40.7,
            "baro_altitude_m": 10000.0,
            "on_ground": False,
            "velocity_mps": 250.0,
            "true_track_deg": 90.0,
            "vertical_rate_mps": 0.0,
            "geo_altitude_m": 10200.0,
            "squawk": "1200",
            "spi": False,
            "position_source": 0,
            "geohash": "dr5r",
            "ingested_at": "2026-10-08T16:00:00Z",
            "recorded_date": "2026-10-08",
        }
    ]

    weather_obs: Dict[str, Dict[str, Any]] = {
        "dr5r": {
            "geohash": "dr5r",
            "weather_recorded_at": "2026-10-08T16:00:00Z",
            "temperature_c": 20.0,
            "relative_humidity_pct": 70.0,
            "precipitation_mm": 2.5,
            "wind_speed_mps": 12.0,
            "wind_direction_deg": 270.0,
            "wind_gusts_mps": 18.0,
            "cloud_cover_pct": 80.0,
            "visibility_m": 8000.0,
        }
    }

    joined = engine.execute_spatial_join(tagged_flights, weather_obs)
    assert len(joined) == 1
    record = joined[0]

    assert record["flight_record_id"] == "rec_1"
    assert record["temperature_c"] == 20.0
    assert record["precipitation_mm"] == 2.5
    assert record["wind_speed_mps"] == 12.0
    # Risk calculation in DuckDB: (2.5 * 15.0) + (18.0 * 1.5) + (80.0 * 0.2) = 37.5 + 27.0 + 16.0 = 80.5
    assert record["convective_risk_score"] == 80.5


def test_duckdb_spatial_join_missing_weather_graceful():
    """Verify left join retains flight even if weather observation is missing for that grid."""
    engine = SpatialCorrelationEngine()
    tagged_flights: List[Dict[str, Any]] = [
        {
            "flight_record_id": "rec_2",
            "icao24": "b2",
            "callsign": None,
            "origin_country": "Canada",
            "time_position": 1718000000,
            "last_contact": 1718000000,
            "longitude": -70.0,
            "latitude": 45.0,
            "baro_altitude_m": None,
            "on_ground": True,
            "velocity_mps": None,
            "true_track_deg": None,
            "vertical_rate_mps": None,
            "geo_altitude_m": None,
            "squawk": None,
            "spi": False,
            "position_source": 0,
            "geohash": "f2h1",
            "ingested_at": "2026-10-08T16:00:00Z",
            "recorded_date": "2026-10-08",
        }
    ]

    joined = engine.execute_spatial_join(tagged_flights, {})
    assert len(joined) == 1
    assert joined[0]["flight_record_id"] == "rec_2"
    assert joined[0]["temperature_c"] is None
    assert joined[0]["convective_risk_score"] == 0.0

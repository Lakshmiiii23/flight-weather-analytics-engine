"""Unit tests for Pydantic schema validation and convective risk calculation."""

import pytest
from src.processing.validation import (
    RawFlightState,
    WeatherObservation,
    EnrichedSilverRecord,
    compute_convective_risk_score,
    compute_deterministic_record_id,
    validate_flight_records,
)


def test_compute_deterministic_record_id():
    """Verify record ID is deterministic, lowercase, and reproducible."""
    id1 = compute_deterministic_record_id("A1B2C3", 1718000000)
    id2 = compute_deterministic_record_id("a1b2c3", 1718000000)
    id3 = compute_deterministic_record_id("a1b2c3", 1718000001)

    assert id1 == id2
    assert id1 != id3
    assert len(id1) == 64  # SHA-256 hex string length


def test_convective_risk_score_calculation():
    """Verify composite convective storm hazard index weights and capping."""
    # Fair weather
    clear_score = compute_convective_risk_score(
        precipitation_mm=0.0, wind_gusts_mps=3.0, cloud_cover_pct=10.0
    )
    assert clear_score == round(0.0 + (3.0 * 1.5) + (10.0 * 0.2), 2)
    assert clear_score < 10.0

    # Severe thunderstorm
    severe_score = compute_convective_risk_score(
        precipitation_mm=5.0, wind_gusts_mps=25.0, cloud_cover_pct=100.0
    )
    # (5*15) + (25*1.5) + (100*0.2) = 75 + 37.5 + 20 = 132.5 -> capped at 100.0
    assert severe_score == 100.0


def test_raw_flight_state_validation():
    """Verify validation rules and normalization."""
    valid_data = {
        "icao24": "  4B1234 ",
        "callsign": " SWR123  ",
        "origin_country": "Switzerland",
        "time_position": 1718000000,
        "last_contact": 1718000000,
        "longitude": 8.54,
        "latitude": 47.37,
        "velocity_mps": 210.5,
        "ingested_at": "2026-10-08T16:00:00Z",
    }
    state = RawFlightState(**valid_data)
    assert state.icao24 == "4b1234"
    assert state.callsign == "SWR123"
    assert state.latitude == 47.37


def test_raw_flight_state_rejection():
    """Verify invalid coordinates and bounds raise validation errors."""
    invalid_data = {
        "icao24": "4b1234",
        "time_position": 1718000000,
        "last_contact": 1718000000,
        "longitude": 200.0,  # Invalid lon (> 180)
        "latitude": 47.37,
        "ingested_at": "2026-10-08T16:00:00Z",
    }
    with pytest.raises(Exception):
        RawFlightState(**invalid_data)


def test_validate_flight_records_quarantine():
    """Verify quarantine separation of good and corrupted records."""
    records = [
        {
            "icao24": "valid1",
            "time_position": 1718000000,
            "last_contact": 1718000000,
            "longitude": -74.0,
            "latitude": 40.7,
            "ingested_at": "2026-10-08T16:00:00Z",
        },
        {
            "icao24": "bad2",
            "time_position": 1718000000,
            "last_contact": 1718000000,
            "longitude": -74.0,
            "latitude": 105.0,  # Bad lat (> 90)
            "ingested_at": "2026-10-08T16:00:00Z",
        },
    ]
    valid, quarantined = validate_flight_records(records)
    assert len(valid) == 1
    assert len(quarantined) == 1
    assert valid[0].icao24 == "valid1"
    assert quarantined[0]["record"]["icao24"] == "bad2"

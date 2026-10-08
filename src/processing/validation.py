"""Pydantic Data Validation Contracts & Hazard Score Calculations.

Guarantees schema integrity, sanitizes drifting data feeds, and isolates
quarantined records before database commitment.
"""

from datetime import datetime, timezone
import hashlib
import logging
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field, field_validator

from config.logging_config import setup_logging

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="data-validation-engine"
)


def compute_deterministic_record_id(icao24: str, time_position: int) -> str:
    """Computes an immutable, reproducible SHA-256 identifier for a flight position state."""
    raw_str = f"{icao24.lower().strip()}_{time_position}"
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()


def compute_convective_risk_score(
    precipitation_mm: Optional[float],
    wind_gusts_mps: Optional[float],
    cloud_cover_pct: Optional[float],
) -> float:
    """Computes a standardized composite hazard index [0.0 - 100.0].

    Weights:
    - Precipitation burst (up to 45 pts): 15.0 pts per mm/hr (>=3mm is heavy storm)
    - Convective wind gusts (up to 35 pts): 1.5 pts per m/s (>=23 m/s is severe gale)
    - Cloud cover density (up to 20 pts): 0.20 pts per %
    """
    precip = max(0.0, precipitation_mm or 0.0)
    gusts = max(0.0, wind_gusts_mps or 0.0)
    clouds = max(0.0, min(100.0, cloud_cover_pct or 0.0))

    score = (precip * 15.0) + (gusts * 1.5) + (clouds * 0.20)
    return round(min(100.0, max(0.0, score)), 2)


class RawFlightState(BaseModel):
    """Pydantic contract validating raw incoming ADS-B vectors."""

    icao24: str = Field(..., min_length=4, max_length=10)
    callsign: Optional[str] = Field(None, max_length=12)
    origin_country: str = Field(default="Unknown")
    time_position: int = Field(..., ge=0)
    last_contact: int = Field(..., ge=0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    latitude: float = Field(..., ge=-90.0, le=90.0)
    baro_altitude_m: Optional[float] = None
    on_ground: bool = Field(default=False)
    velocity_mps: Optional[float] = Field(None, ge=0.0)
    true_track_deg: Optional[float] = Field(None, ge=0.0, le=360.0)
    vertical_rate_mps: Optional[float] = None
    geo_altitude_m: Optional[float] = None
    squawk: Optional[str] = None
    spi: bool = Field(default=False)
    position_source: int = Field(default=0)
    ingested_at: str

    @field_validator("icao24", mode="before")
    @classmethod
    def normalize_icao24(cls, val: Any) -> str:
        return str(val).lower().strip()

    @field_validator("callsign", mode="before")
    @classmethod
    def clean_callsign(cls, val: Any) -> Optional[str]:
        if val is None:
            return None
        cleaned = str(val).strip()
        return cleaned if cleaned else None


class WeatherObservation(BaseModel):
    """Pydantic contract validating localized meteorological variables."""

    geohash: str = Field(..., min_length=3, max_length=12)
    weather_recorded_at: str
    temperature_c: Optional[float] = Field(None, ge=-90.0, le=70.0)
    relative_humidity_pct: Optional[float] = Field(None, ge=0.0, le=100.0)
    precipitation_mm: Optional[float] = Field(None, ge=0.0)
    wind_speed_mps: Optional[float] = Field(None, ge=0.0)
    wind_direction_deg: Optional[float] = Field(None, ge=0.0, le=360.0)
    wind_gusts_mps: Optional[float] = Field(None, ge=0.0)
    cloud_cover_pct: Optional[float] = Field(None, ge=0.0, le=100.0)
    visibility_m: Optional[float] = Field(None, ge=0.0)


class EnrichedSilverRecord(BaseModel):
    """Enriched unified model corresponding to BigQuery Silver telemetry schema."""

    flight_record_id: str
    icao24: str
    callsign: Optional[str] = None
    origin_country: str
    time_position: int
    last_contact: int
    longitude: float
    latitude: float
    baro_altitude_m: Optional[float] = None
    on_ground: bool
    velocity_mps: Optional[float] = None
    true_track_deg: Optional[float] = None
    vertical_rate_mps: Optional[float] = None
    geo_altitude_m: Optional[float] = None
    squawk: Optional[str] = None
    spi: bool
    position_source: int
    geohash: str

    weather_recorded_at: Optional[str] = None
    temperature_c: Optional[float] = None
    relative_humidity_pct: Optional[float] = None
    precipitation_mm: Optional[float] = None
    wind_speed_mps: Optional[float] = None
    wind_direction_deg: Optional[float] = None
    wind_gusts_mps: Optional[float] = None
    cloud_cover_pct: Optional[float] = None
    visibility_m: Optional[float] = None
    convective_risk_score: float

    ingested_at: str
    recorded_date: str


def validate_flight_records(
    raw_records: List[Dict[str, Any]]
) -> Tuple[List[RawFlightState], List[Dict[str, Any]]]:
    """Validates raw flight records against Pydantic schema contracts.

    Returns:
        Tuple: (valid_records, quarantined_records)
    """
    valid: List[RawFlightState] = []
    quarantined: List[Dict[str, Any]] = []

    for item in raw_records:
        try:
            parsed = RawFlightState(**item)
            valid.append(parsed)
        except Exception as err:
            logger.debug(
                "Telemetry record failed contract validation; quarantining",
                extra={"error": str(err), "record": item},
            )
            quarantined.append({"record": item, "rejection_reason": str(err)})

    if quarantined:
        logger.warning(
            "Validation completed with quarantined entries",
            extra={"valid_count": len(valid), "quarantine_count": len(quarantined)},
        )
    return valid, quarantined

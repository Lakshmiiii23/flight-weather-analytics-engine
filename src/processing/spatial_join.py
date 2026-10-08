"""DuckDB Out-of-Core Spatial Vector Correlation Engine.

Performs memory-capped spatial binning (Geohash) and vectorized cross-referencing
between high-velocity aircraft state vectors and localized atmospheric observations.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional, Tuple
import duckdb
import pandas as pd
import pyarrow as pa

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings
from src.processing.validation import compute_deterministic_record_id

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="spatial-join-engine"
)

# Standard Base32 Alphabet for Geohashing
_GEOHASH_BASE32: str = "0123456789bcdefghjkmnpqrstuvwxyz"
_GEOHASH_DECODE_MAP: Dict[str, int] = {char: idx for idx, char in enumerate(_GEOHASH_BASE32)}

# Weather schema columns for fallback when weather data is empty
_WEATHER_COLUMNS: List[str] = [
    "geohash",
    "weather_recorded_at",
    "temperature_c",
    "relative_humidity_pct",
    "precipitation_mm",
    "wind_speed_mps",
    "wind_direction_deg",
    "wind_gusts_mps",
    "cloud_cover_pct",
    "visibility_m",
]


def encode_geohash(latitude: float, longitude: float, precision: int = 4) -> str:
    """Encodes latitude and longitude coordinates into a Geohash string.

    Precision 4 corresponds to a grid box of approximately ~20km x ~20km,
    matching convective cloud and localized storm cell dimensions.
    """
    lat_interval: List[float] = [-90.0, 90.0]
    lon_interval: List[float] = [-180.0, 180.0]
    geohash: List[str] = []
    bits: List[int] = [16, 8, 4, 2, 1]
    bit: int = 0
    ch: int = 0
    even: bool = True

    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2.0
            if longitude > mid:
                ch |= bits[bit]
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2.0
            if latitude > mid:
                ch |= bits[bit]
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(_GEOHASH_BASE32[ch])
            bit = 0
            ch = 0

    return "".join(geohash)


def decode_geohash_centroid(geohash: str) -> Tuple[float, float]:
    """Decodes a Geohash string into its center (latitude, longitude) coordinate pair."""
    lat_interval: List[float] = [-90.0, 90.0]
    lon_interval: List[float] = [-180.0, 180.0]
    even: bool = True

    for char in geohash.lower():
        char_val = _GEOHASH_DECODE_MAP[char]
        for mask in [16, 8, 4, 2, 1]:
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2.0
                if char_val & mask:
                    lon_interval[0] = mid
                else:
                    lon_interval[1] = mid
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2.0
                if char_val & mask:
                    lat_interval[0] = mid
                else:
                    lat_interval[1] = mid
            even = not even

    centroid_lat = round((lat_interval[0] + lat_interval[1]) / 2.0, 4)
    centroid_lon = round((lon_interval[0] + lon_interval[1]) / 2.0, 4)
    return centroid_lat, centroid_lon


class SpatialCorrelationEngine:
    """DuckDB-backed spatial join processor operating within strict memory bounds."""

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        self.settings: AppSettings = settings or load_settings()
        self.max_memory: str = self.settings.duckdb.max_memory
        self.threads: int = self.settings.duckdb.threads
        self.precision: int = self.settings.duckdb.geohash_precision

    def extract_spatial_centroids(
        self, raw_flights: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Tuple[float, float, str]]]:
        """Tags flight records with Geohash and extracts distinct centroids for weather query.

        Args:
            raw_flights: Normalized flight dictionaries with latitude/longitude.

        Returns:
            Tuple: (tagged_flight_records, distinct_weather_query_centroids)
        """
        tagged_records: List[Dict[str, Any]] = []
        unique_hashes: Dict[str, Tuple[float, float]] = {}

        for flight in raw_flights:
            lat = flight["latitude"]
            lon = flight["longitude"]
            ghash = encode_geohash(lat, lon, precision=self.precision)

            # Enrich flight with deterministic ID, geohash, and partition date
            t_pos = flight["time_position"]
            rec_id = compute_deterministic_record_id(flight["icao24"], t_pos)
            rec_date = datetime.fromtimestamp(t_pos, tz=timezone.utc).strftime("%Y-%m-%d")

            enriched_flight = dict(flight)
            enriched_flight["flight_record_id"] = rec_id
            enriched_flight["geohash"] = ghash
            enriched_flight["recorded_date"] = rec_date
            tagged_records.append(enriched_flight)

            if ghash not in unique_hashes:
                c_lat, c_lon = decode_geohash_centroid(ghash)
                unique_hashes[ghash] = (c_lat, c_lon)

        centroids = [(c_lat, c_lon, ghash) for ghash, (c_lat, c_lon) in unique_hashes.items()]
        logger.info(
            "Spatial grid binning complete",
            extra={
                "total_flights": len(tagged_records),
                "distinct_geohashes": len(centroids),
                "compression_ratio": round(len(tagged_records) / max(1, len(centroids)), 2),
            },
        )
        return tagged_records, centroids

    def execute_spatial_join(
        self,
        tagged_flights: List[Dict[str, Any]],
        weather_observations: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Joins flight vectors and weather observations using an in-memory DuckDB table.

        Strictly enforces PRAGMA max_memory = '350MB' to safeguard the e2-micro host.
        """
        if not tagged_flights:
            logger.warning("No flight vectors provided for spatial correlation")
            return []

        flat_weather: List[Dict[str, Any]] = list(weather_observations.values())

        # Build PyArrow / Pandas relation handles
        flights_tbl = pa.Table.from_pylist(tagged_flights)

        if flat_weather:
            weather_tbl = pa.Table.from_pylist(flat_weather)
        else:
            weather_tbl = pd.DataFrame(columns=_WEATHER_COLUMNS)

        # Initialize DuckDB connection with strict memory limits
        con = duckdb.connect(database=":memory:")
        try:
            con.execute(f"PRAGMA max_memory = '{self.max_memory}';")
            con.execute(f"PRAGMA threads = {self.threads};")

            # Register datasets as DuckDB relations
            con.register("flights_table", flights_tbl)
            con.register("weather_table", weather_tbl)

            # Vectorized SQL Join with in-engine hazard scoring
            query = """
            SELECT
                f.flight_record_id,
                f.icao24,
                f.callsign,
                f.origin_country,
                f.time_position,
                f.last_contact,
                f.longitude,
                f.latitude,
                f.baro_altitude_m,
                f.on_ground,
                f.velocity_mps,
                f.true_track_deg,
                f.vertical_rate_mps,
                f.geo_altitude_m,
                f.squawk,
                f.spi,
                f.position_source,
                f.geohash,
                w.weather_recorded_at,
                w.temperature_c,
                w.relative_humidity_pct,
                w.precipitation_mm,
                w.wind_speed_mps,
                w.wind_direction_deg,
                w.wind_gusts_mps,
                w.cloud_cover_pct,
                w.visibility_m,
                CAST(
                    LEAST(100.0, GREATEST(0.0,
                        ROUND(
                            COALESCE(w.precipitation_mm, 0.0) * 15.0 +
                            COALESCE(w.wind_gusts_mps, 0.0) * 1.5 +
                            LEAST(100.0, GREATEST(0.0, COALESCE(w.cloud_cover_pct, 0.0))) * 0.20,
                            2
                        )
                    )) AS DOUBLE
                ) AS convective_risk_score,
                f.ingested_at,
                f.recorded_date
            FROM flights_table f
            LEFT JOIN weather_table w ON f.geohash = w.geohash
            """

            result_df = con.execute(query).fetchdf()
            # Convert NaN / NaT values to None for clean serialization
            records: List[Dict[str, Any]] = result_df.where(result_df.notnull(), None).to_dict(
                orient="records"
            )

            logger.info(
                "Vectorized spatial correlation successfully executed in DuckDB",
                extra={"joined_records_count": len(records), "memory_ceiling": self.max_memory},
            )
            return records
        finally:
            con.close()

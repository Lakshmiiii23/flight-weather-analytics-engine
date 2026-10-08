"""Resilient Open-Meteo Batch Weather Ingestion Client.

Fetches localized meteorological observations using multi-coordinate batched HTTP calls,
tenacity exponential backoff, rate-limit defense, and automatic unit normalization.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional, Tuple
import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings
from src.ingestion.opensky_client import is_transient_network_error

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="weather-ingestion-client"
)


class OpenMeteoClient:
    """Production client for batch querying Open-Meteo free API."""

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        self.settings: AppSettings = settings or load_settings()
        self.base_url: str = self.settings.weather.base_url.rstrip("/")
        self.timeout_seconds: int = self.settings.weather.request_timeout_seconds
        self.batch_chunk_size: int = self.settings.weather.batch_chunk_size
        self.required_parameters: List[str] = self.settings.weather.required_parameters
        self.session: requests.Session = requests.Session()

    @retry(
        retry=retry_if_exception(is_transient_network_error),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _execute_batch_request(
        self, lats: List[float], lons: List[float]
    ) -> List[Dict[str, Any]]:
        """Queries Open-Meteo for a single chunk of coordinate pairs.

        Args:
            lats: List of latitudes in chunk.
            lons: List of longitudes in chunk.

        Returns:
            List[Dict[str, Any]]: List of per-coordinate weather response payloads.
        """
        lat_param = ",".join(f"{lat:.4f}" for lat in lats)
        lon_param = ",".join(f"{lon:.4f}" for lon in lons)
        current_vars = ",".join(self.required_parameters)

        params: Dict[str, Any] = {
            "latitude": lat_param,
            "longitude": lon_param,
            "current": current_vars,
            "wind_speed_unit": "ms",  # Native meters/second matching aviation telemetry
        }

        logger.debug(
            "Dispatching Open-Meteo batch chunk query",
            extra={"batch_size": len(lats), "url": self.base_url},
        )

        response = self.session.get(
            self.base_url, params=params, timeout=self.timeout_seconds
        )
        response.raise_for_status()
        raw_payload = response.json()

        # Open-Meteo returns a single dict if len == 1, or a list of dicts if len > 1
        if isinstance(raw_payload, list):
            return raw_payload
        if isinstance(raw_payload, dict):
            return [raw_payload]
        return []

    def fetch_batch_weather(
        self, centroids: List[Tuple[float, float, str]]
    ) -> Dict[str, Dict[str, Any]]:
        """Queries weather conditions for spatial centroids in batched HTTP requests.

        Args:
            centroids: List of tuples (latitude, longitude, geohash).

        Returns:
            Dict[str, Dict[str, Any]]: Mapping of geohash -> normalized weather observation.
        """
        if not centroids:
            logger.warning("Empty centroid list supplied to weather client")
            return {}

        results: Dict[str, Dict[str, Any]] = {}
        total_centroids = len(centroids)
        logger.info(
            "Initiating batch weather ingestion",
            extra={"total_centroids": total_centroids, "chunk_size": self.batch_chunk_size},
        )

        # Chunk centroids into manageable batch sizes to avoid URL length constraints
        for idx in range(0, total_centroids, self.batch_chunk_size):
            chunk = centroids[idx : idx + self.batch_chunk_size]
            chunk_lats = [c[0] for c in chunk]
            chunk_lons = [c[1] for c in chunk]
            chunk_hashes = [c[2] for c in chunk]

            try:
                responses = self._execute_batch_request(lats=chunk_lats, lons=chunk_lons)
            except Exception as exc:
                logger.error(
                    "Failed to fetch weather for coordinate chunk",
                    extra={"chunk_start_idx": idx, "chunk_size": len(chunk)},
                    exc_info=True,
                )
                continue

            for res, ghash in zip(responses, chunk_hashes):
                current_data = res.get("current", {})
                time_str = current_data.get("time")
                recorded_at = (
                    datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc).isoformat()
                    if time_str
                    else datetime.now(timezone.utc).isoformat()
                )

                results[ghash] = {
                    "geohash": ghash,
                    "weather_recorded_at": recorded_at,
                    "temperature_c": current_data.get("temperature_2m"),
                    "relative_humidity_pct": current_data.get("relative_humidity_2m"),
                    "precipitation_mm": current_data.get("precipitation"),
                    "wind_speed_mps": current_data.get("wind_speed_10m"),
                    "wind_direction_deg": current_data.get("wind_direction_10m"),
                    "wind_gusts_mps": current_data.get("wind_gusts_10m"),
                    "cloud_cover_pct": current_data.get("cloud_cover"),
                    "visibility_m": current_data.get("visibility"),
                }

        logger.info(
            "Batch weather acquisition complete",
            extra={
                "requested_centroids": total_centroids,
                "successfully_mapped_hashes": len(results),
            },
        )
        return results

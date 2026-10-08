"""Resilient OpenSky Network Telemetry Ingestion Client.

Polls the OpenSky Network REST API with exponential backoff, rate-limit defense (HTTP 429),
bounding-box spatial filters, and structured telemetry normalization.
"""

from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional
import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from config.logging_config import setup_logging
from config.settings import AppSettings, BoundingBoxCoordinates, load_settings

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="opensky-ingestion-client"
)


def is_transient_network_error(exception: BaseException) -> bool:
    """Predicate to determine if an HTTP or connection error warrants retry.

    Retries on:
    - HTTP 429 (Too Many Requests / Rate Limit)
    - HTTP 500, 502, 503, 504 (Server-side transient failures)
    - Connection timeouts and socket disconnects
    """
    if isinstance(exception, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return True
    if isinstance(exception, requests.exceptions.HTTPError):
        status_code = getattr(exception.response, "status_code", None)
        if status_code in (429, 500, 502, 503, 504):
            return True
    return False


class OpenSkyClient:
    """Production client for ingesting live ADS-B state vectors from OpenSky Network."""

    def __init__(
        self,
        settings: Optional[AppSettings] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self.settings: AppSettings = settings or load_settings()
        self.base_url: str = self.settings.opensky.base_url.rstrip("/")
        self.endpoint: str = self.settings.opensky.states_endpoint
        self.timeout_seconds: int = self.settings.opensky.request_timeout_seconds
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.session: requests.Session = requests.Session()
        if self.username and self.password:
            self.session.auth = (self.username, self.password)

    @retry(
        retry=retry_if_exception(is_transient_network_error),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _execute_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Executes HTTP GET request against OpenSky API with exponential backoff.

        Args:
            params: Query string parameters (e.g. lamin, lamax, lomin, lomax).

        Returns:
            Dict[str, Any]: Parsed JSON response.

        Raises:
            requests.exceptions.HTTPError: If an unrecoverable HTTP status occurs.
        """
        target_url = f"{self.base_url}{self.endpoint}"
        logger.debug("Requesting OpenSky state vectors", extra={"url": target_url, "params": params})

        response = self.session.get(target_url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()
        return payload

    def fetch_live_states(
        self, custom_box: Optional[BoundingBoxCoordinates] = None
    ) -> List[Dict[str, Any]]:
        """Fetches and normalizes live aircraft state vectors.

        Args:
            custom_box: Optional override for bounding box spatial parameters.

        Returns:
            List[Dict[str, Any]]: Cleaned and validated state vector dictionaries.
        """
        box = custom_box or self.settings.opensky.get_active_box()
        params: Dict[str, Any] = {}

        if box.lamin is not None and box.lamax is not None:
            params["lamin"] = box.lamin
            params["lamax"] = box.lamax
        if box.lomin is not None and box.lomax is not None:
            params["lomin"] = box.lomin
            params["lomax"] = box.lomax

        logger.info(
            "Initiating OpenSky live state query",
            extra={"bounding_box": params, "timeout": self.timeout_seconds},
        )

        try:
            payload = self._execute_request(params=params)
        except Exception as exc:
            logger.error("Failed to fetch OpenSky states after retries", exc_info=True)
            raise

        raw_states: Optional[List[List[Any]]] = payload.get("states")
        server_timestamp: int = payload.get("time", int(datetime.now(timezone.utc).timestamp()))

        if not raw_states:
            logger.warning("OpenSky returned 0 active state vectors for queried window")
            return []

        cleaned_records: List[Dict[str, Any]] = []
        for state in raw_states:
            # Enforce ADS-B specification minimum: 17 elements
            if len(state) < 17:
                continue

            lon: Optional[float] = state[5]
            lat: Optional[float] = state[6]

            # Drop vectors missing spatial coordinates immediately
            if lon is None or lat is None:
                continue

            callsign_raw: Optional[str] = state[1]
            callsign = callsign_raw.strip() if callsign_raw else None

            record: Dict[str, Any] = {
                "icao24": str(state[0]).lower().strip(),
                "callsign": callsign,
                "origin_country": str(state[2]).strip() if state[2] else "Unknown",
                "time_position": state[3] if state[3] is not None else server_timestamp,
                "last_contact": state[4] if state[4] is not None else server_timestamp,
                "longitude": float(lon),
                "latitude": float(lat),
                "baro_altitude_m": float(state[7]) if state[7] is not None else None,
                "on_ground": bool(state[8]),
                "velocity_mps": float(state[9]) if state[9] is not None else None,
                "true_track_deg": float(state[10]) if state[10] is not None else None,
                "vertical_rate_mps": float(state[11]) if state[11] is not None else None,
                "geo_altitude_m": float(state[13]) if state[13] is not None else None,
                "squawk": str(state[14]).strip() if state[14] else None,
                "spi": bool(state[15]),
                "position_source": int(state[16]) if state[16] is not None else 0,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            cleaned_records.append(record)

        logger.info(
            "Successfully extracted and normalized flight records",
            extra={
                "total_raw": len(raw_states),
                "total_valid_spatial": len(cleaned_records),
            },
        )
        return cleaned_records

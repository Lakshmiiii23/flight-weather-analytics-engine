"""Production Webhook Notification & Real-Time Alerting Engine.

Dispatches formatted telemetry heartbeats, high-convective weather alerts,
runtime exception diagnostics, and archival manifests to Discord/Telegram webhooks.
"""

from datetime import datetime, timezone
import logging
import os
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
from config.settings import AppSettings, load_settings
from src.ingestion.opensky_client import is_transient_network_error

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="webhook-alert-dispatcher"
)


class WebhookAlertDispatcher:
    """Dispatches asynchronous observability notifications across multi-platform webhooks."""

    def __init__(
        self,
        settings: Optional[AppSettings] = None,
        webhook_url: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.settings: AppSettings = settings or load_settings()
        self.webhook_url: str = (
            webhook_url
            or os.environ.get("WEBHOOK_URL")
            or self.settings.monitoring.webhook_url
        )
        self.session: requests.Session = session or requests.Session()
        self.enabled: bool = bool(self.webhook_url.strip()) if self.webhook_url else False

    @retry(
        retry=retry_if_exception(is_transient_network_error),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=False,
    )
    def _dispatch_payload(self, payload: Dict[str, Any]) -> bool:
        """Sends JSON payload to configured webhook URL with retry on HTTP 429."""
        if not self.enabled:
            logger.debug("Webhook alerts disabled or URL unconfigured; suppressing dispatch")
            return False

        headers = {"Content-Type": "application/json"}
        response = self.session.post(
            self.webhook_url, json=payload, headers=headers, timeout=10
        )
        response.raise_for_status()
        logger.debug("Webhook notification successfully delivered")
        return True

    def send_heartbeat(self, metrics: Dict[str, Any]) -> bool:
        """Dispatches micro-batch execution heartbeat report."""
        if not self.enabled or not self.settings.monitoring.heartbeat_enabled:
            return False

        embed = {
            "title": "🟢 Flight-Weather Pipeline Heartbeat",
            "description": f"Successfully completed micro-batch iteration `{metrics.get('iteration_id', 'N/A')}`",
            "color": 3066993,  # Green
            "fields": [
                {"name": "Flights Ingested", "value": str(metrics.get("raw_flights", 0)), "inline": True},
                {"name": "Valid & Tracked", "value": str(metrics.get("valid_flights", 0)), "inline": True},
                {"name": "Weather Grids", "value": str(metrics.get("unique_weather_grids", 0)), "inline": True},
                {"name": "Silver Committed", "value": str(metrics.get("records_committed", 0)), "inline": True},
                {"name": "Cycle Latency", "value": f"{metrics.get('duration_seconds', 0.0)}s", "inline": True},
                {"name": "Status", "value": metrics.get("status", "UNKNOWN"), "inline": True},
            ],
            "footer": {"text": "GCP Always-Free Lakehouse Engine"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return self._dispatch_payload({"embeds": [embed]})

    def send_failure_alert(
        self,
        error_message: str,
        error_type: str,
        iteration_id: str,
        traceback_snippet: Optional[str] = None,
    ) -> bool:
        """Dispatches high-priority failure notification with diagnostic context."""
        if not self.enabled or not self.settings.monitoring.alert_on_failure:
            return False

        description_text = f"**Error Type:** `{error_type}`\n**Message:** {error_message}"
        if traceback_snippet:
            description_text += f"\n```python\n{traceback_snippet[-800:]}\n```"

        embed = {
            "title": "🚨 Pipeline Iteration Failure Alert",
            "description": description_text,
            "color": 15158332,  # Red
            "fields": [
                {"name": "Iteration ID", "value": f"`{iteration_id}`", "inline": True},
                {"name": "Timestamp (UTC)", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "inline": True},
            ],
            "footer": {"text": "Autonomous Alert Sentinel"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return self._dispatch_payload({"embeds": [embed]})

    def send_archival_report(self, audit_manifest: Dict[str, Any]) -> bool:
        """Dispatches weekly Zero-Dollar Sentinel archival summary."""
        if not self.enabled or not self.settings.monitoring.alert_on_archival:
            return False

        embed = {
            "title": "📦 Cloud Archival & Eviction Complete",
            "description": f"Successfully evicted partition `{audit_manifest.get('target_partition_date')}` from BigQuery hot tables.",
            "color": 3447003,  # Blue
            "fields": [
                {"name": "Records Evicted", "value": f"{audit_manifest.get('record_count_evicted', 0):,}", "inline": True},
                {"name": "Compressed Size", "value": f"{audit_manifest.get('compressed_parquet_bytes', 0) / 1024:.2f} KB", "inline": True},
                {"name": "Compression Factor", "value": f"{audit_manifest.get('compression_ratio', 1.0)}x", "inline": True},
                {"name": "Destination", "value": f"`{audit_manifest.get('storage_destination_uri', 'N/A')}`", "inline": False},
                {"name": "SHA-256 Checksum", "value": f"`{audit_manifest.get('sha256_checksum', 'N/A')}`", "inline": False},
            ],
            "footer": {"text": "Zero-Dollar Storage Custodian"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return self._dispatch_payload({"embeds": [embed]})

    def send_severe_weather_hazard(self, flight_record: Dict[str, Any]) -> bool:
        """Dispatches operational alert when an aircraft encounters extreme convective hazard."""
        if not self.enabled:
            return False

        embed = {
            "title": "⚠️ Severe Atmospheric Flight Encounter",
            "description": f"Flight **{flight_record.get('callsign') or flight_record.get('icao24')}** registered high convective risk.",
            "color": 15105570,  # Orange
            "fields": [
                {"name": "Callsign", "value": str(flight_record.get("callsign", "N/A")), "inline": True},
                {"name": "ICAO24", "value": str(flight_record.get("icao24", "N/A")), "inline": True},
                {"name": "Convective Risk", "value": f"{flight_record.get('convective_risk_score', 0)} / 100", "inline": True},
                {"name": "Wind Gusts", "value": f"{flight_record.get('wind_gusts_mps', 0)} m/s", "inline": True},
                {"name": "Precipitation", "value": f"{flight_record.get('precipitation_mm', 0)} mm", "inline": True},
                {"name": "Altitude", "value": f"{flight_record.get('baro_altitude_m', 0):.0f} m", "inline": True},
            ],
            "footer": {"text": "Real-time Telemetry Monitor"},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return self._dispatch_payload({"embeds": [embed]})

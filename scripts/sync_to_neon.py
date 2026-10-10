"""Syncs Live Flight & Weather Telemetry Directly to Neon Serverless PostgreSQL.

Supports automatic table provisioning, micro-batch streaming, dry-run validation,
and automated Discord ChatOps notifications for CI/CD daemons.
"""

import argparse
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import sys
import time
import traceback
from typing import Any, Dict, Optional
import requests

# Ensure repository root is on sys.path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings
from src.ingestion.opensky_client import OpenSkyClient
from src.ingestion.weather_client import OpenMeteoClient
from src.processing.spatial_join import SpatialCorrelationEngine
from src.processing.validation import validate_flight_records
from src.storage.neon_manager import NeonPostgresManager

logger = setup_logging(log_level="INFO", service_name="neon-sync-runner")


def get_discord_webhook_url() -> Optional[str]:
    """Retrieves the configured Discord Webhook URL from the runtime environment."""
    url = os.environ.get("DISCORD_WEBHOOK_URL") or os.environ.get("WEBHOOK_URL")
    return url.strip() if url and url.strip() else None


def send_discord_success_alert(
    webhook_url: str,
    timestamp: str,
    total_silver: int,
    total_gold: int,
    duration_seconds: Optional[float] = None,
) -> bool:
    """Dispatches a structured operational success markdown notification to Discord."""
    markdown_lines = [
        "🟢 **Flight-Weather Pipeline Success**",
        f"• **Execution Timestamp:** `{timestamp}`",
        f"• **Airborne Vectors Ingested (Silver):** `{total_silver:,}`",
        f"• **Distinct Spatial Geohash Centroids (Gold):** `{total_gold:,}`",
    ]
    if duration_seconds is not None:
        markdown_lines.append(f"• **Execution Duration:** `{duration_seconds:.2f}s`")

    content = "\n".join(markdown_lines)
    embed = {
        "title": "🟢 Flight-Weather Pipeline Success",
        "description": "Hourly automated micro-batch committed to Neon PostgreSQL lakehouse.",
        "color": 3066993,  # Discord green #2ecc71
        "fields": [
            {"name": "Execution Timestamp", "value": f"`{timestamp}`", "inline": False},
            {"name": "Silver Airborne Vectors", "value": f"**{total_silver:,}** records", "inline": True},
            {"name": "Gold Geohash Centroids", "value": f"**{total_gold:,}** cells", "inline": True},
        ],
        "footer": {"text": "Flight-Weather Analytics Lakehouse • ChatOps Sentinel"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if duration_seconds is not None:
        embed["fields"].append({"name": "Duration", "value": f"`{duration_seconds:.2f}s`", "inline": True})

    payload = {
        "content": content,
        "embeds": [embed],
    }

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        logger.info("Successfully delivered Discord success notification")
        return True
    except Exception as exc:
        logger.warning("Failed to deliver Discord success notification: %s", exc)
        return False


def send_discord_failure_alert(
    webhook_url: str,
    error: Exception,
    traceback_str: str,
    timestamp: str,
) -> bool:
    """Dispatches a high-priority red alert warning and stack trace directly to the Discord Webhook."""
    error_type = type(error).__name__
    error_msg = str(error)
    tb_snippet = traceback_str[-1000:] if len(traceback_str) > 1000 else traceback_str

    content = (
        "❌ **Flight Ingestion Daemon Failure Alert**\n"
        f"• **Execution Timestamp:** `{timestamp}`\n"
        f"• **Error Type:** `{error_type}`\n"
        f"• **Details:** `{error_msg}`\n\n"
        "```python\n"
        f"{tb_snippet}\n"
        "```"
    )
    embed = {
        "title": "❌ Flight Ingestion Daemon Failure Alert",
        "description": (
            "An unhandled exception or API throttle interrupted the ingestion daemon.\n\n"
            f"**Error Type:** `{error_type}`\n"
            f"**Message:** {error_msg}\n\n"
            f"```python\n{tb_snippet}\n```"
        ),
        "color": 15158332,  # Discord red #e74c3c
        "fields": [
            {"name": "Timestamp", "value": f"`{timestamp}`", "inline": True},
            {"name": "Runner", "value": "`GitHub Actions`", "inline": True},
        ],
        "footer": {"text": "Flight-Weather Analytics Lakehouse • Autonomous Sentinel"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    payload = {
        "content": content,
        "embeds": [embed],
    }

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        logger.info("Successfully delivered Discord failure alert")
        return True
    except Exception as exc:
        logger.warning("Failed to deliver Discord failure alert: %s", exc)
        return False


def run_neon_cycle(
    settings: AppSettings,
    opensky: OpenSkyClient,
    weather: OpenMeteoClient,
    spatial: SpatialCorrelationEngine,
    neon_mgr: Optional[NeonPostgresManager],
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Executes a single ingestion, spatial correlation, and Neon persistence cycle.

    Returns:
        Dict[str, Any]: Execution summary statistics for telemetry and ChatOps alerting.
    """
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    logger.info("Starting live telemetry cycle targeting Neon PostgreSQL", extra={"dry_run": dry_run})

    # Step 1: Ingest live flights
    raw_flights = opensky.fetch_live_states()
    if not raw_flights:
        logger.warning("No flights received from OpenSky; skipping cycle")
        return {
            "status": "EMPTY",
            "raw_flights": 0,
            "valid_flights": 0,
            "centroids_count": 0,
            "silver_count": 0,
            "inserted_count": 0,
            "timestamp": timestamp_str,
        }

    # Step 2: Validate
    valid_flights, _ = validate_flight_records(raw_flights)
    valid_dicts = [f.model_dump() for f in valid_flights]

    # Step 3: Spatial Binning
    tagged, centroids = spatial.extract_spatial_centroids(valid_dicts)

    # Step 4: Batch Weather
    weather_obs = weather.fetch_batch_weather(centroids)

    # Step 5: Vectorized DuckDB Spatial Join
    silver_records = spatial.execute_spatial_join(tagged, weather_obs)

    # Step 6: Stream to Neon PostgreSQL
    inserted = 0
    if not dry_run and neon_mgr is not None:
        inserted = neon_mgr.load_silver_records(silver_records)
        neon_mgr.refresh_gold_kpis(target_date=today_date)
        logger.info(
            "Successfully synced telemetry to Neon PostgreSQL",
            extra={"records": len(silver_records), "inserted": inserted},
        )
    else:
        logger.info(
            "Dry-run active: Bypassed Neon PostgreSQL write",
            extra={"records_staged": len(silver_records)},
        )

    return {
        "status": "SUCCESS",
        "raw_flights": len(raw_flights),
        "valid_flights": len(valid_dicts),
        "centroids_count": len(centroids),
        "silver_count": len(silver_records),
        "inserted_count": inserted if inserted > 0 else len(silver_records),
        "timestamp": timestamp_str,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Live Telemetry to Neon Serverless PostgreSQL")
    parser.add_argument("--init-schema", action="store_true", help="Provision tables and indices in Neon")
    parser.add_argument("--dry-run", action="store_true", help="Run ingestion and join without writing to Neon")
    parser.add_argument("--once", action="store_true", help="Execute single cycle and exit")
    parser.add_argument("--continuous", action="store_true", help="Force continuous loop mode even in CI")
    parser.add_argument("--max-cycles", type=int, default=None, help="Maximum number of cycles before exiting")
    parser.add_argument("--interval", type=int, default=600, help="Polling interval in seconds (default: 600)")
    parser.add_argument("--database-url", type=str, default=None, help="Explicit Neon connection string")

    args = parser.parse_args()
    settings = load_settings()

    opensky = OpenSkyClient(settings=settings)
    weather = OpenMeteoClient(settings=settings)
    spatial = SpatialCorrelationEngine(settings=settings)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    is_ci_env = os.environ.get("GITHUB_ACTIONS") == "true" or os.environ.get("CI") == "true"
    discord_webhook = get_discord_webhook_url()

    try:
        neon_mgr = None
        if not args.dry_run:
            neon_mgr = NeonPostgresManager(database_url=args.database_url, settings=settings)
            if args.init_schema or is_ci_env:
                print("[SCHEMA] Provisioning Neon PostgreSQL tables and indices...")
                neon_mgr.initialize_schema()
                print("[SUCCESS] Schema successfully initialized in Neon!")

        max_cycles = args.max_cycles
        if max_cycles is None and is_ci_env and not args.continuous:
            max_cycles = int(os.environ.get("MAX_CYCLES", 1))

        print(
            f"[RUNNER] Starting Neon sync runner (Interval: {args.interval}s, "
            f"Dry-run: {args.dry_run}, Max-cycles: {max_cycles or 'infinite'})..."
        )
        cycle_count = 0
        while True:
            cycle_count += 1
            start_time = time.time()
            try:
                summary = run_neon_cycle(
                    settings=settings,
                    opensky=opensky,
                    weather=weather,
                    spatial=spatial,
                    neon_mgr=neon_mgr,
                    dry_run=args.dry_run,
                )
            except Exception as cycle_exc:
                if is_ci_env or args.once:
                    raise
                logger.error("Error during Neon sync iteration: %s", cycle_exc, exc_info=True)
                if discord_webhook:
                    send_discord_failure_alert(
                        webhook_url=discord_webhook,
                        error=cycle_exc,
                        traceback_str=traceback.format_exc(),
                        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    )
                time.sleep(min(args.interval, 60))
                continue

            elapsed = time.time() - start_time

            # When running in GitHub Actions (or whenever Discord Webhook is configured),
            # dispatch the structured operational success notification once committed to Neon.
            if discord_webhook and (is_ci_env or args.once):
                send_discord_success_alert(
                    webhook_url=discord_webhook,
                    timestamp=summary.get(
                        "timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    ),
                    total_silver=summary.get("silver_count", 0),
                    total_gold=summary.get("centroids_count", 0),
                    duration_seconds=elapsed,
                )

            if args.once or args.dry_run or (max_cycles is not None and cycle_count >= max_cycles):
                break

            print(f"[WAIT] Sleeping {args.interval}s until next cycle...")
            time.sleep(args.interval)

    except Exception as exc:
        tb_str = traceback.format_exc()
        timestamp_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        logger.error("Fatal failure in flight ingestion daemon: %s", exc, exc_info=True)
        if discord_webhook:
            send_discord_failure_alert(
                webhook_url=discord_webhook,
                error=exc,
                traceback_str=tb_str,
                timestamp=timestamp_now,
            )
        sys.exit(1)


if __name__ == "__main__":
    main()

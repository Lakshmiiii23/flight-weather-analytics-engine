"""Syncs Live Flight & Weather Telemetry Directly to Neon Serverless PostgreSQL.

Supports automatic table provisioning, micro-batch streaming, and dry-run validation.
"""

import argparse
from datetime import datetime, timezone
import logging
from pathlib import Path
import sys
import time
from typing import Optional

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


def run_neon_cycle(
    settings: AppSettings,
    opensky: OpenSkyClient,
    weather: OpenMeteoClient,
    spatial: SpatialCorrelationEngine,
    neon_mgr: Optional[NeonPostgresManager],
    dry_run: bool = False,
) -> None:
    """Executes a single ingestion, spatial correlation, and Neon persistence cycle."""
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info("Starting live telemetry cycle targeting Neon PostgreSQL", extra={"dry_run": dry_run})

    # Step 1: Ingest live flights
    raw_flights = opensky.fetch_live_states()
    if not raw_flights:
        logger.warning("No flights received from OpenSky; skipping cycle")
        return

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


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Live Telemetry to Neon Serverless PostgreSQL")
    parser.add_argument("--init-schema", action="store_true", help="Provision tables and indices in Neon")
    parser.add_argument("--dry-run", action="store_true", help="Run ingestion and join without writing to Neon")
    parser.add_argument("--once", action="store_true", help="Execute single cycle and exit")
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

    neon_mgr = None
    if not args.dry_run:
        neon_mgr = NeonPostgresManager(database_url=args.database_url, settings=settings)
        if args.init_schema:
            print("[SCHEMA] Provisioning Neon PostgreSQL tables and indices...")
            neon_mgr.initialize_schema()
            print("[SUCCESS] Schema successfully initialized in Neon!")

    print(f"[RUNNER] Starting Neon sync runner (Interval: {args.interval}s, Dry-run: {args.dry_run})...")
    while True:
        try:
            run_neon_cycle(
                settings=settings,
                opensky=opensky,
                weather=weather,
                spatial=spatial,
                neon_mgr=neon_mgr,
                dry_run=args.dry_run,
            )
        except Exception as exc:
            logger.error("Error during Neon sync iteration", exc_info=True)

        if args.once or args.dry_run:
            break

        print(f"[WAIT] Sleeping {args.interval}s until next cycle...")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()

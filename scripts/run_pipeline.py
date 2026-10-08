"""Main Micro-Batch Pipeline Orchestrator.

Ties together resilient ingestion from OpenSky, Geohash spatial binning,
batch weather extraction from Open-Meteo, DuckDB out-of-core transformation,
and BigQuery lakehouse persistence.
"""

import argparse
from datetime import datetime, timezone
import logging
from pathlib import Path
import signal
import sys
import time
from typing import Any, Dict, Optional
import uuid

# Ensure repository root is on sys.path for direct CLI or cron invocation
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings
from src.ingestion.opensky_client import OpenSkyClient
from src.ingestion.weather_client import OpenMeteoClient
from src.processing.spatial_join import SpatialCorrelationEngine
from src.processing.validation import validate_flight_records
from src.storage.bigquery_manager import BigQueryManager

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="pipeline-orchestrator"
)

# Global flag for graceful termination
_RUNNING: bool = True


def _signal_handler(signum: int, frame: Any) -> None:
    global _RUNNING
    logger.info("Received termination signal; gracefully halting pipeline loop", extra={"signal": signum})
    _RUNNING = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def run_pipeline_iteration(
    settings: AppSettings,
    opensky_client: OpenSkyClient,
    weather_client: OpenMeteoClient,
    spatial_engine: SpatialCorrelationEngine,
    bq_manager: Optional[BigQueryManager],
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Executes a single end-to-end micro-batch iteration.

    Returns:
        Dict[str, Any]: Metrics and lineage telemetry for the run.
    """
    iteration_id = str(uuid.uuid4())
    start_time = time.time()
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info(
        "Beginning micro-batch pipeline iteration",
        extra={"iteration_id": iteration_id, "dry_run": dry_run, "date": today_date},
    )

    # Step 1: Ingest live flight state vectors from OpenSky
    raw_flights = opensky_client.fetch_live_states()
    if not raw_flights:
        logger.warning("Zero active flights received from OpenSky; skipping cycle", extra={"iteration_id": iteration_id})
        return {"status": "SKIPPED_EMPTY_FLIGHTS", "raw_count": 0}

    # Step 2: Validate against Pydantic contracts & isolate quarantine
    valid_flights, quarantined = validate_flight_records(raw_flights)
    valid_flight_dicts = [f.model_dump() for f in valid_flights]

    # Step 3: Spatial Binning & Centroid Extraction
    tagged_flights, centroids = spatial_engine.extract_spatial_centroids(valid_flight_dicts)

    # Step 4: Batch Weather Ingestion for Centroids
    weather_observations = weather_client.fetch_batch_weather(centroids)

    # Step 5: DuckDB Vectorized Spatial Join & Convective Risk Scoring
    silver_records = spatial_engine.execute_spatial_join(tagged_flights, weather_observations)

    # Step 6: Lakehouse Persistence (BigQuery)
    if not dry_run and bq_manager is not None:
        try:
            bq_manager.ensure_dataset()
            bq_manager.initialize_tables()
            loaded_count = bq_manager.load_silver_records(silver_records)
            bq_manager.refresh_gold_kpis(target_date=today_date)
        except Exception as bq_err:
            logger.error("Failed persisting records to BigQuery", exc_info=True)
            raise
    else:
        loaded_count = len(silver_records)
        logger.info(
            "Dry-run active: bypassed BigQuery write",
            extra={"records_staged": len(silver_records)},
        )

    duration = round(time.time() - start_time, 2)
    metrics = {
        "iteration_id": iteration_id,
        "status": "SUCCESS",
        "raw_flights": len(raw_flights),
        "valid_flights": len(valid_flight_dicts),
        "quarantined_flights": len(quarantined),
        "unique_weather_grids": len(centroids),
        "weather_observations": len(weather_observations),
        "silver_records_produced": len(silver_records),
        "records_committed": loaded_count,
        "duration_seconds": duration,
    }

    logger.info("Micro-batch iteration completed successfully", extra=metrics)
    return metrics


def main() -> None:
    """CLI Entrypoint for the pipeline orchestrator."""
    parser = argparse.ArgumentParser(
        description="Eco-Friendly Hybrid Flight-Weather Analytics Pipeline Orchestrator"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run ingestion, validation, and spatial join without committing to BigQuery",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Execute a single iteration and terminate immediately",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Polling interval in seconds between micro-batches (overrides settings.yaml)",
    )

    args = parser.parse_args()
    settings: AppSettings = load_settings()
    interval: int = args.interval or settings.pipeline.run_interval_seconds

    opensky_client = OpenSkyClient(settings=settings)
    weather_client = OpenMeteoClient(settings=settings)
    spatial_engine = SpatialCorrelationEngine(settings=settings)
    bq_manager = None if args.dry_run else BigQueryManager(settings=settings)

    logger.info(
        "Initialized pipeline engine",
        extra={"interval_seconds": interval, "dry_run": args.dry_run, "once": args.once},
    )

    while _RUNNING:
        try:
            run_pipeline_iteration(
                settings=settings,
                opensky_client=opensky_client,
                weather_client=weather_client,
                spatial_engine=spatial_engine,
                bq_manager=bq_manager,
                dry_run=args.dry_run,
            )
        except Exception as exc:
            logger.error("Unhandled exception during iteration execution", exc_info=True)

        if args.once:
            logger.info("--once flag specified; terminating orchestrator")
            break

        logger.info(f"Sleeping for {interval} seconds until next micro-batch cycle...")
        for _ in range(interval):
            if not _RUNNING:
                break
            time.sleep(1)


if __name__ == "__main__":
    main()

"""Neon Serverless PostgreSQL Database Manager.

Manages connection pooling, schema initialization, idempotent batch streaming,
and partition maintenance on Neon PostgreSQL.
"""

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, OperationalError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from dotenv import load_dotenv

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings

load_dotenv()


logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="neon-database-manager"
)


class NeonPostgresManager:
    """Manages cloud persistence to Neon Serverless PostgreSQL."""

    def __init__(
        self,
        database_url: Optional[str] = None,
        settings: Optional[AppSettings] = None,
        engine: Optional[sa.Engine] = None,
    ) -> None:
        self.settings: AppSettings = settings or load_settings()

        if engine is not None:
            self.engine = engine
        else:
            raw_url = (
                database_url
                or os.environ.get("NEON_DATABASE_URL")
                or os.environ.get("DATABASE_URL")
            )
            if not raw_url:
                raise ValueError(
                    "NEON_DATABASE_URL is not configured! Please set it in your .env file or environment."
                )

            # Normalize URI scheme for SQLAlchemy compatibility (use installed psycopg2 driver)
            if raw_url.startswith("postgres://"):
                raw_url = raw_url.replace("postgres://", "postgresql+psycopg2://", 1)
            elif raw_url.startswith("postgresql://") and not raw_url.startswith("postgresql+"):
                raw_url = raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)

            self.engine = sa.create_engine(
                raw_url,
                pool_pre_ping=True,       # Prevents stale connection drops on serverless idle
                pool_recycle=300,         # Recycles connections every 5 minutes
                connect_args={"sslmode": "require"},  # Neon requires SSL
            )

    @retry(
        retry=retry_if_exception_type((OperationalError, DBAPIError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def initialize_schema(self, ddl_path: Optional[Path] = None) -> None:
        """Executes DDL to create Silver, Gold, and Audit tables in Neon PostgreSQL."""
        schema_file = ddl_path or (
            Path(__file__).resolve().parent.parent.parent / "sql" / "ddl" / "create_neon_tables.sql"
        )
        if not schema_file.exists():
            raise FileNotFoundError(f"Missing Neon DDL script at {schema_file}")

        ddl_content = schema_file.read_text(encoding="utf-8")
        # Split individual SQL statements separated by semicolons
        statements = [stmt.strip() for stmt in ddl_content.split(";") if stmt.strip()]

        with self.engine.begin() as conn:
            for statement in statements:
                conn.execute(sa.text(statement))

        logger.info("Successfully provisioned Neon PostgreSQL tables and indices")

    @retry(
        retry=retry_if_exception_type((OperationalError, DBAPIError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def load_silver_records(self, records: List[Dict[str, Any]]) -> int:
        """Loads enriched flight-weather telemetry into Neon PostgreSQL.

        Uses ON CONFLICT (flight_record_id) DO NOTHING to ensure idempotency.
        """
        if not records:
            logger.warning("Empty records list provided to Neon Silver loader")
            return 0

        logger.info(
            "Inserting records into Neon silver_flight_weather_telemetry",
            extra={"record_count": len(records)},
        )

        metadata = sa.MetaData()
        silver_table = sa.Table(
            "silver_flight_weather_telemetry",
            metadata,
            autoload_with=self.engine,
        )

        stmt = pg_insert(silver_table).values(records)
        upsert_stmt = stmt.on_conflict_do_nothing(index_elements=["flight_record_id"])

        with self.engine.begin() as conn:
            result = conn.execute(upsert_stmt)
            inserted_rows = result.rowcount

        logger.info(
            "Silver records committed to Neon PostgreSQL",
            extra={"staged": len(records), "newly_inserted": inserted_rows},
        )
        return inserted_rows or len(records)

    @retry(
        retry=retry_if_exception_type((OperationalError, DBAPIError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def refresh_gold_kpis(self, target_date: Optional[str] = None) -> int:
        """Executes Gold analytical SQL rollup and upserts into gold_delay_weather_matrix."""
        partition_date = (
            target_date
            if target_date
            else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )

        upsert_query = sa.text("""
        INSERT INTO gold_delay_weather_matrix (
            window_start,
            window_end,
            recorded_date,
            geohash,
            weather_condition_category,
            total_active_flights,
            total_cruising_flights,
            avg_velocity_mps,
            baseline_velocity_mps,
            velocity_deficit_pct,
            avg_precipitation_mm,
            max_precipitation_mm,
            avg_wind_speed_mps,
            peak_wind_gust_mps,
            avg_cloud_cover_pct,
            min_visibility_m,
            mean_convective_risk,
            flights_impacted_count,
            computed_at
        )
        SELECT
            date_trunc('hour', weather_recorded_at) AS window_start,
            date_trunc('hour', weather_recorded_at) + interval '1 hour' AS window_end,
            recorded_date,
            geohash,
            CASE
                WHEN convective_risk_score >= 60.0 OR precipitation_mm >= 3.0 OR wind_gusts_mps >= 22.0 THEN 'SEVERE_STORM'
                WHEN wind_speed_mps >= 14.0 OR wind_gusts_mps >= 18.0 THEN 'HIGH_WIND_SHEAR'
                WHEN precipitation_mm >= 0.5 THEN 'MODERATE_RAIN'
                WHEN visibility_m IS NOT NULL AND visibility_m < 3000.0 THEN 'LOW_VISIBILITY'
                ELSE 'CLEAR'
            END AS weather_condition_category,
            COUNT(DISTINCT icao24) AS total_active_flights,
            COUNT(CASE WHEN baro_altitude_m >= 3000.0 THEN 1 END) AS total_cruising_flights,
            ROUND(AVG(velocity_mps)::numeric, 2) AS avg_velocity_mps,
            230.0 AS baseline_velocity_mps,
            ROUND((((230.0 - AVG(velocity_mps)) / 230.0) * 100.0)::numeric, 2) AS velocity_deficit_pct,
            ROUND(AVG(COALESCE(precipitation_mm, 0.0))::numeric, 2) AS avg_precipitation_mm,
            ROUND(MAX(COALESCE(precipitation_mm, 0.0))::numeric, 2) AS max_precipitation_mm,
            ROUND(AVG(COALESCE(wind_speed_mps, 0.0))::numeric, 2) AS avg_wind_speed_mps,
            ROUND(MAX(COALESCE(wind_gusts_mps, 0.0))::numeric, 2) AS peak_wind_gust_mps,
            ROUND(AVG(COALESCE(cloud_cover_pct, 0.0))::numeric, 2) AS avg_cloud_cover_pct,
            ROUND(MIN(COALESCE(visibility_m, 10000.0))::numeric, 2) AS min_visibility_m,
            ROUND(AVG(COALESCE(convective_risk_score, 0.0))::numeric, 2) AS mean_convective_risk,
            COUNT(CASE WHEN convective_risk_score >= 50.0 THEN 1 END) AS flights_impacted_count,
            CURRENT_TIMESTAMP AS computed_at
        FROM silver_flight_weather_telemetry
        WHERE recorded_date = :target_date
          AND weather_recorded_at IS NOT NULL
          AND velocity_mps IS NOT NULL
        GROUP BY
            date_trunc('hour', weather_recorded_at),
            recorded_date,
            geohash,
            5
        ON CONFLICT (window_start, geohash, weather_condition_category, recorded_date)
        DO UPDATE SET
            total_active_flights = EXCLUDED.total_active_flights,
            total_cruising_flights = EXCLUDED.total_cruising_flights,
            avg_velocity_mps = EXCLUDED.avg_velocity_mps,
            velocity_deficit_pct = EXCLUDED.velocity_deficit_pct,
            avg_precipitation_mm = EXCLUDED.avg_precipitation_mm,
            max_precipitation_mm = EXCLUDED.max_precipitation_mm,
            avg_wind_speed_mps = EXCLUDED.avg_wind_speed_mps,
            peak_wind_gust_mps = EXCLUDED.peak_wind_gust_mps,
            avg_cloud_cover_pct = EXCLUDED.avg_cloud_cover_pct,
            min_visibility_m = EXCLUDED.min_visibility_m,
            mean_convective_risk = EXCLUDED.mean_convective_risk,
            flights_impacted_count = EXCLUDED.flights_impacted_count,
            computed_at = EXCLUDED.computed_at;
        """)

        with self.engine.begin() as conn:
            result = conn.execute(upsert_query, {"target_date": partition_date})
            affected = result.rowcount

        logger.info(
            "Gold KPI rollup executed in Neon PostgreSQL",
            extra={"partition_date": partition_date, "affected_rows": affected},
        )
        return affected

    @retry(
        retry=retry_if_exception_type((OperationalError, DBAPIError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def delete_partition(self, table_name: str, partition_date: str) -> int:
        """Prunes a partition date from a hot table on Neon PostgreSQL."""
        delete_sql = sa.text(f"DELETE FROM {table_name} WHERE recorded_date = :target_date")

        with self.engine.begin() as conn:
            result = conn.execute(delete_sql, {"target_date": partition_date})
            deleted = result.rowcount

        logger.info(
            "Pruned partition from Neon table",
            extra={"table": table_name, "date": partition_date, "rows_deleted": deleted},
        )
        return deleted

    def query_records(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Executes arbitrary SQL query and returns list of dictionaries."""
        with self.engine.connect() as conn:
            result = conn.execute(sa.text(query), params or {})
            return [dict(row._mapping) for row in result]

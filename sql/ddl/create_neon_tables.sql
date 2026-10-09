-- ==============================================================================
-- DDL: Neon Serverless PostgreSQL Table Definitions
-- Architecture: Medallion Lakehouse on PostgreSQL / Neon
-- Purpose: Complete schema with Primary Keys, B-Tree Indexes, and Idempotency
-- ==============================================================================

-- 1. Silver Layer: Cleaned and Weather-Enriched Telemetry
CREATE TABLE IF NOT EXISTS silver_flight_weather_telemetry (
    flight_record_id VARCHAR(64) PRIMARY KEY,
    icao24 VARCHAR(10) NOT NULL,
    callsign VARCHAR(12),
    origin_country VARCHAR(100),
    time_position BIGINT,
    last_contact BIGINT,
    longitude DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    baro_altitude_m DOUBLE PRECISION,
    on_ground BOOLEAN DEFAULT FALSE,
    velocity_mps DOUBLE PRECISION,
    true_track_deg DOUBLE PRECISION,
    vertical_rate_mps DOUBLE PRECISION,
    geo_altitude_m DOUBLE PRECISION,
    squawk VARCHAR(10),
    spi BOOLEAN DEFAULT FALSE,
    position_source INTEGER DEFAULT 0,
    geohash VARCHAR(12) NOT NULL,
    weather_recorded_at TIMESTAMP WITH TIME ZONE,
    temperature_c DOUBLE PRECISION,
    relative_humidity_pct DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_mps DOUBLE PRECISION,
    wind_direction_deg DOUBLE PRECISION,
    wind_gusts_mps DOUBLE PRECISION,
    cloud_cover_pct DOUBLE PRECISION,
    visibility_m DOUBLE PRECISION,
    convective_risk_score DOUBLE PRECISION,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    recorded_date DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_silver_recorded_date ON silver_flight_weather_telemetry(recorded_date);
CREATE INDEX IF NOT EXISTS idx_silver_geohash ON silver_flight_weather_telemetry(geohash);
CREATE INDEX IF NOT EXISTS idx_silver_callsign ON silver_flight_weather_telemetry(callsign);

-- 2. Gold Layer: Aggregated KPI Matrix
CREATE TABLE IF NOT EXISTS gold_delay_weather_matrix (
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    recorded_date DATE NOT NULL,
    geohash VARCHAR(12) NOT NULL,
    weather_condition_category VARCHAR(50) NOT NULL,
    total_active_flights BIGINT NOT NULL,
    total_cruising_flights BIGINT NOT NULL,
    avg_velocity_mps DOUBLE PRECISION,
    baseline_velocity_mps DOUBLE PRECISION,
    velocity_deficit_pct DOUBLE PRECISION,
    avg_precipitation_mm DOUBLE PRECISION,
    max_precipitation_mm DOUBLE PRECISION,
    avg_wind_speed_mps DOUBLE PRECISION,
    peak_wind_gust_mps DOUBLE PRECISION,
    avg_cloud_cover_pct DOUBLE PRECISION,
    min_visibility_m DOUBLE PRECISION,
    mean_convective_risk DOUBLE PRECISION,
    flights_impacted_count BIGINT NOT NULL,
    computed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (window_start, geohash, weather_condition_category, recorded_date)
);

CREATE INDEX IF NOT EXISTS idx_gold_recorded_date ON gold_delay_weather_matrix(recorded_date);
CREATE INDEX IF NOT EXISTS idx_gold_geohash ON gold_delay_weather_matrix(geohash);

-- 3. Audit Layer: Immutable Cryptographic Custody Ledger
CREATE TABLE IF NOT EXISTS cloud_archive_audit_logs (
    audit_id VARCHAR(64) PRIMARY KEY,
    archival_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    target_partition_date DATE NOT NULL,
    record_count_evicted BIGINT NOT NULL,
    raw_uncompressed_bytes BIGINT NOT NULL,
    compressed_parquet_bytes BIGINT NOT NULL,
    compression_ratio DOUBLE PRECISION NOT NULL,
    sha256_checksum VARCHAR(64) NOT NULL,
    storage_destination_uri VARCHAR(512) NOT NULL,
    eviction_status VARCHAR(50) NOT NULL,
    truncated_table_name VARCHAR(100) NOT NULL,
    execution_duration_seconds DOUBLE PRECISION,
    metadata_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_target_date ON cloud_archive_audit_logs(target_partition_date);

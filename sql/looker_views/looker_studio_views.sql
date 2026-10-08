-- ==============================================================================
-- Google Looker Studio Analytical Views (Free-Tier Optimized)
-- Architecture: Optimized reporting views with 7-day partition prune
-- Purpose: Sub-second dashboard rendering with zero full-table scans
-- ==============================================================================

-- VIEW 1: Active Weather Hotspots & Speed Deficits
CREATE OR REPLACE VIEW `flight_weather_analytics.v_looker_realtime_hotspots` AS
SELECT
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
FROM `flight_weather_analytics.gold_delay_weather_matrix`
WHERE recorded_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);

-- VIEW 2: Hourly Time-Series Delay Correlation
CREATE OR REPLACE VIEW `flight_weather_analytics.v_looker_hourly_delay_trends` AS
SELECT
    window_start,
    weather_condition_category,
    SUM(total_active_flights) AS total_flights_in_hour,
    ROUND(AVG(avg_velocity_mps), 2) AS hourly_avg_velocity_mps,
    ROUND(AVG(velocity_deficit_pct), 2) AS hourly_avg_deficit_pct,
    ROUND(MAX(peak_wind_gust_mps), 2) AS max_gust_in_hour,
    ROUND(AVG(mean_convective_risk), 2) AS hourly_convective_risk,
    SUM(flights_impacted_count) AS total_impacted_aircraft
FROM `flight_weather_analytics.gold_delay_weather_matrix`
WHERE recorded_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY window_start, weather_condition_category;

-- VIEW 3: Zero-Dollar Archival Storage Audit & Compression Savings
CREATE OR REPLACE VIEW `flight_weather_analytics.v_looker_archival_audit_trail` AS
SELECT
    audit_id,
    archival_timestamp,
    target_partition_date,
    record_count_evicted,
    ROUND(raw_uncompressed_bytes / (1024 * 1024), 2) AS uncompressed_mb,
    ROUND(compressed_parquet_bytes / (1024 * 1024), 2) AS compressed_parquet_mb,
    compression_ratio,
    sha256_checksum,
    storage_destination_uri,
    eviction_status,
    truncated_table_name,
    execution_duration_seconds
FROM `flight_weather_analytics.cloud_archive_audit_logs`;

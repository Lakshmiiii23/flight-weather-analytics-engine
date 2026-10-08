-- ==============================================================================
-- Gold KPI Rollup Transformation: Weather Hazards vs. Flight Velocity Deficit
-- Target Table: flight_weather_analytics.gold_delay_weather_matrix
-- Idempotency: Uses MERGE to allow re-running across the active partition date
-- ==============================================================================

MERGE `flight_weather_analytics.gold_delay_weather_matrix` T
USING (
    WITH raw_windowed AS (
        SELECT
            TIMESTAMP_TRUNC(weather_recorded_at, HOUR) AS window_start,
            TIMESTAMP_ADD(TIMESTAMP_TRUNC(weather_recorded_at, HOUR), INTERVAL 1 HOUR) AS window_end,
            recorded_date,
            geohash,
            CASE
                WHEN convective_risk_score >= 60.0 OR precipitation_mm >= 3.0 OR wind_gusts_mps >= 22.0 THEN 'SEVERE_STORM'
                WHEN wind_speed_mps >= 14.0 OR wind_gusts_mps >= 18.0 THEN 'HIGH_WIND_SHEAR'
                WHEN precipitation_mm >= 0.5 THEN 'MODERATE_RAIN'
                WHEN visibility_m IS NOT NULL AND visibility_m < 3000.0 THEN 'LOW_VISIBILITY'
                ELSE 'CLEAR'
            END AS weather_condition_category,
            icao24,
            velocity_mps,
            baro_altitude_m,
            precipitation_mm,
            wind_speed_mps,
            wind_gusts_mps,
            cloud_cover_pct,
            visibility_m,
            convective_risk_score
        FROM `flight_weather_analytics.silver_flight_weather_telemetry`
        WHERE recorded_date = DATE(@target_date)
          AND weather_recorded_at IS NOT NULL
          AND velocity_mps IS NOT NULL
    )
    SELECT
        window_start,
        window_end,
        recorded_date,
        geohash,
        weather_condition_category,
        COUNT(DISTINCT icao24) AS total_active_flights,
        COUNTIF(baro_altitude_m >= 3000.0) AS total_cruising_flights,
        ROUND(AVG(velocity_mps), 2) AS avg_velocity_mps,
        230.0 AS baseline_velocity_mps,
        ROUND(SAFE_DIVIDE(230.0 - AVG(velocity_mps), 230.0) * 100.0, 2) AS velocity_deficit_pct,
        ROUND(AVG(COALESCE(precipitation_mm, 0.0)), 2) AS avg_precipitation_mm,
        ROUND(MAX(COALESCE(precipitation_mm, 0.0)), 2) AS max_precipitation_mm,
        ROUND(AVG(COALESCE(wind_speed_mps, 0.0)), 2) AS avg_wind_speed_mps,
        ROUND(MAX(COALESCE(wind_gusts_mps, 0.0)), 2) AS peak_wind_gust_mps,
        ROUND(AVG(COALESCE(cloud_cover_pct, 0.0)), 2) AS avg_cloud_cover_pct,
        ROUND(MIN(COALESCE(visibility_m, 10000.0)), 2) AS min_visibility_m,
        ROUND(AVG(COALESCE(convective_risk_score, 0.0)), 2) AS mean_convective_risk,
        COUNTIF(convective_risk_score >= 50.0) AS flights_impacted_count,
        CURRENT_TIMESTAMP() AS computed_at
    FROM raw_windowed
    GROUP BY
        window_start,
        window_end,
        recorded_date,
        geohash,
        weather_condition_category
) S
ON T.window_start = S.window_start
   AND T.geohash = S.geohash
   AND T.weather_condition_category = S.weather_condition_category
   AND T.recorded_date = S.recorded_date
WHEN MATCHED THEN
    UPDATE SET
        total_active_flights = S.total_active_flights,
        total_cruising_flights = S.total_cruising_flights,
        avg_velocity_mps = S.avg_velocity_mps,
        baseline_velocity_mps = S.baseline_velocity_mps,
        velocity_deficit_pct = S.velocity_deficit_pct,
        avg_precipitation_mm = S.avg_precipitation_mm,
        max_precipitation_mm = S.max_precipitation_mm,
        avg_wind_speed_mps = S.avg_wind_speed_mps,
        peak_wind_gust_mps = S.peak_wind_gust_mps,
        avg_cloud_cover_pct = S.avg_cloud_cover_pct,
        min_visibility_m = S.min_visibility_m,
        mean_convective_risk = S.mean_convective_risk,
        flights_impacted_count = S.flights_impacted_count,
        computed_at = S.computed_at
WHEN NOT MATCHED THEN
    INSERT (
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
    VALUES (
        S.window_start,
        S.window_end,
        S.recorded_date,
        S.geohash,
        S.weather_condition_category,
        S.total_active_flights,
        S.total_cruising_flights,
        S.avg_velocity_mps,
        S.baseline_velocity_mps,
        S.velocity_deficit_pct,
        S.avg_precipitation_mm,
        S.max_precipitation_mm,
        S.avg_wind_speed_mps,
        S.peak_wind_gust_mps,
        S.avg_cloud_cover_pct,
        S.min_visibility_m,
        S.mean_convective_risk,
        S.flights_impacted_count,
        S.computed_at
    );

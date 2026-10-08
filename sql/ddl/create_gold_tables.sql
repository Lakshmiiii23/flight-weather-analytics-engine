-- ==============================================================================
-- DDL: Gold Analytical KPI Mart (Weather Impact & Velocity Deficit Matrix)
-- Architecture: Medallion Architecture (Gold Business-Aggregated Layer)
-- Optimization: Day-partitioned on recorded_date, clustered on geohash & hazard level
-- Serving: Optimized for sub-second Looker Studio / Power BI BI dashboards
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `flight_weather_analytics.gold_delay_weather_matrix`
(
    window_start TIMESTAMP NOT NULL OPTIONS(description="Start timestamp of aggregation window (UTC)"),
    window_end TIMESTAMP NOT NULL OPTIONS(description="End timestamp of aggregation window (UTC)"),
    recorded_date DATE NOT NULL OPTIONS(description="Partition date of the analytical window"),
    geohash STRING NOT NULL OPTIONS(description="Spatial cluster geohash bucket"),
    weather_condition_category STRING NOT NULL OPTIONS(description="Categorical hazard level: CLEAR, MODERATE_RAIN, SEVERE_STORM, HIGH_WIND_SHEAR, LOW_VISIBILITY"),
    
    -- Traffic Metrics
    total_active_flights INT64 NOT NULL OPTIONS(description="Count of distinct aircraft active in grid window"),
    total_cruising_flights INT64 NOT NULL OPTIONS(description="Count of flights above 3000m barometric altitude"),
    
    -- Telemetry & Delay Metrics
    avg_velocity_mps FLOAT64 OPTIONS(description="Mean observed ground speed in meters/second"),
    baseline_velocity_mps FLOAT64 OPTIONS(description="Expected historical/fair-weather speed baseline"),
    velocity_deficit_pct FLOAT64 OPTIONS(description="Percentage slowdown vs expected speed [positive = delay/slowdown]"),
    
    -- Meteorological Correlates
    avg_precipitation_mm FLOAT64 OPTIONS(description="Average precipitation across grid cell"),
    max_precipitation_mm FLOAT64 OPTIONS(description="Peak precipitation burst in grid cell"),
    avg_wind_speed_mps FLOAT64 OPTIONS(description="Mean surface wind speed in meters/second"),
    peak_wind_gust_mps FLOAT64 OPTIONS(description="Maximum wind gust recorded in meters/second"),
    avg_cloud_cover_pct FLOAT64 OPTIONS(description="Mean total cloud cover percentage"),
    min_visibility_m FLOAT64 OPTIONS(description="Minimum surface visibility in meters"),
    
    -- Risk Index
    mean_convective_risk FLOAT64 OPTIONS(description="Average composite convective storm hazard index [0-100]"),
    flights_impacted_count INT64 NOT NULL OPTIONS(description="Count of aircraft experiencing severe convective risk (>50)"),
    
    -- Execution Lineage
    computed_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when aggregation job produced this rollup")
)
PARTITION BY recorded_date
CLUSTER BY geohash, weather_condition_category
OPTIONS(
    description="Gold KPI Mart: Correlated flight delays and ground speed deficits across localized meteorological hazards",
    require_partition_filter=false
);

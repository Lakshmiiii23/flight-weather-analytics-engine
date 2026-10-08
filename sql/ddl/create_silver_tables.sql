-- ==============================================================================
-- DDL: Silver Flight & Weather Telemetry Table
-- Architecture: Medallion Architecture (Silver Clean & Enriched Layer)
-- Partitioning: Day-partitioned on recorded_date (prevents full-table scan charges)
-- Clustering: Geohash & Callsign (optimizes localized spatial queries & airline tracking)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `flight_weather_analytics.silver_flight_weather_telemetry`
(
    flight_record_id STRING NOT NULL OPTIONS(description="Unique deterministic ID (SHA-256 of icao24 + time_position)"),
    icao24 STRING NOT NULL OPTIONS(description="Unique ICAO 24-bit transponder address in hex format"),
    callsign STRING OPTIONS(description="Flight callsign (8 chars max, trimmed)"),
    origin_country STRING OPTIONS(description="Country of aircraft registration inferred from ICAO"),
    time_position INT64 OPTIONS(description="Unix timestamp in seconds for last position update"),
    last_contact INT64 OPTIONS(description="Unix timestamp in seconds for last transponder signal received"),
    longitude FLOAT64 NOT NULL OPTIONS(description="WGS-84 Longitude in decimal degrees [-180, 180]"),
    latitude FLOAT64 NOT NULL OPTIONS(description="WGS-84 Latitude in decimal degrees [-90, 90]"),
    baro_altitude_m FLOAT64 OPTIONS(description="Barometric altitude in meters above sea level"),
    on_ground BOOL OPTIONS(description="Flag indicating if the aircraft is broadcasting surface status"),
    velocity_mps FLOAT64 OPTIONS(description="Ground speed in meters per second"),
    true_track_deg FLOAT64 OPTIONS(description="True track angle in decimal degrees clockwise from north [0, 360)"),
    vertical_rate_mps FLOAT64 OPTIONS(description="Climb or descent rate in meters per second"),
    geo_altitude_m FLOAT64 OPTIONS(description="Geometric altitude in meters above WGS-84 ellipsoid"),
    squawk STRING OPTIONS(description="4-digit transponder squawk code"),
    spi BOOL OPTIONS(description="Special Purpose Indicator transponder ping"),
    position_source INT64 OPTIONS(description="Origin of navigation telemetry: 0=ADS-B, 1=ASTERIX, 2=MLAT"),
    
    -- Spatial Indexing
    geohash STRING NOT NULL OPTIONS(description="Geohash spatial bucket (precision 4, ~20km cell resolution)"),

    -- Enriched Weather Metrics
    weather_recorded_at TIMESTAMP OPTIONS(description="Timestamp of localized meteorological measurement"),
    temperature_c FLOAT64 OPTIONS(description="Ambient temperature at 2m in Celsius"),
    relative_humidity_pct FLOAT64 OPTIONS(description="Relative humidity percentage [0-100]"),
    precipitation_mm FLOAT64 OPTIONS(description="Precipitation intensity in millimeters"),
    wind_speed_mps FLOAT64 OPTIONS(description="Wind speed at 10m in meters per second"),
    wind_direction_deg FLOAT64 OPTIONS(description="Wind direction in degrees clockwise from north"),
    wind_gusts_mps FLOAT64 OPTIONS(description="Wind gusts at 10m in meters per second"),
    cloud_cover_pct FLOAT64 OPTIONS(description="Convective and total cloud cover percentage [0-100]"),
    visibility_m FLOAT64 OPTIONS(description="Horizontal surface visibility in meters"),
    convective_risk_score FLOAT64 OPTIONS(description="Composite risk metric [0-100] derived from precipitation, gusts & cloud cover"),

    -- Ingestion Metadata
    ingested_at TIMESTAMP NOT NULL OPTIONS(description="UTC timestamp when record was written by pipeline"),
    recorded_date DATE NOT NULL OPTIONS(description="Partition date extracted from time_position")
)
PARTITION BY recorded_date
CLUSTER BY geohash, callsign
OPTIONS(
    description="Silver layer: cleaned, deduplicated, and spatially merged flight vectors with localized weather",
    require_partition_filter=false
);

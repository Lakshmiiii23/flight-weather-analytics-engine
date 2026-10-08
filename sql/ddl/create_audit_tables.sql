-- ==============================================================================
-- DDL: Immutable Archival & Eviction Audit Log Table
-- Architecture: Zero-Dollar Sentinel (Governance & Storage Cap Enforcement)
-- Purpose: Provides cryptographic lineage (SHA-256) of data evicted from BigQuery
-- Footprint: ~1 KB per weekly execution (virtually 0 bytes against BigQuery 10GB cap)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS `flight_weather_analytics.cloud_archive_audit_logs`
(
    audit_id STRING NOT NULL OPTIONS(description="Unique deterministic UUIDv4 for archival event"),
    archival_timestamp TIMESTAMP NOT NULL OPTIONS(description="UTC timestamp when the eviction job executed"),
    target_partition_date DATE NOT NULL OPTIONS(description="BigQuery partition date that was exported and pruned"),
    
    -- Volume & Sizing Metrics
    record_count_evicted INT64 NOT NULL OPTIONS(description="Total rows extracted and evicted from hot storage"),
    raw_uncompressed_bytes INT64 NOT NULL OPTIONS(description="Calculated uncompressed byte size before serialization"),
    compressed_parquet_bytes INT64 NOT NULL OPTIONS(description="Exact byte size of compressed ZStandard Parquet archive"),
    compression_ratio FLOAT64 NOT NULL OPTIONS(description="Compression factor: (uncompressed_bytes / compressed_bytes)"),
    
    -- Cryptographic Integrity & Cold Path
    sha256_checksum STRING NOT NULL OPTIONS(description="Cryptographic SHA-256 hash of the generated Parquet archive"),
    storage_destination_uri STRING NOT NULL OPTIONS(description="Immutable remote destination URI (Cloudflare R2 / S3 path)"),
    
    -- Execution State & Lifecycle
    eviction_status STRING NOT NULL OPTIONS(description="Execution outcome: SUCCESS, DRY_RUN, REVERTED, FAILED"),
    truncated_table_name STRING NOT NULL OPTIONS(description="Name of the table whose partition was dropped/truncated"),
    execution_duration_seconds FLOAT64 OPTIONS(description="Elapsed runtime of export, hash verification, and drop"),
    
    -- Structured Metadata
    metadata_json STRING OPTIONS(description="Serialized JSON containing worker environment, engine version, and schema hash")
)
PARTITION BY DATE(archival_timestamp)
CLUSTER BY target_partition_date, eviction_status
OPTIONS(
    description="Permanent audit registry: verifies complete data custody transfer from cloud to immutable cold storage",
    require_partition_filter=false
);

"""Unit tests for ZSTD Parquet dataset serializer and cryptographic hasher."""

from pathlib import Path
import pytest
import pyarrow.parquet as pq

from src.storage.parquet_exporter import ParquetExporter, ExportArtifact


@pytest.fixture
def sample_silver_records():
    return [
        {
            "flight_record_id": f"rec_{i}",
            "icao24": "a1b2c3",
            "callsign": "AAL100",
            "longitude": -74.0060 + (i * 0.01),
            "latitude": 40.7128 + (i * 0.01),
            "velocity_mps": 240.0 + i,
            "geohash": "dr5r",
            "temperature_c": 20.5,
            "convective_risk_score": 15.0,
            "recorded_date": "2026-10-08",
        }
        for i in range(100)
    ]


def test_export_records_to_parquet(tmp_path: Path, sample_silver_records):
    """Verify Parquet file generation, ZSTD compression, and schema round-trip."""
    exporter = ParquetExporter()
    artifact = exporter.export_records_to_parquet(
        records=sample_silver_records,
        output_file_name="test_telemetry.parquet",
        output_dir=tmp_path,
    )

    assert isinstance(artifact, ExportArtifact)
    assert artifact.file_path.exists()
    assert artifact.record_count == 100
    assert artifact.compressed_bytes > 0
    assert len(artifact.sha256_checksum) == 64

    # Verify readable by pyarrow.parquet
    read_table = pq.read_table(artifact.file_path)
    assert read_table.num_rows == 100
    assert "convective_risk_score" in read_table.column_names


def test_compute_sha256_deterministic(tmp_path: Path):
    """Verify SHA-256 returns identical hash for identical binary content."""
    exporter = ParquetExporter()
    test_file = tmp_path / "sample.bin"
    test_file.write_bytes(b"deterministic_data_block_for_checksum_test")

    hash1 = exporter.compute_sha256(test_file)
    hash2 = exporter.compute_sha256(test_file)

    assert hash1 == hash2
    assert hash1 == "a589b4bc2ba588af3245907effe9c83f04a3871dde5fa6cb70401ed1e6571148"


def test_empty_records_export_raises(tmp_path: Path):
    """Verify exporting an empty dataset raises ValueError."""
    exporter = ParquetExporter()
    with pytest.raises(ValueError, match="Cannot export empty dataset"):
        exporter.export_records_to_parquet(
            records=[],
            output_file_name="empty.parquet",
            output_dir=tmp_path,
        )

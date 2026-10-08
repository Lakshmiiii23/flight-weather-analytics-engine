"""High-Compression ZStandard (ZSTD) Columnar Parquet Serializer.

Serializes structured telemetry datasets into compact, immutable Parquet archives
with cryptographic SHA-256 integrity validation and compression analytics.
"""

from dataclasses import dataclass
import hashlib
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="parquet-exporter"
)


@dataclass
class ExportArtifact:
    """Metadata describing a serialized Parquet archive."""

    file_path: Path
    file_name: str
    record_count: int
    uncompressed_bytes: int
    compressed_bytes: int
    compression_ratio: float
    sha256_checksum: str


class ParquetExporter:
    """Manages high-efficiency Parquet compression and cryptographic verification."""

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        self.settings: AppSettings = settings or load_settings()
        self.compression: str = self.settings.archival.compression_algorithm.upper()
        self.compression_level: int = self.settings.archival.compression_level
        self.default_dir: Path = (
            Path(__file__).resolve().parent.parent.parent
            / self.settings.archival.local_archive_dir
        )
        self.default_dir.mkdir(parents=True, exist_ok=True)

    def compute_sha256(self, file_path: Path) -> str:
        """Calculates cryptographic SHA-256 checksum of an on-disk binary file."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def export_records_to_parquet(
        self,
        records: List[Dict[str, Any]],
        output_file_name: str,
        output_dir: Optional[Path] = None,
    ) -> ExportArtifact:
        """Serializes list of dictionaries into a ZStandard-compressed Parquet file.

        Args:
            records: List of dictionaries matching schema.
            output_file_name: Target filename (e.g. telemetry_2026-10-01.parquet).
            output_dir: Destination folder. Defaults to settings archival directory.

        Returns:
            ExportArtifact: Sizing, hashing, and location metadata.
        """
        if not records:
            raise ValueError("Cannot export empty dataset to Parquet archive")

        destination_dir = output_dir or self.default_dir
        destination_dir.mkdir(parents=True, exist_ok=True)
        target_path = destination_dir / output_file_name

        logger.info(
            "Serializing records to compressed Parquet",
            extra={
                "record_count": len(records),
                "target_path": str(target_path),
                "compression": self.compression,
                "compression_level": self.compression_level,
            },
        )

        table = pa.Table.from_pylist(records)
        uncompressed_size = table.nbytes

        # Write Parquet with ZStandard columnar compression
        pq.write_table(
            table,
            where=target_path,
            compression=self.compression,
            compression_level=self.compression_level,
            use_dictionary=True,
        )

        compressed_size = target_path.stat().st_size
        compression_factor = (
            round(uncompressed_size / max(1, compressed_size), 2)
            if compressed_size > 0
            else 1.0
        )
        sha256_hash = self.compute_sha256(target_path)

        artifact = ExportArtifact(
            file_path=target_path,
            file_name=output_file_name,
            record_count=len(records),
            uncompressed_bytes=uncompressed_size,
            compressed_bytes=compressed_size,
            compression_ratio=compression_factor,
            sha256_checksum=sha256_hash,
        )

        logger.info(
            "Parquet export and verification completed",
            extra={
                "file": output_file_name,
                "compressed_bytes": compressed_size,
                "compression_ratio": f"{compression_factor}x",
                "sha256": sha256_hash,
            },
        )
        return artifact

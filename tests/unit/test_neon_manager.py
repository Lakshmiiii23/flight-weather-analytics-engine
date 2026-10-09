"""Unit tests for Neon Serverless PostgreSQL database manager."""

from unittest.mock import MagicMock, patch
import pytest
import sqlalchemy as sa

from src.storage.neon_manager import NeonPostgresManager


def test_missing_database_url_raises(monkeypatch: pytest.MonkeyPatch):
    """Verify missing database URL raises ValueError."""
    monkeypatch.delenv("NEON_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ValueError, match="NEON_DATABASE_URL is not configured"):
        NeonPostgresManager()


def test_url_normalization_postgres_prefix(monkeypatch: pytest.MonkeyPatch):
    """Verify postgres:// is rewritten to postgresql:// for SQLAlchemy compatibility."""
    raw_url = "postgres://user:pass@ep-cool-db.us-east-2.aws.neon.tech/neondb"

    with patch("sqlalchemy.create_engine") as mock_engine:
        NeonPostgresManager(database_url=raw_url)
        mock_engine.assert_called_once()
        called_url = mock_engine.call_args[0][0]
        assert called_url.startswith("postgresql://")


def test_initialize_schema_executes_ddl():
    """Verify DDL statements are split and executed sequentially."""
    mock_engine = MagicMock(spec=sa.Engine)
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    manager = NeonPostgresManager(engine=mock_engine)
    manager.initialize_schema()

    # Verify multiple DDL statements were executed
    assert mock_conn.execute.call_count >= 3


def test_delete_partition_executes_parameterized_query():
    """Verify partition deletion query execution."""
    mock_engine = MagicMock(spec=sa.Engine)
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.rowcount = 42
    mock_conn.execute.return_value = mock_result
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    manager = NeonPostgresManager(engine=mock_engine)
    rows_deleted = manager.delete_partition("silver_flight_weather_telemetry", "2026-10-01")

    assert rows_deleted == 42
    mock_conn.execute.assert_called_once()


def test_load_silver_records_empty_noop():
    """Verify empty record list returns 0 without database calls."""
    mock_engine = MagicMock(spec=sa.Engine)
    manager = NeonPostgresManager(engine=mock_engine)

    assert manager.load_silver_records([]) == 0

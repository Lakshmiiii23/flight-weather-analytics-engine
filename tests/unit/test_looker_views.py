"""Unit tests for Looker Studio SQL reporting views."""

from pathlib import Path
import pytest


@pytest.fixture
def looker_sql_path() -> Path:
    base_dir = Path(__file__).resolve().parent.parent.parent
    return base_dir / "sql" / "looker_views" / "looker_studio_views.sql"


def test_looker_views_syntax_and_tables(looker_sql_path: Path):
    """Verify Looker Studio views script contains all required reporting views and partition filters."""
    assert looker_sql_path.exists(), "looker_studio_views.sql must exist"

    sql_content = looker_sql_path.read_text(encoding="utf-8")
    sql_upper = sql_content.upper()

    assert "V_LOOKER_REALTIME_HOTSPOTS" in sql_upper
    assert "V_LOOKER_HOURLY_DELAY_TRENDS" in sql_upper
    assert "V_LOOKER_ARCHIVAL_AUDIT_TRAIL" in sql_upper

    # Verify partition boundary filter (crucial for free tier scan reduction)
    assert "RECORDED_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)" in sql_upper

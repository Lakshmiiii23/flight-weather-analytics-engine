"""Unit tests for Webhook alert dispatcher module."""

from unittest.mock import MagicMock
import pytest
import requests

from config.settings import AppSettings
from src.monitoring.webhook_alerts import WebhookAlertDispatcher


@pytest.fixture
def mock_session() -> MagicMock:
    session = MagicMock(spec=requests.Session)
    mock_resp = MagicMock()
    mock_resp.status_code = 204
    mock_resp.raise_for_status.return_value = None
    session.post.return_value = mock_resp
    return session


def test_send_heartbeat(mock_session: MagicMock):
    """Verify heartbeat builds valid Discord embed and dispatches POST."""
    dispatcher = WebhookAlertDispatcher(
        webhook_url="https://discord.com/api/webhooks/mocked",
        session=mock_session,
    )

    metrics = {
        "iteration_id": "test-uuid-123",
        "raw_flights": 100,
        "valid_flights": 98,
        "unique_weather_grids": 45,
        "records_committed": 98,
        "duration_seconds": 4.5,
        "status": "SUCCESS",
    }

    result = dispatcher.send_heartbeat(metrics)
    assert result is True
    mock_session.post.assert_called_once()
    payload = mock_session.post.call_args[1]["json"]
    assert "embeds" in payload
    assert payload["embeds"][0]["title"] == "🟢 Flight-Weather Pipeline Heartbeat"


def test_send_failure_alert(mock_session: MagicMock):
    """Verify failure alert builds red embed with error diagnostics."""
    dispatcher = WebhookAlertDispatcher(
        webhook_url="https://discord.com/api/webhooks/mocked",
        session=mock_session,
    )

    result = dispatcher.send_failure_alert(
        error_message="Connection timed out to OpenSky",
        error_type="TimeoutError",
        iteration_id="test-iter-456",
        traceback_snippet="Traceback: line 42 in fetch_live_states",
    )
    assert result is True
    payload = mock_session.post.call_args[1]["json"]
    assert payload["embeds"][0]["color"] == 15158332  # Red


def test_send_archival_report(mock_session: MagicMock):
    """Verify archival report serializes metrics and destination URI."""
    dispatcher = WebhookAlertDispatcher(
        webhook_url="https://discord.com/api/webhooks/mocked",
        session=mock_session,
    )

    manifest = {
        "target_partition_date": "2026-10-01",
        "record_count_evicted": 5000,
        "compressed_parquet_bytes": 1048576,
        "compression_ratio": 7.5,
        "storage_destination_uri": "r2://cold-bucket/test.parquet",
        "sha256_checksum": "a589b4bc2ba588af3245907effe9c83f04a3871dde5fa6cb70401ed1e6571148",
    }

    result = dispatcher.send_archival_report(manifest)
    assert result is True
    payload = mock_session.post.call_args[1]["json"]
    assert "Cloud Archival & Eviction Complete" in payload["embeds"][0]["title"]


def test_unconfigured_webhook_suppresses_dispatch(mock_session: MagicMock):
    """Verify missing webhook URL gracefully suppresses network calls."""
    dispatcher = WebhookAlertDispatcher(webhook_url="", session=mock_session)
    result = dispatcher.send_heartbeat({"raw_flights": 10})
    assert result is False
    mock_session.post.assert_not_called()


def test_discord_webhook_url_env_resolution(monkeypatch: pytest.MonkeyPatch):
    """Verify WebhookAlertDispatcher respects DISCORD_WEBHOOK_URL environment variable."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/test-discord-env")
    dispatcher = WebhookAlertDispatcher()
    assert dispatcher.webhook_url == "https://discord.com/api/webhooks/test-discord-env"
    assert dispatcher.enabled is True


def test_send_discord_success_alert(monkeypatch: pytest.MonkeyPatch):
    """Verify send_discord_success_alert sends markdown and embed with silver and gold stats."""
    from unittest.mock import patch
    from scripts.sync_to_neon import send_discord_success_alert

    with patch("requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        success = send_discord_success_alert(
            webhook_url="https://discord.com/api/webhooks/mocked",
            timestamp="2026-10-10 12:00:00 UTC",
            total_silver=1023,
            total_gold=440,
            duration_seconds=12.45,
        )

        assert success is True
        mock_post.assert_called_once()
        payload = mock_post.call_args[1]["json"]
        assert "🟢 **Flight-Weather Pipeline Success**" in payload["content"]
        assert "1,023" in payload["content"]
        assert "440" in payload["content"]
        assert payload["embeds"][0]["color"] == 3066993


def test_send_discord_failure_alert(monkeypatch: pytest.MonkeyPatch):
    """Verify send_discord_failure_alert sends red alert warning with traceback."""
    from unittest.mock import patch
    from scripts.sync_to_neon import send_discord_failure_alert

    with patch("requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp

        err = requests.exceptions.HTTPError("429 Client Error: Too Many Requests")
        success = send_discord_failure_alert(
            webhook_url="https://discord.com/api/webhooks/mocked",
            error=err,
            traceback_str="Traceback (most recent call last):\n  File 'test.py', line 10\n    raise err",
            timestamp="2026-10-10 12:00:00 UTC",
        )

        assert success is True
        mock_post.assert_called_once()
        payload = mock_post.call_args[1]["json"]
        assert "❌ **Flight Ingestion Daemon Failure Alert**" in payload["content"]
        assert "HTTPError" in payload["content"]
        assert payload["embeds"][0]["color"] == 15158332


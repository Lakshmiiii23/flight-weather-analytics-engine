"""Unit tests for OpenSky Network ingestion client using mocked HTTP adapters."""

from typing import Any, Dict
import pytest
import requests
from requests.exceptions import HTTPError, Timeout

from config.settings import AppSettings, BoundingBoxCoordinates
from src.ingestion.opensky_client import OpenSkyClient, is_transient_network_error


@pytest.fixture
def sample_opensky_payload() -> Dict[str, Any]:
    return {
        "time": 1718000000,
        "states": [
            [
                "a1b2c3", "UAL123  ", "United States", 1718000000, 1718000000,
                -73.9851, 40.7488, 10500.0, False, 240.5, 90.0, 0.0,
                None, 10600.0, "1200", False, 0
            ],
            [
                "d4e5f6", "DAL456  ", "United States", 1718000000, 1718000000,
                None, None, 500.0, True, 0.0, 0.0, 0.0,
                None, 520.0, "7000", False, 0
            ],
            [
                "short", "INVAL"
            ]
        ]
    }


def test_fetch_live_states_parsing(monkeypatch: pytest.MonkeyPatch, sample_opensky_payload: Dict[str, Any]):
    """Verify state vector cleaning, callsign trimming, and filtering out null coordinates."""
    client = OpenSkyClient()

    class MockResponse:
        status_code = 200
        def json(self):
            return sample_opensky_payload
        def raise_for_status(self):
            pass

    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: MockResponse())

    records = client.fetch_live_states()
    assert len(records) == 1  # Only 1 of 3 rows had complete lat/lon & >=17 elements

    valid_record = records[0]
    assert valid_record["icao24"] == "a1b2c3"
    assert valid_record["callsign"] == "UAL123"  # Trailing whitespace stripped
    assert valid_record["latitude"] == 40.7488
    assert valid_record["longitude"] == -73.9851
    assert valid_record["velocity_mps"] == 240.5
    assert valid_record["on_ground"] is False


def test_transient_error_detection():
    """Verify is_transient_network_error accurately classifies retryable HTTP codes."""
    assert is_transient_network_error(Timeout("Timed out")) is True
    
    response_429 = requests.Response()
    response_429.status_code = 429
    assert is_transient_network_error(HTTPError(response=response_429)) is True

    response_503 = requests.Response()
    response_503.status_code = 503
    assert is_transient_network_error(HTTPError(response=response_503)) is True

    response_404 = requests.Response()
    response_404.status_code = 404
    assert is_transient_network_error(HTTPError(response=response_404)) is False


def test_empty_payload_handling(monkeypatch: pytest.MonkeyPatch):
    """Verify client gracefully handles null states."""
    client = OpenSkyClient()

    class MockEmptyResponse:
        status_code = 200
        def json(self):
            return {"time": 1718000000, "states": None}
        def raise_for_status(self):
            pass

    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: MockEmptyResponse())
    records = client.fetch_live_states()
    assert records == []


def test_custom_bounding_box_passed(monkeypatch: pytest.MonkeyPatch):
    """Verify custom bounding box parameters are correctly serialized in query params."""
    client = OpenSkyClient()
    captured_params = {}

    def mock_get(url, params, **kwargs):
        nonlocal captured_params
        captured_params = params
        resp = requests.Response()
        resp.status_code = 200
        resp._content = b'{"time": 1718000000, "states": []}'
        return resp

    monkeypatch.setattr(client.session, "get", mock_get)

    custom_box = BoundingBoxCoordinates(lamin=10.0, lamax=20.0, lomin=30.0, lomax=40.0)
    client.fetch_live_states(custom_box=custom_box)

    assert captured_params["lamin"] == 10.0
    assert captured_params["lamax"] == 20.0
    assert captured_params["lomin"] == 30.0
    assert captured_params["lomax"] == 40.0

"""Unit tests for Open-Meteo batch weather ingestion client."""

from typing import Any, Dict, List, Tuple
import pytest
import requests

from config.settings import AppSettings
from src.ingestion.weather_client import OpenMeteoClient


@pytest.fixture
def sample_weather_response_list() -> List[Dict[str, Any]]:
    return [
        {
            "latitude": 40.7,
            "longitude": -74.0,
            "current": {
                "time": "2026-10-08T16:00",
                "temperature_2m": 18.5,
                "relative_humidity_2m": 65.0,
                "precipitation": 1.2,
                "wind_speed_10m": 8.4,
                "wind_direction_10m": 240.0,
                "wind_gusts_10m": 14.1,
                "cloud_cover": 75.0,
                "visibility": 10000.0,
            },
        },
        {
            "latitude": 41.2,
            "longitude": -73.5,
            "current": {
                "time": "2026-10-08T16:00",
                "temperature_2m": 16.0,
                "relative_humidity_2m": 80.0,
                "precipitation": 0.0,
                "wind_speed_10m": 4.2,
                "wind_direction_10m": 180.0,
                "wind_gusts_10m": 6.5,
                "cloud_cover": 20.0,
                "visibility": 15000.0,
            },
        },
    ]


def test_fetch_batch_weather_mapping(
    monkeypatch: pytest.MonkeyPatch, sample_weather_response_list: List[Dict[str, Any]]
):
    """Verify batch request parses multiple centroid weather results and maps by geohash."""
    client = OpenMeteoClient()

    class MockBatchResponse:
        status_code = 200
        def json(self):
            return sample_weather_response_list
        def raise_for_status(self):
            pass

    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: MockBatchResponse())

    centroids: List[Tuple[float, float, str]] = [
        (40.7, -74.0, "dr5r"),
        (41.2, -73.5, "dr72"),
    ]

    weather_map = client.fetch_batch_weather(centroids)
    assert len(weather_map) == 2
    assert "dr5r" in weather_map
    assert "dr72" in weather_map

    ny_weather = weather_map["dr5r"]
    assert ny_weather["temperature_c"] == 18.5
    assert ny_weather["precipitation_mm"] == 1.2
    assert ny_weather["wind_speed_mps"] == 8.4
    assert ny_weather["cloud_cover_pct"] == 75.0


def test_single_location_dict_response(monkeypatch: pytest.MonkeyPatch):
    """Verify client handles single dict return when querying only 1 location."""
    client = OpenMeteoClient()

    single_response = {
        "latitude": 40.7,
        "longitude": -74.0,
        "current": {
            "time": "2026-10-08T16:00",
            "temperature_2m": 22.0,
            "relative_humidity_2m": 50.0,
            "precipitation": 0.0,
            "wind_speed_10m": 5.0,
            "wind_direction_10m": 90.0,
            "wind_gusts_10m": 7.0,
            "cloud_cover": 10.0,
            "visibility": 20000.0,
        },
    }

    class MockSingleResponse:
        status_code = 200
        def json(self):
            return single_response
        def raise_for_status(self):
            pass

    monkeypatch.setattr(client.session, "get", lambda *args, **kwargs: MockSingleResponse())

    weather_map = client.fetch_batch_weather([(40.7, -74.0, "dr5r")])
    assert len(weather_map) == 1
    assert weather_map["dr5r"]["temperature_c"] == 22.0


def test_chunking_oversized_centroid_list(monkeypatch: pytest.MonkeyPatch):
    """Verify centroid lists larger than batch_chunk_size are cleanly divided into multiple calls."""
    settings = AppSettings()
    settings.weather.batch_chunk_size = 2  # Artificially low chunk size for test
    client = OpenMeteoClient(settings=settings)

    call_count = 0

    def mock_get(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        resp = requests.Response()
        resp.status_code = 200
        resp._content = b'[{"current": {"temperature_2m": 20.0}}, {"current": {"temperature_2m": 21.0}}]'
        return resp

    monkeypatch.setattr(client.session, "get", mock_get)

    centroids = [
        (40.0, -74.0, "h1"),
        (40.1, -74.1, "h2"),
        (40.2, -74.2, "h3"),
        (40.3, -74.3, "h4"),
        (40.4, -74.4, "h5"),
    ]
    # 5 items with chunk size 2 = 3 requests
    weather_map = client.fetch_batch_weather(centroids)
    assert call_count == 3
    assert len(weather_map) == 5

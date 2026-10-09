"""Unit tests for Streamlit & PyDeck dashboard helper functions and rendering logic."""

import pandas as pd
import pydeck as pdk
import pytest

from unittest.mock import patch, MagicMock

from src.dashboard.app import (
    build_pydeck_3d_map,
    calculate_risk_color,
    categorize_hazard,
    is_sync_worker_alive,
    start_background_sync_worker,
)



def test_calculate_risk_color():
    """Verify color mapping across risk thresholds."""
    green = calculate_risk_color(10.0)
    assert green[0] == 46 and green[1] == 204  # Green

    yellow = calculate_risk_color(30.0)
    assert yellow[0] == 241 and yellow[1] == 196  # Yellow

    orange = calculate_risk_color(55.0)
    assert orange[0] == 230 and orange[1] == 126  # Orange

    red = calculate_risk_color(85.0)
    assert red[0] == 231 and red[1] == 76  # Red


def test_categorize_hazard_rules():
    """Verify hazard categorization classification logic."""
    severe_record = {
        "convective_risk_score": 75.0,
        "precipitation_mm": 4.0,
        "wind_gusts_mps": 25.0,
    }
    assert categorize_hazard(severe_record) == "SEVERE_STORM"

    wind_shear_record = {
        "convective_risk_score": 30.0,
        "wind_speed_mps": 16.0,
        "wind_gusts_mps": 19.0,
    }
    assert categorize_hazard(wind_shear_record) == "HIGH_WIND_SHEAR"

    rain_record = {
        "convective_risk_score": 15.0,
        "precipitation_mm": 1.2,
        "wind_speed_mps": 5.0,
        "wind_gusts_mps": 8.0,
    }
    assert categorize_hazard(rain_record) == "MODERATE_RAIN"

    clear_record = {
        "convective_risk_score": 5.0,
        "precipitation_mm": 0.0,
        "wind_speed_mps": 4.0,
        "wind_gusts_mps": 6.0,
        "visibility_m": 15000.0,
    }
    assert categorize_hazard(clear_record) == "CLEAR"


def test_build_pydeck_3d_map_structure():
    """Verify PyDeck map object generation with ColumnLayer and ScatterplotLayer."""
    df = pd.DataFrame([
        {
            "latitude": 40.7128,
            "longitude": -74.0060,
            "baro_altitude_m": 10000.0,
            "velocity_mps": 240.0,
            "callsign": "AAL100",
            "icao24": "a1b2c3",
            "convective_risk_score": 25.0,
            "hazard_category": "CLEAR",
            "precipitation_mm": 0.0,
            "wind_gusts_mps": 8.0,
            "velocity_deficit_pct": 5.0,
            "color": [46, 204, 113, 180],
        }
    ])

    deck = build_pydeck_3d_map(df)
    assert isinstance(deck, pdk.Deck)
    assert len(deck.layers) == 2
    assert deck.layers[0].type == "ScatterplotLayer"
    assert deck.layers[1].type == "ColumnLayer"


def test_build_pydeck_3d_map_empty_df():
    """Verify empty DataFrame safely returns empty PyDeck object without crashing."""
    deck = build_pydeck_3d_map(pd.DataFrame())
    assert isinstance(deck, pdk.Deck)
    assert len(deck.layers) == 0


def test_start_background_sync_worker_idempotence():
    """Verify background worker launch returns boolean and handles double invocation safely."""
    with patch("threading.Thread") as mock_thread_cls:
        mock_thread = MagicMock()
        mock_thread_cls.return_value = mock_thread

        # First invocation starts the thread
        res1 = start_background_sync_worker(interval_seconds=600)
        assert res1 is True

        # Second invocation is an idempotent no-op
        res2 = start_background_sync_worker(interval_seconds=600)
        assert res2 is True


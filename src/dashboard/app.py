"""Interactive Streamlit & PyDeck 3D Flight-Weather Analytics Web Dashboard.

Provides real-time 3D flight vector rendering, localized atmospheric hazard overlays,
velocity deficit KPIs, and cryptographic archival audit lineage.
"""

from datetime import datetime, timezone
import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import pydeck as pdk
import streamlit as st

# Ensure repository root is on sys.path
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config.logging_config import setup_logging
from config.settings import AppSettings, load_settings
from src.ingestion.opensky_client import OpenSkyClient
from src.ingestion.weather_client import OpenMeteoClient
from src.processing.spatial_join import SpatialCorrelationEngine
from src.processing.validation import validate_flight_records
from src.storage.bigquery_manager import BigQueryManager

logger: logging.Logger = setup_logging(
    log_level="INFO", service_name="analytics-dashboard-ui"
)


def calculate_risk_color(score: float) -> List[int]:
    """Maps convective risk score [0-100] to RGBA color spectrum.

    Green (Safe) -> Yellow (Moderate) -> Orange (High) -> Red (Severe)
    """
    if score >= 70.0:
        return [231, 76, 60, 220]   # Red
    elif score >= 45.0:
        return [230, 126, 34, 210]  # Orange
    elif score >= 20.0:
        return [241, 196, 15, 200]  # Yellow
    else:
        return [46, 204, 113, 180]  # Green


def categorize_hazard(record: Dict[str, Any]) -> str:
    """Classifies weather hazard category for a single record."""
    risk = float(record.get("convective_risk_score") or 0.0)
    precip = float(record.get("precipitation_mm") or 0.0)
    gusts = float(record.get("wind_gusts_mps") or 0.0)
    wind = float(record.get("wind_speed_mps") or 0.0)
    vis = record.get("visibility_m")

    if risk >= 60.0 or precip >= 3.0 or gusts >= 22.0:
        return "SEVERE_STORM"
    elif wind >= 14.0 or gusts >= 18.0:
        return "HIGH_WIND_SHEAR"
    elif precip >= 0.5:
        return "MODERATE_RAIN"
    elif vis is not None and float(vis) < 3000.0:
        return "LOW_VISIBILITY"
    return "CLEAR"


def fetch_live_stream_snapshot(
    settings: AppSettings,
    opensky_client: OpenSkyClient,
    weather_client: OpenMeteoClient,
    spatial_engine: SpatialCorrelationEngine,
) -> pd.DataFrame:
    """Fetches a real-time live flight and weather snapshot in memory."""
    raw_flights = opensky_client.fetch_live_states()
    if not raw_flights:
        return pd.DataFrame()

    valid_flights, _ = validate_flight_records(raw_flights)
    valid_dicts = [f.model_dump() for f in valid_flights]
    tagged, centroids = spatial_engine.extract_spatial_centroids(valid_dicts)
    weather_obs = weather_client.fetch_batch_weather(centroids)
    silver_records = spatial_engine.execute_spatial_join(tagged, weather_obs)

    df = pd.DataFrame(silver_records)
    if not df.empty:
        df["color"] = df["convective_risk_score"].apply(calculate_risk_color)
        df["hazard_category"] = df.apply(categorize_hazard, axis=1)
        # Calculate velocity deficit vs 230 m/s commercial cruising baseline
        df["velocity_deficit_pct"] = df["velocity_mps"].apply(
            lambda v: round(((230.0 - v) / 230.0) * 100.0, 2) if v and v > 0 else 0.0
        )
    return df


def fetch_bigquery_lakehouse_data(settings: AppSettings) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Queries BigQuery Silver telemetry and Audit logs tables."""
    bq_manager = BigQueryManager(settings=settings)
    silver_table = f"{bq_manager.project_id}.{bq_manager.dataset_id}.{settings.bigquery_tables.silver_table}"
    audit_table = f"{bq_manager.project_id}.{bq_manager.dataset_id}.{settings.bigquery_tables.audit_table}"

    silver_query = f"""
    SELECT *
    FROM `{silver_table}`
    WHERE recorded_date = CURRENT_DATE()
    ORDER BY weather_recorded_at DESC
    LIMIT 2000;
    """

    audit_query = f"""
    SELECT *
    FROM `{audit_table}`
    ORDER BY archival_timestamp DESC
    LIMIT 50;
    """

    try:
        silver_rows = [dict(row) for row in bq_manager.client.query(silver_query)]
        audit_rows = [dict(row) for row in bq_manager.client.query(audit_query)]

        silver_df = pd.DataFrame(silver_rows)
        audit_df = pd.DataFrame(audit_rows)

        if not silver_df.empty:
            silver_df["color"] = silver_df["convective_risk_score"].apply(calculate_risk_color)
            silver_df["hazard_category"] = silver_df.apply(categorize_hazard, axis=1)
            silver_df["velocity_deficit_pct"] = silver_df["velocity_mps"].apply(
                lambda v: round(((230.0 - v) / 230.0) * 100.0, 2) if v and v > 0 else 0.0
            )

        return silver_df, audit_df
    except Exception as exc:
        logger.warning("BigQuery cloud query failed; fallback will be used", exc_info=True)
        return pd.DataFrame(), pd.DataFrame()


def fetch_neon_lakehouse_data(settings: AppSettings) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Queries Neon PostgreSQL silver telemetry and audit logs tables."""
    try:
        from src.storage.neon_manager import NeonPostgresManager
        neon_mgr = NeonPostgresManager(settings=settings)
        silver_rows = neon_mgr.query_records(
            "SELECT * FROM silver_flight_weather_telemetry ORDER BY weather_recorded_at DESC LIMIT 2000;"
        )
        audit_rows = neon_mgr.query_records(
            "SELECT * FROM cloud_archive_audit_logs ORDER BY archival_timestamp DESC LIMIT 50;"
        )
        silver_df = pd.DataFrame(silver_rows)
        audit_df = pd.DataFrame(audit_rows)

        if not silver_df.empty:
            silver_df["color"] = silver_df["convective_risk_score"].apply(calculate_risk_color)
            silver_df["hazard_category"] = silver_df.apply(categorize_hazard, axis=1)
            silver_df["velocity_deficit_pct"] = silver_df["velocity_mps"].apply(
                lambda v: round(((230.0 - v) / 230.0) * 100.0, 2) if v and v > 0 else 0.0
            )

        return silver_df, audit_df
    except Exception as exc:
        logger.warning("Neon cloud query failed; fallback will be used", exc_info=True)
        return pd.DataFrame(), pd.DataFrame()


def build_pydeck_3d_map(df: pd.DataFrame) -> pdk.Deck:
    """Builds interactive 3D PyDeck visualization of aircraft vectors and weather cells."""
    if df.empty:
        return pdk.Deck()

    mean_lat = float(df["latitude"].mean())
    mean_lon = float(df["longitude"].mean())

    # Layer 1: 3D Aircraft Columns (Height = Barometric Altitude in meters)
    column_layer = pdk.Layer(
        "ColumnLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_elevation="baro_altitude_m",
        elevation_scale=1.0,
        radius=4000,
        get_fill_color="color",
        pickable=True,
        auto_highlight=True,
    )

    # Layer 2: Surface Scatter Circles (Radar footprint)
    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["longitude", "latitude"],
        get_radius=8000,
        get_fill_color="color",
        opacity=0.3,
        pickable=False,
    )

    view_state = pdk.ViewState(
        latitude=mean_lat,
        longitude=mean_lon,
        zoom=5.5,
        pitch=45,
        bearing=0,
    )

    tooltip = {
        "html": """
        <b>Flight:</b> {callsign} (ICAO: <code>{icao24}</code>)<br/>
        <b>Altitude:</b> {baro_altitude_m} m | <b>Speed:</b> {velocity_mps} m/s<br/>
        <b>Hazard Tier:</b> {hazard_category}<br/>
        <b>Convective Risk:</b> {convective_risk_score} / 100<br/>
        <b>Precipitation:</b> {precipitation_mm} mm | <b>Wind Gusts:</b> {wind_gusts_mps} m/s<br/>
        <b>Speed Deficit:</b> {velocity_deficit_pct}%
        """,
        "style": {"backgroundColor": "#1e272e", "color": "white", "fontSize": "12px", "padding": "8px"},
    }

    return pdk.Deck(
        layers=[scatter_layer, column_layer],
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="mapbox://styles/mapbox/dark-v10",
    )


def main() -> None:
    """Main Streamlit application layout and event handlers."""
    st.set_page_config(
        page_title="Flight-Weather Lakehouse Analytics",
        page_icon="🛫",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    settings: AppSettings = load_settings()
    opensky_client = OpenSkyClient(settings=settings)
    weather_client = OpenMeteoClient(settings=settings)
    spatial_engine = SpatialCorrelationEngine(settings=settings)

    # Sidebar Controls
    st.sidebar.title("🛫 Engine Controls")
    st.sidebar.caption("GCP Always-Free Lakehouse Monitor")

    data_source_mode = st.sidebar.radio(
        "Data Source Mode",
        ["Live Stream (Real-Time API)", "Neon Serverless PostgreSQL", "BigQuery Cloud Lakehouse"],
    )

    convective_filter = st.sidebar.slider(
        "Minimum Convective Hazard Risk", min_value=0.0, max_value=100.0, value=0.0, step=5.0
    )

    altitude_range = st.sidebar.slider(
        "Altitude Range (Meters)", min_value=0, max_value=14000, value=(0, 14000), step=500
    )

    refresh_btn = st.sidebar.button("🔄 Refresh Telemetry Snapshot")

    # Main Dashboard Header
    st.title("🛰️ Global Flight Delays & Weather Correlation Engine")
    st.markdown(
        "Real-time stream correlation between live **ADS-B transponder vectors** and **localized convective weather**, "
        "operating strictly inside **GCP Always-Free Tier** with **Zero-Dollar Archival**."
    )

    # Data Retrieval
    with st.spinner("Streaming & correlating live telemetry..."):
        if data_source_mode == "Neon Serverless PostgreSQL":
            df, audit_df = fetch_neon_lakehouse_data(settings)
            if df.empty:
                st.info("No records found in Neon PostgreSQL. Falling back to real-time live API stream.")
                df = fetch_live_stream_snapshot(settings, opensky_client, weather_client, spatial_engine)
                audit_df = pd.DataFrame()
        elif data_source_mode == "BigQuery Cloud Lakehouse":
            df, audit_df = fetch_bigquery_lakehouse_data(settings)
            if df.empty:
                st.info("No records found in active BigQuery partition. Falling back to real-time live API stream.")
                df = fetch_live_stream_snapshot(settings, opensky_client, weather_client, spatial_engine)
                audit_df = pd.DataFrame()
        else:
            df = fetch_live_stream_snapshot(settings, opensky_client, weather_client, spatial_engine)
            audit_df = pd.DataFrame()

    if df.empty:
        st.warning("No active aircraft detected within selected corridor bounds.")
        return

    # Apply Filters
    filtered_df = df[
        (df["convective_risk_score"] >= convective_filter)
        & (df["baro_altitude_m"].fillna(0) >= altitude_range[0])
        & (df["baro_altitude_m"].fillna(0) <= altitude_range[1])
    ].copy()

    # Row 1: KPI Metrics
    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)
    with kpi_col1:
        st.metric("Total Active Aircraft", f"{len(filtered_df):,}")
    with kpi_col2:
        avg_speed = filtered_df["velocity_mps"].dropna().mean() if not filtered_df.empty else 0.0
        st.metric("Avg Ground Speed", f"{avg_speed:.1f} m/s", f"{avg_speed * 1.94384:.0f} knots")
    with kpi_col3:
        avg_deficit = filtered_df["velocity_deficit_pct"].dropna().mean() if not filtered_df.empty else 0.0
        st.metric("Avg Speed Deficit", f"{avg_deficit:.1f}%")
    with kpi_col4:
        severe_count = len(filtered_df[filtered_df["convective_risk_score"] >= 50.0])
        st.metric("Severe Weather Encounters", f"{severe_count}")
    with kpi_col5:
        unique_grids = filtered_df["geohash"].nunique()
        st.metric("Active Weather Grids", f"{unique_grids}")

    st.divider()

    # Row 2: 3D PyDeck Map
    st.subheader("🌐 3D Spatial Vector & Weather Radar Map")
    st.caption("Pillars represent aircraft altitude (meters). Color indicates convective atmospheric risk (Green = Safe, Red = Severe Storm).")
    deck_map = build_pydeck_3d_map(filtered_df)
    st.pydeck_chart(deck_map)

    st.divider()

    # Row 3: Analytical Breakdown
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("⛈️ Flight Density by Hazard Category")
        if "hazard_category" in filtered_df.columns:
            hazard_counts = filtered_df["hazard_category"].value_counts()
            st.bar_chart(hazard_counts)

    with col_right:
        st.subheader("💨 Wind Gusts vs. Airspeed Deficit")
        if not filtered_df.empty and "wind_gusts_mps" in filtered_df.columns:
            chart_data = filtered_df[["wind_gusts_mps", "velocity_deficit_pct"]].dropna()
            st.scatter_chart(chart_data, x="wind_gusts_mps", y="velocity_deficit_pct")

    # Row 4: Raw Telemetry Table
    with st.expander("🔍 Inspect Live Telemetry Stream Records", expanded=False):
        display_cols = [
            "callsign", "icao24", "origin_country", "latitude", "longitude",
            "baro_altitude_m", "velocity_mps", "hazard_category",
            "convective_risk_score", "precipitation_mm", "wind_gusts_mps", "geohash"
        ]
        available_cols = [c for c in display_cols if c in filtered_df.columns]
        st.dataframe(filtered_df[available_cols], width="stretch")

    # Row 5: Archival Audit Trail Tab
    if not audit_df.empty:
        st.divider()
        st.subheader("📜 Zero-Dollar Cloud Archival & Cryptographic Audit Ledger")
        st.caption("Verified immutable records of partitions evicted from BigQuery to Cloudflare R2.")
        st.dataframe(audit_df, width="stretch")


if __name__ == "__main__":
    main()

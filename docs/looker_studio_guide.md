# 📊 Google Looker Studio Free Dashboard Setup Guide

This guide walks through configuring a production-grade, 100% free interactive dashboard in **Google Looker Studio** connected directly to BigQuery without paying for hosting or BI tools.

---

## 🔗 Step 1: Connect BigQuery to Looker Studio

1. Open [Google Looker Studio](https://lookerstudio.google.com/).
2. Click **Create** > **Data Source**.
3. Select the **BigQuery** connector.
4. Select your:
   - **Project:** `flight-weather-free-tier` (or your GCP Project ID)
   - **Dataset:** `flight_weather_analytics`
   - **View / Table:** Select `v_looker_realtime_hotspots`
5. Click **Connect** (top-right).

---

## 🎨 Step 2: Build the Core Visualizations

### 1. Headline KPI Cards
Add 4 Scorecard components across the top header:
- **Card 1: Active Tracked Aircraft**
  - Metric: `total_active_flights` (Aggregation: `SUM`)
- **Card 2: Average Airspeed Deficit**
  - Metric: `velocity_deficit_pct` (Aggregation: `AVG`)
  - Target: Compact number with `%` suffix
- **Card 3: Aircraft in Severe Weather**
  - Metric: `flights_impacted_count` (Aggregation: `SUM`)
- **Card 4: Maximum Wind Gusts**
  - Metric: `peak_wind_gust_mps` (Aggregation: `MAX`)
  - Suffix: `m/s`

---

### 2. Geospatial Weather Hazard Map
1. Add a **Geo Map** / **Google Maps Bubble Chart**.
2. **Location Field:** Set to `geohash`.
3. **Size Metric:** Set to `flights_impacted_count`.
4. **Color Dimension:** Set to `weather_condition_category`.
   - Configure Color Palette:
     - `SEVERE_STORM` ➔ Red (`#E74C3C`)
     - `HIGH_WIND_SHEAR` ➔ Orange (`#E67E22`)
     - `MODERATE_RAIN` ➔ Blue (`#3498DB`)
     - `CLEAR` ➔ Green (`#2ECC71`)
5. **Tooltip:** `avg_velocity_mps`, `mean_convective_risk`.

---

### 3. Hourly Delay Trend Chart (Time-Series)
1. Add a **Time Series Chart**.
2. **Dimension:** `window_start`.
3. **Breakdown Dimension:** `weather_condition_category`.
4. **Metric:** `hourly_avg_deficit_pct` (from `v_looker_hourly_delay_trends`).
5. **Observation:** Shows how flight delays spike in tandem with convective storm arrival.

---

### 4. Zero-Dollar Archival Audit Table
1. Add a **Table** component at the bottom of the canvas.
2. Select Data Source: `v_looker_archival_audit_trail`.
3. **Dimensions:**
   - `target_partition_date`
   - `record_count_evicted`
   - `uncompressed_mb`
   - `compressed_parquet_mb`
   - `compression_ratio`
   - `sha256_checksum`
   - `eviction_status`
4. This serves as an executive proof-of-work table proving continuous zero-cost compliance.

---

## ⚙️ Step 3: Cache & Free-Tier Optimization

- In Looker Studio, open **Resource > Manage added data sources > Edit > Data Freshness**.
- Set Data Freshness to **Every 4 hours** or **Every 12 hours** to avoid continuous query executions against BigQuery, ensuring query scans stay below 2% of the free monthly 1 TB quota.

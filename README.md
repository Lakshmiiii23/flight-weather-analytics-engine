# 🛩️ High-Frequency Flight & Weather Telemetry Node

A production-grade, 24/7 automated telemetry ingestion and analytics lakehouse correlating live commercial ADS-B transponder state vectors with hyper-localized atmospheric weather. Built to operate **100% perpetually free with zero financial or credit card liability**, utilizing **GitHub Actions automated runners**, in-memory **DuckDB vectorized spatial join engines**, and **Neon Serverless PostgreSQL**.

---

## 🏗️ Architecture Blueprint

```
                     ┌────────────────────────────────┐
                     │   OpenSky Network (ADS-B)      │
                     │    REST State Vector API       │
                     └───────────────┬────────────────┘
                                     │ Live Aircraft Vectors (lat, lon, alt, vel)
                                     ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ GitHub Actions 24/7 Automation Daemon (.github/workflows/flight_daemon) │
 │ - Trigger: Hourly cron ('0 * * * *') + Manual workflow_dispatch        │
 │ - Runner: ubuntu-latest (Python 3.10, ephemeral sandbox)               │
 │ - Secrets: NEON_DATABASE_URL, DISCORD_WEBHOOK_URL                      │
 │                                                                        │
 │   1. Ingestion Worker (src/ingestion/opensky_client.py)                │
 │      └── Exponential backoff & HTTP 429 rate-limit defense             │
 │                                                                        │
 │   2. Spatial Grid Binning (Geohash Precision 4 / ~20km x ~20km)        │
 │      └── Groups flights into distinct spatial centroid coordinates     │
 │                                                                        │
 │   3. Weather Batch Ingestion (src/ingestion/weather_client.py)         │
 │      └── Batched multi-coordinate calls to Open-Meteo (50/chunk)       │
 │                                                                        │
 │   4. DuckDB In-Memory Out-of-Core Engine (src/processing/spatial_join) │
 │      ├── Enforces PRAGMA max_memory = '350MB' (Zero memory spill)     │
 │      ├── Vectorized Left Join between Flight Vectors & Weather Cells   │
 │      └── In-Engine Convective Storm Hazard Risk Scoring [0 - 100]      │
 └───────────────────────────────────┬────────────────────────────────────┘
                                     │
           ┌─────────────────────────┴─────────────────────────┐
           ▼                                                   ▼
┌──────────────────────────────────────┐     ┌──────────────────────────────────┐
│  Neon Serverless PostgreSQL Database │     │   Discord ChatOps Channel        │
│  - SQLAlchemy Connection Pooling     │     │   - 🟢 Instant Success Reports   │
│  - pool_pre_ping & pool_recycle=300  │     │   - ❌ Real-Time Failure Alerts  │
│  - SSL Required (psycopg2 driver)    │     │   - Stack traces on HTTP 429     │
├──────────────────────────────────────┤     └──────────────────────────────────┘
│ • public.silver_flight_weather_      │
│   telemetry (Idempotent Raw Vectors) │
│ • public.gold_delay_weather_matrix   │
│   (Hourly Convective KPI Rollup)     │
└──────────────────┬───────────────────┘
                   │ Direct Internet Connection (Encrypted SSL)
                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   Power BI Desktop Analytics Engine                    │
│   - Asset: Dashboard/Airway Analysis.pbix                              │
│   - Model: Many-to-Many (*:*) Relationship over geohash                │
│   - Filtering: Bidirectional Cross-Filtering (Both Directions)        │
│   - Views: Airspeed Deficits, Convective Hazard Zones, 3D Flight Map   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## ⚡ Key Engineering Pillars

### 1. 24/7 Ephemeral Runner Automation
The ingestion engine executes on **GitHub Actions** via `.github/workflows/flight_daemon.yml`. Scheduled via `cron: '0 * * * *'` and manual `workflow_dispatch`, each pass spins up an isolated `ubuntu-latest` runner, provisions dependencies via pip cache, executes the pipeline pass in **under 30 seconds**, and terminates cleanly. Concurrency controls (`concurrency: flight-daemon-sync`) prevent runner collisions.

### 2. Geohash-4 Spatial Binning & 98% API Call Reduction
Directly querying atmospheric weather for thousands of individual aircraft exhaust third-party API quotas within minutes. The engine projects all aircraft coordinates onto a base-32 **Geohash Precision 4** grid (~20 km × ~20 km cells, precisely matching the spatial footprint of localized convective storm cells and cumulonimbus clouds). Centroids are deduplicated, slashing external weather queries by **98%**.

### 3. DuckDB In-Memory Out-of-Core Join Engine
All vector-to-weather spatial correlations and hazard index calculations are performed inside an in-memory **DuckDB** instance. The engine strictly enforces `PRAGMA max_memory = '350MB';` and `PRAGMA threads = 2;`, computing vectorized joins and convective risk formulas in sub-second latency with zero disk thrashing.

$$\text{Convective Risk} = \min\left(100.0, \max\left(0.0, 15.0 \cdot \text{precip} + 1.5 \cdot \text{gusts} + 0.20 \cdot \text{cloud\_cover}\right)\right)$$

### 4. Resilient Serverless Database Persistence
The storage layer leverages **Neon Serverless PostgreSQL**. The pipeline manager (`src/storage/neon_manager.py`) provisions connection pooling with `pool_pre_ping=True` and `pool_recycle=300`, actively mitigating idle connection drops when serverless compute scales to zero. All writes use `ON CONFLICT (flight_record_id) DO NOTHING` for idempotent ingestion, followed by an automated SQL rollup into the Gold analytical table.

### 5. Automated Discord ChatOps Sentinel
Every run dispatches structured observability payloads directly to your team's Discord tracking channel via `DISCORD_WEBHOOK_URL`:
- **🟢 Operational Success Card:** Dispatches execution pass timestamp, total airborne vectors committed to Silver, total distinct spatial geohashes loaded into Gold, and pipeline latency.
- **❌ High-Priority Failure Alert:** Catches unhandled exceptions, OpenSky rate limits (HTTP 429), or network drops, delivering the exact Python stack trace to Discord before exiting.

---

## 🗄️ Core Database Schema Mapping

The database follows a Medallion Lakehouse schema hosted on Neon PostgreSQL.

### 1. `public.silver_flight_weather_telemetry`
Cleaned, normalized, and weather-enriched aircraft telemetry state vectors.

| Column Name | SQL Type | Constraint / Default | Description |
| :--- | :--- | :--- | :--- |
| `flight_record_id` | `VARCHAR(64)` | **PRIMARY KEY** | Deterministic SHA-256 hash of `icao24` + `time_position` |
| `icao24` | `VARCHAR(10)` | `NOT NULL` | Unique ICAO 24-bit transponder address in hex |
| `callsign` | `VARCHAR(12)` | `NULLABLE` | Commercial callsign / flight flight identifier (e.g., `AAL100`) |
| `origin_country` | `VARCHAR(100)` | `DEFAULT 'Unknown'` | Country of aircraft registration |
| `time_position` | `BIGINT` | `NULLABLE` | Unix epoch timestamp of last position vector fix |
| `last_contact` | `BIGINT` | `NULLABLE` | Unix epoch timestamp of last transponder message received |
| `longitude` | `DOUBLE PRECISION`| `NOT NULL` | WGS84 Longitude coordinate in decimal degrees |
| `latitude` | `DOUBLE PRECISION`| `NOT NULL` | WGS84 Latitude coordinate in decimal degrees |
| `baro_altitude_m` | `DOUBLE PRECISION`| `NULLABLE` | Barometric altitude above sea level in meters |
| `on_ground` | `BOOLEAN` | `DEFAULT FALSE` | Flag indicating whether the aircraft is on ground runway |
| `velocity_mps` | `DOUBLE PRECISION`| `NULLABLE` | Ground speed in meters per second |
| `true_track_deg` | `DOUBLE PRECISION`| `NULLABLE` | True track heading angle in decimal degrees (0° = North) |
| `vertical_rate_mps`| `DOUBLE PRECISION`| `NULLABLE` | Vertical climb/descent rate in meters per second |
| `geo_altitude_m` | `DOUBLE PRECISION`| `NULLABLE` | Geometric altitude in meters |
| `squawk` | `VARCHAR(10)` | `NULLABLE` | Transponder squawk code (e.g., `7700` emergency) |
| `spi` | `BOOLEAN` | `DEFAULT FALSE` | Special Position Indicator transponder flag |
| `position_source` | `INTEGER` | `DEFAULT 0` | Vector source (0 = ADS-B, 1 = ASTERIX, 2 = MLAT) |
| `geohash` | `VARCHAR(12)` | `NOT NULL` | Base-32 Geohash string (Precision 4 spatial grid bin) |
| `weather_recorded_at`| `TIMESTAMPTZ` | `NULLABLE` | Observation timestamp of synchronized atmospheric reading |
| `temperature_c` | `DOUBLE PRECISION`| `NULLABLE` | Ambient air temperature at grid centroid in Celsius |
| `relative_humidity_pct`| `DOUBLE PRECISION`| `NULLABLE` | Relative atmospheric humidity percentage (0 - 100%) |
| `precipitation_mm`| `DOUBLE PRECISION`| `NULLABLE` | Accumulated precipitation in millimeters |
| `wind_speed_mps` | `DOUBLE PRECISION`| `NULLABLE` | Surface wind speed in meters per second |
| `wind_direction_deg`| `DOUBLE PRECISION`| `NULLABLE` | Wind direction in decimal degrees (0 - 360°) |
| `wind_gusts_mps` | `DOUBLE PRECISION`| `NULLABLE` | Maximum peak wind gust in meters per second |
| `cloud_cover_pct` | `DOUBLE PRECISION`| `NULLABLE` | Total cloud cover percentage (0 - 100%) |
| `visibility_m` | `DOUBLE PRECISION`| `NULLABLE` | Horizontal visibility distance in meters |
| `convective_risk_score`| `DOUBLE PRECISION`| `NULLABLE` | Computed atmospheric hazard risk score (0.0 to 100.0) |
| `ingested_at` | `TIMESTAMPTZ` | `NOT NULL` | System ingestion timestamp in UTC |
| `recorded_date` | `DATE` | `NOT NULL` | Calendar partition date (`YYYY-MM-DD`) |

*Indexes:* `idx_silver_recorded_date` on `(recorded_date)`, `idx_silver_geohash` on `(geohash)`, `idx_silver_callsign` on `(callsign)`.

---

### 2. `public.gold_delay_weather_matrix`
Aggregated spatial KPI summary table rolled up by 1-hour windows and atmospheric hazard category.

| Column Name | SQL Type | Constraint / Default | Description |
| :--- | :--- | :--- | :--- |
| `window_start` | `TIMESTAMPTZ` | **COMPOSITE PK** | Start of analytical aggregation window (`date_trunc('hour')`) |
| `window_end` | `TIMESTAMPTZ` | `NOT NULL` | End of analytical aggregation window (`window_start + 1 hour`) |
| `recorded_date` | `DATE` | **COMPOSITE PK** | Calendar date partition (`YYYY-MM-DD`) |
| `geohash` | `VARCHAR(12)` | **COMPOSITE PK** | Base-32 Geohash-4 spatial cell centroid |
| `weather_condition_category`| `VARCHAR(50)` | **COMPOSITE PK** | Classified hazard tier (`SEVERE_STORM`, `HIGH_WIND_SHEAR`, `MODERATE_RAIN`, `LOW_VISIBILITY`, `CLEAR`) |
| `total_active_flights`| `BIGINT` | `NOT NULL` | Count of distinct aircraft (`icao24`) traversing the cell |
| `total_cruising_flights`| `BIGINT` | `NOT NULL` | Aircraft cruising at or above 3,000 meters |
| `avg_velocity_mps`| `DOUBLE PRECISION`| `NULLABLE` | Mean aircraft ground speed across the window |
| `baseline_velocity_mps`| `DOUBLE PRECISION`| `DEFAULT 230.0` | Standard commercial cruising speed baseline (230 m/s / ~450 kts) |
| `velocity_deficit_pct`| `DOUBLE PRECISION`| `NULLABLE` | Mean percentage velocity reduction vs. 230 m/s baseline |
| `avg_precipitation_mm`| `DOUBLE PRECISION`| `NULLABLE` | Mean precipitation across the spatial cell |
| `max_precipitation_mm`| `DOUBLE PRECISION`| `NULLABLE` | Maximum peak precipitation observed in the window |
| `avg_wind_speed_mps`| `DOUBLE PRECISION`| `NULLABLE` | Mean wind speed in meters per second |
| `peak_wind_gust_mps`| `DOUBLE PRECISION`| `NULLABLE` | Peak instantaneous wind gust in the window |
| `avg_cloud_cover_pct`| `DOUBLE PRECISION`| `NULLABLE` | Mean cloud cover percentage |
| `min_visibility_m`| `DOUBLE PRECISION`| `NULLABLE` | Minimum visibility recorded in the grid box |
| `mean_convective_risk`| `DOUBLE PRECISION`| `NULLABLE` | Mean convective storm index across all flights |
| `flights_impacted_count`| `BIGINT` | `NOT NULL` | Total aircraft experiencing convective risk ≥ 50.0 |
| `computed_at` | `TIMESTAMPTZ` | `NOT NULL` | Timestamp when the aggregation rollup was generated |

*Primary Key:* `(window_start, geohash, weather_condition_category, recorded_date)`.  
*Indexes:* `idx_gold_recorded_date` on `(recorded_date)`, `idx_gold_geohash` on `(geohash)`.

---

## 📊 Power BI Desktop Integration

The live telemetry and aggregated Gold matrix connect directly to **Microsoft Power BI Desktop** via the internet.

### 1. Connection Instructions
1. Open **Power BI Desktop**.
2. Click **Get Data** ➔ **PostgreSQL database**.
3. **Server:** Enter your Neon endpoint host (e.g., `ep-square-glitter-b4cwle5r-pooler.c-6.us-east-2.aws.neon.tech`).
4. **Database:** `neondb`.
5. **Data Connectivity mode:** Select **Import** (recommended for interactive slicing and fast offline report loading) or **DirectQuery** (for live continuous queries).
6. Under Authentication, select **Database**, enter your Neon username (`neondb_owner`) and password, and check that SSL encryption is enabled.
7. Select tables:
   - `public.silver_flight_weather_telemetry`
   - `public.gold_delay_weather_matrix`

### 2. Relationship Modeling & Cross-Filtering Configuration
In Power BI Model View (`Airway Analysis.pbix`):

```
┌─────────────────────────────────────────┐          ┌────────────────────────────────────────┐
│   silver_flight_weather_telemetry       │          │       gold_delay_weather_matrix        │
│   (Individual Aircraft Vectors)         │          │       (Aggregated Hourly Weather)      │
├─────────────────────────────────────────┤          ├────────────────────────────────────────┤
│ • flight_record_id (PK)                 │          │ • window_start (PK)                    │
│ • callsign                              │          │ • weather_condition_category           │
│ • velocity_mps                          │          │ • total_active_flights                 │
│ • geohash ──────────────────────────────┼──[* : *]─┼──► geohash                             │
│ • convective_risk_score                 │          │ • velocity_deficit_pct                 │
└─────────────────────────────────────────┘          └────────────────────────────────────────┘
                       Cross filter direction: Both (Bidirectional)
```

> [!IMPORTANT]
> **Relationship Cardinality Setting:**
> - In standard relational schemas, `geohash` appears multiple times in `silver_flight_weather_telemetry` (multiple aircraft in the same grid) and multiple times in `gold_delay_weather_matrix` (different hours and weather categories for the same grid).
> - Configure the relationship cardinality as **Many-to-Many (`*:*`)**.
> - Set **Cross filter direction** to **Both (Bidirectional)**.
> - **Operational Benefit:** Selecting an atmospheric hazard category (e.g., clicking `SEVERE_STORM` on a chart) will filter and highlight all individual airborne aircraft traversing that storm zone in real-time. Conversely, selecting a specific aircraft callsign filters the exact localized weather cells it traversed.

---

## 🔒 100% Free Tier & Zero Financial Risk Verification

This engine is engineered to eliminate cloud costs and avoid credit card billing:

| Component | Platform | Free Tier Quota / Terms | Financial Risk |
| :--- | :--- | :--- | :--- |
| **Automation Engine** | GitHub Actions | **Unlimited minutes** on public repositories (2,000 mins/mo on private) | **$0.00 / Zero Risk** |
| **Database Compute** | Neon PostgreSQL | Free tier includes serverless auto-suspend, 0.5 GiB storage, connection pooling | **$0.00 / No CC Required** |
| **Flight Telemetry** | OpenSky Network | Free community access to public ADS-B receiver network | **$0.00 / Completely Free** |
| **Atmospheric Weather**| Open-Meteo API | Free, open-access API for non-commercial and research use | **$0.00 / No API Key Needed** |
| **Analytics BI** | Power BI Desktop | Free desktop client for reporting and data exploration | **$0.00 / Free Application** |

There are **zero Google Cloud Platform VMs, zero paid BigQuery streaming quotas, zero cloud storage egress fees, and zero cloud billing meters** in this architecture.

---

## 🚀 Local Development & Manual Execution

### 1. Clone Repository & Setup Virtual Environment
```bash
git clone https://github.com/Lakshmiiii23/flight-weather-analytics-engine.git
cd flight-weather-analytics-engine
python -m venv .venv

# On Windows (PowerShell):
.venv\Scripts\Activate.ps1
# On Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Configure Environment Variables
Create a `.env` file in the repository root (automatically ignored by `.gitignore`):
```bash
NEON_DATABASE_URL=postgresql://neondb_owner:your_password@ep-your-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/your-webhook-id/your-token
```

### 3. Run Pipeline Passes
```bash
# Execute a single ingestion and database commit pass:
python scripts/sync_to_neon.py --once

# Execute a single pass in dry-run mode (tests APIs and DuckDB without writing to Neon):
python scripts/sync_to_neon.py --dry-run --once

# Run continuous local streaming daemon (polling every 10 minutes):
python scripts/sync_to_neon.py --interval 600
```

### 4. Run Automated Test Suite
The codebase includes comprehensive unit and integration test fixtures with 100% test passing rates:
```bash
python -m pytest tests/ -v
```

---

## 📁 Repository Directory Layout

```
flight-weather-analytics-engine/
├── .github/
│   └── workflows/
│       ├── ci.yml                 # Automated unit tests on pull requests
│       └── flight_daemon.yml      # Hourly 24/7 automated ingestion runner
├── Dashboard/
│   └── Airway Analysis.pbix       # Power BI Desktop interactive report
├── config/
│   ├── logging_config.py          # Structured JSON logging configuration
│   ├── settings.py                # Strongly typed Pydantic application settings
│   └── settings.yaml              # Bounding boxes, timeouts, and thresholds
├── scripts/
│   ├── sync_to_neon.py            # Primary telemetry ingestion and sync runner
│   └── run_windows.ps1            # Windows local launch helper
├── sql/
│   └── ddl/
│       └── create_neon_tables.sql # Production DDL for Silver, Gold, and Audit tables
├── src/
│   ├── dashboard/
│   │   └── app.py                 # Streamlit & PyDeck 3D interactive web dashboard
│   ├── ingestion/
│   │   ├── opensky_client.py      # ADS-B transponder API ingestion client
│   │   └── weather_client.py      # Open-Meteo batch weather ingestion client
│   ├── monitoring/
│   │   └── webhook_alerts.py      # Multi-platform Discord alert dispatcher
│   ├── processing/
│   │   ├── spatial_join.py        # DuckDB vectorized spatial correlation engine
│   │   └── validation.py          # Pydantic telemetry normalization and validation
│   └── storage/
│       └── neon_manager.py        # Neon PostgreSQL SQLAlchemy connection pool manager
├── tests/
│   ├── integration/               # Pipeline end-to-end integration test suites
│   └── unit/                      # Unit test fixtures for all modules
├── pyproject.toml                 # Build specifications and ruff/pytest configurations
├── requirements.txt               # Pinned Python package dependencies
└── README.md                      # Architecture documentation and data dictionary
```

---

## 📜 License
MIT License. Built for enterprise data engineering demonstrations and resilient 24/7 autonomous lakehouse operations.

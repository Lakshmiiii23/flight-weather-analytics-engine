# 🛫 Eco-Friendly Hybrid Flight-Weather Analytics Engine

A production-grade, streaming/micro-batch Lakehouse engine correlating live commercial flight vectors with localized atmospheric weather. Built to operate **100% perpetually free** using **Google Cloud Platform (GCP) Always-Free Tier** and **Cloudflare R2** zero-egress cold storage with automated cryptographic eviction.

---

## 🌟 Key Engineering Innovations

1. **Spatial Grid Binning (Geohash Precision 4):** Resolves the external API rate-limit pitfall. Instead of querying weather per individual aircraft (which exhausts quotas within 20 minutes), flights are clustered into ~20 km spatial cells and queried in batched multi-coordinate requests, slashing external API calls by **98%**.
2. **DuckDB Out-Of-Core Engine:** Vectorized in-memory spatial join and convective storm hazard index calculation enforced strictly within a `350MB` RAM ceiling (`PRAGMA max_memory = '350MB';`), safeguarding the free `e2-micro` Ubuntu host.
3. **Zero-Cost BigQuery Ingestion:** Utilizes BigQuery batch load jobs (`load_table_from_json`) with partition targeting (`PARTITION BY recorded_date`), bypassing BigQuery streaming insertion billing fees.
4. **The Zero-Dollar Sentinel (Autonomous Eviction):** Prunes aged weekly partitions from BigQuery, serializes records into compressed **ZStandard (ZSTD) Apache Parquet**, streams them to **Cloudflare R2** (10 GB free forever, $0 egress fees), logs an immutable **SHA-256 cryptographic audit manifest**, and wipes hot storage to permanently stay under GCP's 10 GB limit.
5. **Laptop-Side Zero-Egress Sync Agent:** On-demand CLI utility (`scripts/local_archive_pull.py`) that syncs cloud Parquet archives down to the developer's laptop and cryptographically verifies the SHA-256 hash against BigQuery audit logs.

---

## 🏗️ Architecture & Data Flow

```
[ OpenSky Network ADS-B API ]         [ Open-Meteo Batch Weather API ]
             │                                       │
             ▼                                       ▼
    [ Python Ingestion Worker ] ────────► [ Geohash Spatial Clustering ]
                                                     │
                                                     ▼
                                        [ DuckDB Spatial Join Engine ]
                                        (Capped at 350 MB RAM ceiling)
                                                     │
                                                     ▼
                                     [ Google Cloud BigQuery Lakehouse ]
                                     ├── silver_flight_weather_telemetry (Partitioned)
                                     ├── gold_delay_weather_matrix (Aggregated KPIs)
                                     └── cloud_archive_audit_logs (Immutable Lineage)
                                                     │
                             ┌───────────────────────┴───────────────────────┐
                             │                                               │
                             ▼ (Weekly Sunday Eviction)                      ▼ (Live BI Connector)
                [ Zero-Dollar Sentinel ]                           [ Looker Studio Dashboard ]
                ├── ZStandard Parquet Compaction                   (Interactive Real-Time Map)
                ├── Cloudflare R2 Upload ($0 Egress)
                └── BigQuery Partition Truncation
```

---

## 🚀 Quickstart & Local Execution

### 1. Prerequisites & Virtual Environment
```bash
git clone https://github.com/<your-username>/flight-weather-analytics-engine.git
cd flight-weather-analytics-engine
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Environment Configuration
Create a `.env` file in the root directory (protected by `.gitignore`):
```bash
GCP_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
WEBHOOK_URL=https://discord.com/api/webhooks/your-webhook-id/token
R2_ACCESS_KEY_ID=your-cloudflare-r2-key
R2_SECRET_ACCESS_KEY=your-cloudflare-r2-secret
```

### 3. Run Pipeline Micro-Batch
```bash
# Execute a single cycle in dry-run mode (no cloud writes)
python scripts/run_pipeline.py --dry-run --once

# Execute continuous streaming production micro-batch
python scripts/run_pipeline.py --interval 600
```

### 4. Run Automated Test Suite
```bash
pytest tests/ -v
```

---

## ☁️ Zero-Touch Cloud VM Deployment (GCP e2-micro)

1. Provision an **e2-micro VM** on GCP in `us-central1`, `us-east1`, or `us-west1` with Ubuntu 22.04 LTS (Always-Free Tier).
2. SSH into your VM and run the automated provisioning script:
```bash
curl -fsSL https://raw.githubusercontent.com/<your-username>/flight-weather-analytics-engine/main/scripts/setup_vm.sh | bash
```
3. The script automatically:
   - Configures a **2 GB Linux swapfile** with low swappiness to prevent OOM termination.
   - Installs dependencies and prepares `/opt/flight-weather-engine`.
   - Provisions a `systemd` background service (`flight-engine.service`) with auto-restart.
   - Configures a weekly cron job to run the `EvictionSentinel` every Sunday at 23:50 UTC.

---

## 📊 Free Looker Studio Dashboard Setup

1. Open [Google Looker Studio](https://lookerstudio.google.com/) (100% Free).
2. Click **Create** > **Data Source** > Select **BigQuery**.
3. Choose your project and table: `gold_delay_weather_matrix`.
4. Recommended Visualizations:
   - **Choropleth/Bubble Geo Map:** Dimension `geohash`, Metric `flights_impacted_count`, Color by `weather_condition_category`.
   - **KPI Card:** Average Airspeed Deficit (`velocity_deficit_pct`).
   - **Time Series Line Chart:** Flight density vs. peak wind gusts (`peak_wind_gust_mps`).

---

## 📜 License
MIT License. Built for enterprise data architecture demonstrations and production portfolio deployment.

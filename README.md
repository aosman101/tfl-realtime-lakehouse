# 🚦 tfl-realtime-lakehouse

[![Ready to run](https://img.shields.io/badge/status-ready_to_run-brightgreen)](#run-it-now-copy-paste)
[![Runtime](https://img.shields.io/badge/runtime-Docker%20Compose-2496ED?logo=docker&logoColor=white)](docker-compose.yaml)
[![Orchestration](https://img.shields.io/badge/orchestration-Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Transforms](https://img.shields.io/badge/transforms-dbt%20%2B%20DuckDB-FF694B)](dbt_project)
[![Data quality](https://img.shields.io/badge/data_quality-Great%20Expectations-F9A03C)](airflow/dags/tfl_transform_dag.py)
[![Lineage](https://img.shields.io/badge/lineage-OpenLineage%20%2B%20Marquez-4F46E5)](docker-compose.override.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

This is a laptop-friendly real-time lakehouse that utilises live Transport for London (TfL) arrivals. Docker Compose is used to deploy Airflow (with Celery) alongside Marquez. The ingestion Directed Acyclic Graph (DAG) creates Parquet snapshots, while dbt and DuckDB build data marts. Inline validations are performed using Great Expectations, and OpenLineage metadata is stored in Marquez, allowing you to track runs and datasets from start to finish.

## Stack at a glance
- Ingest: `tfl_ingest_dag` hits the TfL Unified API for your StopPoint IDs and writes Parquet to `data/raw/date=*/`.
- Transform: `tfl_transform_dag` runs `dbt-ol build` in `dbt_project/`, producing `data/silver/tfl.duckdb`.
- Validate: inline GX suite checks `staging.stg_arrivals` (nullability, time-to-station bounds).
- Observe: OpenLineage → Marquez UI at `http://localhost:3000`, Airflow UI/API at `http://localhost:8080`.
- Runtime: Docker Compose with a custom Airflow image (see `docker-compose.override.yml`) that bakes in dbt-ol, DuckDB, and GX.

## Run It Now copy-paste
Prereqs: Docker + Docker Compose; optional TfL API keys to avoid throttling.

```bash
# Clone + configure env
git clone https://github.com/aosman101/tfl-realtime-lakehouse.git
cd tfl-realtime-lakehouse
cp .env.example .env
printf 'TFL_STOPPOINT_IDS=490008660N,490009133G\n' >> .env   # edit to the stops you want
# Optional (avoids TfL throttling):
# printf 'TFL_APP_ID=...\nTFL_APP_KEY=...\n' >> .env

# Start the stack (Airflow, Marquez, DuckDB/dbt, GX baked in)
docker compose up --build -d
docker compose ps

# Pull live arrivals -> Parquet
docker compose exec airflow-scheduler bash -lc "airflow dags trigger tfl_ingest_dag"
sleep 12
find data/raw -name 'arrivals_*.parquet' | head -n 5

# Build marts + validate (dbt-ol + inline GX)
docker compose exec airflow-scheduler bash -lc "airflow dags trigger tfl_transform_dag"
docker compose exec airflow-scheduler bash -lc "ls -lah /opt/airflow/data/silver || true"

# Quick peek at data (inside the Airflow image)
docker compose exec airflow-scheduler bash -lc "duckdb -c \"SELECT * FROM '/opt/airflow/data/raw/date=*/arrivals_*.parquet' LIMIT 5;\""
```

UIs:
- Airflow: http://localhost:8080
- Marquez UI: http://localhost:3000 (API: http://localhost:5050)

## Fast checks (screenshot-friendly)
- `curl -s http://localhost:8080/api/v2/version | jq`
- `docker compose exec airflow-scheduler bash -lc "airflow dags list | grep tfl_"`
- `ls -lah data/raw | head -n 20`

## Project Structure
```
tfl-realtime-lakehouse/
├─ airflow/
│  ├─ dags/
│  │  ├─ tfl_ingest_dag.py        # Fetch TfL arrivals -> Parquet
│  │  └─ tfl_transform_dag.py     # dbt-ol build + GX validation
│  └─ requirements.txt
├─ dbt_project/
│  ├─ dbt_project.yml
│  ├─ models/
│  │  ├─ staging/
│  │  └─ marts/
│  └─ profiles.yml                # DuckDB profile baked into the image
├─ data/
│  ├─ raw/                        # Parquet snapshots (date-partitioned)
│  └─ silver/                     # DuckDB file + marts outputs
├─ config/airflow.cfg             # Local Airflow overrides
├─ docker-compose.yaml
├─ docker-compose.override.yml    # Bakes dbt-ol, DuckDB, GX into Airflow image
├─ tfl_align.py                   # Standalone fetch helper
├─ requirements.txt
├─ .env.example
└─ README.md
```

## Project map
- `airflow/dags/tfl_ingest_dag.py` — fetch TfL arrivals with retries and polite headers; writes partitioned Parquet snapshots.
- `airflow/dags/tfl_transform_dag.py` — runs `dbt-ol build` then a minimal GX validation on `staging.stg_arrivals`.
- `dbt_project/` — DuckDB project and profiles baked into the Airflow image.
- `docker-compose.yaml` + `docker-compose.override.yml` — Airflow Celery stack with Marquez and baked dependencies.
- `config/airflow.cfg` — local Airflow overrides.
- `tfl_align.py` — standalone fetch/align helper using the same env vars.

## Add another StopPoint
```bash
# Comma-separated IDs in .env
sed -i '' 's/^TFL_STOPPOINT_IDS=.*/TFL_STOPPOINT_IDS=490008660N,490009133G,4900XXXXXX/' .env
docker compose exec airflow-scheduler bash -lc "airflow dags trigger tfl_ingest_dag"
find data/raw -name 'arrivals_*.parquet' | tail
```

## Troubleshooting
- No arrivals written: confirm `TFL_STOPPOINT_IDS` and optional TfL keys; check scheduler logs `docker compose logs airflow-scheduler`.
- UIs unavailable: wait for health checks (`docker compose ps`), then inspect `docker compose logs airflow-apiserver`.
- dbt build fails: `docker compose exec airflow-scheduler bash -lc "dbt-ol debug --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"`.

## Contributing
Pull requests are welcome—please include tests (dbt or GX) for any new models, sample StopPoints when possible, and a brief note of the commands you ran to validate changes.

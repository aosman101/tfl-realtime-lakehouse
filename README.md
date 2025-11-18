# 🚦 tfl-realtime-lakehouse

[![Status](https://img.shields.io/badge/status-running-brightgreen)](#)
[![Airflow](https://img.shields.io/badge/orchestration-Airflow-blue)](#)
[![dbt+DuckDB](https://img.shields.io/badge/transform-dbt%20%2B%20DuckDB-blue)](#)
[![GX](https://img.shields.io/badge/data%20quality-Great%20Expectations-blue)](#)
[![OpenLineage+Marquez](https://img.shields.io/badge/lineage-OpenLineage%20%2B%20Marquez-blue)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

This project offers a laptop-friendly, cost-free opportunity to learn data engineering by featuring a comprehensive, small-scale, and real-time "lakehouse" pipeline that utilises live arrival data from Transport for London (TfL) as its primary source. It is designed to be interactive, enabling you to explore each layer, iterate quickly, and gain a deeper understanding of the integration points related to data ingestion, storage, transformation, quality assurance, and lineage tracking.

Here's an overview of the high-level workflow:
- **Ingest (Realtime):** Retrieve live arrival information by calling the TfL Unified API.
- **Store:** Persist raw snapshots as Parquet files within a local data lake.
- **Transform:** Utilise dbt and DuckDB to transform staging data into data marts.
- **Validate:** Employ Great Expectations to ensure data quality on staging and marts.
- **Observe:** Emit OpenLineage metadata via Marquez to monitor jobs, datasets, and runs.

---

## **Interactive Walkthrough: What You Will Do**

This README is a guided, interactive walkthrough with copy-pasteable commands and quick health checks you can screenshot.

1. Start the platform services (Airflow, Marquez, any services in Docker Compose).
2. Trigger the ingest DAG to collect live TfL arrival data.
3. Inspect Parquet files written to data/raw/ (schema, record samples).
4. Run dbt models to build marts (with DuckDB as the execution engine).
5. Run Great Expectations checks and review data docs.
6. Explore lineage in Marquez/OpenLineage and inspect job/dataset relationships.

---

## Quick Start

Prerequisites
- Docker & Docker Compose (or Docker Desktop)
- (Optional) Python 3.9+ if you want to run custom scripts locally
- TfL API credentials (optional but recommended for higher rate-limits)
  - TFL_APP_ID
  - TFL_APP_KEY
- StopPoint IDs (NaPTAN) you want to track (comma-separated)

1. Clone the repo
```bash
git clone https://github.com/aosman101/tfl-realtime-lakehouse.git
cd tfl-realtime-lakehouse
```

2. Copy the env template and configure
```bash
cp .env.example .env
# edit .env and set the values below
```

Required environment variables (.env)
```env
## Airflow runtime (set by docker-compose during runtime)

AIRFLOW_UID=

## TfL credentials (recommended - avoids throttling)
TFL_APP_ID=
TFL_APP_KEY=

## Comma-separated IDs of StopPoints for which to retrieve arrival data
TFL_STOPPOINT_IDS=490008660N,490009133G

## OpenLineage / Marquez
OPENLINEAGE_NAMESPACE=tfl-realtime
OPENLINEAGE_URL=http://marquez:5000
```

3. Start services (Docker Compose)
Use Docker Compose (requires Docker Desktop):
```bash
docker compose up --build -d
# or for older Docker Compose:
docker-compose up --build -d
```

This will bring up the Airflow web server/scheduler (and any other services defined, such as Marquez). If you prefer to run only Airflow locally without containers, you can follow the Airflow docs and point Airflow to use the local directories in this repo.

4. Open the UIs
- Airflow (API/UI): http://localhost:8080
- Marquez (Lineage):
  - API: http://localhost:5050
  - Web: http://localhost:3000

Look for two DAGs in Airflow:
- tfl_ingest_dag
- tfl_transform_dag

5. Trigger the ingest DAG (manually)
- From the UI: click "tfl_ingest_dag" → Trigger DAG.
- Or via CLI inside the scheduler container:

```bash
docker compose exec airflow-scheduler bash -lc "airflow dags trigger tfl_ingest_dag"
```

After a successful run, check the data folder.

6. Inspect the generated data
- Raw/landing parquet files are written under:
  - data/raw/
- Silver/marts (after transform) are under:
  - data/silver/

Example quick check using DuckDB (locally)

```bash
## Launch a DuckDB shell in the specified folder.
duckdb
-- inside duckdb shell:
.read data/duckdb_init.sql  -- if provided, or use:
INSTALL httpfs;
LOAD httpfs;
-- Then query the parquet file:
SELECT * FROM 'data/raw/arrivals_snapshot_2025-11-11.parquet' LIMIT 10;
```

7. Run dbt transforms
From the repo root:
```bash
# If you have dbt installed locally and a DuckDB profile in dbt_project/profiles.yml:
cd dbt_project
dbt debug
dbt deps
dbt run
dbt test
```

This will initiate your DBT models (staging -> marts) that materialise into DuckDB tables or Parquet outputs, depending on your profile.

8. Run Great Expectations validations
From the project root:
```bash
## Example pattern: run expectation suite or checkpoint

great_expectations --v3-api checkpoint run <checkpoint-name>

## Or:

great_expectations --v3-api suite run <suite-name>

Great Expectations will validate data in staging/marts and can build data-docs for review.

9. Inspect lineage
- Visit Marquez (OPENLINEAGE_URL) to see jobs, runs, and dataset lineage emitted by the Airflow operators or by dbt with OpenLineage integration.

---

## Project Structure

tfl-realtime-lakehouse/

  ├─ airflow/
  │   ├─ dags/
  │   │   ├─ tfl_ingest_dag.py        # Ingest snapshots from TfL and persist as Parquet.
  │   │   └─ tfl_transform_dag.py     # Orchestrates dbt transforms + validations.
  │   └─ requirements.txt
  ├─ dbt_project/
  │   ├─ dbt_project.yml
  │   ├─ models/
  │   │   ├─ staging/
  │   │   └─ marts/
  │   └─ profiles.yml                 # Local DuckDB profile.
  ├─ great_expectations/              # GX configuration, expectations, and documentation.
  ├─ data/
  │   ├─ raw/                         # Parquet snapshots (landing stage).
  │   └─ silver/                      # Transformed markets / DuckDB outputs.
  ├─ docker-compose.yml
  ├─ .env.example
  ├─ README.md
  └─ LICENSE

---

## DAGs (what they do)

- tfl_ingest_dag
  - Purpose: Periodically call the TfL Unified API for the configured StopPoint IDs, capture arrival snapshots, and persist them as partitioned Parquet files in data/raw/.
  - Behaviour: Each DAG run should write a snapshot file with a timestamp in the filename. Files are append-only snapshots, allowing historical replays.

- tfl_transform_dag
  - Purpose: Run dbt (DuckDB) to transform staging data into marts; then run Great Expectations checks, and emit OpenLineage metadata.
  - Behaviour: Can be scheduled less frequently than ingestion (e.g., hourly) and will depend on the latest raw snapshots.

If you want to examine the code for each DAG, open:
- airflow/dags/tfl_ingest_dag.py
- airflow/dags/tfl_transform_dag.py

---

## Data quality & Observability

- Great Expectations (GX)
  - Expectations live under great_expectations/ and include checks for schema, nullability, and business rules (e.g., arrival time within expected horizon).
  - Run GX checkpoints from the transform DAG or manually to validate models.

- Lineage (OpenLineage + Marquez)
  - When jobs run (Airflow tasks, dbt), OpenLineage metadata is emitted so you can visualise datasets, job runs, and upstream/downstream relationships in Marquez.

- Logs & Debugging
  - Airflow task logs are visible in the UI (click on task -> logs).
  - If a DAG fails, inspect the logs and /tmp or data folder artefacts for diagnostics.

---

## Please add a new Stop Point for monitoring

1. Edit .env and append the NaPTAN ID to TFL_STOPPOINT_IDS:
"`env
TFL_STOPPOINT_IDS=490008660N,490009133G,490012345A

2. Trigger the ingest DAG from Airflow UI or run the specific ingestion task for a single StopPoint (if implemented).

3. Verify a new parquet file is written to data/raw/ with arrivals for the new StopPoint.

---

## Development: add a new dbt model

1. Create SQL model under dbt_project/models/marts/new_model.sql.
2. Add tests in dbt (schema.yml) or Great Expectations suites for the new model.
3. Run dbt locally:

```bash
cd dbt_project
dbt run --models marts.new_model
dbt test --models marts.new_model
```

4. Update tfl_transform_dag if you need the DAG to run additional tasks for the model (e.g., additional validations or lineage metadata).

---

## Important commands

- Start everything
```bash
docker compose up --build -d
```

- Health checks (screenshot-friendly)
```bash
# Compose status
docker compose ps

# Airflow API version (no auth needed)
curl -s http://localhost:8080/api/v2/version | jq

# List DAGs via CLI in scheduler
docker compose exec airflow-scheduler bash -lc "airflow dags list | head -n 20"
```

- Trigger a DAG (Airflow CLI inside scheduler)
```bash
docker compose exec airflow-scheduler bash -lc "airflow dags trigger tfl_ingest_dag"
# (optional) Trigger transform
docker compose exec airflow-scheduler bash -lc "airflow dags trigger tfl_transform_dag"
```

- List Parquet files
```bash
ls -lah data/raw
```

- Inspect a parquet with DuckDB
```bash
duckdb "SELECT * FROM 'data/raw/<your-file>.parquet' LIMIT 10;"
```

- Run dbt
```bash
cd dbt_project
dbt run
dbt test
```

- Run Great Expectations
```bash
great_expectations --v3-api checkpoint run <checkpoint>
```

---

## Final sanity test (copy, run, screenshot)

From the repo root while services are running:

```bash
# 1) Show containers and health
docker compose ps

# 2) Airflow API version
curl -s http://localhost:8080/api/v2/version | jq

# 3) List DAGs via CLI
docker compose exec airflow-scheduler bash -lc "airflow dags list | head -n 20"

# 4) Trigger ingest and inspect raw outputs
docker compose exec airflow-scheduler bash -lc "airflow dags trigger tfl_ingest_dag"
sleep 10
ls -lah data/raw | head -n 30
```

These four blocks typically produce clean, screenshot-ready outputs for status, version info, visible DAGs, and files being written to `data/raw/`.

---

## Troubleshooting

- Airflow web UI not accessible:
  - Confirm container started: docker compose ps.
  - Check container logs: docker compose logs airflow-webserver.

- No data written after ingest:
  - Verify .env has valid TFL_STOPPOINT_IDS.
  - Check network access (some environments block external calls).
  - Review ingest task logs for HTTP errors (rate-limiting or 401/403 if TFL keys are invalid).

- dbt failing:
  - Run dbt debug to ensure profiles.yml is correctly configured for DuckDB.
  - Check that the raw parquet files exist and paths in the staging models match the actual file locations.

---

## Roadmap

- Implement Continuous Integration (CI) to run dbt tests on pull requests (PRs) using GitHub Actions.
- Add unit tests for the tasks in the Directed Acyclic Graph (DAG) using pytest and Airflow testing helpers.
- Incorporate incremental dbt models to enable more efficient data transformations.
- Include a small front-end dashboard to display the latest arrivals and historical trends.
- Enhance the lineage tracking by incorporating the OpenLineage client into the ingestion code.

---

## Attribution & Credits

- TfL Unified API — data source for realtime transport information.
- Great Expectations — data testing and documentation.
- dbt, DuckDB — transformation & analytical engine.
- Marquez / OpenLineage — observability and lineage tracking.

---

## Contributing

Contributions are welcome! Feel free to:
- Open issues for bugs, enhancements, or feature requests.
- Open PRs with changes to DAGs, dbt models, or docs.
- Add more StopPoint examples and sample data for reproducible demos.

When opening a PR:

- Add a clear description of the change and why it helps.
- If you add models, include tests (dbt or GX) and sample outputs.

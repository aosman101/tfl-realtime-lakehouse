# 🚦 tfl-realtime-lakehouse

[![Status](https://img.shields.io/badge/status-in_progress-yellow)](#)
[![Airflow](https://img.shields.io/badge/orchestration-Airflow-blue)](#)
[![dbt+DuckDB](https://img.shields.io/badge/transform-dbt%20%2B%20DuckDB-blue)](#)
[![GX](https://img.shields.io/badge/data%20quality-Great%20Expectations-blue)](#)
[![OpenLineage+Marquez](https://img.shields.io/badge/lineage-OpenLineage%20%2B%20Marquez-blue)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A laptop-friendly, **zero-cost** data engineering project:
- **Ingest (real-time)**: Retrieve live arrival information from the **TfL Unified API**.
- **Action**: Save the data as **Parquet** format in a specified local data lake folder.
- **Transformation**: Use the **dbt + DuckDB** model to move data from staging to marts.
- **Validation**: **Great Expectations** compliance checks.
- **Observation**: **OpenLineage and Marquez** are used for tracking data lineage.

---

## Table of Contents
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [DAGs](#dags)
- [Data Quality](#data-quality)
- [Lineage](#lineage)
- [Roadmap](#roadmap)
- [Attribution & Credits](#attribution--credits)
- [License](#license)

---

## Project Structure

tfl-realtime-lakehouse/

  ├─ airflow/
  
  │   ├─ dags/
  
  │   │   ├─ tfl_ingest_dag.py
  
  │   │   └─ tfl_transform_dag.py
  
  │   └─ requirements.txt
  
  ├─ dbt_project/
  
  │   ├─ dbt_project.yml
  
  │   ├─ models/{staging,marts}/
  
  │   └─ profiles.yml   # Local DuckDB profile.
  
  ├─ great_expectations/  # Created by GX Initialisation.
  
  ├─ data/{raw,silver}/   # Mounted volumes.
  
  ├─ docker-compose.yml
  
  ├─ .env                 # (Create from .env.example; contains TFL keys).
  
  ├─ README.md
  
  └─ LICENSE


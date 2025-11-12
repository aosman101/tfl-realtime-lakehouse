from __future__ import annotations
from datetime import datetime, timedelta, timezone
import os, pathlib, requests, pyarrow as pa, pyarrow.parquet as pq, logging
from airflow import DAG
from airflow.operators.python import PythonOperator

APP_ID  = os.getenv("TFL_APP_ID")
APP_KEY = os.getenv("TFL_APP_KEY")
STOP_IDS = [s for s in os.getenv("TFL_STOPPOINT_IDS","").split(",") if s]

def fetch_and_write(**ctx):
    now_utc = datetime.now(timezone.utc)
    out_dir = pathlib.Path("/opt/airflow/data/raw/date="+now_utc.strftime("%Y-%m-%d"))
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = now_utc.strftime("%Y%m%d_%H%M%S")

    rows = []
    for stop in STOP_IDS:
        url = f"https://api.tfl.gov.uk/StopPoint/{stop}/Arrivals"
        params = {"app_id": APP_ID, "app_key": APP_KEY} if APP_ID and APP_KEY else {}
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        rows.extend(r.json())

    if not rows:
        logging.warning("No arrivals returned; check STOPPOINT IDS or API limits.")
        return

    table = pa.Table.from_pylist([
        {
          "stopId": r.get("naptanId") or r.get("stationName"),
          "lineId": r.get("lineId"),
          "platformName": r.get("platformName"),
          "destinationName": r.get("destinationName"),
          "timeToStation": r.get("timeToStation"),
          "timestamp": r.get("timestamp"),
        } for r in rows
    ])
    pq.write_table(table, out_dir / f"arrivals_{ts}.parquet")

default_args = {"retries": 2, "retry_delay": timedelta(minutes = 2)}
with DAG(
    dag_id="tfl_ingest",
    start_date=datetime(2025,1,1),
    schedule="*/2 * * * *",  # be polite; predictions refresh ~30s
    catchup=False,
    max_active_runs = 1,
    default_args = default_args,
    tags=["tfl","ingest"],
) as dag:
    PythonOperator(task_id = "fetch_and_write", python_callable = fetch_and_write)

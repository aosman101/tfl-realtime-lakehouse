from __future__ import annotations
from datetime import datetime, timedelta, timezone
import os, pathlib, requests, pyarrow as pa, pyarrow.parquet as pq, logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    from airflow import DAG
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow not installed (e.g., running fetch locally)
    DAG = None
    PythonOperator = None

APP_ID  = os.getenv("TFL_APP_ID")
APP_KEY = os.getenv("TFL_APP_KEY")

# Normalize, strip whitespace, and de-duplicate StopPoint IDs while preserving order
_raw_ids = os.getenv("TFL_STOPPOINT_IDS", "")
STOP_IDS = []
seen = set()
for s in [p.strip() for p in _raw_ids.split(",") if p.strip()]:
    if s not in seen:
        STOP_IDS.append(s)
        seen.add(s)

# Shared requests Session with retry/backoff and polite headers
_session = requests.Session()
_retry = Retry(total=3, connect=3, read=3, status_forcelist=(429, 500, 502, 503, 504), backoff_factor=0.5, allowed_methods=("GET",))
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)
_HEADERS = {"User-Agent": "tfl-realtime-lakehouse/1.0 (+https://github.com/aosman101/tfl-realtime-lakehouse)"}

_default_raw = pathlib.Path("/opt/airflow/data/raw")
_detected_raw = None
for parent in pathlib.Path(__file__).resolve().parents:
    candidate = parent / "data"
    if candidate.exists():
        _detected_raw = candidate / "raw"
        break

RAW_OUTPUT_DIR = pathlib.Path(
    os.getenv("RAW_ARRIVALS_DIR", _detected_raw or _default_raw)
)

def fetch_and_write(**ctx):
    now_utc = datetime.now(timezone.utc)
    out_dir = RAW_OUTPUT_DIR / f"date={now_utc.strftime('%Y-%m-%d')}"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = now_utc.strftime("%Y%m%d_%H%M%S")

    rows = []
    for stop in STOP_IDS:
        url = f"https://api.tfl.gov.uk/StopPoint/{stop}/Arrivals"
        params = {"app_id": APP_ID, "app_key": APP_KEY} if APP_ID and APP_KEY else {}
        try:
            r = _session.get(url, params=params, headers=_HEADERS, timeout=20)
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, list):
                rows.extend(payload)
            else:
                logging.warning("Non-list payload for stop %s: %s", stop, type(payload))
        except Exception as e:
            logging.error("Failed to fetch arrivals for stop %s: %s", stop, e)

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

if DAG is not None:
    with DAG(
        dag_id="tfl_ingest_dag",
        start_date=datetime(2025,1,1),
        schedule="*/2 * * * *",  # be polite; predictions refresh ~30s
        catchup=False,
        max_active_runs = 1,
        default_args = default_args,
        tags=["tfl","ingest"],
    ) as dag:
        PythonOperator(task_id = "fetch_and_write", python_callable = fetch_and_write)

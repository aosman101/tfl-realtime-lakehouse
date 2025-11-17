#!/usr/bin/env python3
"""tfl_align.py

Ready-to-run example to align arrivals for a TfL line.

What it does:
- resolves canonical line id via /Line/{ids}
- fetches StopPoints for the canonical line
- fetches Arrivals for each stop (parallelized)
- builds a single pandas DataFrame containing snapshot_ts and line_id
- writes a parquet file to disk

Usage:
  python tfl_align.py "Central" --outdir data/bronze --workers 8

Environment variables (optional):
    TFL_APP_ID         - TfL application id (recommended)
    TFL_APP_KEY        - TfL application key (recommended)
    TFL_KEY_LINE       - legacy single key for Line API (fallback)
    TFL_KEY_STOPPOINT  - legacy single key for StopPoint API (fallback)
    TFL_KEY_ARRIVALS   - legacy single key for Arrivals API (fallback)

If keys are not set the script will call the API without an app_key (may be rate-limited).
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://api.tfl.gov.uk"

# Credentials
APP_ID = os.getenv("TFL_APP_ID")
APP_KEY = os.getenv("TFL_APP_KEY")

# Legacy fallbacks if APP_ID/APP_KEY not provided
KEY_LINE = os.getenv("TFL_KEY_LINE")
KEY_STOPPOINT = os.getenv("TFL_KEY_STOPPOINT")
KEY_ARRIVALS = os.getenv("TFL_KEY_ARRIVALS")


def _make_session(retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


_SESSION = _make_session()


def _get(path: str, *, key: Optional[str] = None, timeout: int = 15) -> Tuple[Any, Dict[str, str]]:
    """GET helper that prefers app_id+app_key if provided, else falls back to single key.

    Returns: (json_body, response_headers)
    """
    params: Dict[str, Any] = {}
    headers = {"User-Agent": "tfl-realtime-lakehouse/1.0 (+https://github.com/aosman101/tfl-realtime-lakehouse)"}
    if APP_ID and APP_KEY:
        params["app_id"] = APP_ID
        params["app_key"] = APP_KEY
    elif key:
        # Some TfL endpoints accepted single-key access historically
        params["app_key"] = key
    url = f"{BASE}{path}"
    r = _SESSION.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    # return JSON-decoded body and headers dict
    return r.json(), dict(r.headers)


def canonical_line_id(line_input: str) -> str:
    """Call /Line/{ids} and return the canonical `id` from the API.

    Raises RuntimeError if not found or unexpected response.
    """
    data, _ = _get(f"/Line/{line_input}", key=KEY_LINE)
    if isinstance(data, list) and data:
        return data[0].get("id")
    raise RuntimeError(f"Line not found or unexpected response for '{line_input}'")


def get_stoppoints_for_line(line_id: str) -> List[Dict[str, Any]]:
    data, _ = _get(f"/Line/{line_id}/StopPoints", key=KEY_STOPPOINT)
    if isinstance(data, list):
        return data
    # unexpected; return empty list
    return []


def get_arrivals_for_stop(naptan_id: str) -> List[Dict[str, Any]]:
    data, _ = _get(f"/StopPoint/{naptan_id}/Arrivals", key=KEY_ARRIVALS)
    if isinstance(data, list):
        return data
    return []


def safe_snapshot_ts() -> Tuple[str, str]:
    # human-readable ISO for data and filesystem-safe timestamp
    ts = datetime.datetime.utcnow().replace(microsecond=0)
    iso = ts.isoformat() + "Z"
    fs = ts.strftime("%Y%m%dT%H%M%SZ")
    return iso, fs


def align_line_across_subscriptions(line_input: str, outdir: str = "data/bronze", workers: int = 8) -> pd.DataFrame:
    """Main logic: resolve line id, fetch stops, fetch arrivals and write parquet.

    Returns the combined DataFrame.
    """
    line_id = canonical_line_id(line_input)
    snapshot_iso, snapshot_fs = safe_snapshot_ts()

    stops = get_stoppoints_for_line(line_id)

    rows: List[Dict[str, Any]] = []

    # fetch arrivals in parallel per stop
    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_to_stop = {}
        for s in stops:
            naptan = s.get("naptanId")
            if not naptan:
                continue
            future = ex.submit(_safe_get_arrivals, naptan)
            future_to_stop[future] = (s, naptan)

        for fut in as_completed(future_to_stop):
            s, naptan = future_to_stop[fut]
            try:
                arrivals = fut.result()
            except Exception as e:
                # log and continue
                print(f"Warning: failed to fetch arrivals for {naptan}: {e}", file=sys.stderr)
                arrivals = []

            for a in arrivals:
                # Build a flattened row for each arrival
                rows.append(
                    {
                        "snapshot_ts": snapshot_iso,
                        "line_id": line_id,
                        "lineName": a.get("lineName"),
                        "naptanId": naptan,
                        "stationName": a.get("stationName") or s.get("commonName"),
                        "destinationName": a.get("destinationName"),
                        "expectedArrival": a.get("expectedArrival"),
                        "timeToStation": a.get("timeToStation"),
                        "vehicleId": a.get("vehicleId"),
                        "platformName": a.get("platformName"),
                        # include raw arrival JSON for downstream: store as JSON string
                        "raw": a,
                    }
                )

    df = pd.DataFrame(rows)

    # normalize types where sensible
    if not df.empty and "expectedArrival" in df.columns:
        try:
            df["expectedArrival_ts"] = pd.to_datetime(df["expectedArrival"], utc=True, errors="coerce")
        except Exception:
            df["expectedArrival_ts"] = pd.NaT

    # ensure outdir exists
    os.makedirs(outdir, exist_ok=True)
    out_fn = os.path.join(outdir, f"arrivals_{line_id}_{snapshot_fs}.parquet")

    # write parquet; pandas uses pyarrow or fastparquet if installed
    # For portability we write the raw JSON column as JSON strings
    if not df.empty and "raw" in df.columns:
        df = df.copy()
        df["raw"] = df["raw"].apply(lambda x: json.dumps(x, ensure_ascii=False))

    df.to_parquet(out_fn, index=False)
    print(f"Wrote {len(df)} rows to {out_fn}")
    return df


def _safe_get_arrivals(naptan: str) -> List[Dict[str, Any]]:
    # small wrapper to catch and retry in short loop if transient
    for attempt in range(3):
        try:
            return get_arrivals_for_stop(naptan)
        except Exception as e:
            if attempt < 2:
                time.sleep(1 + attempt)
                continue
            raise


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Align TfL arrivals for a line into a single parquet snapshot")
    p.add_argument("line", help="Line name or id to align (e.g. 'Central')")
    p.add_argument("--outdir", default="data/bronze", help="Directory to write output parquet")
    p.add_argument("--workers", type=int, default=8, help="Number of threads to fetch arrivals")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    try:
        df = align_line_across_subscriptions(args.line, outdir=args.outdir, workers=args.workers)
        print(df.head().to_string(index=False) if not df.empty else "No rows returned")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

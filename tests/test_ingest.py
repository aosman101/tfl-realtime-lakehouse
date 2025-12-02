import importlib.util
import sys
import types
from pathlib import Path

import pyarrow.parquet as pq
import pytest


def _load_ingest_module():
    """Load tfl_ingest_dag directly from its path to avoid requiring Airflow."""
    path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "tfl_ingest_dag.py"
    spec = importlib.util.spec_from_file_location("tfl_ingest_under_test", path)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module  # allow intra-module imports if any
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_fetch_and_write_requires_stop_ids(monkeypatch, tmp_path):
    monkeypatch.setenv("TFL_STOPPOINT_IDS", "")
    monkeypatch.setenv("RAW_ARRIVALS_DIR", str(tmp_path))
    mod = _load_ingest_module()

    with pytest.raises(RuntimeError):
        mod.fetch_and_write()


def test_fetch_and_write_writes_parquet(monkeypatch, tmp_path):
    monkeypatch.setenv("TFL_STOPPOINT_IDS", "490000001A")
    monkeypatch.setenv("RAW_ARRIVALS_DIR", str(tmp_path))
    mod = _load_ingest_module()

    payload = [
        {
            "naptanId": "490000001A",
            "lineId": "central",
            "platformName": "1",
            "destinationName": "Liverpool Street",
            "timeToStation": 120,
            "timestamp": "2024-01-01T00:00:00Z",
        }
    ]

    class DummyResponse:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return self._data

    def fake_get(url, params=None, headers=None, timeout=None):  # noqa: ARG001
        return DummyResponse(payload)

    monkeypatch.setattr(mod, "_session", types.SimpleNamespace(get=fake_get))

    mod.fetch_and_write()

    files = list(tmp_path.rglob("arrivals_*.parquet"))
    assert files, "Expected a parquet file to be written"

    table = pq.read_table(files[0])
    assert table.num_rows == len(payload)
    assert {"stopId", "lineId", "timestamp"} <= set(table.column_names)

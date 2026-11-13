"""Shared fixtures: isolated SQLite files + in-process worker per test."""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app import config
from app.main import create_app

T0 = 1_731_400_000


@pytest.fixture()
def env(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(config, "DATA_DIR", data_dir.resolve())
    monkeypatch.setattr(config, "DB_PATH", (data_dir / "portal.db").resolve())
    monkeypatch.setattr(config, "WORK_DIR", (data_dir / "work").resolve())
    monkeypatch.setattr(config, "POLL_INTERVAL_SECONDS", 0.05)
    return data_dir


@pytest.fixture()
def client(env, monkeypatch):
    # The worker runs as a task inside the application's own event loop.
    monkeypatch.setattr(config, "RUN_WORKER_IN_PROCESS", True)
    app = create_app()
    with TestClient(app) as c:
        yield c


def ingest(c, logs):
    return c.post("/logs", json={"logs": logs}).raise_for_status().json()


def wait_idle(c, visitor_id, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = c.get(f"/visitors/{visitor_id}/sessions").raise_for_status().json()
        if not resp["pending_rebuild"]:
            return resp
        time.sleep(0.05)
    raise AssertionError("rebuild did not finish in time")

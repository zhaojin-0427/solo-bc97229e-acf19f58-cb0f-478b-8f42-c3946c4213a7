"""Offline smoke test using only the Python standard library.

The real test suite (tests/) needs FastAPI/aiosqlite/pytest, which are
installed from requirements.txt inside Docker. This script verifies the
reconstruction rules and the SQLite-backed CAS publish flow with stdlib
sqlite3 only, so it can run in an air-gapped environment.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.sessions import reconstruct_sessions  # noqa: E402
from app import analytics  # noqa: E402  (stdlib-only imports; aiosqlite is lazy)

T0 = 1_731_400_000
failures = []


def check(name: str, cond: bool, detail="") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


# ---------------------------------------------------------------- pure rules
events = [
    {"log_id": "l1", "ts": T0, "action": "visit", "page_path": "/home"},
    {"log_id": "l2", "ts": T0 + 60, "action": "visit", "page_path": "/news"},
    {"log_id": "l3", "ts": T0 + 120, "action": "exit", "page_path": None},
    {"log_id": "l4", "ts": T0 + 3000, "action": "visit", "page_path": "/later"},
]
sessions = reconstruct_sessions(events)
check("exit splits and is excluded", len(sessions) == 2
      and sessions[0]["page_paths"] == ["/home", "/news"]
      and sessions[1]["page_paths"] == ["/later"])

gap_events = [
    {"log_id": "a", "ts": T0, "action": "visit", "page_path": "/a"},
    {"log_id": "b", "ts": T0 + 1800, "action": "visit", "page_path": "/b"},
    {"log_id": "c", "ts": T0 + 3601, "action": "visit", "page_path": "/c"},
]
gs = reconstruct_sessions(gap_events)
check(">30m gap splits, ==30m stays", len(gs) == 2 and gs[0]["page_paths"] == ["/a", "/b"])

check("lone exit -> empty", reconstruct_sessions(
    [{"log_id": "x", "ts": T0, "action": "exit", "page_path": None}]) == [])

interleaved = [
    {"log_id": "z", "ts": T0 + 1, "action": "visit", "page_path": "/tab2"},
    {"log_id": "a", "ts": T0 + 1, "action": "visit", "page_path": "/tab1"},
    {"log_id": "m", "ts": T0, "action": "visit", "page_path": "/home"},
]
check("(ts, log_id) ordering merges tabs",
      reconstruct_sessions(interleaved)[0]["page_paths"] == ["/home", "/tab1", "/tab2"])

# --------------------------------------------------- CAS publish simulation
with tempfile.TemporaryDirectory() as tmp:
    db = Path(tmp) / "portal.db"
    conn = sqlite3.connect(db, isolation_level=None)
    conn.executescript(
        """
        CREATE TABLE raw_logs (log_id TEXT PRIMARY KEY, visitor_id TEXT NOT NULL,
            ts INTEGER NOT NULL, action TEXT NOT NULL, page_path TEXT);
        CREATE TABLE visitor_versions (visitor_id TEXT PRIMARY KEY, version INTEGER NOT NULL);
        CREATE TABLE pending_rebuilds (visitor_id TEXT PRIMARY KEY, target_version INTEGER NOT NULL,
            requested_at INTEGER NOT NULL, status TEXT NOT NULL);
        CREATE TABLE sessions (id INTEGER PRIMARY KEY AUTOINCREMENT, visitor_id TEXT NOT NULL,
            version INTEGER NOT NULL, session_index INTEGER NOT NULL, start_ts INTEGER NOT NULL,
            end_ts INTEGER NOT NULL, page_paths TEXT NOT NULL,
            UNIQUE(visitor_id, version, session_index));
        CREATE TABLE published_generations (visitor_id TEXT NOT NULL, version INTEGER NOT NULL,
            published_at INTEGER NOT NULL, session_count INTEGER NOT NULL,
            PRIMARY KEY (visitor_id, version));
        """
    )

    def publish(visitor_id: str, target_version: int, new_sessions: list) -> None:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT version FROM visitor_versions WHERE visitor_id=?",
                           (visitor_id,)).fetchone()
        current = row[0] if row else 0
        if current != target_version:
            conn.execute("ROLLBACK")
            raise RuntimeError("stale")
        conn.execute("DELETE FROM pending_rebuilds WHERE visitor_id=?", (visitor_id,))
        conn.executemany(
            "INSERT INTO sessions(visitor_id, version, session_index, start_ts, end_ts, page_paths)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            [(visitor_id, target_version, i, s["start_ts"], s["end_ts"],
              json.dumps(s["page_paths"])) for i, s in enumerate(new_sessions)])
        conn.execute(
            "INSERT INTO published_generations VALUES (?, ?, ?, ?)",
            (visitor_id, target_version, 0, len(new_sessions)))
        conn.execute(
            "DELETE FROM published_generations WHERE visitor_id=? AND version NOT IN "
            "(SELECT version FROM published_generations WHERE visitor_id=? "
            "ORDER BY version DESC LIMIT 2)",
            (visitor_id, visitor_id))
        conn.execute(
            "DELETE FROM sessions WHERE visitor_id=? AND version NOT IN "
            "(SELECT version FROM published_generations WHERE visitor_id=?)",
            (visitor_id, visitor_id))
        conn.commit()

    # v1 ingest + publish
    conn.execute("INSERT INTO raw_logs VALUES ('l1','v1',?,'visit','/a')", (T0,))
    conn.execute("INSERT INTO visitor_versions VALUES ('v1',1)")
    conn.execute("INSERT INTO pending_rebuilds VALUES ('v1',1,0,'processing')")
    publish("v1", 1, reconstruct_sessions(
        [{"log_id": "l1", "ts": T0, "action": "visit", "page_path": "/a"}]))

    # correction bumps version while an old (v1) rebuild is still in flight
    conn.execute("UPDATE raw_logs SET page_path='/b' WHERE log_id='l1'")
    conn.execute("UPDATE visitor_versions SET version=version+1 WHERE visitor_id='v1'")
    stale_blocked = False
    try:
        publish("v1", 1, [])  # late computation targeting the old version
    except RuntimeError:
        stale_blocked = True
    check("late computation rejected by CAS", stale_blocked)

    # the up-to-date computation publishes and old generation is retained
    publish("v1", 2, reconstruct_sessions(
        [{"log_id": "l1", "ts": T0, "action": "visit", "page_path": "/b"}]))
    versions = [r[0] for r in conn.execute(
        "SELECT DISTINCT version FROM sessions WHERE visitor_id='v1' ORDER BY version DESC")]
    check("two generations retained", versions == [2, 1])
    pending = conn.execute("SELECT COUNT(*) FROM pending_rebuilds WHERE visitor_id='v1'").fetchone()[0]
    check("queue row removed after publish", pending == 0)

    # visitor reassignment: both sides versioned; old owner becomes empty
    conn.execute("UPDATE raw_logs SET visitor_id='v2' WHERE log_id='l1'")
    conn.execute("INSERT INTO visitor_versions VALUES('v1',3) ON CONFLICT(visitor_id)"
                 " DO UPDATE SET version=version+1")
    conn.execute("INSERT INTO visitor_versions VALUES('v2',1) ON CONFLICT(visitor_id)"
                 " DO UPDATE SET version=version+1")
    publish("v1", 3, [])
    check("empty generation is still published",
          conn.execute("SELECT session_count FROM published_generations "
                       "WHERE visitor_id='v1' AND version=3").fetchone()[0] == 0)
    check("empty generation has zero session rows",
          conn.execute("SELECT COUNT(*) FROM sessions WHERE visitor_id='v1' AND version=3")
          .fetchone()[0] == 0)
    conn.close()

# --------------------------------------------------- analytics sandbox engine
# execute_analyst_query is pure stdlib: in-memory sandbox DB, authorizer,
# progress-handler deadline. Verify the security properties offline.
_snapshot = [
    ("visitor-7", 2, 0, T0, T0 + 60, '["/news/ai-2026", "/products/42"]'),
    ("visitor-7", 2, 1, T0 + 4000, T0 + 4000, '["/news/llm"]'),
    ("visitor-9", 1, 0, T0 + 10, T0 + 70, '["/news/ai-2026"]'),
]
_cancel = threading.Event()
_deadline = time.monotonic() + 10

cols, rows, truncated = analytics.execute_analyst_query(
    "SELECT visitor_id, COUNT(*) AS sessions, SUM(end_ts - start_ts) AS secs "
    "FROM current_sessions GROUP BY visitor_id ORDER BY visitor_id",
    _snapshot, deadline=_deadline, cancel_event=_cancel, max_rows=1000)
check("analytics aggregation",
      cols == ["visitor_id", "sessions", "secs"]
      and rows == [["visitor-7", 2, 60], ["visitor-9", 1, 60]])

_, rows, _ = analytics.execute_analyst_query(
    "SELECT je.value AS page, COUNT(*) FROM current_sessions s "
    "JOIN json_each(s.page_paths) je GROUP BY je.value ORDER BY je.value",
    _snapshot, deadline=_deadline, cancel_event=_cancel, max_rows=1000)
check("json_each expansion", rows == [["/news/ai-2026", 2], ["/news/llm", 1],
                                      ["/products/42", 1]])

_, rows, _ = analytics.execute_analyst_query(
    "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x<3) "
    "SELECT COUNT(*) FROM seq",
    _snapshot, deadline=_deadline, cancel_event=_cancel, max_rows=1000)
check("WITH RECURSIVE + COUNT(*) over CTE", rows == [[3]])

_, rows, truncated = analytics.execute_analyst_query(
    "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x<5000) "
    "SELECT x FROM seq",
    _snapshot, deadline=_deadline, cancel_event=_cancel, max_rows=1000)
check("row cap truncates at 1000", len(rows) == 1000 and truncated)


def _expect(exc_type, sql):
    try:
        analytics.execute_analyst_query(
            sql, _snapshot, deadline=_deadline, cancel_event=_cancel, max_rows=10)
    except exc_type:
        return True
    except Exception as e:  # wrong error type
        print(f"    (unexpected {type(e).__name__}: {e})")
        return False
    return False


check("raw_logs read refused", _expect(analytics.ForbiddenError,
                                       "SELECT * FROM raw_logs"))
check("old-version table refused", _expect(analytics.ForbiddenError,
                                           "SELECT * FROM sessions"))
check("schema read refused", _expect(analytics.ForbiddenError,
                                     "SELECT sql FROM sqlite_master"))
check("subquery equally restricted",
      _expect(analytics.ForbiddenError,
              "SELECT * FROM current_sessions WHERE visitor_id IN "
              "(SELECT visitor_id FROM raw_logs)"))
check("load_extension refused", _expect(analytics.ForbiddenError,
                                        "SELECT load_extension('/tmp/x')"))
check("write refused", _expect(analytics.AnalyticsError,
                               "DELETE FROM current_sessions"))
check("attach refused", _expect(analytics.AnalyticsError,
                                "ATTACH DATABASE '/tmp/x.db' AS stolen"))
check("non-select refused", _expect(analytics.BadQueryError, "PRAGMA user_version"))

# timeout: a huge recursive scan must be interrupted at the deadline
late = time.monotonic() + 0.2
started = time.monotonic()
timed_out = False
try:
    analytics.execute_analyst_query(
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq "
        "WHERE x<1000000000) SELECT SUM(x) FROM seq",
        _snapshot, deadline=late, cancel_event=threading.Event(), max_rows=10)
except analytics.QueryTimeoutError:
    timed_out = True
check("deadline terminates execution", timed_out and time.monotonic() - started < 5)

print()
if failures:
    print(f"{len(failures)} FAILURE(S): {failures}")
    sys.exit(1)
print("all stdlib smoke checks passed")

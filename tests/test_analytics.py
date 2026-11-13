"""Tests for the read-only analytics SQL endpoint (/analytics/query)."""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import Request

from app import analytics, config

from .conftest import T0, ingest as _ingest, wait_idle as _wait_idle


def _query(c, sql):
    return c.post("/analytics/query", json={"sql": sql})


def _seed_two_visitors(c):
    """visitor-7: two sessions (one with an exit, one after a >30m gap);
    visitor-9: one session. Returns after everything is published."""
    _ingest(c, [
        {"log_id": "a1", "visitor_id": "visitor-7", "ts": T0,
         "action": "visit", "page_path": "/news/ai-2026"},
        {"log_id": "a2", "visitor_id": "visitor-7", "ts": T0 + 60,
         "action": "visit", "page_path": "/products/42"},
        {"log_id": "a3", "visitor_id": "visitor-7", "ts": T0 + 90, "action": "exit"},
        {"log_id": "a4", "visitor_id": "visitor-7", "ts": T0 + 4000,
         "action": "visit", "page_path": "/news/llm"},
        {"log_id": "b1", "visitor_id": "visitor-9", "ts": T0 + 10,
         "action": "visit", "page_path": "/news/ai-2026"},
        {"log_id": "b2", "visitor_id": "visitor-9", "ts": T0 + 70,
         "action": "visit", "page_path": "/pricing"},
    ])
    _wait_idle(c, "visitor-7")
    _wait_idle(c, "visitor-9")


# --------------------------------------------------------------------- happy path

def test_basic_aggregation_over_current_sessions(client):
    _seed_two_visitors(client)
    resp = _query(client, """
        SELECT visitor_id, COUNT(*) AS sessions,
               SUM(end_ts - start_ts) AS total_seconds
        FROM current_sessions
        GROUP BY visitor_id
        ORDER BY visitor_id
    """)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["columns"] == ["visitor_id", "sessions", "total_seconds"]
    assert body["rows"] == [["visitor-7", 2, 60], ["visitor-9", 1, 60]]
    assert body["row_count"] == 2
    assert body["truncated"] is False


def test_column_order_and_types(client):
    _seed_two_visitors(client)
    body = _query(client, """
        SELECT visitor_id, version, session_index, start_ts, end_ts, page_paths
        FROM current_sessions ORDER BY visitor_id, session_index
    """).json()
    assert body["columns"] == [
        "visitor_id", "version", "session_index", "start_ts", "end_ts", "page_paths"
    ]
    first = body["rows"][0]
    assert first[0] == "visitor-7"
    assert first[1] == 1  # published version
    assert first[2] == 0  # session_index
    assert first[3] == T0 and first[4] == T0 + 60
    assert first[5] == '["/news/ai-2026", "/products/42"]'  # JSON text


def test_json_expansion_and_page_set_filter(client):
    """The analyst use case: pick a page combination, count sessions/seconds."""
    _seed_two_visitors(client)
    body = _query(client, """
        SELECT COUNT(*) AS sessions, SUM(end_ts - start_ts) AS total_seconds
        FROM current_sessions AS s
        WHERE EXISTS (
            SELECT 1 FROM json_each(s.page_paths) AS je
            WHERE je.value IN ('/news/ai-2026', '/news/llm')
        )
    """).json()
    # visitor-7: 60s session + 0s single-visit session; visitor-9: 60s session.
    assert body["rows"] == [[3, 120]]

    per_page = _query(client, """
        SELECT je.value AS page_path, COUNT(*) AS visits
        FROM current_sessions AS s
        JOIN json_each(s.page_paths) AS je
        GROUP BY je.value ORDER BY visits DESC, page_path
    """).json()
    assert per_page["rows"][0] == ["/news/ai-2026", 2]


def test_with_clause_and_window_functions(client):
    _seed_two_visitors(client)
    body = _query(client, """
        WITH per_visitor AS (
            SELECT visitor_id, COUNT(*) AS sessions
            FROM current_sessions GROUP BY visitor_id
        )
        SELECT visitor_id, sessions,
               RANK() OVER (ORDER BY sessions DESC) AS rnk
        FROM per_visitor ORDER BY rnk, visitor_id
    """).json()
    assert body["rows"][0] == ["visitor-7", 2, 1]


def test_scalar_and_datetime_functions(client):
    _seed_two_visitors(client)
    body = _query(client, """
        SELECT visitor_id,
               strftime('%Y-%m-%d', start_ts, 'unixepoch') AS day,
               upper(substr(page_paths, 2, 5)),
               round(AVG(end_ts - start_ts), 1)
        FROM current_sessions GROUP BY visitor_id ORDER BY visitor_id
    """).json()
    assert body["rows"][0][1] == time.strftime("%Y-%m-%d", time.gmtime(T0))
    assert body["rows"][0][2] == '"/NEW'  # first chars of the JSON text, upper-cased


def test_json_arrow_operators(client):
    _seed_two_visitors(client)
    body = _query(client, """
        SELECT page_paths -> 0 AS first_raw, page_paths ->> 0 AS first_page
        FROM current_sessions ORDER BY visitor_id, session_index
    """).json()
    assert body["rows"][0] == ['"/news/ai-2026"', "/news/ai-2026"]


def test_only_current_version_is_visible(client):
    """Older published generations must never leak into current_sessions."""
    _seed_two_visitors(client)
    # Correct twice -> versions 2 and 3 are published; version 1 pruned.
    _ingest(client, [{"log_id": "a4", "visitor_id": "visitor-7", "ts": T0 + 5000,
                      "action": "visit", "page_path": "/news/llm"}])
    _wait_idle(client, "visitor-7")
    _ingest(client, [{"log_id": "a4", "visitor_id": "visitor-7", "ts": T0 + 6000,
                      "action": "visit", "page_path": "/news/llm"}])
    final = _wait_idle(client, "visitor-7")
    assert final["current_version"] == 3

    body = _query(client,
                  "SELECT DISTINCT version FROM current_sessions "
                  "WHERE visitor_id = 'visitor-7'").json()
    assert body["rows"] == [[3]]  # never versions 1 or 2


def test_empty_snapshot_returns_zero_rows(client):
    body = _query(client, "SELECT COUNT(*) AS n FROM current_sessions").json()
    assert body["rows"] == [[0]]
    body = _query(client, "SELECT * FROM current_sessions").json()
    assert body["columns"] == [
        "visitor_id", "version", "session_index", "start_ts", "end_ts", "page_paths"
    ]
    assert body["rows"] == [] and body["truncated"] is False


def test_query_works_while_rebuild_pending(client):
    """During backfill + background publishing, analytics still answers from
    the last consistent publish snapshot."""
    _seed_two_visitors(client)
    _ingest(client, [{"log_id": "c1", "visitor_id": "visitor-7", "ts": T0 + 8000,
                      "action": "visit", "page_path": "/blog"}])
    # The correction is queued but maybe not published yet: either way the
    # analytics answer must be a single consistent snapshot (v1 or v2).
    resp = _query(client, "SELECT DISTINCT version FROM current_sessions "
                          "WHERE visitor_id = 'visitor-7'")
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 1  # one snapshot, never a mix


# --------------------------------------------------------------------- limits

def test_row_cap_and_truncation_flag(client):
    big = _query(client, """
        WITH RECURSIVE seq(x) AS (
            SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 5000
        )
        SELECT x FROM seq
    """)
    assert big.status_code == 200
    body = big.json()
    assert body["row_count"] == 1000
    assert body["truncated"] is True
    assert body["rows"][-1] == [1000]

    small = _query(client, """
        WITH RECURSIVE seq(x) AS (
            SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 3
        )
        SELECT x FROM seq
    """).json()
    assert small["row_count"] == 3 and small["truncated"] is False


def test_timeout_terminates_query_and_returns_error(client, monkeypatch):
    monkeypatch.setattr(config, "ANALYTICS_TIMEOUT_SECONDS", 0.3)
    started = time.monotonic()
    resp = _query(client, """
        WITH RECURSIVE seq(x) AS (
            SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 1000000000
        )
        SELECT SUM(x) FROM seq
    """)
    elapsed = time.monotonic() - started
    assert resp.status_code == 504
    assert "budget" in resp.json()["detail"]
    assert elapsed < 5  # actually terminated, not just reported

    # A normal query right afterwards is unaffected.
    ok = _query(client, "SELECT 1 AS one")
    assert ok.status_code == 200 and ok.json()["rows"] == [[1]]


def test_parallel_queries_and_ingestion_stay_responsive(client):
    _seed_two_visitors(client)
    slow_sql = """
        WITH RECURSIVE seq(x) AS (
            SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 200000
        )
        SELECT COUNT(*) FROM seq
    """
    with ThreadPoolExecutor(max_workers=4) as pool:
        slow = pool.submit(_query, client, slow_sql)
        fast1 = pool.submit(_query, client, "SELECT COUNT(*) AS n FROM current_sessions")
        write = pool.submit(_ingest, client, [
            {"log_id": "p1", "visitor_id": "visitor-5", "ts": T0,
             "action": "visit", "page_path": "/home"}
        ])
        fast2 = pool.submit(_query, client, "SELECT 42 AS answer")
        for resp in (slow.result(), fast1.result(), fast2.result()):
            assert resp.status_code == 200, resp.text
        assert write.result()["inserted"] == 1
    assert fast2.result().json()["rows"] == [[42]]


# --------------------------------------------------------------------- refusal

@pytest.mark.parametrize("sql", [
    "SELECT * FROM raw_logs",                     # raw logs
    "SELECT * FROM sessions",                     # old generations live here
    "SELECT * FROM published_generations",
    "SELECT * FROM visitor_versions",
    "SELECT * FROM pending_rebuilds",
    "SELECT name, sql FROM sqlite_master",        # schema
    "SELECT * FROM sqlite_schema",
    "SELECT * FROM main.sessions",                # schema-qualified still denied
    "SELECT * FROM pragma_table_info('current_sessions')",  # schema via vtab
    "SELECT * FROM dbstat",
])
def test_forbidden_tables_are_rejected(client, sql):
    _seed_two_visitors(client)
    resp = _query(client, sql)
    assert resp.status_code == 403, (sql, resp.text)
    assert "detail" in resp.json()


def test_forbidden_via_subquery_and_cte(client):
    """Subqueries are restricted exactly like the outer query."""
    _seed_two_visitors(client)
    resp = _query(client, """
        SELECT * FROM current_sessions
        WHERE visitor_id IN (SELECT visitor_id FROM raw_logs)
    """)
    assert resp.status_code == 403

    resp = _query(client, "WITH x AS (SELECT * FROM sessions) SELECT * FROM x")
    assert resp.status_code == 403


@pytest.mark.parametrize("fn", [
    "load_extension('/tmp/evil')",
    "sqlite_version()",
    "random()",
    "randomblob(16)",
    "readfile('/etc/passwd')",
    "writefile('/tmp/x', 'y')",
    "last_insert_rowid()",
])
def test_forbidden_functions_are_rejected(client, fn):
    resp = _query(client, f"SELECT {fn}")
    assert resp.status_code == 403, (fn, resp.text)


@pytest.mark.parametrize("sql", [
    "INSERT INTO current_sessions VALUES ('x', 1, 0, 0, 0, '[]')",
    "UPDATE current_sessions SET visitor_id = 'x'",
    "DELETE FROM current_sessions",
    "DROP TABLE current_sessions",
    "CREATE TABLE hacked (a)",
    "ATTACH DATABASE '/data/portal.db' AS stolen",
    "PRAGMA table_info(current_sessions)",
    "VACUUM",
    "WITH x AS (SELECT 1) INSERT INTO current_sessions VALUES ('x', 1, 0, 0, 0, '[]')",
])
def test_writes_attach_and_pragma_are_rejected(client, sql):
    resp = _query(client, sql)
    assert resp.status_code in (400, 403), (sql, resp.text)


def test_multiple_statements_are_rejected(client):
    resp = _query(client, "SELECT 1; SELECT 2")
    assert resp.status_code == 400
    resp = _query(client, "SELECT 1; DROP TABLE current_sessions")
    assert resp.status_code == 400


def test_syntax_error_is_a_clean_400(client):
    resp = _query(client, "SELECT FROM WHERE")
    assert resp.status_code == 400
    assert "detail" in resp.json()


# --------------------------------------------------------------------- cancellation

def test_cancel_event_aborts_the_actual_computation():
    """Unit level: setting the cancel event stops a running query quickly."""
    cancel = threading.Event()
    deadline = time.monotonic() + 30
    threading.Timer(0.1, cancel.set).start()
    started = time.monotonic()
    with pytest.raises(analytics.QueryAbortedError):
        analytics.execute_analyst_query(
            "WITH RECURSIVE seq(x) AS ("
            "  SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 1000000000"
            ") SELECT SUM(x) FROM seq",
            snapshot_rows=[("v1", 1, 0, 0, 10, '["/a"]')],
            deadline=deadline,
            cancel_event=cancel,
            max_rows=1000,
        )
    assert time.monotonic() - started < 5


def test_client_disconnect_aborts_query(client, monkeypatch):
    """When the HTTP client goes away the endpoint aborts the computation."""
    _seed_two_visitors(client)

    async def _always_disconnected(self):  # noqa: ANN001
        return True

    monkeypatch.setattr(Request, "is_disconnected", _always_disconnected)
    started = time.monotonic()
    resp = _query(client, """
        WITH RECURSIVE seq(x) AS (
            SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 1000000000
        )
        SELECT SUM(x) FROM seq
    """)
    assert resp.status_code == 499
    assert time.monotonic() - started < 5


# --------------------------------------------------------------------- sandbox unit checks

@pytest.mark.parametrize("sql,expected", [
    ("SELECT 1", set()),
    ("WITH t AS (SELECT 1) SELECT * FROM t", {"t"}),
    ("WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x<3) "
     "SELECT * FROM seq", {"seq"}),
    ("WITH a AS (WITH b AS (SELECT 1) SELECT * FROM b) SELECT * FROM a", {"a", "b"}),
    ('WITH "My Cte" AS (SELECT 1) SELECT * FROM "My Cte"', {"my cte"}),
    ("WITH `tick` AS (SELECT 1), [brack et](x) AS (SELECT 2) SELECT * FROM tick, "
     "[brack et]", {"tick", "brack et"}),
    # strings/comments that merely look like CTE definitions are ignored
    ("SELECT 'with fake as (' -- with nope as (\nSELECT 1", set()),
    ("/* with nope as ( */ WITH real AS (SELECT 1) SELECT * FROM real", {"real"}),
    # WITH inside a scalar subquery
    ("SELECT (WITH t AS (SELECT 1) SELECT * FROM t)", {"t"}),
])
def test_cte_name_extraction(sql, expected):
    assert analytics.extract_cte_names(sql) == expected


def test_count_star_over_cte_is_allowed():
    """COUNT(*) fires a READ authorization against the CTE name itself."""
    cancel = threading.Event()
    deadline = time.monotonic() + 5
    columns, out, _ = analytics.execute_analyst_query(
        "WITH t AS (SELECT visitor_id FROM current_sessions) "
        "SELECT COUNT(*) FROM t",
        [("v1", 1, 0, 0, 10, '["/a"]')],
        deadline=deadline, cancel_event=cancel, max_rows=1000,
    )
    assert columns == ["COUNT(*)"] and out == [[1]]


def test_sandbox_executor_directly():
    rows = [
        ("v1", 2, 0, 100, 160, '["/a", "/b"]'),
        ("v1", 2, 1, 5000, 5060, '["/c"]'),
        ("v2", 1, 0, 100, 130, '["/a"]'),
    ]
    cancel = threading.Event()
    deadline = time.monotonic() + 5
    columns, out, truncated = analytics.execute_analyst_query(
        "SELECT visitor_id, COUNT(*) FROM current_sessions GROUP BY visitor_id "
        "ORDER BY visitor_id",
        rows, deadline=deadline, cancel_event=cancel, max_rows=1000,
    )
    assert columns == ["visitor_id", "COUNT(*)"]
    assert out == [["v1", 2], ["v2", 1]]
    assert truncated is False

    # json_each expansion works after the sandbox warmup
    _, out, _ = analytics.execute_analyst_query(
        "SELECT je.value FROM current_sessions s, json_each(s.page_paths) je "
        "WHERE s.visitor_id = 'v1' ORDER BY je.value",
        rows, deadline=deadline, cancel_event=cancel, max_rows=1000,
    )
    assert out == [["/a"], ["/b"], ["/c"]]

    # sqlite_master stays unreachable even though json_each was used
    with pytest.raises(analytics.ForbiddenError):
        analytics.execute_analyst_query(
            "SELECT sql FROM sqlite_master",
            rows, deadline=deadline, cancel_event=cancel, max_rows=1000,
        )

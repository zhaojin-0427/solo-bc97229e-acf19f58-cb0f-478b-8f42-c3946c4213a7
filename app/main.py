"""FastAPI application: log ingestion, correction and session queries."""
from __future__ import annotations

import asyncio
import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

import aiosqlite
from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from . import analytics, config
from .analytics import AnalyticsError
from .config import ensure_dirs
from .db import connect, init_db
from .schemas import (
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
    IngestReport,
    IngestRequest,
    IngestResponse,
    SessionOut,
    VisitorSessionsResponse,
    VisitorVersionOut,
)
from .worker import RebuildWorker


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def create_app() -> FastAPI:
    ensure_dirs()
    app = FastAPI(
        title="Portal Reading-Session Reconstruction API",
        version="1.0.0",
        description=(
            "Batch ingestion of portal access logs, idempotent corrections by "
            "log_id, and reading-session reconstruction in the background. "
            "Sessions split on an exit event or a gap greater than 30 minutes."
        ),
    )

    state: dict[str, Any] = {"db": None, "worker": None}

    @app.exception_handler(AnalyticsError)
    async def _analytics_error_handler(_request: Request, exc: AnalyticsError):
        return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})

    @app.on_event("startup")
    async def _startup() -> None:
        state["db"] = await connect()
        await init_db(state["db"])
        if config.RUN_WORKER_IN_PROCESS:
            state["worker"] = RebuildWorker()
            await state["worker"].start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        if state["worker"] is not None:
            await state["worker"].stop()
        if state["db"] is not None:
            await state["db"].close()

    @app.get("/", include_in_schema=False)
    async def root() -> dict[str, str]:
        return {"service": "session-reconstruction", "docs": "/docs"}

    @app.get("/health")
    async def health() -> dict[str, Any]:
        db: aiosqlite.Connection = state["db"]
        (one,) = (await db.execute_fetchall("SELECT 1"))[0]
        return {"status": "ok", "db": "ok" if one == 1 else "error"}

    # ------------------------------------------------------------------ ingestion
    @app.post("/logs", response_model=IngestResponse, tags=["logs"])
    async def ingest(payload: IngestRequest) -> IngestResponse:
        """Accept a batch of raw logs.

        Re-submitting a known ``log_id`` corrects it. If the visitor changes,
        both the old and the new visitor are version-bumped and rebuilt, so
        both sides stay consistent. Every change is durable and queued before
        this endpoint returns; the rebuild itself runs in the background and
        never blocks ingestion or reads.
        """
        db: aiosqlite.Connection = state["db"]
        now = int(time.time())

        inserted = corrected = unchanged = 0
        report: list[IngestReport] = []
        # visitor -> newest effective (log_id, payload) for this batch, so a
        # log_id repeated inside one batch ends with the last value.
        affected: dict[str, set[str]] = {}
        requeued_visitors: set[str] = set()

        await db.execute("BEGIN IMMEDIATE")
        try:
            for entry in payload.logs:
                existing = await db.execute_fetchall(
                    "SELECT log_id, visitor_id, ts, action, page_path "
                    "FROM raw_logs WHERE log_id = ?",
                    (entry.log_id,),
                )

                old_visitor: str | None = None
                outcome: str
                if existing:
                    old = existing[0]
                    old_visitor = old["visitor_id"]
                    same = (
                        old["visitor_id"] == entry.visitor_id
                        and old["ts"] == entry.ts
                        and old["action"] == entry.action
                        and (old["page_path"] or None)
                        == (entry.page_path.strip() if entry.page_path else None)
                    )
                    if same:
                        outcome = "unchanged"
                        unchanged += 1
                        report.append(
                            IngestReport(
                                log_id=entry.log_id,
                                visitor_id=entry.visitor_id,
                                outcome="unchanged",
                                old_visitor_id=old_visitor,
                            )
                        )
                        continue

                    await db.execute(
                        "UPDATE raw_logs SET visitor_id = ?, ts = ?, action = ?, "
                        "page_path = ? WHERE log_id = ?",
                        (
                            entry.visitor_id,
                            entry.ts,
                            entry.action,
                            entry.page_path.strip() if entry.page_path else None,
                            entry.log_id,
                        ),
                    )
                    outcome = "corrected"
                    corrected += 1
                else:
                    await db.execute(
                        "INSERT INTO raw_logs(log_id, visitor_id, ts, action, page_path) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            entry.log_id,
                            entry.visitor_id,
                            entry.ts,
                            entry.action,
                            entry.page_path.strip() if entry.page_path else None,
                        ),
                    )
                    outcome = "inserted"
                    inserted += 1

                report.append(
                    IngestReport(
                        log_id=entry.log_id,
                        visitor_id=entry.visitor_id,
                        outcome=outcome,  # type: ignore[arg-type]
                        old_visitor_id=old_visitor,
                    )
                )

                # Mark every visitor whose input changed: the new owner, and
                # the previous owner when the event was reassigned. Versions
                # are bumped once per visitor per batch (see below).
                touched = {entry.visitor_id}
                if old_visitor:
                    touched.add(old_visitor)
                for vid in touched:
                    affected.setdefault(vid, set()).add(entry.log_id)
                    requeued_visitors.add(vid)

            for vid in requeued_visitors:
                # One accepted batch moves a visitor's input version forward
                # by exactly one, no matter how many of its events changed.
                await db.execute(
                    "INSERT INTO visitor_versions(visitor_id, version) VALUES (?, 1) "
                    "ON CONFLICT(visitor_id) DO UPDATE SET version = version + 1",
                    (vid,),
                )
                ver_row = await db.execute_fetchall(
                    "SELECT version FROM visitor_versions WHERE visitor_id = ?", (vid,)
                )
                target_version = ver_row[0][0]
                await db.execute(
                    "INSERT INTO pending_rebuilds(visitor_id, target_version, "
                    "requested_at, status) VALUES (?, ?, ?, 'pending') "
                    "ON CONFLICT(visitor_id) DO UPDATE SET "
                    "target_version = excluded.target_version, "
                    "requested_at = excluded.requested_at, status = 'pending'",
                    (vid, target_version, now),
                )
            await db.execute("COMMIT")
        except Exception:
            await db.execute("ROLLBACK")
            raise

        affected_visitors = sorted(affected.keys())
        return IngestResponse(
            accepted=len(payload.logs),
            inserted=inserted,
            corrected=corrected,
            unchanged=unchanged,
            affected_visitors=affected_visitors,
            pending_rebuild=bool(requeued_visitors),
            report=report,
        )

    # ------------------------------------------------------------------ queries
    @app.get(
        "/visitors/{visitor_id}/sessions",
        response_model=VisitorSessionsResponse,
        tags=["sessions"],
    )
    async def visitor_sessions(visitor_id: str) -> VisitorSessionsResponse:
        """Read the complete newest two session generations of one visitor.

        A single read-only transaction is used. ``pending_rebuild`` marks
        whether the raw input has changes not yet reflected in the current
        published generation.
        """
        db: aiosqlite.Connection = state["db"]
        await db.execute("BEGIN")
        try:
            pending = await db.execute_fetchall(
                "SELECT 1 FROM pending_rebuilds WHERE visitor_id = ? LIMIT 1",
                (visitor_id,),
            )
            input_ver = await db.execute_fetchall(
                "SELECT version FROM visitor_versions WHERE visitor_id = ?",
                (visitor_id,),
            )
            input_version = input_ver[0][0] if input_ver else None

            # The generation list comes from published_generations so that
            # empty generations (zero sessions) are represented too.
            gen_rows = await db.execute_fetchall(
                "SELECT version FROM published_generations "
                "WHERE visitor_id = ? ORDER BY version DESC LIMIT 2",
                (visitor_id,),
            )
            kept_versions = [r["version"] for r in gen_rows]
            rows = []
            if kept_versions:
                placeholders = ",".join("?" for _ in kept_versions)
                rows = await db.execute_fetchall(
                    f"SELECT version, session_index, start_ts, end_ts, page_paths "
                    f"FROM sessions WHERE visitor_id = ? AND version IN ({placeholders}) "
                    f"ORDER BY version DESC, session_index ASC",
                    (visitor_id, *kept_versions),
                )
            await db.execute("COMMIT")
        except Exception:
            await db.execute("ROLLBACK")
            raise

        generations: dict[int, list[SessionOut]] = {v: [] for v in kept_versions}
        for r in rows:
            generations[r["version"]].append(
                SessionOut(
                    session_index=r["session_index"],
                    start_ts=r["start_ts"],
                    end_ts=r["end_ts"],
                    start_time=_iso(r["start_ts"]),
                    end_time=_iso(r["end_ts"]),
                    page_paths=json.loads(r["page_paths"]),
                )
            )

        gen_out = [
            VisitorVersionOut(version=version, sessions=sessions)
            for version, sessions in sorted(generations.items(), reverse=True)
        ]
        current_version = gen_out[0].version if gen_out else None
        return VisitorSessionsResponse(
            visitor_id=visitor_id,
            pending_rebuild=bool(pending),
            current_version=current_version,
            input_version=input_version,
            generations=gen_out,
        )

    # ------------------------------------------------------------------ analytics
    @app.post(
        "/analytics/query",
        response_model=AnalyticsQueryResponse,
        tags=["analytics"],
        responses={
            400: {"description": "Not a single SELECT/WITH statement or invalid SQL"},
            403: {"description": "Query stepped outside the current_sessions allowlist"},
            499: {"description": "Client disconnected; the query was aborted"},
            504: {"description": "Execution + fetching exceeded the 2 second budget"},
        },
    )
    async def analytics_query(
        request: Request,
        payload: AnalyticsQueryRequest = Body(
            ...,
            openapi_examples={
                "topic_reading_depth": {
                    "summary": "专题阅读深度：自选页面组合的会话数与持续秒数",
                    "value": {
                        "sql": "SELECT COUNT(*) AS sessions, "
                        "SUM(end_ts - start_ts) AS total_seconds "
                        "FROM current_sessions AS s "
                        "WHERE EXISTS (SELECT 1 FROM json_each(s.page_paths) AS je "
                        "WHERE je.value IN ('/news/ai-2026', '/products/42'))"
                    },
                },
                "per_visitor_stats": {
                    "summary": "每访客的会话数与总阅读秒数",
                    "value": {
                        "sql": "SELECT visitor_id, COUNT(*) AS sessions, "
                        "SUM(end_ts - start_ts) AS total_seconds "
                        "FROM current_sessions "
                        "GROUP BY visitor_id ORDER BY total_seconds DESC"
                    },
                },
                "per_page_stats": {
                    "summary": "逐页面访问统计（json_each 展开 page_paths）",
                    "value": {
                        "sql": "SELECT je.value AS page_path, COUNT(*) AS visits "
                        "FROM current_sessions AS s "
                        "JOIN json_each(s.page_paths) AS je "
                        "GROUP BY je.value ORDER BY visits DESC"
                    },
                },
                "forbidden_raw_logs": {
                    "summary": "越权示例：读取原始日志（被拒绝，403）",
                    "value": {"sql": "SELECT * FROM raw_logs"},
                },
                "forbidden_attach": {
                    "summary": "越权示例：ATTACH 附加库（被拒绝，400）",
                    "value": {"sql": "ATTACH DATABASE '/data/portal.db' AS stolen"},
                },
            },
        ),
    ) -> AnalyticsQueryResponse | Response:
        """Run one read-only SQL query over the ``current_sessions`` table.

        ``current_sessions`` exposes the newest published session generation
        of every visitor: ``visitor_id``, ``version``, ``session_index``,
        ``start_ts``, ``end_ts`` and ``page_paths`` (JSON array). A single
        SQLite ``SELECT`` (``WITH`` clauses and JSON functions such as
        ``json_extract`` / ``json_each`` supported) is executed against one
        consistent publish snapshot while ingestion and background publishing
        continue. At most 1000 rows are returned (``truncated`` flags more).
        Execution + fetching is capped at 2 seconds; exceeding queries are
        terminated and answered with 504. Reading raw logs, older versions,
        the schema or any other table — from subqueries as well — is rejected
        with 403, as are writes, ATTACH and disallowed functions.
        """
        cancel = threading.Event()
        deadline = time.monotonic() + config.ANALYTICS_TIMEOUT_SECONDS

        async def _run() -> AnalyticsQueryResponse:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise analytics.QueryTimeoutError(
                    "no time budget left before the query even started"
                )
            try:
                snapshot = await asyncio.wait_for(
                    analytics.extract_current_sessions(), timeout=remaining
                )
            except asyncio.TimeoutError as exc:
                raise analytics.QueryTimeoutError(
                    "extracting the publish snapshot exceeded the time budget"
                ) from exc
            columns, rows, truncated = await asyncio.to_thread(
                analytics.execute_analyst_query,
                payload.sql,
                snapshot,
                deadline=deadline,
                cancel_event=cancel,
                max_rows=config.ANALYTICS_MAX_ROWS,
            )
            return AnalyticsQueryResponse(
                columns=columns, rows=rows, row_count=len(rows), truncated=truncated
            )

        async def _watch_disconnect() -> None:
            while True:
                if await request.is_disconnected():
                    return
                await asyncio.sleep(0.05)

        query_task = asyncio.create_task(_run())
        disconnect_task = asyncio.create_task(_watch_disconnect())
        try:
            done, _ = await asyncio.wait(
                {query_task, disconnect_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if disconnect_task in done and not query_task.done():
                # The client went away: cancel the actual computation (the
                # SQLite progress handler interrupts the VM) and wait for the
                # worker thread to release its connection before responding.
                cancel.set()
                try:
                    await asyncio.wait_for(asyncio.shield(query_task), timeout=3.0)
                except Exception:
                    pass
                return Response(status_code=499)
            return query_task.result()
        finally:
            disconnect_task.cancel()
            if not query_task.done():
                cancel.set()

    @app.post("/admin/visitors/{visitor_id}/rebuild", tags=["admin"])
    async def force_rebuild(visitor_id: str) -> dict[str, Any]:
        """Re-queue one visitor for rebuilding (demo/operator helper)."""
        db: aiosqlite.Connection = state["db"]
        await db.execute("BEGIN IMMEDIATE")
        try:
            ver_row = await db.execute_fetchall(
                "SELECT version FROM visitor_versions WHERE visitor_id = ?",
                (visitor_id,),
            )
            if not ver_row:
                await db.execute("ROLLBACK")
                raise HTTPException(status_code=404, detail="unknown visitor_id")
            target_version = ver_row[0][0]
            await db.execute(
                "INSERT INTO pending_rebuilds(visitor_id, target_version, "
                "requested_at, status) VALUES (?, ?, ?, 'pending') "
                "ON CONFLICT(visitor_id) DO UPDATE SET "
                "target_version = excluded.target_version, "
                "requested_at = excluded.requested_at, status = 'pending'",
                (visitor_id, target_version, int(time.time())),
            )
            await db.execute("COMMIT")
        except HTTPException:
            raise
        except Exception:
            await db.execute("ROLLBACK")
            raise
        return {
            "visitor_id": visitor_id,
            "target_version": target_version,
            "status": "queued",
        }

    return app


app = create_app()

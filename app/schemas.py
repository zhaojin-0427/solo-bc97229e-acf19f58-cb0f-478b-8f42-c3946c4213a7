"""Pydantic request/response models."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class LogEntry(BaseModel):
    """One portal access event.

    ``ts`` is a UTC timestamp in whole seconds (Unix epoch). A ``visit`` must
    carry ``page_path``; an ``exit`` must not (the exit belongs to no page).
    The same ``log_id`` may be re-submitted to correct a previously accepted
    event, including reassigning it to another ``visitor_id``.
    """

    log_id: str = Field(..., min_length=1, examples=["log-1001"])
    visitor_id: str = Field(..., min_length=1, examples=["visitor-7"])
    ts: int = Field(..., ge=0, examples=[1731400000])
    action: Literal["visit", "exit"]
    page_path: str | None = Field(default=None, examples=["/news/ai-2026"])

    @model_validator(mode="after")
    def _check_page_path(self) -> "LogEntry":
        if self.action == "visit" and not (self.page_path and self.page_path.strip()):
            raise ValueError("page_path is required for a 'visit' event")
        if self.action == "exit" and self.page_path is not None:
            raise ValueError("an 'exit' event must not carry a page_path")
        if not self.log_id.strip() or not self.visitor_id.strip():
            raise ValueError("log_id and visitor_id must not be blank")
        return self


class IngestRequest(BaseModel):
    logs: list[LogEntry] = Field(..., min_length=1)


class IngestReport(BaseModel):
    log_id: str
    visitor_id: str
    outcome: Literal["inserted", "corrected", "unchanged"]
    old_visitor_id: str | None = None


class IngestResponse(BaseModel):
    accepted: int
    inserted: int
    corrected: int
    unchanged: int
    affected_visitors: list[str]
    pending_rebuild: bool
    report: list[IngestReport]


class SessionOut(BaseModel):
    session_index: int
    start_ts: int
    end_ts: int
    start_time: str
    end_time: str
    page_paths: list[str]


class VisitorVersionOut(BaseModel):
    version: int
    sessions: list[SessionOut]


class VisitorSessionsResponse(BaseModel):
    visitor_id: str
    pending_rebuild: bool
    current_version: int | None
    # Version of the raw input; equals current_version once the rebuild of the
    # latest correction has been published.
    input_version: int | None = None
    generations: list[VisitorVersionOut]


class AnalyticsQueryRequest(BaseModel):
    """One read-only SQL statement for the analytics endpoint.

    Must be a single SQLite ``SELECT`` (``WITH`` clauses allowed) reading
    only the ``current_sessions`` table and the JSON expansion of its
    ``page_paths`` column (``json_each`` / ``json_tree``).
    """

    sql: str = Field(
        ...,
        min_length=1,
        max_length=100_000,
        examples=[
            "SELECT visitor_id, COUNT(*) AS sessions, "
            "SUM(end_ts - start_ts) AS total_seconds "
            "FROM current_sessions GROUP BY visitor_id"
        ],
    )


class AnalyticsQueryResponse(BaseModel):
    # Column names of the result set, in order.
    columns: list[str]
    # Result rows as JSON value arrays aligned with ``columns``; at most
    # ANALYTICS_MAX_ROWS (1000) rows are returned.
    rows: list[list[Any]]
    # Number of rows actually returned (len(rows)).
    row_count: int
    # True when the query produced more rows than the 1000-row cap.
    truncated: bool

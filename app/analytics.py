"""Read-only analytics queries over the ``current_sessions`` snapshot.

The documentation team's analysts run ad-hoc SQLite ``SELECT`` statements
against a single logical table, ``current_sessions``:

    visitor_id TEXT, version INTEGER, session_index INTEGER,
    start_ts INTEGER, end_ts INTEGER, page_paths TEXT  -- JSON array

It contains exactly the newest published session generation of every
visitor (older generations, raw logs and all internal tables stay hidden).

Design
------

* **One publish snapshot per query.** Before the analyst SQL runs, the
  current generation of every visitor is extracted inside ONE read-only
  transaction on the official database. WAL readers never block the writer,
  so log ingestion and background publishing keep running while a query is
  prepared and executed.
* **Physical isolation.** The extracted rows are loaded into a private
  in-memory SQLite database that contains *only* ``current_sessions``.
  Analyst SQL runs there, so raw logs, old versions, the real schema and
  every other table are unreachable no matter what the query says.
* **Defense in depth inside the sandbox connection.**
    1. the statement must be a single ``SELECT`` / ``WITH ... SELECT``;
    2. a SQLite authorizer allows reads of ``current_sessions`` (including
       the ``json_each`` / ``json_tree`` expansion of ``page_paths``) and an
       allowlist of scalar / aggregate / JSON / window functions — every
       other table, function or operation (writes, DDL, ATTACH, PRAGMA,
       transactions, ``sqlite_master``, ...) is denied, in subqueries too;
    3. ``PRAGMA query_only = ON`` and ``SQLITE_LIMIT_ATTACHED = 0`` make the
       connection read-only and attach-proof even if the layers above had a
       hole;
    4. a progress handler aborts the running query as soon as the time
       budget is exhausted or the request was cancelled (e.g. the client
       disconnected), so the computation actually stops and the connection
       is released instead of lingering.
* **Bounded results.** At most ``max_rows`` rows are returned and the
  response says whether the result was truncated.

This module is importable with the standard library alone (``aiosqlite`` is
imported lazily inside the async extractor) so the offline smoke test can
exercise the sandbox executor without third-party packages.
"""
from __future__ import annotations

import math
import re
import sqlite3
import threading
import time
from typing import Any, Iterable, Sequence

from . import config

# --------------------------------------------------------------------------- errors


class AnalyticsError(Exception):
    """Base class for analytics failures; carries the HTTP status to return."""

    status_code = 500


class BadQueryError(AnalyticsError):
    """Malformed SQL, unsupported statement type, multiple statements, ..."""

    status_code = 400


class ForbiddenError(AnalyticsError):
    """The query stepped outside the current_sessions allowlist."""

    status_code = 403


class QueryTimeoutError(AnalyticsError):
    """Execution + fetching exceeded the configured time budget."""

    status_code = 504


class QueryAbortedError(AnalyticsError):
    """The request was cancelled (client disconnected) while running."""

    status_code = 499  # nginx-style "client closed request"


# --------------------------------------------------------------------------- schema

TABLE_NAME = "current_sessions"

SANDBOX_SCHEMA = """
CREATE TABLE current_sessions (
    visitor_id    TEXT    NOT NULL,
    version       INTEGER NOT NULL,
    session_index INTEGER NOT NULL,
    start_ts      INTEGER NOT NULL,
    end_ts        INTEGER NOT NULL,
    page_paths    TEXT    NOT NULL
)
"""

# The newest published generation per visitor, in one consistent read.
EXTRACTION_SQL = """
SELECT s.visitor_id, s.version, s.session_index, s.start_ts, s.end_ts, s.page_paths
FROM sessions AS s
JOIN (
    SELECT visitor_id, MAX(version) AS version
    FROM published_generations
    GROUP BY visitor_id
) AS cur ON cur.visitor_id = s.visitor_id AND cur.version = s.version
ORDER BY s.visitor_id, s.session_index
"""

# --------------------------------------------------------------------------- allowlists

# Tables the analyst may read. json_each / json_tree appear as table names in
# the authorizer when used as table-valued functions; expanding the JSON page
# sequence is explicitly allowed ("读取该表及其JSON展开").
READABLE_TABLES = frozenset({"current_sessions", "json_each", "json_tree"})

# Every function name here was verified to exist in the bundled SQLite
# (3.40.x). Anything not listed — load_extension, random, randomblob,
# readfile/writefile, sqlite_version, sqlite_source_id, changes,
# last_insert_rowid, fts*/rtree/zipfile helpers, ... — is denied.
ALLOWED_FUNCTIONS = frozenset(
    {
        # scalar core
        "abs", "char", "coalesce", "format", "glob", "hex", "ifnull", "iif",
        "instr", "length", "like", "likelihood", "likely", "lower", "ltrim",
        "max", "min", "nullif", "printf", "quote", "replace", "round",
        "rtrim", "sign", "soundex", "substr", "substring", "trim", "typeof",
        "unicode", "unlikely", "upper",
        # date & time
        "date", "time", "datetime", "julianday", "unixepoch", "strftime",
        # math
        "acos", "asin", "atan", "atan2", "ceil", "ceiling", "cos", "cosh",
        "degrees", "exp", "floor", "ln", "log", "log10", "log2", "mod", "pi",
        "pow", "power", "radians", "sin", "sinh", "sqrt", "tan", "tanh",
        "trunc",
        # aggregates
        "avg", "count", "group_concat", "sum", "total",
        # JSON (json_insert/replace/set/remove are pure value functions;
        # -> and ->> are the operator spellings of json_extract)
        "json", "json_array", "json_array_length", "json_extract",
        "json_insert", "json_object", "json_patch", "json_quote",
        "json_remove", "json_replace", "json_set", "json_type", "json_valid",
        "json_group_array", "json_group_object", "->", "->>",
        # window functions
        "row_number", "rank", "dense_rank", "percent_rank", "cume_dist",
        "ntile", "lag", "lead", "first_value", "last_value", "nth_value",
    }
)

_ACTION_NAMES = {
    sqlite3.SQLITE_INSERT: "INSERT",
    sqlite3.SQLITE_UPDATE: "UPDATE",
    sqlite3.SQLITE_DELETE: "DELETE",
    sqlite3.SQLITE_TRANSACTION: "BEGIN/COMMIT/ROLLBACK",
    sqlite3.SQLITE_ATTACH: "ATTACH",
    sqlite3.SQLITE_DETACH: "DETACH",
    sqlite3.SQLITE_PRAGMA: "PRAGMA",
    sqlite3.SQLITE_ALTER_TABLE: "ALTER TABLE",
    sqlite3.SQLITE_REINDEX: "REINDEX",
    sqlite3.SQLITE_ANALYZE: "ANALYZE",
    sqlite3.SQLITE_CREATE_VTABLE: "CREATE VIRTUAL TABLE",
    sqlite3.SQLITE_DROP_VTABLE: "DROP VIRTUAL TABLE",
    sqlite3.SQLITE_SAVEPOINT: "SAVEPOINT",
    sqlite3.SQLITE_CREATE_INDEX: "CREATE INDEX",
    sqlite3.SQLITE_CREATE_TABLE: "CREATE TABLE",
    sqlite3.SQLITE_CREATE_TEMP_INDEX: "CREATE TEMP INDEX",
    sqlite3.SQLITE_CREATE_TEMP_TABLE: "CREATE TEMP TABLE",
    sqlite3.SQLITE_CREATE_TEMP_TRIGGER: "CREATE TEMP TRIGGER",
    sqlite3.SQLITE_CREATE_TEMP_VIEW: "CREATE TEMP VIEW",
    sqlite3.SQLITE_CREATE_TRIGGER: "CREATE TRIGGER",
    sqlite3.SQLITE_CREATE_VIEW: "CREATE VIEW",
    sqlite3.SQLITE_DROP_INDEX: "DROP INDEX",
    sqlite3.SQLITE_DROP_TABLE: "DROP TABLE",
    sqlite3.SQLITE_DROP_TEMP_INDEX: "DROP TEMP INDEX",
    sqlite3.SQLITE_DROP_TEMP_TABLE: "DROP TEMP TABLE",
    sqlite3.SQLITE_DROP_TEMP_TRIGGER: "DROP TEMP TRIGGER",
    sqlite3.SQLITE_DROP_TEMP_VIEW: "DROP TEMP VIEW",
    sqlite3.SQLITE_DROP_TRIGGER: "DROP TRIGGER",
    sqlite3.SQLITE_DROP_VIEW: "DROP VIEW",
}

# --------------------------------------------------------------------------- authorizer


def _make_authorizer(denials: list[str], cte_names: frozenset[str]):
    """Build an authorizer that records WHY the first denial happened.

    ``cte_names`` are the (lower-cased) common table expression names declared
    by the query itself; reading them is of course fine. Everything that is
    neither a CTE nor an explicitly exposed table is denied — including
    ``sqlite_master`` and any table-valued function other than json_each /
    json_tree (those are blocked earlier, when their module resolution tries
    an internal sqlite_master write that the default-deny below rejects).
    """

    def authorizer(action: int, arg1: Any, arg2: Any, _db: Any, _src: Any) -> int:
        if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_RECURSIVE):
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_READ:
            # arg1 = table, arg2 = column ('' for COUNT(*)). CTE references
            # normally produce no READ at all, but COUNT(*) over a CTE does.
            if arg1 in READABLE_TABLES or (arg1 or "").lower() in cte_names:
                return sqlite3.SQLITE_OK
            denials.append(
                f"reading '{arg1}' is not allowed: only the current_sessions "
                "table (and its JSON expansion) is exposed"
            )
            return sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_FUNCTION:
            if (arg2 or "").lower() in ALLOWED_FUNCTIONS:
                return sqlite3.SQLITE_OK
            denials.append(f"function '{arg2}' is not in the analytics allowlist")
            return sqlite3.SQLITE_DENY
        name = _ACTION_NAMES.get(action, f"action #{action}")
        denials.append(
            f"{name} is not allowed: analytics queries are read-only SELECTs"
        )
        return sqlite3.SQLITE_DENY

    return authorizer


# --------------------------------------------------------------------------- validation

_LEADING_NOISE = re.compile(r"\s+|--[^\n]*(?:\n|$)|/\*.*?\*/", re.DOTALL)
_KEYWORD = re.compile(r"[A-Za-z]+")


def _first_keyword(sql: str) -> str:
    """First SQL keyword after leading whitespace/comments (UX-level check).

    This is NOT the security boundary — the authorizer is. It only produces a
    clear 400 for statements that are obviously not a single SELECT.
    """
    pos = 0
    while True:
        match = _LEADING_NOISE.match(sql, pos)
        if not match:
            break
        pos = match.end()
    keyword = _KEYWORD.match(sql, pos)
    return keyword.group(0).upper() if keyword else ""


_TOKEN = re.compile(
    r"""
      \s+                                      # whitespace
    | --[^\n]*(?:\n|$)                         # line comment
    | /\*.*?\*/                                # block comment
    | '(?:[^']|'')*'                           # string literal
    | (?P<qident>"(?:[^"]|"")*"|\[[^\]]*\]|`(?:[^`]|``)*`)
    | (?P<ident>[A-Za-z_][A-Za-z0-9_$]*)
    | (?P<lpar>\()
    | (?P<rpar>\))
    | .                                        # anything else
    """,
    re.VERBOSE | re.DOTALL,
)


def _unquote_ident(text: str) -> str:
    if text.startswith('"'):
        return text[1:-1].replace('""', '"')
    if text.startswith("["):
        return text[1:-1]
    if text.startswith("`"):
        return text[1:-1].replace("``", "`")
    return text


def extract_cte_names(sql: str) -> frozenset[str]:
    """Collect the names of every common table expression the query declares.

    Scans for the ``name [(columns)] AS (`` pattern at any nesting depth
    (CTEs may also appear inside subqueries). String literals, comments and
    quoted identifiers are tokenized properly. Over-collecting is harmless
    (a bogus name can never match a real sandbox table); the authorizer
    remains the enforcement layer.
    """
    tokens: list[tuple[str, str]] = []
    for match in _TOKEN.finditer(sql):
        text = match.group(0)
        if match.lastgroup == "qident":
            tokens.append(("ident", _unquote_ident(text)))
        elif match.lastgroup == "ident":
            tokens.append(("ident", text))
        elif match.lastgroup == "lpar":
            tokens.append(("(", text))
        elif match.lastgroup == "rpar":
            tokens.append((")", text))
        # whitespace, comments, strings and punctuation carry no structure

    names: set[str] = set()
    n = len(tokens)
    for i, (kind, text) in enumerate(tokens):
        if kind != "ident":
            continue
        j = i + 1
        # Optional column-name list: name(c1, c2) AS ( ... )
        if j < n and tokens[j][0] == "(":
            depth = 0
            k = j
            while k < n:
                if tokens[k][0] == "(":
                    depth += 1
                elif tokens[k][0] == ")":
                    depth -= 1
                    if depth == 0:
                        break
                k += 1
            if k >= n:
                break  # unbalanced: SQLite will reject the statement anyway
            j = k + 1
        if (
            j + 1 < n
            and tokens[j][0] == "ident"
            and tokens[j][1].upper() == "AS"
            and tokens[j + 1][0] == "("
        ):
            names.add(text.lower())
    return frozenset(names)


def validate_statement(sql: str) -> None:
    keyword = _first_keyword(sql)
    if keyword not in ("SELECT", "WITH"):
        raise BadQueryError(
            "only a single read-only SELECT (optionally starting with WITH) "
            f"is accepted, got {keyword or 'an empty statement'}"
        )


# --------------------------------------------------------------------------- sandbox execution


def _jsonable(value: Any) -> Any:
    if isinstance(value, bytes):
        return "0x" + value.hex()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)  # 'inf' / '-inf' / 'nan' are not valid JSON numbers
    return value


def execute_analyst_query(
    sql: str,
    snapshot_rows: Iterable[Sequence[Any]],
    *,
    deadline: float,
    cancel_event: threading.Event,
    max_rows: int,
) -> tuple[list[str], list[list[Any]], bool]:
    """Run one analyst SELECT inside a throwaway in-memory database.

    ``deadline`` is a ``time.monotonic()`` timestamp shared by the whole
    request; the progress handler aborts the SQLite VM as soon as it passes
    or ``cancel_event`` is set. Returns ``(columns, rows, truncated)`` where
    ``rows`` holds at most ``max_rows`` entries.
    """
    validate_statement(sql)
    cte_names = extract_cte_names(sql)

    conn = sqlite3.connect(":memory:", isolation_level=None)
    try:
        conn.execute(SANDBOX_SCHEMA)
        conn.executemany(
            "INSERT INTO current_sessions VALUES (?, ?, ?, ?, ?, ?)",
            [tuple(r) for r in snapshot_rows],
        )
        # Warm up the JSON table-valued functions BEFORE the authorizer is
        # installed: their first use performs an internal sqlite_master
        # lookup that the (correctly) strict authorizer would deny.
        conn.execute("SELECT 1 FROM json_each('[]')").fetchall()
        conn.execute("SELECT 1 FROM json_tree('[]')").fetchall()

        # Belt and suspenders around the authorizer.
        conn.execute("PRAGMA query_only = ON")
        conn.setlimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
        conn.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 100_000)
        conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 16_000_000)
        conn.setlimit(sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, 100)

        denials: list[str] = []
        conn.set_authorizer(_make_authorizer(denials, cte_names))

        def _progress() -> int:
            if cancel_event.is_set() or time.monotonic() > deadline:
                return 1  # SQLITE_INTERRUPT: abort the actual computation
            return 0

        conn.set_progress_handler(_progress, 1000)

        try:
            cursor = conn.execute(sql)
            columns = [d[0] for d in cursor.description or []]
            rows = cursor.fetchmany(max_rows + 1)
        except sqlite3.ProgrammingError as exc:
            # e.g. more than one statement was supplied
            raise BadQueryError(
                f"only a single SELECT statement is allowed ({exc})"
            ) from exc
        except sqlite3.DatabaseError as exc:
            # OperationalError (interrupt), DatabaseError (not authorized), ...
            _raise_classified(exc, denials, cancel_event)
            raise AssertionError("unreachable")  # _raise_classified always raises

        truncated = len(rows) > max_rows
        kept = rows[:max_rows]
        return columns, [[_jsonable(v) for v in row] for row in kept], truncated
    finally:
        conn.close()


def _raise_classified(
    exc: sqlite3.DatabaseError,
    denials: list[str],
    cancel_event: threading.Event,
) -> None:
    msg = str(exc)
    if "interrupted" in msg:
        if cancel_event.is_set():
            raise QueryAbortedError(
                "query aborted: the client disconnected"
            ) from exc
        raise QueryTimeoutError(
            f"query exceeded the {config.ANALYTICS_TIMEOUT_SECONDS:g}s "
            "execution+fetch budget and was terminated"
        ) from exc
    if "not authorized" in msg or "prohibited" in msg or "authorization denied" in msg:
        raise ForbiddenError(
            denials[0] if denials else "operation not allowed in analytics queries"
        ) from exc
    if "vtable constructor failed" in msg and denials:
        # An eponymous virtual table (dbstat, pragma_*, ...) whose module
        # resolution was vetoed by the authorizer.
        raise ForbiddenError(denials[0]) from exc
    no_table = re.search(r"no such table: (\S+)", msg)
    if no_table:
        # The sandbox holds only current_sessions, so any other name — raw
        # logs, old session versions, internal tables — lands here.
        raise ForbiddenError(
            f"table '{no_table.group(1)}' is not exposed: only "
            "current_sessions is queryable"
        ) from exc
    no_function = re.search(r"no such function: (\S+)", msg)
    if no_function:
        raise ForbiddenError(
            f"function '{no_function.group(1)}' is not in the analytics allowlist"
        ) from exc
    raise BadQueryError(f"invalid SQL: {msg}") from exc


# --------------------------------------------------------------------------- snapshot extraction


async def extract_current_sessions() -> list[tuple[Any, ...]]:
    """Materialize current_sessions from ONE read-only transaction.

    A dedicated short-lived connection is used (never the API's shared
    connection), and ``query_only`` makes the read-only intent explicit. WAL
    mode guarantees this snapshot read does not block ingestion or the
    background publisher, and is not blocked by them.
    """
    import aiosqlite  # lazy: keep this module stdlib-importable

    conn = await aiosqlite.connect(str(config.DB_PATH), isolation_level=None)
    try:
        await conn.execute("PRAGMA query_only = ON")
        await conn.execute("PRAGMA busy_timeout = 5000")
        await conn.execute("BEGIN")
        try:
            cursor = await conn.execute(EXTRACTION_SQL)
            rows = await cursor.fetchall()
            await conn.execute("COMMIT")
        except Exception:
            await conn.execute("ROLLBACK")
            raise
        return [tuple(row) for row in rows]
    finally:
        await conn.close()

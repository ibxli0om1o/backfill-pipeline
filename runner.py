"""Serial, resumable execution of a backfill plan.

A partition is claimed, executed and acknowledged inside one transaction, so an
interrupted run leaves at most one partition in ``running`` and every completed
partition is recorded exactly once.
"""

from __future__ import annotations

from datetime import date

from .partitions import label
from .plan import Plan, PlanError

_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS backfill_runs (
    run_id        text PRIMARY KEY,
    table_name    text NOT NULL,
    partition_col text NOT NULL,
    template_sha  text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
)
"""

_PARTITIONS_DDL = """
CREATE TABLE IF NOT EXISTS backfill_partitions (
    run_id         text NOT NULL
                   REFERENCES backfill_runs (run_id) ON DELETE CASCADE,
    partition_date date NOT NULL,
    status         text NOT NULL
                   CHECK (status IN ('pending', 'running', 'done', 'failed')),
    rows_affected  bigint,
    error          text,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, partition_date)
)
"""

_CLAIMABLE = ("pending", "failed")


def _bootstrap(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_RUNS_DDL)
        cur.execute(_PARTITIONS_DDL)
    conn.commit()


def _register(conn, plan: Plan, run_id: str) -> None:
    """Create the run, refusing an id whose template changed underneath it."""
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO backfill_runs (run_id, table_name, partition_col, template_sha)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (run_id, plan.table, plan.column, plan.template_sha),
        )
        cur.execute("SELECT template_sha FROM backfill_runs WHERE run_id = %s", (run_id,))
        stored = cur.fetchone()[0]
        if stored != plan.template_sha:
            raise PlanError(
                f"run {run_id} was started with template {stored}, "
                f"current template is {plan.template_sha}"
            )
        cur.execute(
            """
            INSERT INTO backfill_partitions (run_id, partition_date, status)
            SELECT %s, day, 'pending' FROM unnest(%s::date[]) AS day
            ON CONFLICT (run_id, partition_date) DO NOTHING
            """,
            (run_id, list(plan.days)),
        )


def _next_partitions(conn, run_id: str, days: tuple[date, ...]) -> list[date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT partition_date FROM backfill_partitions
            WHERE run_id = %s AND partition_date = ANY(%s)
              AND status = ANY(%s)
            ORDER BY partition_date
            """,
            (run_id, list(days), list(_CLAIMABLE)),
        )
        return [row[0] for row in cur.fetchall()]


def _mark(conn, run_id: str, day: date, status: str, rows: int | None = None,
          error: str | None = None) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            UPDATE backfill_partitions
            SET status = %s, rows_affected = %s, error = %s, updated_at = now()
            WHERE run_id = %s AND partition_date = %s
            """,
            (status, rows, error, run_id, day),
        )


def _run_partition(conn, plan: Plan, run_id: str, day: date) -> int:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            UPDATE backfill_partitions
            SET status = 'running', updated_at = now()
            WHERE run_id = %s AND partition_date = %s
            """,
            (run_id, day),
        )
        cur.execute(plan.render(day))
        rows = max(cur.rowcount, 0)
        cur.execute(
            """
            UPDATE backfill_partitions
            SET status = 'done', rows_affected = %s, error = NULL, updated_at = now()
            WHERE run_id = %s AND partition_date = %s
            """,
            (rows, run_id, day),
        )
    return rows


def run(
    dsn: str,
    plan: Plan,
    run_id: str,
    *,
    execute: bool,
    limit: int | None = None,
    log=print,
) -> int:
    """Execute (or merely describe) the plan and return a process exit code."""
    days = plan.days[:limit] if limit else plan.days
    if not execute:
        for day in days:
            log(f"plan\t{plan.table}\t{day.isoformat()}\t{label(day)}")
        log(f"dry run: {len(days)} partition(s), run id {run_id}")
        return 0

    import psycopg

    failures = 0
    with psycopg.connect(dsn) as conn:
        _bootstrap(conn)
        _register(conn, plan, run_id)
        for day in _next_partitions(conn, run_id, days):
            try:
                rows = _run_partition(conn, plan, run_id, day)
            except psycopg.Error as exc:
                failures += 1
                _mark(conn, run_id, day, "failed", error=str(exc).strip()[:1500])
                log(f"failed\t{day.isoformat()}\t{type(exc).__name__}: "
                    + " ".join(str(exc).split()))
                continue
            log(f"done\t{day.isoformat()}\t{rows}")
    return 1 if failures else 0

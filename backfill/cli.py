"""Command line entry point: build the plan, then hand it to the runner."""

from __future__ import annotations

import argparse
import os
import sys

from .partitions import PartitionError, parse_day
from .plan import PlanError, build_plan
from .policy import RetryPolicy
from .runner import run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill",
        description="Idempotently rebuild table partitions over a date range.",
    )
    parser.add_argument("--table", required=True,
                        help="target table, optionally schema-qualified")
    parser.add_argument("--column", required=True,
                        help="date partition column of the target table")
    parser.add_argument("--start", required=True, type=parse_day,
                        help="first partition, inclusive, YYYY-MM-DD")
    parser.add_argument("--end", required=True, type=parse_day,
                        help="last partition, exclusive, YYYY-MM-DD")
    parser.add_argument("--sql", required=True,
                        help="template file with the placeholders from the README")
    parser.add_argument("--run-id",
                        help="resume an existing run instead of starting a new one")
    parser.add_argument("--dsn", default=os.environ.get("BACKFILL_DSN", ""),
                        help="Postgres DSN, defaults to $BACKFILL_DSN")
    parser.add_argument("--limit", type=int, metavar="N",
                        help="stop after N partitions")
    parser.add_argument("--max-retries", type=int, default=2, metavar="N",
                        help="extra attempts for transient errors, default 2")
    parser.add_argument("--retry-delay", type=float, default=2.0, metavar="SECONDS",
                        help="first backoff delay, doubled per attempt, default 2.0")
    parser.add_argument("--execute", action="store_true",
                        help="write to the warehouse; default is a dry run")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.execute and not args.dsn:
        parser.error("--execute needs --dsn or $BACKFILL_DSN")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")

    try:
        plan = build_plan(args.table, args.column, args.start, args.end, args.sql)
    except (OSError, PartitionError, PlanError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        policy = RetryPolicy(max_attempts=args.max_retries + 1, delay=args.retry_delay)
    except ValueError as exc:
        parser.error(str(exc))

    run_id = args.run_id or (
        f"{plan.table}:{plan.days[0].isoformat()}:{plan.days[-1].isoformat()}:"
        f"{plan.template_sha}"
    )
    return run(args.dsn, plan, run_id, execute=args.execute, limit=args.limit,
               policy=policy)

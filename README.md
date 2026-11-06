# backfill-pipeline

`backfill-pipeline` replays a warehouse table one partition at a time after an
upstream fix, and it is safe to rerun. A control table remembers which
partitions finished, so a crashed or cancelled run resumes instead of starting
over, and a partition is only marked done when its transaction committed.

## Why

Backfills are the part of a data platform nobody instruments. A hand-written
loop over `2026-10-01 .. 2026-10-31` double-writes rows when it is interrupted
and restarted, hides which days are still missing, and leaves the next person
guessing. This runner makes the range explicit, the state durable, and the
second attempt a no-op.

## Install

Requires Python 3.11+ and PostgreSQL 13+ (for `ON CONFLICT` and `unnest` over
date arrays).

```console
$ python3 -m venv .venv && . .venv/bin/activate
$ pip install -e .
```

The only runtime dependency is `psycopg[binary]` 3.x.

## Usage

Dry run first. The default is a plan, no writes:

```console
$ backfill --table analytics.events --column event_date \
    --start 2026-10-01 --end 2026-10-08 \
    --sql sql/example_backfill.sql
plan	analytics.events	2026-10-01	20261001
...
dry run: 7 partition(s), run id analytics.events:2026-10-01:2026-10-07:6b1f0c3a91d2
```

Execute with a DSN, reusing the printed run id:

```console
$ backfill --table analytics.events --column event_date \
    --start 2026-10-01 --end 2026-10-08 \
    --sql sql/example_backfill.sql \
    --run-id analytics.events:2026-10-01:2026-10-07:6b1f0c3a91d2 \
    --dsn "$WAREHOUSE_DSN" --execute
done	2026-10-01	48213
...
```

`--end` is exclusive. `--limit N` stops after N partitions, which is handy for a
first canary. Exit code is `0` only when every planned partition is `done`.

Serialization failures, deadlocks, lock timeouts and dropped connections are
retried in place: `--max-retries` extra attempts (default 2) with a backoff that
starts at `--retry-delay` (default 2s) and doubles. Constraint violations and
bad SQL are not retried, because waiting cannot fix them.

## Data contract

The template is plain SQL. Three placeholders are required; `{partition_label}`
is optional and only useful for physical partition names:

| placeholder | substitution |
| --- | --- |
| `{table}`, `{column}` | validated `[a-z_][a-z0-9_]*`, optionally one dot for a schema |
| `{partition_date}` | `YYYY-MM-DD`, non-negotiable from the CLI |
| `{partition_label}` | `YYYYMMDD`, for names like `events_20261001` |

Rules a template must follow:

1. **Replace, don't append.** Delete (or `MERGE`) the whole partition before
   inserting it, so a half-finished attempt leaves no duplicate rows.
2. Emit no `BEGIN`/`COMMIT`; the runner opens one transaction per partition and
   commits only after the statement succeeds.
3. Keep it to one statement (a `WITH ... INSERT` counts). Extra result sets are
   ignored.
4. Rows must satisfy the table's constraints on their own; the runner does not
   pre-validate.

The runner owns two tables and creates them on first execute:
`backfill_runs` (one row per run, including the template hash) and
`backfill_partitions` (one row per partition, `pending`/`running`/`done`/`failed`,
plus `rows_affected` and the truncated error). A second run with the same id but
a different template hash is rejected, so a half-applied SQL edit cannot spread.

## Sample DAG

```python
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="events_backfill",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    BashOperator(
        task_id="repair_week",
        bash_command=(
            "backfill --table analytics.events --column event_date "
            "--start {{ params.start }} --end {{ params.end }} "
            "--sql sql/example_backfill.sql --execute"
        ),
        env={"BACKFILL_DSN": "{{ conn.warehouse.get_uri() }}"},
        params={"start": "2026-10-01", "end": "2026-10-08"},
        append_env=True,
    )
```

## Runbook

- **Inspect a run.** `SELECT partition_date, status, rows_affected, error
  FROM backfill_partitions WHERE run_id = $1 ORDER BY partition_date;`
- **Resume after a crash.** Same command and `--run-id`; `done` partitions are
  skipped, `running` and `failed` ones are retried.
- **Retry one partition.** Delete its row from `backfill_partitions` and resume
  the run; the runner re-inserts it as `pending`.
- **Watch retries.** Every retry logs `retry<TAB>day<TAB>attempt N<TAB>SQLSTATE
  <TAB>seconds`; a `failed` line means the budget ran out or the error was not
  transient.
- **Abort.** `Ctrl-C` is safe: the open partition rolls back and stays
  `running`, every committed partition stays committed.
- **Roll back a finished partition.** Point the template at the previous
  transform and run a fresh run id for that single day.

## Layout

```
backfill/cli.py         argument parsing, plan construction, exit codes
backfill/partitions.py  strict ISO dates and range expansion
backfill/runner.py      control tables, claiming, one transaction per partition
sql/example_backfill.sql  reference template for a raw-event rebuild
```

## TODO

- Adaptive partition granularity (hourly) is not supported yet; daily only.

## License

MIT, see [LICENSE](LICENSE).

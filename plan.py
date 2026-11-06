"""Validate a SQL template and render it for one partition day."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .partitions import iter_days, label

REQUIRED_PLACEHOLDERS = ("table", "column", "partition_date")
OPTIONAL_PLACEHOLDERS = ("partition_label",)
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$")


class PlanError(ValueError):
    """Raised when the template or target identifiers are unusable."""


@dataclass(frozen=True)
class Plan:
    """A validated template plus the exact partitions to replay."""

    table: str
    column: str
    days: tuple[date, ...]
    template: str
    template_sha: str

    def render(self, day: date) -> str:
        return self.template.format(
            table=self.table,
            column=self.column,
            partition_date=day.isoformat(),
            partition_label=label(day),
        )


def _identifier(value: str, field: str) -> str:
    if not _IDENTIFIER.match(value):
        raise PlanError(f"{field} is not a plain SQL identifier: {value!r}")
    return value


def build_plan(
    table: str,
    column: str,
    start: date,
    end: date,
    template_path: Path | str,
) -> Plan:
    """Read ``template_path`` and bind it to ``[start, end)``."""
    sql = Path(template_path).read_text(encoding="utf-8").strip()
    if not sql:
        raise PlanError(f"template {template_path} is empty")
    missing = [name for name in REQUIRED_PLACEHOLDERS if "{" + name + "}" not in sql]
    if missing:
        raise PlanError("template lacks placeholders: " + ", ".join(missing))
    return Plan(
        table=_identifier(table, "table"),
        column=_identifier(column, "column"),
        days=tuple(iter_days(start, end)),
        template=sql,
        template_sha=hashlib.sha256(sql.encode("utf-8")).hexdigest()[:12],
    )

"""Partition range parsing for daily backfills."""

from __future__ import annotations

from datetime import date, timedelta


class PartitionError(ValueError):
    """Raised for malformed or empty partition ranges."""


def parse_day(text: str) -> date:
    """Parse a strict ``YYYY-MM-DD`` value."""
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise PartitionError(f"not an ISO date: {text!r}") from exc


def iter_days(start: date, end: date) -> list[date]:
    """Return every partition day in ``[start, end)``, oldest first."""
    if end <= start:
        raise PartitionError(f"end {end} must be after start {start}")
    days = []
    cursor = start
    while cursor < end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def label(day: date) -> str:
    """Compact label used for partition names such as ``events_20261001``."""
    return f"{day.year:04d}{day.month:02d}{day.day:02d}"

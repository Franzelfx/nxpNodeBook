#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/utils/time.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Timestamp discipline. Everything in this service is UTC.

  The rule this module exists to enforce is the leak trap from Epic 1.2: a
  capture taken at 12:03 describes the market at 12:03, so the earliest grid
  point that may carry it is 12:05 — the CEILING, never the floor. Flooring it
  onto 12:00 would publish, at 12:00, a number that did not exist until 12:03.
  Every anchor in this node therefore goes through `ceil_step`, and there is no
  second implementation that could quietly floor instead.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterator

UTC = timezone.utc


def ensure_utc(dt: datetime) -> datetime:
    """Treat naive datetimes as UTC; convert aware ones to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_iso(text: str) -> datetime:
    """Parse a configured RFC3339 instant, tolerating the Z suffix."""
    return ensure_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))


def floor_step(dt: datetime, step_seconds: int) -> datetime:
    """Floor a UTC datetime onto the grid of `step_seconds`."""
    dt = ensure_utc(dt).replace(microsecond=0)
    epoch = int(dt.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % step_seconds), tz=UTC)


def ceil_step(dt: datetime, step_seconds: int) -> datetime:
    """Ceil a UTC datetime onto the grid of `step_seconds`.

    This is the function that keeps an observation from being published before
    it was observed. Anchoring anything with `floor_step` is a leak.
    """
    floored = floor_step(dt, step_seconds)
    if floored == ensure_utc(dt).replace(microsecond=0):
        return floored
    return floored + timedelta(seconds=step_seconds)


def grid_points(start: datetime, end: datetime, step_seconds: int) -> Iterator[datetime]:
    """Yield grid timestamps in [start, end], both aligned to the step.

    `end` is inclusive: the grid is a set of sample points, not a set of
    half-open buckets, so the newest sample is the one at `end`.
    """
    cur = ceil_step(start, step_seconds)
    stop = floor_step(end, step_seconds)
    step = timedelta(seconds=step_seconds)
    while cur <= stop:
        yield cur
        cur += step


def ms(dt: datetime) -> int:
    """Epoch milliseconds — the unit every Deribit timestamp uses."""
    return int(ensure_utc(dt).timestamp() * 1000)


def from_ms(value: int) -> datetime:
    """Deribit epoch milliseconds → aware UTC datetime."""
    return datetime.fromtimestamp(value / 1000.0, tz=UTC)


def iso(dt: datetime) -> str:
    """RFC3339 string with a Z suffix."""
    return ensure_utc(dt).isoformat().replace("+00:00", "Z")

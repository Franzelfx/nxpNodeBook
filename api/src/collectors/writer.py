#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/collectors/writer.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  The one database writer every collector hands rows to.

  Collectors must never block on the database: a stalled INSERT would stall
  the WebSocket read, the venue's send buffer would fill, and the venue
  would drop the connection — turning a slow database into a hole in the
  historical record. So collectors append to in-memory queues and return
  immediately; this writer drains them in batches via COPY.

  BOUNDED, AND HONEST ABOUT IT. If the database is down long enough for a
  queue to reach its cap, the OLDEST rows are dropped and counted. That is a
  loss, and `dropped_rows` is on /health so it is a visible one — unlike the
  alternative, where an unbounded queue takes the process down with an OOM
  and loses everything that was queued plus everything until the restart.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any, Deque, Dict, Tuple

from ..adapters.database.repositories.raw import RawRepository
from ..core.config import settings
from ..core.logging import get_logger

logger = get_logger(__name__)

TABLES = ("book_events", "book_snapshots", "trades")
MAX_QUEUE_ROWS = 100_000


class DBWriter:
    def __init__(self, repo: RawRepository) -> None:
        self._repo = repo
        self._queues: Dict[str, Deque[Tuple[Any, ...]]] = {t: deque() for t in TABLES}
        self._dropped: Dict[str, int] = {t: 0 for t in TABLES}
        self._written: Dict[str, int] = {t: 0 for t in TABLES}
        self._stopping = False
        self._task: asyncio.Task | None = None
        #: In-flight notify tasks. asyncio holds only a weak reference to a
        #: running task, so without a strong one here the garbage collector
        #: can cancel a notification mid-flight (doc/54 E5 review).
        self._notify_tasks: set[asyncio.Task] = set()
        self._last_error: str | None = None

    # ── producer side ────────────────────────────────────────────────────
    def put(self, table: str, row: Tuple[Any, ...]) -> None:
        q = self._queues[table]
        if len(q) >= MAX_QUEUE_ROWS:
            q.popleft()
            self._dropped[table] += 1
            if self._dropped[table] % 1000 == 1:
                logger.error(
                    "[writer] %s queue full (%d rows) — DROPPING oldest; dropped so far: %d",
                    table, MAX_QUEUE_ROWS, self._dropped[table],
                )
        q.append(row)

    @property
    def backlog(self) -> int:
        return sum(len(q) for q in self._queues.values())

    @property
    def dropped(self) -> Dict[str, int]:
        return dict(self._dropped)

    @property
    def written(self) -> Dict[str, int]:
        return dict(self._written)

    @property
    def last_error(self) -> str | None:
        return self._last_error

    # ── consumer side ────────────────────────────────────────────────────
    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="db-writer")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=30.0)
            except asyncio.TimeoutError:
                self._task.cancel()
        # One last drain so a clean shutdown loses nothing that was queued.
        await self._flush_all()

    async def _run(self) -> None:
        interval = settings.write_flush_seconds
        while not self._stopping:
            started = time.monotonic()
            try:
                await self._flush_all()
            except Exception:  # noqa: BLE001 — the loop must survive
                logger.exception("[writer] flush failed")
            elapsed = time.monotonic() - started
            # Flush again immediately while any queue is over the batch bound;
            # otherwise wait out the interval.
            if max(len(q) for q in self._queues.values()) < settings.write_batch_rows:
                await asyncio.sleep(max(0.05, interval - elapsed))

    async def _flush_all(self) -> None:
        before = sum(self._written.values())
        for table in TABLES:
            await self._flush(table)

        # doc/54 E5 — tell the warehouse rows landed, so a chart does not wait
        # for its next poll to find out.
        #
        # Fire and forget, on this loop, never awaited by a COPY: the writer's
        # own contract is that nothing here may block a collector, and a slow
        # warehouse must not become a hole in the historical record. `announce`
        # rate-limits itself and swallows everything.
        written = sum(self._written.values()) - before
        if written:
            # Held in a set until it finishes. asyncio keeps only a WEAK
            # reference to a running task, so a bare `create_task` can be
            # garbage-collected mid-flight — the notification vanishes with no
            # error and no log, which for an optimisation nobody watches is
            # the hardest kind of bug to notice.
            task = asyncio.create_task(self._announce(written))
            self._notify_tasks.add(task)
            task.add_done_callback(self._notify_tasks.discard)

    async def _announce(self, rows: int) -> None:
        try:
            from src.collectors.notify import announce

            await announce(rows)
        except Exception as exc:  # noqa: BLE001 — an optimisation, never a duty
            logger.debug("[writer] notify skipped: %s", exc)

    async def _flush(self, table: str) -> None:
        q = self._queues[table]
        if not q:
            return
        # Take up to a few batches per pass so a backlog drains in large COPYs
        # rather than one small one per second.
        n = min(len(q), settings.write_batch_rows * 10)
        rows = [q[i] for i in range(n)]
        try:
            if table == "book_events":
                await self._repo.write_events(rows)
            elif table == "book_snapshots":
                await self._repo.write_snapshots(rows)
            else:
                await self._repo.write_trades(rows)
        except Exception as exc:  # noqa: BLE001
            self._last_error = f"{type(exc).__name__}: {exc}"
            logger.error("[writer] %s COPY failed (%d rows kept queued): %s", table, n, self._last_error)
            await asyncio.sleep(2.0)
            return
        for _ in range(n):
            q.popleft()
        self._written[table] += n
        self._last_error = None

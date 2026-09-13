#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/scheduler/runner.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  The node's derive scheduler. Two jobs, both idempotent and both allowed
  to be late:

    derive_tail  every 5 min — republish the trailing window so fresh ticks
                 reach the warehouse within one grid step.
    derive_full  daily — recompute the whole grid from the raw layers.

  CAPTURE IS NOT HERE. The collectors run on their own tasks under
  CollectorManager and this loop cannot block them: a derive that takes a
  minute costs nothing, a WebSocket read that waits a minute costs history.
  A failing job is recorded in derive_runs and the loop continues.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, List

from ..core.logging import get_logger
from ..derive.runner import MODE_FULL, MODE_TAIL, DeriveRunner

logger = get_logger(__name__)


@dataclass(slots=True)
class Job:
    name: str
    interval_seconds: int
    run: Callable[[], Awaitable[None]]
    run_at_start: bool = False
    _next_due: float = 0.0


class SchedulerRunner:
    TICK_SECONDS = 10

    def __init__(self, derive: DeriveRunner, *, derive_tail_seconds: int, derive_full_seconds: int) -> None:
        self._derive = derive
        self._stopping = False
        self._jobs: List[Job] = [
            Job("derive_tail", derive_tail_seconds, self._derive_tail, run_at_start=False),
            Job("derive_full", derive_full_seconds, self._derive_full, run_at_start=False),
        ]

    def stop(self) -> None:
        self._stopping = True

    async def run(self) -> None:
        now = time.monotonic()
        for job in self._jobs:
            job._next_due = now if job.run_at_start else now + job.interval_seconds
        logger.info("[scheduler] started: %s", ", ".join(f"{j.name}/{j.interval_seconds}s" for j in self._jobs))
        while not self._stopping:
            now = time.monotonic()
            for job in self._jobs:
                if self._stopping:
                    break
                if now < job._next_due:
                    continue
                job._next_due = now + job.interval_seconds
                try:
                    await job.run()
                except Exception:
                    logger.exception("[scheduler] job %s failed", job.name)
            await asyncio.sleep(self.TICK_SECONDS)
        logger.info("[scheduler] stopped")

    async def _derive_tail(self) -> None:
        outcome = await self._derive.run(MODE_TAIL)
        if outcome.status == "aborted":
            logger.error("[scheduler] derive_tail aborted by gates: %s", [g["name"] for g in outcome.gates if not g["passed"]])

    async def _derive_full(self) -> None:
        outcome = await self._derive.run(MODE_FULL)
        if outcome.status == "aborted":
            logger.error("[scheduler] derive_full aborted by gates: %s", [g["name"] for g in outcome.gates if not g["passed"]])

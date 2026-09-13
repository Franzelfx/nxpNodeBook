#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/collectors/manager.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Owns one StreamCollector per configured stream and the shared DBWriter.
  Each collector runs on its own task with its own reconnect loop, so one
  venue's outage never touches another's capture.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from ..adapters.database.repositories.raw import RawRepository
from ..adapters.sources.deribit import make_source
from ..core.config import StreamSpec, settings
from ..core.logging import get_logger
from .stream import StreamCollector
from .writer import DBWriter

logger = get_logger(__name__)


class CollectorManager:
    def __init__(self, repo: RawRepository) -> None:
        self._repo = repo
        self.writer = DBWriter(repo)
        self.collectors: Dict[str, StreamCollector] = {}
        for spec in settings.streams:
            self.collectors[spec.key] = StreamCollector(spec, make_source(spec.venue, spec.symbol), repo, self.writer)
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    @property
    def specs(self) -> List[StreamSpec]:
        return [c.spec for c in self.collectors.values()]

    async def start(self) -> None:
        orphans = await self._repo.close_orphan_runs()
        if orphans:
            logger.warning("[collectors] closed %d orphan stream session(s) from a previous run", orphans)
        self.writer.start()
        for c in self.collectors.values():
            c.start()
        self._running = True
        logger.info("[collectors] started %d stream(s): %s", len(self.collectors), ", ".join(self.collectors))

    async def stop(self) -> None:
        self._running = False
        # Concurrently: each stop waits on a venue close handshake, and three
        # of them in sequence would outlast the container's grace period.
        await asyncio.gather(*(c.stop() for c in self.collectors.values()), return_exceptions=True)
        await self.writer.stop()
        for c in self.collectors.values():
            aclose = getattr(c.source, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except Exception:  # noqa: BLE001
                    pass
        logger.info("[collectors] stopped")

    def get(self, venue: str, symbol: str) -> Optional[StreamCollector]:
        return self.collectors.get(f"{venue}:{symbol.upper()}")

    def by_label(self, label: str) -> Optional[StreamCollector]:
        for c in self.collectors.values():
            if c.spec.label == label:
                return c
        return None

    def states(self) -> List[dict]:
        out = []
        for c in self.collectors.values():
            s = c.snapshot_state()
            s["queue_depth"] = self.writer.backlog
            out.append(s)
        return out

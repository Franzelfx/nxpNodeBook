#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/services/health.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Health for the book node, conforming to the shared node health contract
  from nxp-node-contract.

  The check this node has that the others do not is CAPTURE freshness per
  stream, read from collector memory — no database, no venue call. A stream
  that is disconnected, unsynced, or silent for longer than
  `max_stream_lag_seconds` is not fresh, and that alone makes `ok` false:
  the grid can always be rebuilt, the tick stream cannot.

  Per-metric staleness on the grid is checked the same way the other nodes
  do it, one index probe per declared column.

  The endpoint never raises. A database outage is `database_connected:
  false`, which makes `ok` false through compute_ok.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional, Tuple

from nxp_node_contract.health import CONTRACT_VERSION, compute_ok

from ..adapters.database.connection import DatabasePool
from ..adapters.database.repositories.grid import GridRepository
from ..collectors.manager import CollectorManager
from ..core.config import settings
from ..core.logging import get_logger
from ..domain.metrics import METRIC_SPECS
from ..domain.schemas import CaptureStreamState, HealthResponse, MetricStaleness, StreamHealth

logger = get_logger(__name__)

_PERIOD = "5m"


class HealthService:
    def __init__(self, db: DatabasePool, grid: GridRepository, collectors: Optional[CollectorManager]) -> None:
        self._db = db
        self._grid = grid
        self._collectors = collectors
        self._scheduler_running = False

    def set_scheduler_running(self, running: bool) -> None:
        self._scheduler_running = running

    async def _check_metrics(self) -> Tuple[bool, List[MetricStaleness]]:
        pairs = [(s.asset, s.metric) for s in METRIC_SPECS]
        last_by_pair = await self._grid.metric_last_ts(pairs)
        now = datetime.now(timezone.utc)
        stale: List[MetricStaleness] = []
        for spec in METRIC_SPECS:
            last = last_by_pair.get((spec.asset, spec.metric))
            threshold = spec.max_gap_seconds or settings.max_grid_lag_seconds
            if last is None:
                stale.append(
                    MetricStaleness(
                        stream=settings.node_id, metric_name=spec.name, last_ts=None,
                        lag_seconds=None, max_lag_seconds=threshold, reason="never_written",
                    )
                )
                continue
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            lag = int((now - last).total_seconds())
            if lag > threshold:
                stale.append(
                    MetricStaleness(
                        stream=settings.node_id, metric_name=spec.name, last_ts=last,
                        lag_seconds=lag, max_lag_seconds=threshold, reason="stale",
                    )
                )
        return (not stale), stale

    async def check(self) -> HealthResponse:
        connected = await self._db.ping()
        now = datetime.now(timezone.utc)
        grid_last: Optional[datetime] = None
        grid_lag: Optional[int] = None
        last_status: Optional[str] = None
        last_at: Optional[datetime] = None
        failed_gates: List[str] = []
        stale_metrics: List[MetricStaleness] = []
        metrics_fresh = True

        if connected:
            try:
                _, grid_last, _ = await self._grid.bounds()
                if grid_last is not None:
                    grid_lag = int((now - grid_last).total_seconds())
                last_run = await self._grid.last_derive_run()
                if last_run is not None:
                    last_status = last_run["status"]
                    last_at = last_run["finished_at"] or last_run["started_at"]
                    for gate in last_run["gates"] or []:
                        if not gate.get("passed", True):
                            failed_gates.append(gate.get("name", "unknown"))
                metrics_fresh, stale_metrics = await self._check_metrics()
            except Exception as exc:  # noqa: BLE001
                logger.warning("[health] database unreadable during check: %s", exc)
                connected = False
                metrics_fresh = False

        # ── capture, from memory ─────────────────────────────────────────
        capture_streams: List[CaptureStreamState] = []
        stream_health: List[StreamHealth] = []
        capture_fresh = True
        producer_running = self._scheduler_running
        backlog = 0
        if self._collectors is not None:
            producer_running = producer_running and self._collectors.running
            backlog = self._collectors.writer.backlog
            for st in self._collectors.states():
                cs = CaptureStreamState(**st)
                capture_streams.append(cs)
                fresh = (
                    cs.connected and cs.synced
                    and cs.event_lag_seconds is not None
                    and cs.event_lag_seconds <= settings.max_stream_lag_seconds
                )
                capture_fresh = capture_fresh and fresh
                stream_health.append(
                    StreamHealth(
                        stream=f"{settings.node_id}:{cs.label}:capture",
                        period="tick",
                        last_ts=cs.last_event_at,
                        lag_seconds=cs.event_lag_seconds,
                        max_lag_seconds=settings.max_stream_lag_seconds,
                        fresh=fresh,
                    )
                )
            # A "skipped" derive (nothing captured yet) is not a bad publish.
        else:
            capture_fresh = False

        grid_fresh = grid_lag is not None and grid_lag <= settings.max_grid_lag_seconds
        data_fresh = grid_fresh and metrics_fresh and capture_fresh

        streams = [
            StreamHealth(
                stream=f"{settings.node_id}:{_PERIOD}",
                period=_PERIOD,
                last_ts=grid_last,
                lag_seconds=grid_lag,
                max_lag_seconds=settings.max_grid_lag_seconds,
                fresh=grid_fresh,
            ),
            *stream_health,
        ]

        ok = compute_ok(
            database_connected=connected,
            producer_running=producer_running,
            data_fresh=data_fresh,
            failed_gates=failed_gates,
            last_publish_status=last_status,
        )
        return HealthResponse(
            contract_version=CONTRACT_VERSION,
            ok=ok,
            node_id=settings.node_id,
            checked_at=now,
            database_connected=connected,
            producer_running=producer_running,
            data_fresh=data_fresh,
            streams=streams,
            stale_metrics=stale_metrics,
            failed_gates=failed_gates,
            last_publish_status=last_status,
            last_publish_at=last_at,
            capture_fresh=capture_fresh,
            capture_streams=capture_streams,
            max_stream_lag_seconds=settings.max_stream_lag_seconds,
            writer_backlog_rows=backlog,
        )

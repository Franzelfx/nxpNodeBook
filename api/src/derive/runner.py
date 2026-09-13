#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/derive/runner.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Recomputes and republishes the derived 5-min grid behind the QA gates.

  Two modes, same code path:
    tail   the last `derive_tail_hours` — every 5 min, so fresh ticks reach
           the warehouse within one grid step.
    full   from the first captured message — daily. The recomputability
           property: change the derive, bump DERIVE_SPEC_VERSION, and the
           whole history is rebuilt from the raw layers.

  The publish path is: stage → gate → swap the range in ONE transaction.
  A failed gate never reaches the swap; the served grid keeps its last
  good state and /health shows the failure.

  This runner also builds /coverage, because it owns the facts the report
  needs: where each stream's raw layer begins, how many sessions and resyncs
  it has had, and what the last derive said.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

from nxp_node_contract.coverage import COVERAGE_CONTRACT_VERSION, FULL_COVERAGE_THRESHOLD, classify

from ..adapters.database.repositories.grid import GridRepository
from ..adapters.database.repositories.raw import RawRepository
from ..core.config import StreamSpec, settings
from ..core.logging import get_logger
from ..domain.metrics import DERIVE_SPEC_VERSION, METRIC_SPECS
from ..qa import gates as G
from ..utils.time import ceil_step, ensure_utc, floor_step, iso
from . import features as F

logger = get_logger(__name__)

MODE_FULL = "full"
MODE_TAIL = "tail"


@dataclass(slots=True)
class DeriveOutcome:
    status: str
    mode: str
    grid_start: datetime
    grid_end: datetime
    rows_written: int
    gates: List[Dict[str, Any]]
    detail: Dict[str, Any]

    @property
    def published(self) -> bool:
        return self.status == "published"


class DeriveRunner:
    def __init__(self, raw: RawRepository, grid: GridRepository) -> None:
        self._raw = raw
        self._grid = grid
        self._streams: List[StreamSpec] = settings.streams
        self._qty_notional: Dict[str, bool] = {s.key: s.venue == "deribit" for s in self._streams}

    async def run(self, mode: str = MODE_TAIL, *, now: Optional[datetime] = None) -> DeriveOutcome:
        step = settings.grid_step_seconds
        now = ensure_utc(now or datetime.now(timezone.utc))
        grid_end = floor_step(now, step)

        if mode == MODE_TAIL:
            grid_start = floor_step(grid_end - timedelta(hours=settings.derive_tail_hours), step)
        else:
            first = await self._first_capture()
            if first is None:
                logger.info("[derive] nothing captured yet; skipping %s", mode)
                return DeriveOutcome("skipped", mode, grid_end, grid_end, 0, [], {"reason": "no raw data"})
            grid_start = floor_step(first, step)

        if grid_start >= grid_end:
            return DeriveOutcome("skipped", mode, grid_start, grid_end, 0, [], {"reason": "empty range"})

        run_id = await self._grid.start_derive_run(mode, grid_start, grid_end)
        logger.info("[derive] run %s mode=%s range=%s … %s", run_id, mode, iso(grid_start), iso(grid_end))
        try:
            outcome = await self._execute(mode, grid_start, grid_end, step)
        except Exception as exc:
            await self._grid.finish_derive_run(run_id, status="failed", detail={"error": str(exc)})
            raise
        await self._grid.finish_derive_run(
            run_id,
            status=outcome.status,
            rows_written=outcome.rows_written,
            gates=outcome.gates,
            detail={**outcome.detail, "derive_spec_version": DERIVE_SPEC_VERSION},
        )
        return outcome

    async def _first_capture(self) -> Optional[datetime]:
        firsts = []
        for s in self._streams:
            first, _ = await self._raw.event_bounds(s.venue, s.symbol)
            if first is not None:
                firsts.append(ensure_utc(first))
        return min(firsts) if firsts else None

    async def _execute(self, mode: str, grid_start: datetime, grid_end: datetime, step: int) -> DeriveOutcome:
        stats: Dict[str, G.MetricStats] = {}
        crossed: Dict[str, int] = {}
        snapshots_total: Dict[str, int] = {}
        bands = settings.depth_bands

        rows: List[Tuple[datetime, str, str, float]] = []
        for s in self._streams:
            book_b = await self._raw.bucket_book_stats(
                s.venue, s.symbol, grid_start, grid_end, step, qty_is_notional=self._qty_notional[s.key]
            )
            trade_b = await self._raw.bucket_trade_stats(s.venue, s.symbol, grid_start, grid_end, step)
            snaps = await self._raw.snapshots_in(s.venue, s.symbol, grid_start, grid_end)

            snapshots_total[s.key] = len(snaps)
            crossed[s.key] = sum(1 for sn in snaps if _is_crossed(sn))

            rows.extend(F.book_rows(s, book_b, step))
            rows.extend(F.trade_rows(s, trade_b, step))
            rows.extend(
                F.snapshot_rows(s, snaps, bands, step, qty_is_notional=self._qty_notional[s.key])
            )

        # Bucket end is the instant a bucket became knowable; a snapshot's
        # ceiling is its own. Both equal ts by construction, which is what
        # the leak gate verifies rather than assumes.
        def records() -> Iterator[Tuple[datetime, str, str, float]]:
            for ts, asset, metric, value in rows:
                name = f"{asset}.{metric}"
                stats.setdefault(name, G.MetricStats()).observe(name, ts, value, ts)
                if ts <= grid_end:
                    yield ts, asset, metric, value

        await self._grid.reset_staging()
        staged = await self._grid.copy_to_staging(records())

        results = [
            G.gate_no_future_leak(stats),
            G.gate_value_sanity(stats),
            G.gate_crossed_books(crossed, snapshots_total),
        ]
        gates = [r.as_dict() for r in results]
        detail = {
            "staged_rows": staged,
            "streams": {
                s.key: {"snapshots": snapshots_total[s.key], "crossed": crossed[s.key]}
                for s in self._streams
            },
        }
        if any(not r.passed for r in results):
            await self._grid.reset_staging()
            logger.error("[derive] aborted by gates: %s", [r.name for r in results if not r.passed])
            return DeriveOutcome("aborted", mode, grid_start, grid_end, 0, gates, detail)

        # The swap deletes the range then inserts; the grid ts is a bucket
        # END, so the published range is (grid_start, grid_end].
        written = await self._grid.publish_staging(grid_start + timedelta(seconds=1), grid_end)
        logger.info("[derive] published %d rows (%s)", written, mode)
        return DeriveOutcome("published", mode, grid_start, grid_end, written, gates, detail)

    # ── /coverage ────────────────────────────────────────────────────────
    async def build_coverage(self) -> Dict[str, Any]:
        step = settings.grid_step_seconds
        streams_out: List[Dict[str, Any]] = []
        first_by_label: Dict[str, datetime] = {}

        for s in self._streams:
            first, last = await self._raw.event_bounds(s.venue, s.symbol)
            run_stats = await self._raw.run_stats(s.venue, s.symbol)
            per_day = await self._raw.snapshot_counts_by_day(s.venue, s.symbol)
            last_day = await self._raw.table_counts(s.venue, s.symbol)
            if first is not None:
                first_by_label[s.label] = ensure_utc(first)
            expected_per_day = 86_400 // settings.snapshot_interval_seconds
            streams_out.append(
                {
                    "venue": s.venue,
                    "symbol": s.symbol,
                    "label": s.label,
                    "first_event_at": iso(first) if first else None,
                    "last_event_at": iso(last) if last else None,
                    "sessions": int(run_stats.get("sessions") or 0),
                    "resyncs": int(run_stats.get("resyncs") or 0),
                    "connected_seconds": int(run_stats.get("connected_seconds") or 0),
                    "rows_last_24h": last_day,
                    "snapshots_per_day": {
                        d["day"].date().isoformat(): {
                            "snapshots": int(d["snapshots"]),
                            "syncs": int(d["syncs"]),
                            "expected": expected_per_day,
                            "density": round(int(d["snapshots"]) / expected_per_day, 4),
                        }
                        for d in per_day
                    },
                    "irreplaceable": True,
                    "note": (
                        "No venue serves a past order book; a gap between two sessions "
                        "can never be backfilled from any source."
                    ),
                }
            )

        catalog = {f"{c['asset']}.{c['metric']}": c for c in await self._grid.catalog()}
        metrics: List[Dict[str, Any]] = []
        problems: List[str] = []
        for spec in METRIC_SPECS:
            published = catalog.get(spec.name)
            first_ts = published["first_ts"] if published else None
            last_ts = published["last_ts"] if published else None
            points = int(published["point_count"]) if published else 0
            expected = 0
            density: Optional[float] = None
            if first_ts and last_ts:
                expected = max(0, int((last_ts - first_ts).total_seconds() // step) + 1)
                density = points / expected if expected else None
            status = classify(
                points=points, observed_density=density, structural_absence=spec.structural_absence
            )
            if status in ("missing", "incomplete"):
                problems.append(spec.name)
            metrics.append(
                {
                    "name": spec.name,
                    "status": status,
                    "declared": {
                        "description": spec.description,
                        "kind": spec.kind.value,
                        "density": spec.density.value,
                        "unit": spec.unit,
                        "bounds": list(spec.bounds) if spec.bounds else None,
                        "assumed_lag_seconds": spec.assumed_lag_seconds,
                        "max_gap_seconds": spec.max_gap_seconds,
                        "structural_absence": spec.structural_absence,
                        "available_from": (
                            iso(first_by_label[spec.stream_label]) if spec.stream_label in first_by_label else None
                        ),
                    },
                    "first_ts": iso(first_ts) if first_ts else None,
                    "last_ts": iso(last_ts) if last_ts else None,
                    "points": points,
                    "expected_points": expected,
                    "observed_density": density,
                    "provenance": "derived",
                    "repaired_points": 0,
                    "per_year": {},
                }
            )

        last_run = await self._grid.last_derive_run()
        grid_first, grid_last, grid_total = await self._grid.bounds()
        return {
            "coverage_contract_version": COVERAGE_CONTRACT_VERSION,
            "node_id": settings.node_id,
            "period": "5m",
            "computed_at": iso(datetime.now(timezone.utc)),
            "stale_report": False,
            "full_coverage_threshold": FULL_COVERAGE_THRESHOLD,
            "ok": not problems,
            "problem_metrics": problems,
            "derive_spec_version": DERIVE_SPEC_VERSION,
            "streams": streams_out,
            "grid": {
                "step_seconds": step,
                "published_first_ts": iso(grid_first) if grid_first else None,
                "published_last_ts": iso(grid_last) if grid_last else None,
                "published_points": grid_total,
            },
            "metrics": metrics,
            "last_derive_run": (
                {
                    "mode": last_run["mode"],
                    "status": last_run["status"],
                    "started_at": iso(last_run["started_at"]),
                    "finished_at": iso(last_run["finished_at"]) if last_run["finished_at"] else None,
                    "rows_written": last_run["rows_written"],
                    "gates": last_run["gates"],
                }
                if last_run
                else None
            ),
        }


def _is_crossed(snap: Dict[str, Any]) -> bool:
    bids = json.loads(snap["bids"]) if isinstance(snap["bids"], str) else snap["bids"]
    asks = json.loads(snap["asks"]) if isinstance(snap["asks"], str) else snap["asks"]
    if not bids or not asks:
        return False
    return float(bids[0][0]) >= float(asks[0][0])

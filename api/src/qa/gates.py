#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/qa/gates.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  The QA gates. Publish aborts, never ships garbage.

    1. no_future_leak   every point is anchored at or after the instant its
                        inputs became knowable (bucket end, snapshot ceiling).
    2. value_sanity     finite values, declared bounds respected, imbalance
                        within [-1, 1], nothing published outside the catalog.
    3. crossed_books    a snapshot whose best bid meets or exceeds its best
                        ask is a book that drifted from the venue; the derive
                        skips it, and this gate fails when the SHARE of such
                        snapshots in the window says the collector is broken
                        rather than one message was late.

  A bad snapshot costs one absent bucket; a run of them stops the publish.
  The rate threshold is here, not in configuration, because there is no
  market state in which a fifth of a stream's snapshots are crossed.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from ..domain.metrics import METRIC_SPEC_BY_NAME
from ..utils.time import iso

MAX_CROSSED_SHARE = 0.2


class GateFailure(RuntimeError):
    def __init__(self, results: Sequence["GateResult"]) -> None:
        failed = [r.name for r in results if not r.passed]
        super().__init__(f"QA gates failed: {', '.join(failed)}")
        self.results = list(results)


@dataclass(slots=True)
class GateResult:
    name: str
    passed: bool
    detail: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class MetricStats:
    count: int = 0
    min_value: float = math.inf
    max_value: float = -math.inf
    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None
    non_finite: int = 0
    out_of_bounds: List[str] = field(default_factory=list)
    leaks: List[str] = field(default_factory=list)
    leak_count: int = 0

    def observe(self, name: str, ts: datetime, value: float, available_at: datetime) -> None:
        spec = METRIC_SPEC_BY_NAME.get(name)
        self.count += 1
        if self.first_ts is None or ts < self.first_ts:
            self.first_ts = ts
        if self.last_ts is None or ts > self.last_ts:
            self.last_ts = ts
        if not math.isfinite(value):
            self.non_finite += 1
        else:
            self.min_value = min(self.min_value, value)
            self.max_value = max(self.max_value, value)
        if spec is not None and math.isfinite(value):
            if spec.bounds is not None:
                low, high = spec.bounds
                if value < low - 1e-9 or value > high + 1e-9:
                    if len(self.out_of_bounds) < 5:
                        self.out_of_bounds.append(f"{iso(ts)}={value}")
            if spec.non_negative and value < 0:
                if len(self.out_of_bounds) < 5:
                    self.out_of_bounds.append(f"{iso(ts)}={value} (negative)")
        if available_at > ts:
            self.leak_count += 1
            if len(self.leaks) < 5:
                self.leaks.append(f"{iso(ts)} available_at={iso(available_at)}")

    def as_dict(self) -> Dict[str, Any]:
        return {
            "count": self.count,
            "min": None if self.min_value == math.inf else self.min_value,
            "max": None if self.max_value == -math.inf else self.max_value,
            "first_ts": iso(self.first_ts) if self.first_ts else None,
            "last_ts": iso(self.last_ts) if self.last_ts else None,
        }


def gate_no_future_leak(stats: Dict[str, MetricStats]) -> GateResult:
    total = sum(st.leak_count for st in stats.values())
    return GateResult(
        name="no_future_leak",
        passed=total == 0,
        detail={
            "points_checked": sum(st.count for st in stats.values()),
            "violations": total,
            "examples": {name: st.leaks for name, st in stats.items() if st.leak_count},
        },
    )


def gate_value_sanity(stats: Dict[str, MetricStats]) -> GateResult:
    problems: List[str] = []
    for name, st in stats.items():
        if st.non_finite:
            problems.append(f"{name}: {st.non_finite} non-finite values")
        if st.out_of_bounds:
            problems.append(f"{name}: out of bounds {st.out_of_bounds}")
        if METRIC_SPEC_BY_NAME.get(name) is None:
            problems.append(f"{name}: not in the published metric catalog")
    return GateResult(
        name="value_sanity",
        passed=not problems,
        detail={"metrics": {n: st.as_dict() for n, st in stats.items()}, "problems": problems},
    )


def gate_crossed_books(crossed: Dict[str, int], total: Dict[str, int]) -> GateResult:
    """Per stream: share of snapshots in the window that were crossed."""
    shares: Dict[str, float] = {}
    offenders: List[str] = []
    for key, n in total.items():
        if n == 0:
            continue
        share = crossed.get(key, 0) / n
        shares[key] = round(share, 4)
        if share > MAX_CROSSED_SHARE:
            offenders.append(key)
    return GateResult(
        name="crossed_books",
        passed=not offenders,
        detail={"max_share": MAX_CROSSED_SHARE, "share_by_stream": shares, "offenders": offenders},
    )


def raise_if_failed(results: Sequence[GateResult]) -> None:
    if any(not r.passed for r in results):
        raise GateFailure(results)

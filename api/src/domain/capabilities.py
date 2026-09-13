#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/domain/capabilities.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Projects this node's metric catalog onto the SHARED capability contract
  from nxp-node-contract. metrics.py stays the source of truth.

  WHY EVERY COLUMN IS fill=none
  ─────────────────────────────
  Each column summarises the bucket that ended at its timestamp. A bucket
  with no row is a bucket in which the collector was not connected — there is
  no observation, and carrying the previous bucket's spread or flow across it
  would present a disconnect as a calm market. Absent stays absent.

  Every column is also structurally absent before the first capture: no
  venue serves a past book. That travels as `available_from` /
  `structural_absence`, discovered from the raw layer at runtime.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from typing import Dict, List, Optional
from datetime import datetime

from nxp_node_contract.capability import (
    EmptyBucket,
    Fill,
    MetricCapability,
    NodeCapabilities,
    service_doc_from_app,
)

from ..core.config import settings
from .metrics import ASSET_BOOK, ASSET_MICRO, DERIVE_SPEC_VERSION, METRIC_SPECS, MetricSpec

READ_SIDE_CONTRACT: Dict[str, object] = {
    "fill_mode": "none",
    "why": (
        "Absent points mean NaN. A missing bucket is one in which the collector "
        "was not connected; carrying the previous value across it would present "
        "a disconnect as a calm market."
    ),
    "units": (
        "Depth and flow columns are USD notional on every venue (price × qty on "
        "Binance, qty as-is on Deribit's inverse perpetual). spread_bps is basis "
        "points of mid; imbalance_* is a ratio in [-1, 1]."
    ),
    "anchor": (
        "A value at ts summarises the bucket [ts - 5min, ts). Nothing is "
        "published before it could be known."
    ),
    "sign_convention": (
        "cvd_usd and ofi_usd: positive means aggressor buying / resting demand "
        "growing; imbalance_*: positive means more resting bid than ask notional."
    ),
    "never": "back-fill; a value may not move backwards in time",
}


def to_capability(
    spec: MetricSpec,
    *,
    period_seconds: int,
    available_from: Optional[datetime] = None,
) -> MetricCapability:
    return MetricCapability(
        name=spec.name,
        asset=spec.asset,
        metric=spec.metric,
        empty_bucket=EmptyBucket.ABSENT,
        fill=Fill.NONE,
        max_carry_seconds=None,
        kind=spec.kind,
        unit=spec.unit,
        description=spec.description,
        bounds=list(spec.bounds) if spec.bounds else None,
        cadence_seconds=period_seconds,
        sample_interval_s=None,
        assumed_lag_seconds=spec.assumed_lag_seconds,
        max_gap_seconds=spec.max_gap_seconds,
        available_from=available_from,
        structural_absence=spec.structural_absence,
    )


def node_capabilities(
    period: str = "5m",
    *,
    app=None,
    available_from: Optional[Dict[str, datetime]] = None,
) -> NodeCapabilities:
    """This node's full capability document.

    `available_from` maps a stream label to its first captured instant, when
    known; the route passes it from the collector state so the document can
    be answered without a database read.
    """
    period_seconds = {"5m": 300}.get(period, settings.grid_step_seconds)
    available_from = available_from or {}

    metrics: List[MetricCapability] = [
        to_capability(
            spec,
            period_seconds=period_seconds,
            available_from=available_from.get(spec.stream_label),
        )
        for spec in METRIC_SPECS
    ]

    return NodeCapabilities(
        node_id=settings.node_id,
        periods=["5m"],
        asset_prefixes=[ASSET_BOOK, ASSET_MICRO],
        derive_spec_version=DERIVE_SPEC_VERSION,
        read_side_contract=READ_SIDE_CONTRACT,
        service=service_doc_from_app(app) if app is not None else None,
        metrics=metrics,
    )

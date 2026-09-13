#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/derive/features.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Pure feature arithmetic for one stream over one derive window. Given the
  per-bucket aggregates the repository already computed in SQL (spread, mid,
  updates, OFI, trade stats) and the raw snapshots in the window, produce
  (ts, asset, metric, value) rows on the grid.

  Every value is anchored at the END of its bucket via `ceil_step`: the
  bucket [12:00, 12:05) is published at 12:05, and a snapshot taken at
  12:03:10 contributes to 12:05, never to 12:00.

  Depth and imbalance come from the snapshots because they need the whole
  ladder, and the periodic snapshot cadence equals the grid step by default
  — one snapshot per bucket. When several land in one bucket (a resync plus
  the periodic one) they are averaged; when none does, the bucket is absent.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Iterator, List, Sequence, Tuple

from ..core.config import StreamSpec
from ..domain.book import depth_within
from ..domain.metrics import ASSET_BOOK, ASSET_MICRO
from ..utils.time import ceil_step, ensure_utc

Row = Tuple[datetime, str, str, float]


def _finite(x: Any) -> bool:
    try:
        return x is not None and math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def book_rows(
    stream: StreamSpec, buckets: Iterable[Dict[str, Any]], step_seconds: int
) -> Iterator[Row]:
    """From bucket_book_stats: spread_bps, mid, updates, ofi_usd."""
    for b in buckets:
        ts = ensure_utc(b["bucket"]) + timedelta(seconds=step_seconds)
        if _finite(b.get("spread_bps")):
            yield ts, ASSET_BOOK, f"{stream.label}.spread_bps", float(b["spread_bps"])
        if _finite(b.get("mid_last")):
            yield ts, ASSET_BOOK, f"{stream.label}.mid", float(b["mid_last"])
        if _finite(b.get("updates")):
            yield ts, ASSET_BOOK, f"{stream.label}.updates", float(b["updates"])
        if _finite(b.get("ofi")):
            yield ts, ASSET_MICRO, f"{stream.label}.ofi_usd", float(b["ofi"])


def trade_rows(
    stream: StreamSpec, buckets: Iterable[Dict[str, Any]], step_seconds: int
) -> Iterator[Row]:
    """From bucket_trade_stats: cvd_usd, volume_usd, trades, p50, p90."""
    for b in buckets:
        ts = ensure_utc(b["bucket"]) + timedelta(seconds=step_seconds)
        for key, feature in (
            ("cvd_usd", "cvd_usd"),
            ("volume_usd", "volume_usd"),
            ("trades", "trades"),
            ("p50", "trade_p50_usd"),
            ("p90", "trade_p90_usd"),
        ):
            if _finite(b.get(key)):
                yield ts, ASSET_MICRO, f"{stream.label}.{feature}", float(b[key])


def snapshot_rows(
    stream: StreamSpec,
    snapshots: Sequence[Dict[str, Any]],
    bands_bps: Sequence[int],
    step_seconds: int,
    *,
    qty_is_notional: bool,
) -> Iterator[Row]:
    """From book_snapshots: depth_bid/ask_<b>bp and imbalance_<b>bp."""
    acc: Dict[Tuple[datetime, str], List[float]] = defaultdict(list)
    for snap in snapshots:
        bids = json.loads(snap["bids"]) if isinstance(snap["bids"], str) else snap["bids"]
        asks = json.loads(snap["asks"]) if isinstance(snap["asks"], str) else snap["asks"]
        if not bids or not asks:
            continue
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            continue  # a crossed or empty snapshot is not a market state
        mid = (best_bid + best_ask) / 2.0
        ts = ceil_step(ensure_utc(snap["ts"]), step_seconds)
        notional = not qty_is_notional
        for band in bands_bps:
            db = depth_within(bids, mid, band, notional=notional)
            da = depth_within(asks, mid, band, notional=notional)
            acc[(ts, f"depth_bid_{band}bp")].append(db)
            acc[(ts, f"depth_ask_{band}bp")].append(da)
            total = db + da
            if total > 0:
                acc[(ts, f"imbalance_{band}bp")].append((db - da) / total)
    for (ts, feature), values in sorted(acc.items()):
        if values:
            yield ts, ASSET_BOOK, f"{stream.label}.{feature}", sum(values) / len(values)

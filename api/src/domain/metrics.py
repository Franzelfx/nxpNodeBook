#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/domain/metrics.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  The derived-column catalog, and the properties the QA gates check each
  column against.

  Naming follows the warehouse convention: asset prefix, then the stream
  label, then the feature. Wide columns are asset + "." + metric, so these
  become BOOK.btc.spot.spread_bps, MICRO.btc.perp.cvd_usd, … on the gold sink.

    BOOK.*   state of the resting book — spread, mid, depth, imbalance
    MICRO.*  flow through it — order-flow imbalance, signed volume, prints

  ONE UNIT FOR DEPTH AND FLOW: USD NOTIONAL. Binance quotes quantities in BTC,
  Deribit's inverse perpetual quotes them in USD; a depth column that changed
  unit per venue would be unreadable across them. Every notional column is
  therefore price × qty on Binance and qty as-is on Deribit, and says so.

  ANCHORING. Every column is a statement about the bucket that ENDED at its
  grid timestamp: a value at 12:05 summarises [12:00, 12:05). That is the
  ceiling rule the other nodes use — nothing is published before it could be
  known — and it means a consumer joining on ts sees no future.

  EVERY COLUMN IS ABSENT, NOT ZERO, WHEN THE STREAM WAS DOWN. A count of zero
  trades in a bucket is a real observation only if the collector was
  connected; when it was not, there is no observation. The derive job emits a
  row only where the raw layer has messages for that bucket, so a reconnect
  gap shows as NaN downstream and as `incomplete` in /coverage. It never shows
  as a quiet market.

  STRUCTURAL ABSENCE. No venue serves a past order book and no free L2 archive
  exists, so every column here begins the day this node started capturing.
  That is a property of the world, reported as `structural_absence` so a
  screen can tell the floor from a broken collector.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

from nxp_node_contract.capability import MetricKind

from ..core.config import StreamSpec, settings


class Period(str, Enum):
    """Only the native grid resolution is served."""

    m5 = "5m"


ASSET_BOOK = "BOOK"
ASSET_MICRO = "MICRO"

_STRUCTURAL_ABSENCE = (
    "order books are live-only — no venue serves a past book and no free L2 "
    "archive exists, so this column can only exist from the first capture"
)

# The derive spec. Bump when a column's construction changes meaning, so a
# consumer holding old numbers knows they were recomputed.
DERIVE_SPEC_VERSION = 1


class Density(str, Enum):
    DENSE = "dense"            # defined at every grid point the stream was up
    NAN_GAPPED = "nan_gapped"  # legitimately undefined at times


@dataclass(frozen=True, slots=True)
class MetricSpec:
    asset: str
    metric: str
    description: str
    kind: MetricKind
    unit: str
    density: Density = Density.DENSE
    bounds: Optional[Tuple[float, float]] = None
    non_negative: bool = False
    assumed_lag_seconds: int = 0
    max_gap_seconds: Optional[int] = None
    structural_absence: Optional[str] = _STRUCTURAL_ABSENCE
    stream_label: str = ""
    feature: str = ""

    @property
    def name(self) -> str:
        return f"{self.asset}.{self.metric}"


# A message every 100 ms and a grid every 5 min; a 30-minute silence on a
# column is the collector or the derive, not the market.
_MAX_GAP = 30 * 60

_BPS_BOUNDS = (0.0, 1_000.0)          # 10 % spread on BTC is a broken book
_USD_BOUNDS = (0.0, 1.0e11)           # generous; catches a unit slip, not a view
_SIGNED_USD_BOUNDS = (-1.0e11, 1.0e11)
_PRICE_BOUNDS = (1.0, 1.0e7)
_IMBALANCE_BOUNDS = (-1.0, 1.0)


def _spec(
    asset: str,
    stream: StreamSpec,
    feature: str,
    description: str,
    kind: MetricKind,
    unit: str,
    bounds: Optional[Tuple[float, float]],
    *,
    non_negative: bool = False,
    density: Density = Density.DENSE,
) -> MetricSpec:
    return MetricSpec(
        asset=asset,
        metric=f"{stream.label}.{feature}",
        description=f"[{stream.venue} {stream.symbol}] {description}",
        kind=kind,
        unit=unit,
        density=density,
        bounds=bounds,
        non_negative=non_negative,
        max_gap_seconds=_MAX_GAP,
        stream_label=stream.label,
        feature=feature,
    )


def specs_for_stream(stream: StreamSpec, bands_bps: List[int]) -> List[MetricSpec]:
    out: List[MetricSpec] = [
        _spec(
            ASSET_BOOK, stream, "spread_bps",
            "Mean quoted spread over the bucket, in basis points of mid, measured "
            "after every applied depth message.",
            MetricKind.RATE, "basis_points", _BPS_BOUNDS, non_negative=True,
        ),
        _spec(
            ASSET_BOOK, stream, "mid",
            "Mid price after the last applied depth message of the bucket, in USD.",
            MetricKind.STATE, "usd", _PRICE_BOUNDS, non_negative=True,
        ),
        _spec(
            ASSET_BOOK, stream, "updates",
            "Depth messages applied to the book during the bucket. Activity, not "
            "volume: a message may change one level or three hundred.",
            MetricKind.ADDITIVE, "count", _USD_BOUNDS, non_negative=True,
        ),
        _spec(
            ASSET_MICRO, stream, "ofi_usd",
            "Order-flow imbalance (Cont, Kukanov & Stoikov) summed over the bucket, "
            "in USD notional: net addition to the best bid minus net addition to "
            "the best ask, taken across consecutive applied depth messages. "
            "Positive means resting demand grew faster than resting supply.",
            MetricKind.ADDITIVE, "usd", _SIGNED_USD_BOUNDS,
        ),
        _spec(
            ASSET_MICRO, stream, "cvd_usd",
            "Signed traded notional over the bucket, in USD: aggressor buys minus "
            "aggressor sells. The bucket's own delta, NOT a running cumulative — "
            "cumulate downstream if a level is wanted.",
            MetricKind.ADDITIVE, "usd", _SIGNED_USD_BOUNDS,
        ),
        _spec(
            ASSET_MICRO, stream, "volume_usd",
            "Traded notional over the bucket, in USD.",
            MetricKind.ADDITIVE, "usd", _USD_BOUNDS, non_negative=True,
        ),
        _spec(
            ASSET_MICRO, stream, "trades",
            "Individual trade prints over the bucket, as the venue's trade stream "
            "delivers them.",
            MetricKind.ADDITIVE, "count", _USD_BOUNDS, non_negative=True,
        ),
        _spec(
            ASSET_MICRO, stream, "trade_p50_usd",
            "Median print size over the bucket, in USD notional.",
            MetricKind.RATE, "usd", _USD_BOUNDS, non_negative=True,
        ),
        _spec(
            ASSET_MICRO, stream, "trade_p90_usd",
            "90th-percentile print size over the bucket, in USD notional. Where "
            "the large prints are.",
            MetricKind.RATE, "usd", _USD_BOUNDS, non_negative=True,
        ),
    ]
    for band in bands_bps:
        out.extend(
            [
                _spec(
                    ASSET_BOOK, stream, f"depth_bid_{band}bp",
                    f"Resting bid notional within {band} bp of mid at the bucket's "
                    f"snapshot, in USD.",
                    MetricKind.STATE, "usd", _USD_BOUNDS, non_negative=True,
                ),
                _spec(
                    ASSET_BOOK, stream, f"depth_ask_{band}bp",
                    f"Resting ask notional within {band} bp of mid at the bucket's "
                    f"snapshot, in USD.",
                    MetricKind.STATE, "usd", _USD_BOUNDS, non_negative=True,
                ),
                _spec(
                    ASSET_BOOK, stream, f"imbalance_{band}bp",
                    f"(bid − ask) / (bid + ask) resting notional within {band} bp of "
                    f"mid at the bucket's snapshot. +1 is all bids, −1 all asks.",
                    MetricKind.STATE, "ratio", _IMBALANCE_BOUNDS,
                ),
            ]
        )
    return out


def build_catalog(streams: List[StreamSpec], bands_bps: List[int]) -> List[MetricSpec]:
    specs: List[MetricSpec] = []
    for stream in streams:
        specs.extend(specs_for_stream(stream, bands_bps))
    return specs


METRIC_SPECS: List[MetricSpec] = build_catalog(settings.streams, settings.depth_bands)
METRIC_SPEC_BY_NAME: Dict[str, MetricSpec] = {m.name: m for m in METRIC_SPECS}
FEATURES: Tuple[str, ...] = tuple(sorted({m.feature for m in METRIC_SPECS}))


def split_metric_name(name: str) -> Tuple[str, str]:
    """Split a served name ("BOOK.btc.spot.spread_bps") into (asset, metric).

    Only the first dot is a separator; the label lives inside the metric.
    """
    if "." not in name:
        raise ValueError(f"metric name must be '<asset>.<metric>', got {name!r}")
    asset, metric = name.split(".", 1)
    if not asset or not metric:
        raise ValueError(f"metric name must be '<asset>.<metric>', got {name!r}")
    return asset, metric

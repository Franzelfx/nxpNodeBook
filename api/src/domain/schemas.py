#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/domain/schemas.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  API response models.

  MetricPoint / MetricQueryResponse mirror the other nodes' metrics APIs field
  for field ("data": [{"ts", "value"}], plus metric_name/node/period), so the
  warehouse adapter for this node is another thin variant of nxp_options.

  HealthResponse carries the shared contract from nxp-node-contract plus the
  per-stream capture state that is THIS node's emergency: a grid can always be
  rebuilt, a missed depth message cannot.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from nxp_node_contract.health import MetricStaleness, NodeHealth, StreamHealth  # noqa: F401

from .metrics import Period


class MetricPoint(BaseModel):
    ts: datetime = Field(..., description="Grid timestamp (UTC) — the END of the bucket it summarises.")
    value: float


class MetricQueryResponse(BaseModel):
    metric_name: str
    node: str
    period: Period
    start: datetime
    end: datetime
    count: int
    data: List[MetricPoint] = Field(default_factory=list)


class CatalogItem(BaseModel):
    name: str
    asset: str
    metric: str
    description: str
    kind: str
    density: str
    unit: str
    assumed_lag_seconds: int
    structural_absence: Optional[str] = None
    available_from: Optional[datetime] = None
    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None
    point_count: int = 0


class CatalogResponse(BaseModel):
    node_id: str
    derive_spec_version: int
    supported_periods: List[Period]
    supported_metrics: List[str]
    available_metrics: List[str]
    catalog: List[CatalogItem] = Field(default_factory=list)
    read_side_contract: Dict[str, Any] = Field(default_factory=dict)


class CaptureStreamState(BaseModel):
    """Live state of one WebSocket collector, from memory — no database read."""

    venue: str
    symbol: str
    label: str
    connected: bool
    synced: bool = Field(..., description="The in-memory book is trustworthy right now.")
    last_event_at: Optional[datetime] = None
    event_lag_seconds: Optional[int] = None
    update_id: Optional[int] = None
    events_session: int = 0
    trades_session: int = 0
    resyncs_session: int = 0
    reconnects: int = 0
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    queue_depth: int = Field(0, description="Rows waiting for the database writer.")


class HealthResponse(NodeHealth):
    """The shared contract plus capture freshness.

    `capture_fresh` is separate from `data_fresh` on purpose. The grid can be
    perfectly current while a collector is dead, and for THIS node that is the
    emergency: the grid can always be rebuilt, the tick stream cannot.
    """

    capture_fresh: bool = Field(
        ..., description="Every configured stream is connected, synced and produced a message within its threshold."
    )
    capture_streams: List[CaptureStreamState] = Field(default_factory=list)
    max_stream_lag_seconds: int
    writer_backlog_rows: int = Field(0, description="Rows queued for the database across all streams.")


class RootResponse(BaseModel):
    api: str
    version: str
    node_id: str
    endpoints: Dict[str, str]
    documentation: str


class BookLevelsResponse(BaseModel):
    """Top of the in-memory book — an operator view, not a warehouse contract."""

    venue: str
    symbol: str
    label: str
    synced: bool
    update_id: Optional[int] = None
    as_of: Optional[datetime] = None
    bids: List[List[float]] = Field(default_factory=list)
    asks: List[List[float]] = Field(default_factory=list)
    mid: Optional[float] = None
    spread_bps: Optional[float] = None

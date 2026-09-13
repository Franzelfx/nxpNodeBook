#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/api/routes/metrics.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Metric query routes. Same shape as the other nodes' metrics APIs, so the
  warehouse adapter stays a thin variant of nxp_options.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Optional

from fastapi import APIRouter, HTTPException, Path, Query

from ...core.dependencies import get_metrics_service
from ...domain.metrics import METRIC_SPECS
from ...domain.metrics import Period
from ...domain.schemas import CatalogResponse, MetricQueryResponse
from ...services.metrics import UnknownMetric

# Every published column name, injected into the OpenAPI schema so Swagger
# renders a DROPDOWN instead of a free-text box.
#
# Deliberately `json_schema_extra` rather than an Enum annotation. An Enum
# would also change VALIDATION — an unknown name would become a 422 from
# FastAPI instead of the 404 this route raises with a pointer to
# /metrics/catalog. The documentation improves; the error contract does not
# move under existing consumers.
_ALL_METRIC_NAMES = [s.name for s in METRIC_SPECS]

router = APIRouter(prefix="/metrics", tags=["Metrics"])


@router.get("/catalog", response_model=CatalogResponse)
async def get_catalog() -> CatalogResponse:
    """Derived columns this node publishes, and the read-side contract."""
    try:
        return await get_metrics_service().catalog()
    except Exception as exc:  # pragma: no cover - surfaced as a 500 either way
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{metric_name}", response_model=MetricQueryResponse)
async def query_metric(
    metric_name: Annotated[
        str,
        Path(
            json_schema_extra={"enum": _ALL_METRIC_NAMES},
            description="Derived column name, e.g. `BOOK.btc.spot.spread_bps`, `MICRO.btc.perp.cvd_usd`.",
            examples=["BOOK.btc.spot.spread_bps"],
            min_length=3,
            max_length=100,
        ),
    ],
    period: Annotated[
        Period,
        Query(description="Grid resolution. Only the native 5m grid is served."),
    ] = Period.m5,
    node: Annotated[
        Optional[str],
        Query(
            description="Node stream identifier; both `nxp-book` and `nxp-book:5m` resolve.",
            examples=["nxp-book:5m"],
        ),
    ] = None,
    start: Annotated[
        Optional[datetime],
        Query(description="Start timestamp (ISO 8601, inclusive). Naive values are treated as UTC."),
    ] = None,
    end: Annotated[
        Optional[datetime],
        Query(description="End timestamp (ISO 8601, exclusive). Defaults to now."),
    ] = None,
    lookback_hours: Annotated[
        Optional[int],
        Query(ge=1, le=24 * 365 * 20, description="Alternative to `start`: last N hours."),
    ] = None,
    limit: Annotated[
        Optional[int],
        Query(ge=1, le=1_000_000, description="Optional cap on returned points."),
    ] = None,
) -> MetricQueryResponse:
    """Query one derived column.

    Absent points are NaN by contract — never zero. A missing bucket is one in
    which the collector was not connected. Read with `fill_mode="none"` downstream.
    """
    try:
        return await get_metrics_service().query(
            metric_name=metric_name,
            period=period,
            node=node,
            start=start,
            end=end,
            lookback_hours=lookback_hours,
            limit=limit,
        )
    except UnknownMetric:
        raise HTTPException(
            status_code=404,
            detail=f"unknown metric {metric_name!r}; see GET /metrics/catalog",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

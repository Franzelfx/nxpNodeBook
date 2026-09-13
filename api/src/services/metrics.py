#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/services/metrics.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Serving logic for the derived grid. Same contract as the other nodes, so
  the warehouse adapter is another thin variant rather than new transport
  code.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from ..adapters.database.repositories.grid import GridRepository
from ..core.config import settings
from ..domain.capabilities import READ_SIDE_CONTRACT
from ..domain.metrics import DERIVE_SPEC_VERSION, METRIC_SPECS, METRIC_SPEC_BY_NAME, Period, split_metric_name
from ..domain.schemas import CatalogItem, CatalogResponse, MetricPoint, MetricQueryResponse
from ..utils.time import ensure_utc

DEFAULT_LOOKBACK_HOURS = 24


class UnknownMetric(KeyError):
    """The requested column is not in the published catalog."""


def resolve_node(node: Optional[str], period: Period, node_id: str) -> str:
    """Accept both "nxp-book" and "nxp-book:5m".

    The warehouse adapter rewrites the suffix of a node id to match the
    requested period, so both spellings arrive here and must resolve the same.
    """
    if not node:
        return node_id
    if ":" in node:
        prefix, suffix = node.rsplit(":", 1)
        if suffix == period.value:
            return prefix
    return node


def resolve_time_range(
    start: Optional[datetime],
    end: Optional[datetime],
    lookback_hours: Optional[int],
) -> Tuple[datetime, datetime]:
    end_utc = ensure_utc(end) if end else datetime.now(timezone.utc)
    if start is not None:
        start_utc = ensure_utc(start)
    elif lookback_hours:
        start_utc = end_utc - timedelta(hours=int(lookback_hours))
    else:
        start_utc = end_utc - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
    if start_utc > end_utc:
        raise ValueError("start must be before end")
    return start_utc, end_utc


class MetricsService:
    """Serves derived grid columns."""

    def __init__(self, repo: GridRepository) -> None:
        self._repo = repo

    async def query(
        self,
        metric_name: str,
        period: Period = Period.m5,
        node: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        lookback_hours: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> MetricQueryResponse:
        asset, metric = split_metric_name(metric_name)
        if metric_name not in METRIC_SPEC_BY_NAME:
            raise UnknownMetric(metric_name)

        start_utc, end_utc = resolve_time_range(start, end, lookback_hours)
        rows = await self._repo.query_series(asset, metric, start_utc, end_utc, limit)

        return MetricQueryResponse(
            metric_name=metric_name,
            node=resolve_node(node, period, settings.node_id),
            period=period,
            start=start_utc,
            end=end_utc,
            count=len(rows),
            data=[MetricPoint(ts=r["ts"], value=r["value"]) for r in rows],
        )

    async def catalog(self) -> CatalogResponse:
        published = {f"{c['asset']}.{c['metric']}": c for c in await self._repo.catalog()}

        items: List[CatalogItem] = []
        for spec in METRIC_SPECS:
            row = published.get(spec.name)
            items.append(
                CatalogItem(
                    name=spec.name,
                    asset=spec.asset,
                    metric=spec.metric,
                    description=spec.description,
                    kind=spec.kind.value,
                    density=spec.density.value,
                    unit=spec.unit,
                    assumed_lag_seconds=spec.assumed_lag_seconds,
                    structural_absence=spec.structural_absence,
                    available_from=row["first_ts"] if row else None,
                    first_ts=row["first_ts"] if row else None,
                    last_ts=row["last_ts"] if row else None,
                    point_count=int(row["point_count"]) if row else 0,
                )
            )

        return CatalogResponse(
            node_id=settings.node_id,
            derive_spec_version=DERIVE_SPEC_VERSION,
            supported_periods=[Period.m5],
            supported_metrics=[spec.name for spec in METRIC_SPECS],
            available_metrics=sorted(published.keys()),
            catalog=items,
            read_side_contract={**READ_SIDE_CONTRACT, "derive_spec_version": DERIVE_SPEC_VERSION},
        )

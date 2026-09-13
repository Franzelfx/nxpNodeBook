#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/adapters/database/repositories/grid.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Data access for the derived grid.

  The publish path is: truncate staging → COPY the freshly derived rows →
  run the gates → swap the affected range into grid_features inside ONE
  transaction. A failed gate simply never reaches the swap, so the served
  grid keeps its last good state.

  Identical in shape to the nxpNodeOptions grid repository, deliberately: the
  nodes publish through the same warehouse adapter path, so their derived
  layers are the same table with different columns in it.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ..connection import DatabasePool


class GridRepository:
    """Reads and publishes derived grid series."""

    def __init__(self, db: DatabasePool) -> None:
        self._db = db

    # ── publish path ─────────────────────────────────────────────────────
    async def reset_staging(self) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute("TRUNCATE grid_features_staging")

    async def copy_to_staging(self, records: Iterable[Tuple[datetime, str, str, float]]) -> int:
        async with self._db.pool.acquire() as conn:
            # `records` stays a generator: the full grid is over a million rows
            # and there is no reason to hold it in memory.
            result = await conn.copy_records_to_table(
                "grid_features_staging",
                records=records,
                columns=["ts", "asset", "metric", "value"],
            )
        # asyncpg returns a status string such as "COPY 12345"
        try:
            return int(str(result).split()[-1])
        except (ValueError, IndexError):
            return 0

    async def publish_staging(self, start: datetime, end: datetime) -> int:
        """Swap the staged range into grid_features atomically."""
        async with self._db.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM grid_features WHERE ts >= $1 AND ts <= $2", start, end
                )
                written = await conn.fetchval(
                    """
                    WITH inserted AS (
                        INSERT INTO grid_features (ts, asset, metric, value)
                        SELECT ts, asset, metric, value FROM grid_features_staging
                        ON CONFLICT (ts, asset, metric) DO UPDATE SET value = EXCLUDED.value
                        RETURNING 1
                    )
                    SELECT COUNT(*) FROM inserted
                    """
                )
                await conn.execute("TRUNCATE grid_features_staging")
        return int(written or 0)

    async def clear_all(self) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute("TRUNCATE grid_features")

    # ── reads ────────────────────────────────────────────────────────────
    async def query_series(
        self,
        asset: str,
        metric: str,
        start: datetime,
        end: datetime,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT ts, value
            FROM grid_features
            WHERE asset = $1 AND metric = $2 AND ts >= $3 AND ts < $4
            ORDER BY ts
        """
        args: List[Any] = [asset, metric, start, end]
        if limit:
            query += " LIMIT $5"
            args.append(int(limit))

        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
        return [{"ts": r["ts"], "value": float(r["value"])} for r in rows]

    async def staged_sample(self, stride: int, per_metric_limit: int) -> List[Dict[str, Any]]:
        """A deterministic every-nth sample of the staged rows, per series.

        Deterministic rather than random so a failing gate names the same
        points on a re-run and can actually be investigated.
        """
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ts, asset, metric, value FROM (
                    SELECT ts, asset, metric, value,
                           ROW_NUMBER() OVER (PARTITION BY asset, metric ORDER BY ts) AS rn
                    FROM grid_features_staging
                ) numbered
                WHERE (rn - 1) % $1::bigint = 0
                  AND rn <= $1::bigint * $2::bigint
                ORDER BY asset, metric, ts
                """,
                int(max(stride, 1)),
                int(max(per_metric_limit, 1)),
            )
        return [dict(r) for r in rows]

    async def catalog(self) -> List[Dict[str, Any]]:
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT asset, metric, MIN(ts) AS first_ts, MAX(ts) AS last_ts, COUNT(*) AS point_count
                FROM grid_features
                GROUP BY asset, metric
                ORDER BY asset, metric
                """
            )
        return [dict(r) for r in rows]

    async def metric_last_ts(
        self, pairs: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], Optional[datetime]]:
        """Newest ts per (asset, metric), for the declared columns only.

        `catalog()` answers the same question with a GROUP BY over the whole
        grid, which is fine for /coverage but too slow for a probe. This drives
        one index lookup per declared column off idx_grid_features_series
        instead of scanning, so /health stays well under its one-second budget
        however large the grid grows.
        """
        if not pairs:
            return {}
        assets = [a for a, _ in pairs]
        metrics = [m for _, m in pairs]
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT w.asset, w.metric, s.last_ts
                FROM unnest($1::text[], $2::text[]) AS w(asset, metric)
                LEFT JOIN LATERAL (
                    SELECT MAX(g.ts) AS last_ts
                    FROM grid_features g
                    WHERE g.asset = w.asset AND g.metric = w.metric
                ) s ON TRUE
                """,
                assets,
                metrics,
            )
        return {(r["asset"], r["metric"]): r["last_ts"] for r in rows}

    async def bounds(self) -> Tuple[Optional[datetime], Optional[datetime], int]:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT MIN(ts) AS first_ts, MAX(ts) AS last_ts, COUNT(*) AS n FROM grid_features"
            )
        return row["first_ts"], row["last_ts"], int(row["n"] or 0)

    # ── derive run bookkeeping ───────────────────────────────────────────
    async def start_derive_run(self, mode: str, grid_start: datetime, grid_end: datetime) -> int:
        async with self._db.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    """
                    INSERT INTO derive_runs (mode, grid_start, grid_end)
                    VALUES ($1, $2, $3) RETURNING id
                    """,
                    mode,
                    grid_start,
                    grid_end,
                )
            )

    async def finish_derive_run(
        self,
        run_id: int,
        *,
        status: str,
        rows_written: int = 0,
        gates: Optional[Sequence[Dict[str, Any]]] = None,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE derive_runs
                   SET finished_at = NOW(), status = $2, rows_written = $3,
                       gates = $4::jsonb, detail = $5::jsonb
                 WHERE id = $1
                """,
                run_id,
                status,
                int(rows_written),
                json.dumps(list(gates or [])),
                json.dumps(detail or {}),
            )

    async def last_derive_run(self, status: Optional[str] = None) -> Optional[Dict[str, Any]]:
        query = "SELECT * FROM derive_runs"
        args: List[Any] = []
        if status:
            query += " WHERE status = $1"
            args.append(status)
        query += " ORDER BY started_at DESC LIMIT 1"

        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(query, *args)
        if not row:
            return None
        out = dict(row)
        for key in ("gates", "detail"):
            if isinstance(out.get(key), str):
                out[key] = json.loads(out[key])
        return out

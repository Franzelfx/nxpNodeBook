#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/adapters/database/repositories/raw.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Data access for the raw layers: book_events, book_snapshots, trades and
  stream_runs.

  WRITES ARE BINARY COPY. At ten to thirty depth messages a second per stream
  plus the trade tape, row-by-row INSERTs would spend most of their time in
  round trips. The collector batches rows and the repository COPYs them; a
  duplicate key (a reconnect that re-delivers a message) is handled by
  copying into a temp table and inserting ON CONFLICT DO NOTHING, which keeps
  the raw layer append-only and idempotent at the same time.

  READS serve two customers: the derive job (windows of applied events,
  trades and snapshots) and /coverage (bounds, counts, session history).

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..connection import DatabasePool

EVENT_COLUMNS = [
    "ts", "venue", "symbol", "update_id", "first_update_id", "prev_update_id",
    "kind", "applied", "bids", "asks",
    "best_bid", "best_bid_qty", "best_ask", "best_ask_qty", "received_at",
]
SNAPSHOT_COLUMNS = [
    "ts", "venue", "symbol", "update_id", "reason", "bid_levels", "ask_levels", "bids", "asks",
]
TRADE_COLUMNS = [
    "ts", "venue", "symbol", "trade_id", "price", "qty", "notional_usd", "is_buyer_maker", "received_at",
]


class RawRepository:
    def __init__(self, db: DatabasePool) -> None:
        self._db = db

    # ── writes ───────────────────────────────────────────────────────────
    async def _copy_dedup(
        self, table: str, columns: Sequence[str], rows: Sequence[Tuple[Any, ...]]
    ) -> int:
        """COPY into a temp clone, then INSERT … ON CONFLICT DO NOTHING.

        The temp table is per-connection and dropped on commit, so concurrent
        writers for different streams never see each other's rows.
        """
        if not rows:
            return 0
        cols = ", ".join(columns)
        async with self._db.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"CREATE TEMP TABLE _in_{table} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                await conn.copy_records_to_table(f"_in_{table}", records=rows, columns=list(columns))
                written = await conn.fetchval(
                    f"""
                    WITH ins AS (
                        INSERT INTO {table} ({cols})
                        SELECT {cols} FROM _in_{table}
                        ON CONFLICT DO NOTHING
                        RETURNING 1
                    )
                    SELECT COUNT(*) FROM ins
                    """
                )
        return int(written or 0)

    async def write_events(self, rows: Sequence[Tuple[Any, ...]]) -> int:
        return await self._copy_dedup("book_events", EVENT_COLUMNS, rows)

    async def write_snapshots(self, rows: Sequence[Tuple[Any, ...]]) -> int:
        return await self._copy_dedup("book_snapshots", SNAPSHOT_COLUMNS, rows)

    async def write_trades(self, rows: Sequence[Tuple[Any, ...]]) -> int:
        return await self._copy_dedup("trades", TRADE_COLUMNS, rows)

    # ── stream sessions ──────────────────────────────────────────────────
    async def start_run(self, venue: str, symbol: str, detail: Optional[Dict[str, Any]] = None) -> int:
        import json

        async with self._db.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    "INSERT INTO stream_runs (venue, symbol, detail) VALUES ($1, $2, $3::jsonb) RETURNING id",
                    venue, symbol, json.dumps(detail or {}),
                )
            )

    async def update_run(
        self,
        run_id: int,
        *,
        first_update_id: Optional[int],
        last_update_id: Optional[int],
        events: int,
        trades: int,
        resyncs: int,
    ) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE stream_runs
                   SET first_update_id = COALESCE(first_update_id, $2),
                       last_update_id = $3, events = $4, trades = $5, resyncs = $6
                 WHERE id = $1
                """,
                run_id, first_update_id, last_update_id, int(events), int(trades), int(resyncs),
            )

    async def finish_run(self, run_id: int, reason: str, **counts: int) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE stream_runs
                   SET disconnected_at = NOW(), reason = $2,
                       last_update_id = COALESCE($3, last_update_id),
                       events = $4, trades = $5, resyncs = $6
                 WHERE id = $1
                """,
                run_id, reason, counts.get("last_update_id"),
                int(counts.get("events", 0)), int(counts.get("trades", 0)), int(counts.get("resyncs", 0)),
            )

    async def close_orphan_runs(self) -> int:
        """Sessions left open by a crash. Closed on startup so the gap they
        represent is declared rather than looking like a session that is
        still alive."""
        async with self._db.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    """
                    WITH closed AS (
                        UPDATE stream_runs SET disconnected_at = NOW(), reason = 'orphan_closed_on_startup'
                         WHERE disconnected_at IS NULL RETURNING 1
                    ) SELECT COUNT(*) FROM closed
                    """
                )
                or 0
            )

    async def recent_runs(self, venue: str, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, connected_at, disconnected_at, first_update_id, last_update_id,
                       events, trades, resyncs, reason
                  FROM stream_runs WHERE venue = $1 AND symbol = $2
                 ORDER BY connected_at DESC LIMIT $3
                """,
                venue, symbol, int(limit),
            )
        return [dict(r) for r in rows]

    async def run_stats(self, venue: str, symbol: str) -> Dict[str, Any]:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS sessions,
                       COALESCE(SUM(resyncs), 0) AS resyncs,
                       MIN(connected_at) AS first_connected_at,
                       COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(disconnected_at, NOW()) - connected_at))), 0) AS connected_seconds
                  FROM stream_runs WHERE venue = $1 AND symbol = $2
                """,
                venue, symbol,
            )
        return dict(row) if row else {}

    # ── bounds & counts for /coverage and /health ────────────────────────
    async def event_bounds(self, venue: str, symbol: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """First and last event via two index probes, not an aggregate over
        the hypertable — this is on the /health path."""
        async with self._db.pool.acquire() as conn:
            first = await conn.fetchval(
                "SELECT ts FROM book_events WHERE venue = $1 AND symbol = $2 ORDER BY ts ASC LIMIT 1",
                venue, symbol,
            )
            last = await conn.fetchval(
                "SELECT ts FROM book_events WHERE venue = $1 AND symbol = $2 ORDER BY ts DESC LIMIT 1",
                venue, symbol,
            )
        return first, last

    async def table_counts(self, venue: str, symbol: str) -> Dict[str, int]:
        """Approximate row counts from Timescale's chunk statistics —
        cheap, and exact enough for a coverage report on tables this size."""
        async with self._db.pool.acquire() as conn:
            rows = {}
            for table in ("book_events", "book_snapshots", "trades"):
                rows[table] = int(
                    await conn.fetchval(
                        f"SELECT COUNT(*) FROM {table} WHERE venue = $1 AND symbol = $2 AND ts > NOW() - INTERVAL '1 day'",
                        venue, symbol,
                    )
                    or 0
                )
        return rows

    async def snapshot_counts_by_day(self, venue: str, symbol: str) -> List[Dict[str, Any]]:
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT date_trunc('day', ts) AS day, COUNT(*) AS snapshots,
                       SUM(CASE WHEN reason <> 'periodic' THEN 1 ELSE 0 END) AS syncs
                  FROM book_snapshots WHERE venue = $1 AND symbol = $2
                 GROUP BY 1 ORDER BY 1
                """,
                venue, symbol,
            )
        return [dict(r) for r in rows]

    # ── derive reads ─────────────────────────────────────────────────────
    async def bucket_book_stats(
        self,
        venue: str,
        symbol: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
        *,
        qty_is_notional: bool = False,
    ) -> List[Dict[str, Any]]:
        """Per-bucket spread, last mid, update count and OFI from the applied
        top-of-book columns. One window pass in SQL; no jsonb is touched.

        OFI is in USD notional: quantities are multiplied by price unless the
        venue already quotes notional (`qty_is_notional`, Deribit).
        Bucket key is the bucket START; the derive anchors it to the end.
        """
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH applied AS (
                    SELECT ts, best_bid, best_bid_qty, best_ask, best_ask_qty,
                           LAG(best_bid)     OVER w AS pb, LAG(best_bid_qty) OVER w AS pbq,
                           LAG(best_ask)     OVER w AS pa, LAG(best_ask_qty) OVER w AS paq
                      FROM book_events
                     WHERE venue = $1 AND symbol = $2 AND applied
                       AND ts >= $3::timestamptz - ($5::int * INTERVAL '1 second') AND ts < $4::timestamptz
                       AND best_bid IS NOT NULL AND best_ask IS NOT NULL
                    WINDOW w AS (ORDER BY ts, update_id)
                ),
                scored AS (
                    SELECT ts,
                           (best_ask - best_bid) / ((best_ask + best_bid) / 2.0) * 10000.0 AS spread_bps,
                           (best_ask + best_bid) / 2.0 AS mid,
                           CASE WHEN pb IS NULL THEN 0.0 ELSE
                               (CASE WHEN best_bid >= pb THEN best_bid_qty * (CASE WHEN $6::bool THEN 1.0 ELSE best_bid END) ELSE 0.0 END)
                             - (CASE WHEN best_bid <= pb THEN pbq * (CASE WHEN $6::bool THEN 1.0 ELSE pb END) ELSE 0.0 END)
                             - (CASE WHEN best_ask <= pa THEN best_ask_qty * (CASE WHEN $6::bool THEN 1.0 ELSE best_ask END) ELSE 0.0 END)
                             + (CASE WHEN best_ask >= pa THEN paq * (CASE WHEN $6::bool THEN 1.0 ELSE pa END) ELSE 0.0 END)
                           END AS ofi
                      FROM applied
                     WHERE ts >= $3::timestamptz
                )
                SELECT to_timestamp(floor(extract(epoch FROM ts) / $5) * $5) AS bucket,
                       AVG(spread_bps) AS spread_bps,
                       (ARRAY_AGG(mid ORDER BY ts DESC))[1] AS mid_last,
                       COUNT(*) AS updates,
                       SUM(ofi) AS ofi
                  FROM scored
                 GROUP BY 1 ORDER BY 1
                """,
                venue, symbol, start, end, int(step_seconds), bool(qty_is_notional),
            )
        return [dict(r) for r in rows]

    async def bucket_trade_stats(
        self, venue: str, symbol: str, start: datetime, end: datetime, step_seconds: int
    ) -> List[Dict[str, Any]]:
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT to_timestamp(floor(extract(epoch FROM ts) / $5) * $5) AS bucket,
                       SUM(CASE WHEN is_buyer_maker THEN -notional_usd ELSE notional_usd END) AS cvd_usd,
                       SUM(notional_usd) AS volume_usd,
                       COUNT(*) AS trades,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY notional_usd) AS p50,
                       percentile_cont(0.9) WITHIN GROUP (ORDER BY notional_usd) AS p90
                  FROM trades
                 WHERE venue = $1 AND symbol = $2 AND ts >= $3 AND ts < $4
                 GROUP BY 1 ORDER BY 1
                """,
                venue, symbol, start, end, int(step_seconds),
            )
        return [dict(r) for r in rows]

    async def snapshots_in(
        self, venue: str, symbol: str, start: datetime, end: datetime
    ) -> List[Dict[str, Any]]:
        async with self._db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ts, update_id, reason, bids::text AS bids, asks::text AS asks
                  FROM book_snapshots
                 WHERE venue = $1 AND symbol = $2 AND ts >= $3 AND ts < $4
                 ORDER BY ts
                """,
                venue, symbol, start, end,
            )
        return [dict(r) for r in rows]

    async def latest_snapshot(self, venue: str, symbol: str) -> Optional[Dict[str, Any]]:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT ts, update_id, reason, bids::text AS bids, asks::text AS asks
                  FROM book_snapshots WHERE venue = $1 AND symbol = $2
                 ORDER BY ts DESC LIMIT 1
                """,
                venue, symbol,
            )
        return dict(row) if row else None

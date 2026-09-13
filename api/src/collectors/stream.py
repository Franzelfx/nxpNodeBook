#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/collectors/stream.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  One collector per configured stream: connect, sync, apply, persist, and
  do it again when the venue hangs up.

  THE JOB THAT CANNOT BE LATE. Every message this loop does not receive is
  history that does not exist. So the loop does exactly three things per
  message — update the in-memory book, hand rows to the writer queue, note
  the time — and never awaits the database, never computes a feature, never
  blocks on anything but the socket.

  STATE MACHINE (per session)

      connect ──► buffering ──► synced ──► (gap) ──► buffering ──► …
                     │                        │
                     └── snapshot arrives ────┘

  REST venues (Binance): while `buffering`, depth events queue up and a
  snapshot fetch runs concurrently. When it lands, the buffer is replayed
  through the venue's SyncRule; if the join holds the book is `synced` and
  live events apply directly. A sequence violation at any point — or a book
  that crosses, which is the same defect seen from the other side — marks the
  book untrustworthy, records the event as not applied, and starts a new
  snapshot fetch. Nothing is thrown away: the not-applied rows are written
  too, so a replay can see exactly where the stream became unreliable.

  In-band venues (Deribit): the snapshot IS a message. A gap is answered by
  resubscribing, which yields a new snapshot.

  WHAT GETS WRITTEN
    book_events    every depth message, applied or not, with the top of book
                   after applying when it was
    book_snapshots the REST snapshot (or in-band snapshot) at every sync, and
                   the in-memory book every `snapshot_interval_seconds`
    trades         every print
    stream_runs    one row per session, counters refreshed every 30 s

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from ..adapters.database.repositories.raw import RawRepository
from ..adapters.sources.base import Trade, VenueSource
from ..core.config import StreamSpec, settings
from ..core.logging import get_logger
from ..domain.book import RULES, DiffEvent, OrderBook, Snapshot, SyncRule
from .writer import DBWriter

logger = get_logger(__name__)

_RUN_UPDATE_SECONDS = 30.0


def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


@dataclass(slots=True)
class StreamState:
    """What /health and /book read. Plain attributes, written by one task."""

    connected: bool = False
    synced: bool = False
    last_event_at: Optional[datetime] = None
    update_id: Optional[int] = None
    events_session: int = 0
    trades_session: int = 0
    resyncs_session: int = 0
    reconnects: int = 0
    first_event_at: Optional[datetime] = None
    last_error: Optional[str] = None
    session_started: Optional[datetime] = None
    events_total: int = 0
    trades_total: int = 0


class StreamCollector:
    def __init__(
        self,
        spec: StreamSpec,
        source: VenueSource,
        repo: RawRepository,
        writer: DBWriter,
    ) -> None:
        self.spec = spec
        self.source = source
        self._repo = repo
        self._writer = writer
        self._rule: SyncRule = RULES[spec.venue]
        self.book = OrderBook()
        self.state = StreamState()
        self._stopping = False
        self._task: Optional[asyncio.Task] = None
        # session-local
        self._buffer: List[Tuple[DiffEvent, datetime]] = []
        self._snapshot_task: Optional[asyncio.Task] = None
        self._awaiting_first = False
        self._run_id: Optional[int] = None
        self._first_update_id: Optional[int] = None
        self._last_snapshot_mono = 0.0
        self._last_run_update_mono = 0.0
        self._resync_reason: Optional[str] = None

    # ── lifecycle ────────────────────────────────────────────────────────
    def start(self) -> None:
        self._task = asyncio.create_task(self.run(), name=f"collector-{self.spec.key}")

    async def stop(self) -> None:
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run(self) -> None:
        backoff = settings.reconnect_min_seconds
        while not self._stopping:
            reason = "closed"
            try:
                await self._open_session()
                backoff = settings.reconnect_min_seconds
                await self._session()
            except asyncio.CancelledError:
                reason = "shutdown"
                await self._close_session(reason)
                raise
            except Exception as exc:  # noqa: BLE001 — reconnect on anything
                reason = f"{type(exc).__name__}: {exc}"[:300]
                self.state.last_error = reason
                logger.warning("[%s] session ended: %s", self.spec.key, reason)
            await self._close_session(reason)
            if self._stopping:
                break
            self.state.reconnects += 1
            delay = backoff + random.uniform(0, backoff * 0.25)
            logger.info("[%s] reconnecting in %.1fs", self.spec.key, delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, settings.reconnect_max_seconds)

    async def _open_session(self) -> None:
        self._buffer.clear()
        self._awaiting_first = False
        self._first_update_id = None
        self.book.invalidate()
        self.state.synced = False
        self.state.events_session = 0
        self.state.trades_session = 0
        self.state.resyncs_session = 0
        await self.source.connect()
        self.state.connected = True
        self.state.session_started = datetime.now(timezone.utc)
        self._run_id = await self._repo.start_run(
            self.spec.venue, self.spec.symbol, self.source.describe()
        )
        self._last_run_update_mono = time.monotonic()
        logger.info("[%s] connected (run %s)", self.spec.key, self._run_id)
        self._start_snapshot_fetch("sync")

    async def _close_session(self, reason: str) -> None:
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
            self._snapshot_task = None
        try:
            await self.source.close()
        except Exception:  # noqa: BLE001
            pass
        self.state.connected = False
        self.state.synced = False
        self.book.invalidate()
        if self._run_id is not None:
            try:
                await self._repo.finish_run(
                    self._run_id,
                    reason,
                    last_update_id=self.book.update_id or None,
                    events=self.state.events_session,
                    trades=self.state.trades_session,
                    resyncs=self.state.resyncs_session,
                )
            except Exception:  # noqa: BLE001
                logger.exception("[%s] could not close run %s", self.spec.key, self._run_id)
            self._run_id = None

    # ── the message loop ─────────────────────────────────────────────────
    async def _session(self) -> None:
        async for msg in self.source.messages():
            now = datetime.now(timezone.utc)
            if isinstance(msg, DiffEvent):
                self._on_depth(msg, now)
            elif isinstance(msg, Trade):
                self._on_trade(msg, now)
            self._maybe_snapshot(now)
            await self._maybe_update_run()
            if self._stopping:
                return

    def _on_trade(self, t: Trade, now: datetime) -> None:
        self.state.trades_session += 1
        self.state.trades_total += 1
        self._writer.put(
            "trades",
            (
                _dt(t.ts_ms), self.spec.venue, self.spec.symbol, t.trade_id,
                t.price, t.qty, t.notional_usd, t.is_buyer_maker, now,
            ),
        )

    def _on_depth(self, ev: DiffEvent, now: datetime) -> None:
        self.state.events_session += 1
        self.state.events_total += 1
        self.state.last_event_at = now
        if self.state.first_event_at is None:
            self.state.first_event_at = now

        # In-band snapshot (Deribit): reset and go live.
        if ev.is_snapshot:
            self._adopt_snapshot(
                Snapshot(update_id=ev.update_id, bids=ev.bids, asks=ev.asks, ts_ms=ev.ts_ms),
                now,
                reason="sync" if self._first_update_id is None else "resync",
            )
            self._emit_event(ev, now, applied=True, kind="snapshot")
            self._awaiting_first = False
            self._buffer.clear()
            return

        if not self.book.synced:
            # Buffering until a snapshot lands. Check whether it has.
            self._buffer.append((ev, now))
            if self._snapshot_task is not None and self._snapshot_task.done():
                self._finish_snapshot_fetch(now)
            return

        ok = (
            self._rule.first_event_ok(ev, self.book.update_id)
            if self._awaiting_first
            else self._rule.next_event_ok(ev, self.book.update_id)
        )
        if not ok:
            self._gap(
                ev, now,
                f"sequence break: first={ev.first_update_id} prev={ev.prev_update_id} "
                f"u={ev.update_id} book={self.book.update_id}",
            )
            return
        self.book.apply(ev)
        self._awaiting_first = False
        if self.book.is_crossed():
            self._gap(ev, now, "book crossed after apply")
            return
        self.state.update_id = self.book.update_id
        self._emit_event(ev, now, applied=True)

    # ── sync / resync ────────────────────────────────────────────────────
    def _start_snapshot_fetch(self, reason: str) -> None:
        self._resync_reason = reason
        if self.spec.venue == "deribit":
            # In-band: nothing to fetch; a resubscribe brings a snapshot.
            if reason != "sync":
                self._snapshot_task = asyncio.create_task(self.source.request_resync())
            return
        if self._snapshot_task is not None and not self._snapshot_task.done():
            return
        self._snapshot_task = asyncio.create_task(self.source.fetch_snapshot())

    def _finish_snapshot_fetch(self, now: datetime) -> None:
        task = self._snapshot_task
        self._snapshot_task = None
        try:
            snapshot = task.result()
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] snapshot fetch failed: %s — retrying", self.spec.key, exc)
            self.state.last_error = f"snapshot: {exc}"[:300]
            self._start_snapshot_fetch(self._resync_reason or "resync")
            return
        if snapshot is None:
            return
        self._adopt_snapshot(snapshot, now, reason=self._resync_reason or "sync")
        self._emit_snapshot_event(snapshot, now)
        self._replay_buffer(snapshot, now)

    def _adopt_snapshot(self, snapshot: Snapshot, now: datetime, *, reason: str) -> None:
        self.book.reset(snapshot)
        self.state.synced = True
        self.state.update_id = snapshot.update_id
        if self._first_update_id is None:
            self._first_update_id = snapshot.update_id
        if reason != "sync":
            self.state.resyncs_session += 1
        self._awaiting_first = True
        self._write_book_snapshot(now, reason)
        self._last_snapshot_mono = time.monotonic()
        logger.info(
            "[%s] %s at update_id=%d (%d bids / %d asks)",
            self.spec.key, reason, snapshot.update_id, len(self.book.bids), len(self.book.asks),
        )

    def _replay_buffer(self, snapshot: Snapshot, now: datetime) -> None:
        buffered = self._buffer
        self._buffer = []
        for ev, received in buffered:
            if self._rule.drop_before_snapshot(ev, snapshot.update_id):
                self._emit_event(ev, received, applied=False)
                continue
            if not self.book.synced:
                # A gap already happened further up this buffer; keep the
                # rest buffered for the next snapshot.
                self._buffer.append((ev, received))
                continue
            ok = (
                self._rule.first_event_ok(ev, snapshot.update_id)
                if self._awaiting_first
                else self._rule.next_event_ok(ev, self.book.update_id)
            )
            if not ok:
                self._gap(ev, received, "sequence break during replay")
                continue
            self.book.apply(ev)
            self._awaiting_first = False
            if self.book.is_crossed():
                self._gap(ev, received, "book crossed during replay")
                continue
            self.state.update_id = self.book.update_id
            self._emit_event(ev, received, applied=True)

    def _gap(self, ev: DiffEvent, now: datetime, why: str) -> None:
        logger.warning("[%s] %s — resyncing", self.spec.key, why)
        self.book.invalidate()
        self.state.synced = False
        if self.spec.venue == "deribit":
            # Nothing to replay against: the next in-band snapshot supersedes
            # everything before it. Record the message as not applied.
            self._emit_event(ev, now, applied=False)
        else:
            # Keep it: the replay against the next snapshot decides whether it
            # is dropped or applied, and emits it either way.
            self._buffer.append((ev, now))
        self._start_snapshot_fetch("resync")

    # ── periodic snapshot & run bookkeeping ──────────────────────────────
    def _maybe_snapshot(self, now: datetime) -> None:
        if not self.book.synced:
            return
        if time.monotonic() - self._last_snapshot_mono >= settings.snapshot_interval_seconds:
            self._write_book_snapshot(now, "periodic")
            self._last_snapshot_mono = time.monotonic()

    async def _maybe_update_run(self) -> None:
        if self._run_id is None:
            return
        if time.monotonic() - self._last_run_update_mono < _RUN_UPDATE_SECONDS:
            return
        self._last_run_update_mono = time.monotonic()
        try:
            await asyncio.wait_for(
                self._repo.update_run(
                    self._run_id,
                    first_update_id=self._first_update_id,
                    last_update_id=self.book.update_id or None,
                    events=self.state.events_session,
                    trades=self.state.trades_session,
                    resyncs=self.state.resyncs_session,
                ),
                timeout=5.0,
            )
        except Exception as exc:  # noqa: BLE001 — bookkeeping never stalls capture
            logger.debug("[%s] run update skipped: %s", self.spec.key, exc)

    # ── row emission ─────────────────────────────────────────────────────
    def _emit_event(self, ev: DiffEvent, received: datetime, *, applied: bool, kind: str = "diff") -> None:
        top = self.book.top() if applied else None
        self._writer.put(
            "book_events",
            (
                _dt(ev.ts_ms), self.spec.venue, self.spec.symbol, ev.update_id,
                ev.first_update_id, ev.prev_update_id, kind, applied,
                json.dumps(ev.bids, separators=(",", ":")),
                json.dumps(ev.asks, separators=(",", ":")),
                top.best_bid if top else None, top.best_bid_qty if top else None,
                top.best_ask if top else None, top.best_ask_qty if top else None,
                received,
            ),
        )

    def _emit_snapshot_event(self, snapshot: Snapshot, now: datetime) -> None:
        """The REST snapshot as a book_events row of kind=snapshot, so a
        replay of book_events alone can find its anchors."""
        top = self.book.top()
        ts = _dt(snapshot.ts_ms) if snapshot.ts_ms else now
        self._writer.put(
            "book_events",
            (
                ts, self.spec.venue, self.spec.symbol, snapshot.update_id,
                None, None, "snapshot", True,
                json.dumps(snapshot.bids, separators=(",", ":")),
                json.dumps(snapshot.asks, separators=(",", ":")),
                top.best_bid, top.best_bid_qty, top.best_ask, top.best_ask_qty, now,
            ),
        )

    def _write_book_snapshot(self, now: datetime, reason: str) -> None:
        bids, asks = self.book.ladder(settings.snapshot_levels)
        self._writer.put(
            "book_snapshots",
            (
                now, self.spec.venue, self.spec.symbol, self.book.update_id, reason,
                len(self.book.bids), len(self.book.asks),
                json.dumps(bids, separators=(",", ":")),
                json.dumps(asks, separators=(",", ":")),
            ),
        )

    # ── operator views ───────────────────────────────────────────────────
    def top_levels(self, n: int = 10) -> Tuple[List[List[float]], List[List[float]]]:
        return self.book.ladder(n)

    def snapshot_state(self) -> dict[str, Any]:
        top = self.book.top()
        now = datetime.now(timezone.utc)
        lag = (
            int((now - self.state.last_event_at).total_seconds())
            if self.state.last_event_at else None
        )
        return {
            "venue": self.spec.venue,
            "symbol": self.spec.symbol,
            "label": self.spec.label,
            "connected": self.state.connected,
            "synced": self.book.synced,
            "last_event_at": self.state.last_event_at,
            "event_lag_seconds": lag,
            "update_id": self.book.update_id or None,
            "events_session": self.state.events_session,
            "trades_session": self.state.trades_session,
            "resyncs_session": self.state.resyncs_session,
            "reconnects": self.state.reconnects,
            "best_bid": top.best_bid,
            "best_ask": top.best_ask,
            "queue_depth": 0,
        }

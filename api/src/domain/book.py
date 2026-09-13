#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/domain/book.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  The in-memory L2 order book, and the per-venue rules that decide whether a
  diff message may be applied to it.

  A diff stream on its own is worthless: it tells you what CHANGED, not what
  the book IS. The venue protocol is snapshot-then-diffs, with sequence
  numbers on both so the join is exact. Getting that join wrong does not fail
  loudly — it produces a book that is plausibly shaped and quietly wrong,
  which is the worst kind of data. So the rules are written here once, per
  venue, as pure functions on sequence numbers, and tested as such.

  BINANCE SPOT  (docs: "How to manage a local order book correctly")
    1. buffer diff events; each carries U (first update id) and u (last).
    2. fetch a REST snapshot with lastUpdateId.
    3. drop every buffered event with u <= lastUpdateId.
    4. the first applied event must have U <= lastUpdateId + 1 <= u.
    5. thereafter each event's U must equal the previous u + 1.
    A violation of 4 or 5 means the book is gone: re-snapshot.

  BINANCE FUTURES  (USDⓈ-M; docs of the same name)
    Same shape, with `pu` (previous u) on every event:
    3. drop every buffered event with u < lastUpdateId.
    4. the first applied event must have U <= lastUpdateId <= u.
    5. thereafter each event's pu must equal the previous u.

  DERIBIT  (book.{instrument}.{interval} raw channel)
    The channel's first message is type "snapshot" with change_id; every
    later one is type "change" with prev_change_id. A prev_change_id that
    does not match the last change_id applied means a message was lost:
    resubscribe, which yields a fresh snapshot.

  The book itself is two dicts price → qty. Sorted views are built on demand
  (top-of-book after every message, the full ladder only on a snapshot), so
  applying a message is O(levels changed), not O(book).

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

Level = Tuple[float, float]  # (price, qty)


@dataclass(slots=True)
class DiffEvent:
    """One venue depth message, normalised.

    `bids`/`asks` are [price, qty] pairs as the venue quoted them (strings kept
    as strings so the raw layer stores exactly what was said). A qty of zero
    removes the level.
    """

    ts_ms: int                       # venue event time, epoch ms
    update_id: int                   # u / change_id
    first_update_id: Optional[int]   # U (Binance) / None
    prev_update_id: Optional[int]    # pu (Binance futures) / prev_change_id (Deribit) / None
    bids: List[List[str]]
    asks: List[List[str]]
    is_snapshot: bool = False        # Deribit sends the snapshot in-band


@dataclass(slots=True)
class Snapshot:
    update_id: int
    bids: List[List[str]]
    asks: List[List[str]]
    ts_ms: Optional[int] = None


class SyncRule:
    """The join between a REST snapshot and the diff stream, per venue."""

    def drop_before_snapshot(self, ev: DiffEvent, snapshot_id: int) -> bool:
        raise NotImplementedError

    def first_event_ok(self, ev: DiffEvent, snapshot_id: int) -> bool:
        raise NotImplementedError

    def next_event_ok(self, ev: DiffEvent, last_update_id: int) -> bool:
        raise NotImplementedError


class BinanceSpotRule(SyncRule):
    def drop_before_snapshot(self, ev: DiffEvent, snapshot_id: int) -> bool:
        return ev.update_id <= snapshot_id

    def first_event_ok(self, ev: DiffEvent, snapshot_id: int) -> bool:
        assert ev.first_update_id is not None
        return ev.first_update_id <= snapshot_id + 1 <= ev.update_id

    def next_event_ok(self, ev: DiffEvent, last_update_id: int) -> bool:
        assert ev.first_update_id is not None
        return ev.first_update_id == last_update_id + 1


class BinanceFuturesRule(SyncRule):
    def drop_before_snapshot(self, ev: DiffEvent, snapshot_id: int) -> bool:
        return ev.update_id < snapshot_id

    def first_event_ok(self, ev: DiffEvent, snapshot_id: int) -> bool:
        assert ev.first_update_id is not None
        return ev.first_update_id <= snapshot_id <= ev.update_id

    def next_event_ok(self, ev: DiffEvent, last_update_id: int) -> bool:
        return ev.prev_update_id == last_update_id


class DeribitRule(SyncRule):
    """Deribit's snapshot arrives in-band, so there is nothing to drop and the
    first event IS the snapshot."""

    def drop_before_snapshot(self, ev: DiffEvent, snapshot_id: int) -> bool:
        return False

    def first_event_ok(self, ev: DiffEvent, snapshot_id: int) -> bool:
        return ev.is_snapshot

    def next_event_ok(self, ev: DiffEvent, last_update_id: int) -> bool:
        return ev.prev_update_id == last_update_id


RULES: Dict[str, SyncRule] = {
    "binance_spot": BinanceSpotRule(),
    "binance_futures": BinanceFuturesRule(),
    "deribit": DeribitRule(),
}


@dataclass(slots=True)
class TopOfBook:
    best_bid: Optional[float]
    best_bid_qty: Optional[float]
    best_ask: Optional[float]
    best_ask_qty: Optional[float]

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread_bps(self) -> Optional[float]:
        mid = self.mid
        if mid is None or mid <= 0:
            return None
        return (self.best_ask - self.best_bid) / mid * 10_000.0


@dataclass(slots=True)
class OrderBook:
    """Two price ladders. Not thread-safe; owned by one collector task."""

    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    update_id: int = 0
    synced: bool = False

    def reset(self, snapshot: Snapshot) -> None:
        self.bids = {float(p): float(q) for p, q in snapshot.bids if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in snapshot.asks if float(q) > 0}
        self.update_id = snapshot.update_id
        self.synced = True

    def apply(self, ev: DiffEvent) -> None:
        _apply_side(self.bids, ev.bids)
        _apply_side(self.asks, ev.asks)
        self.update_id = ev.update_id

    def invalidate(self) -> None:
        self.synced = False

    def top(self) -> TopOfBook:
        if not self.bids or not self.asks:
            return TopOfBook(None, None, None, None)
        bb = max(self.bids)
        ba = min(self.asks)
        return TopOfBook(bb, self.bids[bb], ba, self.asks[ba])

    def ladder(self, levels: int = 0) -> Tuple[List[List[float]], List[List[float]]]:
        """Best-first ladders, optionally truncated to `levels` per side."""
        bids = sorted(self.bids.items(), key=lambda kv: -kv[0])
        asks = sorted(self.asks.items(), key=lambda kv: kv[0])
        if levels > 0:
            bids = bids[:levels]
            asks = asks[:levels]
        return [[p, q] for p, q in bids], [[p, q] for p, q in asks]

    def is_crossed(self) -> bool:
        """A bid at or above the best ask is not a market state; it is a book
        that has drifted from the venue. Treated as a gap."""
        if not self.bids or not self.asks:
            return False
        return max(self.bids) >= min(self.asks)


def _apply_side(side: Dict[float, float], levels: Iterable[Sequence[str]]) -> None:
    for price_s, qty_s in levels:
        price = float(price_s)
        qty = float(qty_s)
        if qty <= 0.0:
            side.pop(price, None)
        else:
            side[price] = qty


def depth_within(
    ladder: Sequence[Sequence[float]], mid: float, band_bps: float, *, notional: bool
) -> float:
    """Resting quantity within `band_bps` of `mid` on one best-first ladder.

    `notional=True` sums price × qty (a venue quoting qty in base units);
    False sums qty as-is (a venue already quoting in quote notional, such as
    Deribit's inverse perpetual whose amounts are USD).
    """
    limit = mid * band_bps / 10_000.0
    total = 0.0
    for price, qty in ladder:
        if abs(price - mid) > limit:
            break
        total += price * qty if notional else qty
    return total

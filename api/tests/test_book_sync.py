"""The snapshot/diff join, per venue — the part that fails silently if wrong."""

from __future__ import annotations

from src.domain.book import (
    BinanceFuturesRule,
    BinanceSpotRule,
    DeribitRule,
    DiffEvent,
    OrderBook,
    Snapshot,
    depth_within,
)


def ev(U, u, pu=None, bids=(), asks=(), snapshot=False) -> DiffEvent:
    return DiffEvent(
        ts_ms=0, update_id=u, first_update_id=U, prev_update_id=pu,
        bids=[list(map(str, b)) for b in bids], asks=[list(map(str, a)) for a in asks],
        is_snapshot=snapshot,
    )


# ── Binance spot ─────────────────────────────────────────────────────────
def test_spot_drops_events_at_or_before_snapshot() -> None:
    r = BinanceSpotRule()
    assert r.drop_before_snapshot(ev(90, 100), snapshot_id=100)
    assert not r.drop_before_snapshot(ev(101, 105), snapshot_id=100)


def test_spot_first_event_must_bracket_snapshot_plus_one() -> None:
    r = BinanceSpotRule()
    assert r.first_event_ok(ev(98, 103), snapshot_id=100)     # 98 <= 101 <= 103
    assert r.first_event_ok(ev(101, 101), snapshot_id=100)    # exactly next
    assert not r.first_event_ok(ev(102, 110), snapshot_id=100)  # a gap: 101 missing
    assert not r.first_event_ok(ev(95, 100), snapshot_id=100)   # entirely before


def test_spot_subsequent_events_must_be_contiguous() -> None:
    r = BinanceSpotRule()
    assert r.next_event_ok(ev(104, 109), last_update_id=103)
    assert not r.next_event_ok(ev(105, 109), last_update_id=103)
    assert not r.next_event_ok(ev(103, 109), last_update_id=103)


# ── Binance futures ──────────────────────────────────────────────────────
def test_futures_rules_use_pu_and_inclusive_bracket() -> None:
    r = BinanceFuturesRule()
    assert r.drop_before_snapshot(ev(90, 99), snapshot_id=100)
    assert not r.drop_before_snapshot(ev(90, 100), snapshot_id=100)
    assert r.first_event_ok(ev(98, 103), snapshot_id=100)
    assert r.first_event_ok(ev(100, 100), snapshot_id=100)
    assert not r.first_event_ok(ev(101, 103), snapshot_id=100)
    assert r.next_event_ok(ev(104, 109, pu=103), last_update_id=103)
    assert not r.next_event_ok(ev(104, 109, pu=102), last_update_id=103)


# ── Deribit ──────────────────────────────────────────────────────────────
def test_deribit_snapshot_is_in_band_and_chain_is_by_prev_change_id() -> None:
    r = DeribitRule()
    assert not r.drop_before_snapshot(ev(None, 5), snapshot_id=0)
    assert r.first_event_ok(ev(None, 5, snapshot=True), snapshot_id=0)
    assert not r.first_event_ok(ev(None, 5), snapshot_id=0)
    assert r.next_event_ok(ev(None, 6, pu=5), last_update_id=5)
    assert not r.next_event_ok(ev(None, 7, pu=5), last_update_id=6)


# ── the book itself ──────────────────────────────────────────────────────
def test_book_apply_removes_zero_qty_and_tracks_top() -> None:
    b = OrderBook()
    b.reset(Snapshot(update_id=1, bids=[["100", "2"], ["99", "1"]], asks=[["101", "3"], ["102", "4"]]))
    assert b.synced and b.top().best_bid == 100 and b.top().best_ask == 101
    b.apply(ev(2, 2, bids=[(100, 0), (99.5, 5)], asks=[(101, 1)]))
    top = b.top()
    assert top.best_bid == 99.5 and top.best_bid_qty == 5
    assert top.best_ask == 101 and top.best_ask_qty == 1
    assert b.update_id == 2
    assert abs(top.spread_bps - (1.5 / 100.25 * 10_000)) < 1e-9
    assert not b.is_crossed()
    b.apply(ev(3, 3, bids=[(101.5, 1)]))
    assert b.is_crossed()


def test_ladder_is_best_first_and_truncated() -> None:
    b = OrderBook()
    b.reset(Snapshot(update_id=1, bids=[["98", "1"], ["100", "1"], ["99", "1"]], asks=[["103", "1"], ["101", "1"]]))
    bids, asks = b.ladder(2)
    assert [p for p, _ in bids] == [100, 99]
    assert [p for p, _ in asks] == [101, 103]


def test_depth_within_band_stops_at_band_edge() -> None:
    mid = 100.0
    bids = [[99.95, 1.0], [99.9, 2.0], [99.0, 100.0]]
    # 10 bp of 100 = 0.1 → includes 99.95 and 99.9, not 99.0
    assert depth_within(bids, mid, 10, notional=False) == 3.0
    assert abs(depth_within(bids, mid, 10, notional=True) - (99.95 + 199.8)) < 1e-9
    assert depth_within(bids, mid, 200, notional=False) == 103.0

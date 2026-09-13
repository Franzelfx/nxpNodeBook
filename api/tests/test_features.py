"""Feature arithmetic on synthetic bucket rows and snapshots."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from src.core.config import StreamSpec
from src.derive import features as F

UTC = timezone.utc
S = StreamSpec(venue="binance_spot", symbol="BTCUSDT", label="btc.spot")
D = StreamSpec(venue="deribit", symbol="BTC-PERPETUAL", label="btc.dperp")
T0 = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)


def test_bucket_rows_anchor_at_bucket_end() -> None:
    rows = list(F.book_rows(S, [{"bucket": T0, "spread_bps": 1.5, "mid_last": 77000.0, "updates": 42, "ofi": -12.5}], 300))
    assert {r[0] for r in rows} == {T0 + timedelta(minutes=5)}
    assert {f"{r[1]}.{r[2]}": r[3] for r in rows} == {
        "BOOK.btc.spot.spread_bps": 1.5,
        "BOOK.btc.spot.mid": 77000.0,
        "BOOK.btc.spot.updates": 42.0,
        "MICRO.btc.spot.ofi_usd": -12.5,
    }


def test_trade_rows_skip_nulls() -> None:
    rows = list(F.trade_rows(S, [{"bucket": T0, "cvd_usd": 10.0, "volume_usd": 30.0, "trades": 3, "p50": None, "p90": 20.0}], 300))
    names = {f"{r[1]}.{r[2]}" for r in rows}
    assert "MICRO.btc.spot.trade_p50_usd" not in names
    assert "MICRO.btc.spot.trade_p90_usd" in names


def _snap(ts, bids, asks, as_text=True):
    return {"ts": ts, "update_id": 1, "reason": "periodic",
            "bids": json.dumps(bids) if as_text else bids, "asks": json.dumps(asks) if as_text else asks}


def test_snapshot_depth_and_imbalance_in_usd_notional() -> None:
    bids = [[99.95, 1.0], [99.9, 1.0], [95.0, 100.0]]   # within 10bp of mid 100: 99.95, 99.9
    asks = [[100.05, 3.0], [110.0, 100.0]]
    snap = _snap(T0 + timedelta(seconds=70), bids, asks)
    rows = {f"{r[1]}.{r[2]}": (r[0], r[3]) for r in F.snapshot_rows(S, [snap], [10], 300, qty_is_notional=False)}
    ts, bid_depth = rows["BOOK.btc.spot.depth_bid_10bp"]
    assert ts == T0 + timedelta(minutes=5)   # 12:01:10 → ceiling 12:05
    assert abs(bid_depth - (99.95 + 99.9)) < 1e-9
    _, ask_depth = rows["BOOK.btc.spot.depth_ask_10bp"]
    assert abs(ask_depth - 300.15) < 1e-9
    _, imb = rows["BOOK.btc.spot.imbalance_10bp"]
    assert abs(imb - ((199.85 - 300.15) / (199.85 + 300.15))) < 1e-9


def test_deribit_amounts_are_not_multiplied_by_price() -> None:
    snap = _snap(T0 + timedelta(seconds=1), [[99.9, 1000.0]], [[100.1, 500.0]])
    rows = {f"{r[1]}.{r[2]}": r[3] for r in F.snapshot_rows(D, [snap], [50], 300, qty_is_notional=True)}
    assert rows["BOOK.btc.dperp.depth_bid_50bp"] == 1000.0
    assert rows["BOOK.btc.dperp.depth_ask_50bp"] == 500.0


def test_crossed_snapshot_is_skipped() -> None:
    snap = _snap(T0, [[101.0, 1.0]], [[100.0, 1.0]])
    assert list(F.snapshot_rows(S, [snap], [10], 300, qty_is_notional=False)) == []


def test_two_snapshots_in_one_bucket_are_averaged() -> None:
    a = _snap(T0 + timedelta(seconds=10), [[99.99, 1.0]], [[100.01, 1.0]])
    b = _snap(T0 + timedelta(seconds=20), [[99.99, 3.0]], [[100.01, 1.0]])
    rows = {f"{r[1]}.{r[2]}": r[3] for r in F.snapshot_rows(S, [a, b], [10], 300, qty_is_notional=False)}
    assert abs(rows["BOOK.btc.spot.depth_bid_10bp"] - (99.99 * 1 + 99.99 * 3) / 2) < 1e-9

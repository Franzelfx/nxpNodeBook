"""Venue payloads → normalised messages."""

from __future__ import annotations

import json

import pytest

from src.adapters.sources.binance import BinanceSource
from src.adapters.sources.deribit import DeribitSource
from src.core.config import parse_streams
from src.domain.book import DiffEvent


class _WS:
    def __init__(self, frames):
        self._frames = [json.dumps(f) for f in frames]
        self.sent = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._frames:
            raise StopAsyncIteration
        return self._frames.pop(0)

    async def send(self, s):
        self.sent.append(json.loads(s))


@pytest.mark.asyncio
async def test_binance_combined_stream_parses_depth_and_aggtrade() -> None:
    src = BinanceSource("binance_futures", "btcusdt")
    src._ws = _WS([
        {"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "E": 1000, "s": "BTCUSDT", "U": 5, "u": 7, "pu": 4,
                                                   "b": [["77000.10", "1.5"]], "a": [["77000.20", "0"]]}},
        {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "E": 1001, "a": 99, "p": "77000.10", "q": "0.002", "T": 1000, "m": False}},
        {"stream": "btcusdt@trade", "data": {"e": "trade", "E": 1002, "t": 100, "p": "77000.10", "q": "0.002", "T": 1001, "m": True}},
    ])
    out = [m async for m in src.messages()]
    d, t, t2 = out
    assert t2.trade_id == 100 and t2.is_buyer_maker is True
    assert isinstance(d, DiffEvent) and d.update_id == 7 and d.first_update_id == 5 and d.prev_update_id == 4
    assert d.bids == [["77000.10", "1.5"]] and d.asks == [["77000.20", "0"]]
    assert t.trade_id == 99 and abs(t.notional_usd - 154.0002) < 1e-6 and t.is_buyer_maker is False
    assert src.symbol == "BTCUSDT"
    assert "fstream" in src.describe()["ws"] and "@trade" in src.describe()["ws"]


@pytest.mark.asyncio
async def test_deribit_snapshot_change_trades_and_heartbeat() -> None:
    src = DeribitSource("BTC-PERPETUAL")
    ws = _WS([
        {"jsonrpc": "2.0", "id": 1, "result": "ok"},
        {"method": "heartbeat", "params": {"type": "test_request"}},
        {"method": "subscription", "params": {"channel": "book.BTC-PERPETUAL.100ms",
         "data": {"type": "snapshot", "timestamp": 5, "change_id": 10, "bids": [["new", 77000.0, 5000.0]], "asks": [["new", 77000.5, 100.0]]}}},
        {"method": "subscription", "params": {"channel": "book.BTC-PERPETUAL.100ms",
         "data": {"type": "change", "timestamp": 6, "change_id": 11, "prev_change_id": 10, "bids": [["delete", 77000.0, 0.0]], "asks": [["change", 77000.5, 200.0]]}}},
        {"method": "subscription", "params": {"channel": "trades.BTC-PERPETUAL.100ms",
         "data": [{"trade_seq": 3, "trade_id": "x", "timestamp": 7, "price": 77000.5, "amount": 1000.0, "direction": "sell"}]}},
    ])
    src._ws = ws
    out = [m async for m in src.messages()]
    snap, chg, trade = out
    assert snap.is_snapshot and snap.update_id == 10 and snap.bids == [["77000.0", "5000.0"]]
    assert not chg.is_snapshot and chg.prev_update_id == 10 and chg.bids == [["77000.0", "0"]] and chg.asks == [["77000.5", "200.0"]]
    assert trade.notional_usd == 1000.0 and trade.qty == 1000.0 and trade.is_buyer_maker is True
    # the heartbeat test_request was answered
    assert any(m["method"] == "public/test" for m in ws.sent)


def test_parse_streams_rejects_bad_entries() -> None:
    specs = parse_streams("binance_spot:btcusdt:btc.spot, deribit:BTC-PERPETUAL:btc.dperp")
    assert [s.label for s in specs] == ["btc.spot", "btc.dperp"]
    for bad in ("kraken:XBT:x", "binance_spot:BTCUSDT", "binance_spot:BTCUSDT:a,deribit:B:a", ""):
        with pytest.raises(ValueError):
            parse_streams(bad)

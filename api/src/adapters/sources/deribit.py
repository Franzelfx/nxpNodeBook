#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/adapters/sources/deribit.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Deribit over its JSON-RPC WebSocket: `book.<instrument>.<interval>` (raw
  book, in-band snapshot) and `trades.<instrument>.<interval>`.

  Differences from Binance that shape this adapter:

  • The snapshot is IN-BAND: the first message on the book channel is
    type "snapshot"; every later one is type "change" with prev_change_id.
    A resync is therefore a resubscribe, not a REST call.
  • Levels come as [action, price, amount] with action new|change|delete.
    Normalised to [price, qty] with qty "0" on delete, so the raw layer and
    the book code see one shape across venues.
  • Amounts on inverse perpetuals are USD, not BTC. `qty_is_notional` tells
    the collector not to multiply by price again.
  • Deribit expects a heartbeat contract: we ask for one, and every
    test_request must be answered with public/test or the server drops us.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import json
from itertools import count
from typing import AsyncIterator, List, Optional

from websockets.asyncio.client import ClientConnection, connect

from ...core.config import settings
from ...core.logging import get_logger
from ...domain.book import DiffEvent, Snapshot
from .base import Message, Trade, VenueSource

logger = get_logger(__name__)


class DeribitSource(VenueSource):
    venue = "deribit"
    qty_is_notional = True

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol.upper()
        self._ws: Optional[ClientConnection] = None
        self._ids = count(1)
        self._book_channel = f"book.{self.symbol}.{settings.deribit_book_interval}"
        self._trade_channel = f"trades.{self.symbol}.{settings.deribit_book_interval}"

    async def _rpc(self, method: str, params: Optional[dict] = None) -> None:
        assert self._ws is not None
        await self._ws.send(
            json.dumps({"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params or {}})
        )

    async def connect(self) -> None:
        self._ws = await connect(
            settings.deribit_ws,
            max_size=8 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=20,
            # A close handshake the venue does not answer must not hold up a
            # shutdown: three sessions closing sequentially at the library's
            # 10 s default were what left orphan runs on 2026-09-13.
            close_timeout=2,
            user_agent_header=settings.user_agent,
        )
        await self._rpc("public/set_heartbeat", {"interval": 30})
        await self._rpc("public/subscribe", {"channels": [self._book_channel, self._trade_channel]})

    async def close(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            finally:
                self._ws = None

    async def aclose(self) -> None:
        await self.close()

    async def request_resync(self) -> None:
        """Resubscribing the book channel yields a fresh in-band snapshot."""
        await self._rpc("public/unsubscribe", {"channels": [self._book_channel]})
        await asyncio.sleep(0.2)
        await self._rpc("public/subscribe", {"channels": [self._book_channel]})

    async def fetch_snapshot(self) -> Optional[Snapshot]:
        return None

    @staticmethod
    def _levels(raw: List[list]) -> List[List[str]]:
        out: List[List[str]] = []
        for action, price, amount in raw:
            qty = "0" if action == "delete" else str(amount)
            out.append([str(price), qty])
        return out

    async def messages(self) -> AsyncIterator[Message]:
        assert self._ws is not None, "connect() first"
        async for raw in self._ws:
            msg = json.loads(raw)
            method = msg.get("method")
            if method == "heartbeat":
                if (msg.get("params") or {}).get("type") == "test_request":
                    await self._rpc("public/test")
                continue
            if method != "subscription":
                # RPC replies (subscribe acks, heartbeat acks, errors). An error
                # is worth a line; a normal ack is not.
                if "error" in msg:
                    logger.warning("[deribit] rpc error: %s", msg["error"])
                continue
            params = msg.get("params") or {}
            channel = params.get("channel", "")
            data = params.get("data")
            if channel == self._book_channel and isinstance(data, dict):
                yield DiffEvent(
                    ts_ms=int(data["timestamp"]),
                    update_id=int(data["change_id"]),
                    first_update_id=None,
                    prev_update_id=int(data["prev_change_id"]) if "prev_change_id" in data else None,
                    bids=self._levels(data.get("bids") or []),
                    asks=self._levels(data.get("asks") or []),
                    is_snapshot=data.get("type") == "snapshot",
                )
            elif channel == self._trade_channel and isinstance(data, list):
                for t in data:
                    price = float(t["price"])
                    amount = float(t["amount"])  # USD on inverse perpetuals
                    yield Trade(
                        ts_ms=int(t["timestamp"]),
                        trade_id=int(t["trade_seq"]),
                        price=price,
                        qty=amount,
                        notional_usd=amount,
                        is_buyer_maker=(t.get("direction") == "sell"),
                    )

    def describe(self) -> dict:
        return {
            "venue": self.venue,
            "symbol": self.symbol,
            "ws": settings.deribit_ws,
            "channels": [self._book_channel, self._trade_channel],
        }


def make_source(venue: str, symbol: str) -> VenueSource:
    if venue == "deribit":
        return DeribitSource(symbol)
    from .binance import BinanceSource

    return BinanceSource(venue, symbol)

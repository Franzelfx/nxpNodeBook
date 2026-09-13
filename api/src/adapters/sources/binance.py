#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/adapters/sources/binance.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Binance spot and USDⓈ-M futures over one combined WebSocket stream:
  `<symbol>@depth@100ms` (diff depth) and `<symbol>@trade` (every print),
  plus the REST depth snapshot the diff stream is joined to.

  `@trade` rather than `@aggTrade`, for two reasons: the futures endpoint
  does not deliver aggTrade on the combined stream at all (measured
  2026-09-13 — the subscription is accepted and nothing arrives), and the
  raw layer should hold the finest tape the venue offers. Both payloads are
  parsed, so the stream can be switched per deployment.

  Two things the venue does that the code has to expect:

  • Connections are closed by the server after 24 hours. That is not an
    error; it is a reconnect, and the collector records it as one session
    ending and another beginning.
  • The server pings every 20 s (spot) / 3 min (futures) and drops a client
    that does not pong. The websockets library answers pings on its own; the
    client's own keepalive ping is left at its default so a half-open
    connection is noticed within about 40 s rather than never.

  Everything is public. No key, no signing, no account.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
from typing import AsyncIterator, Optional

import httpx
from websockets.asyncio.client import ClientConnection, connect

from ...core.config import settings
from ...core.logging import get_logger
from ...domain.book import DiffEvent, Snapshot
from .base import Message, Trade, VenueSource

logger = get_logger(__name__)


class BinanceSource(VenueSource):
    qty_is_notional = False

    def __init__(self, venue: str, symbol: str) -> None:
        assert venue in ("binance_spot", "binance_futures")
        self.venue = venue
        self.symbol = symbol.upper()
        self._futures = venue == "binance_futures"
        self._ws_base = settings.binance_futures_ws if self._futures else settings.binance_spot_ws
        self._rest_base = settings.binance_futures_rest if self._futures else settings.binance_spot_rest
        self._snapshot_limit = (
            settings.binance_snapshot_limit_futures if self._futures else settings.binance_snapshot_limit_spot
        )
        self._ws: Optional[ClientConnection] = None
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(20.0),
            headers={"User-Agent": settings.user_agent},
        )

    # ── transport ────────────────────────────────────────────────────────
    def _stream_url(self) -> str:
        sym = self.symbol.lower()
        streams = f"{sym}@depth@{settings.binance_depth_speed}/{sym}@{settings.binance_trade_stream}"
        return f"{self._ws_base}/stream?streams={streams}"

    async def connect(self) -> None:
        self._ws = await connect(
            self._stream_url(),
            max_size=8 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=20,
            # A close handshake the venue does not answer must not hold up a
            # shutdown: three sessions closing sequentially at the library's
            # 10 s default were what left orphan runs on 2026-09-13.
            close_timeout=2,
            user_agent_header=settings.user_agent,
        )

    async def close(self) -> None:
        if self._ws is not None:
            try:
                await self._ws.close()
            finally:
                self._ws = None

    async def aclose(self) -> None:
        await self.close()
        await self._http.aclose()

    async def messages(self) -> AsyncIterator[Message]:
        assert self._ws is not None, "connect() first"
        async for raw in self._ws:
            msg = json.loads(raw)
            data = msg.get("data")
            if not data:
                continue
            kind = data.get("e")
            if kind == "depthUpdate":
                yield DiffEvent(
                    ts_ms=int(data["E"]),
                    update_id=int(data["u"]),
                    first_update_id=int(data["U"]),
                    prev_update_id=int(data["pu"]) if "pu" in data else None,
                    bids=data.get("b") or [],
                    asks=data.get("a") or [],
                )
            elif kind in ("trade", "aggTrade"):
                price = float(data["p"])
                qty = float(data["q"])
                yield Trade(
                    ts_ms=int(data["T"]),
                    trade_id=int(data["t"] if kind == "trade" else data["a"]),
                    price=price,
                    qty=qty,
                    notional_usd=price * qty,
                    is_buyer_maker=bool(data["m"]),
                )

    # ── snapshot ─────────────────────────────────────────────────────────
    async def fetch_snapshot(self) -> Optional[Snapshot]:
        path = "/fapi/v1/depth" if self._futures else "/api/v3/depth"
        resp = await self._http.get(
            f"{self._rest_base}{path}",
            params={"symbol": self.symbol, "limit": self._snapshot_limit},
        )
        resp.raise_for_status()
        body = resp.json()
        return Snapshot(
            update_id=int(body["lastUpdateId"]),
            bids=body.get("bids") or [],
            asks=body.get("asks") or [],
            ts_ms=int(body["E"]) if "E" in body else None,
        )

    def describe(self) -> dict:
        return {
            "venue": self.venue,
            "symbol": self.symbol,
            "ws": self._stream_url(),
            "snapshot_limit": self._snapshot_limit,
        }

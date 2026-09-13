#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/adapters/sources/base.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  What a venue source looks like to the collector. One interface, so the
  collector's sync/resync/persist loop is written once and every venue is a
  transport adapter under it.

  A source yields two kinds of message on one connection: a DiffEvent (book
  change; on Deribit also the in-band snapshot) and a Trade. It also knows
  how to obtain a snapshot the venue's way — a REST call on Binance, a
  resubscribe on Deribit — and whether the venue's quantities are already
  quote notional (Deribit) or base units (Binance).

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Optional, Union

from ...domain.book import DiffEvent, Snapshot


@dataclass(slots=True)
class Trade:
    ts_ms: int
    trade_id: int
    price: float
    qty: float             # venue unit
    notional_usd: float
    is_buyer_maker: bool   # aggressor sold


Message = Union[DiffEvent, Trade]


class VenueSource:
    venue: str = ""
    symbol: str = ""
    # True when the venue's book/trade quantities are already USD notional.
    qty_is_notional: bool = False

    async def connect(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    def messages(self) -> AsyncIterator[Message]:
        raise NotImplementedError

    async def fetch_snapshot(self) -> Optional[Snapshot]:
        """A REST snapshot, or None when the venue delivers it in-band."""
        raise NotImplementedError

    async def request_resync(self) -> None:
        """Ask an in-band venue for a fresh snapshot. No-op for REST venues."""
        return None

    def describe(self) -> dict:
        return {"venue": self.venue, "symbol": self.symbol}

#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/api/routes/book.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Operator views of the LIVE in-memory books. Not part of the warehouse
  contract — these exist so a human (or a dashboard) can see the book the
  collector is holding without opening a database session.

    GET /book/{label}          top N levels, once
    GET /stream/book/{label}   the same, as a Server-Sent Events stream

  The SSE stream is the one-way fan-out for browsers: it polls the
  collector's state a few times a second and emits only when the venue
  sequence advanced. It reads memory the collector already maintains, so a
  hundred subscribers cost the capture loop nothing.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Annotated, AsyncIterator

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.responses import StreamingResponse

from ...core.dependencies import get_collectors
from ...domain.schemas import BookLevelsResponse

router = APIRouter(tags=["Book"])


def _view(label: str, levels: int) -> BookLevelsResponse:
    c = get_collectors().by_label(label)
    if c is None:
        raise HTTPException(status_code=404, detail=f"unknown stream label {label!r}; see /capabilities")
    bids, asks = c.top_levels(levels)
    top = c.book.top()
    return BookLevelsResponse(
        venue=c.spec.venue,
        symbol=c.spec.symbol,
        label=label,
        synced=c.book.synced,
        update_id=c.book.update_id or None,
        as_of=c.state.last_event_at,
        bids=bids,
        asks=asks,
        mid=top.mid,
        spread_bps=top.spread_bps,
    )


@router.get("/book/{label}", response_model=BookLevelsResponse)
async def get_book(
    label: Annotated[str, Path(description="Stream label, e.g. `btc.spot`.", examples=["btc.spot"])],
    levels: Annotated[int, Query(ge=1, le=500)] = 10,
) -> BookLevelsResponse:
    """Top of the live in-memory book."""
    return _view(label, levels)


@router.get("/stream/book/{label}")
async def stream_book(
    request: Request,
    label: Annotated[str, Path(description="Stream label, e.g. `btc.spot`.")],
    levels: Annotated[int, Query(ge=1, le=100)] = 10,
    interval_ms: Annotated[int, Query(ge=100, le=5000, description="Minimum time between events.")] = 250,
) -> StreamingResponse:
    """Server-Sent Events: the top of the book whenever the venue sequence advanced.

    `event: book` with a JSON payload identical to GET /book/{label};
    `event: ping` every 15 s so idle proxies keep the connection open.
    """
    _view(label, 1)  # 404 before the stream starts

    async def gen() -> AsyncIterator[bytes]:
        last_id = None
        last_ping = datetime.now(timezone.utc)
        while not await request.is_disconnected():
            view = _view(label, levels)
            if view.update_id != last_id:
                last_id = view.update_id
                yield f"event: book\ndata: {json.dumps(view.model_dump(mode='json'))}\n\n".encode()
            now = datetime.now(timezone.utc)
            if (now - last_ping).total_seconds() >= 15:
                last_ping = now
                yield b"event: ping\ndata: {}\n\n"
            await asyncio.sleep(interval_ms / 1000.0)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

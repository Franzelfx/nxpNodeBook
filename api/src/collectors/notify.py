#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/collectors/notify.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-23

Description:
  Telling the warehouse that rows landed — doc/54 E5.

  STRICTLY AN OPTIMISATION, and the whole module is built around that word.
  The warehouse still polls this node on its own schedule and its pipeline
  still runs; this only shortens the wait between a flush here and a chart
  there. A node that never calls it is SLOWER, never wrong.

  Which is why every failure path here is a shrug:

    • not configured      → do nothing, for the life of the process
    • warehouse down      → log at debug, carry on
    • timeout             → 2 seconds, then carry on
    • too soon since last → skip

  It must never slow the writer down. The writer's own docstring explains
  why: a collector that blocks stalls its WebSocket read, the venue's send
  buffer fills, and the venue drops the connection — a slow dependency
  becomes a hole in the historical record. So this is fire-and-forget on the
  writer's own loop, never awaited by a COPY.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from typing import Optional

import httpx

from ..core.logging import get_logger

logger = get_logger(__name__)

#: Never announce more often than this. A flush runs every second or two;
#: the warehouse only needs to know that something landed, not how much.
MIN_INTERVAL_S = 5.0

#: A notify that has not answered in this long has already cost more than it
#: saves.
TIMEOUT_S = 2.0

_last_sent = 0.0
_warned = False


def _config() -> tuple[Optional[str], Optional[str], Optional[str]]:
    """(url, secret, source_id) — all three, or nothing."""
    url = os.getenv("NXP_WAREHOUSE_NOTIFY_URL", "").strip()
    secret = os.getenv("NXP_STREAM_TICKET_SECRET", "").strip()
    source_id = os.getenv("NXP_WAREHOUSE_SOURCE_ID", "").strip()
    if not (url and secret and source_id):
        return None, None, None
    return url, secret, source_id


async def announce(rows: int, watermark: Optional[str] = None) -> bool:
    """
    Tell the warehouse `rows` landed. True when it accepted.

    Returns False for every "did not happen" — unconfigured, rate-limited,
    unreachable, refused. The caller is expected to ignore the answer; it is
    returned for tests and for the health page, not for control flow.
    """
    global _last_sent, _warned

    url, secret, source_id = _config()
    if not url:
        if not _warned:
            logger.info("[notify] no warehouse notify configured; the poll is the only path")
            _warned = True
        return False

    now = time.monotonic()
    if now - _last_sent < MIN_INTERVAL_S:
        return False
    _last_sent = now

    # `ts` is inside the signature: without it a captured request is
    # replayable forever. The warehouse refuses anything outside a 300 s
    # window.
    ts = int(time.time())
    payload = f"{source_id}:{rows}:{watermark or ''}:{ts}".encode()
    signature = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S) as client:
            response = await client.post(
                url,
                json={"source_id": source_id, "rows": rows, "watermark": watermark, "ts": ts},
                headers={"X-Nxp-Signature": signature},
            )
        if response.status_code >= 400:
            logger.debug("[notify] warehouse answered %s", response.status_code)
            return False
        return bool(response.json().get("accepted"))
    except Exception as exc:  # noqa: BLE001 — see the module docstring
        logger.debug("[notify] not delivered: %s", exc)
        return False


__all__ = ["MIN_INTERVAL_S", "TIMEOUT_S", "announce"]

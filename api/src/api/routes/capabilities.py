#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/api/routes/capabilities.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  The capability route — answered from the in-memory catalog and the
  collectors' own state, no database read, no venue call. Polled by the
  warehouse on every ingest run; nothing that can block belongs here.

    /health        is this node writing RIGHT NOW?
    /coverage      can I trust this column over THIS WINDOW?
    /capabilities  what does this column MEAN, and how must it be read?

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from nxp_node_contract.capability import NodeCapabilities

from ...core import dependencies
from ...domain.capabilities import node_capabilities

router = APIRouter(tags=["Contracts"])


@router.get(
    "/capabilities",
    response_model=NodeCapabilities,
    summary="What this node publishes, and how each column must be read",
    description=(
        "Machine-readable declaration of every published column.\n\n"
        "**Three fields are decisive** — everything else is explanation:\n\n"
        "- `empty_bucket` — what a missing point MEANS (`absent` everywhere here: "
        "a missing bucket is one in which the collector was not connected)\n"
        "- `fill` — whether the last value may be carried forward (`none` everywhere)\n"
        "- `max_carry_seconds` — for how long\n\n"
        "**There is no backward fill and never will be.**"
    ),
)
async def get_capabilities(request: Request) -> NodeCapabilities:
    try:
        available_from = {}
        try:
            for c in dependencies.get_collectors().collectors.values():
                if c.state.first_event_at is not None:
                    available_from[c.spec.label] = c.state.first_event_at
        except RuntimeError:
            pass
        return node_capabilities(app=request.app, available_from=available_from)
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc))

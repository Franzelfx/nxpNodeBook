#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/api/routes/root.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Root endpoint.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from fastapi import APIRouter

from ...core.config import settings
from ...domain.schemas import RootResponse

router = APIRouter()


@router.get("/", response_model=RootResponse)
async def root() -> RootResponse:
    return RootResponse(
        api=settings.api_title,
        version=settings.api_version,
        node_id=settings.node_id,
        endpoints={
            "metrics": "/metrics/{metric_name}",
            "catalog": "/metrics/catalog",
            "book": "/book/{label}",
            "stream": "/stream/book/{label}",
            "capabilities": "/capabilities",
            "coverage": "/coverage",
            "health": "/health",
            "docs": "/docs",
        },
        documentation="/docs",
    )

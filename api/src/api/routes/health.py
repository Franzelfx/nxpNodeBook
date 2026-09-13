#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/api/routes/health.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Health route.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from fastapi import APIRouter

from ...core.dependencies import get_health_service
from ...domain.schemas import HealthResponse

router = APIRouter(tags=["Contracts"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Database, per-stream capture freshness, grid freshness and the last derive run."""
    return await get_health_service().check()

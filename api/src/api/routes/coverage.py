#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/api/routes/coverage.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Coverage export. The research side asserts on this before running a screen:
  when each stream began, how many sessions and resyncs it has had, snapshot
  density per day, and what the last derive run's gates said.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from ...core.dependencies import get_derive_runner

router = APIRouter(tags=["Contracts"])


@router.get("/coverage")
async def get_coverage() -> Dict[str, Any]:
    """Per-stream capture coverage and the last run's gates."""
    try:
        return await get_derive_runner().build_coverage()
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc))

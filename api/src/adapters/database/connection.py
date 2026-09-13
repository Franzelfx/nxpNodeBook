#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/adapters/database/connection.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Connection pool for the book store.

  No custom jsonb codec, on purpose. The raw layer is written with binary
  COPY, and a text-format codec registered for jsonb would not apply there;
  the two paths would then disagree about what a jsonb value looks like.
  Instead every writer passes a JSON STRING and every reader json.loads the
  string it gets back — one convention, both paths.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
from typing import Optional

import asyncpg

from ...core.config import settings


class DatabasePool:
    """Manages the asyncpg pool for the book database."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn or settings.database_url
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=settings.book_db_min_pool_size,
                max_size=settings.book_db_max_pool_size,
                command_timeout=300,
            )

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
        return self._pool

    @property
    def is_connected(self) -> bool:
        return self._pool is not None

    async def ping(self, timeout_s: float = 2.0) -> bool:
        """Whether the database answers right now — a bounded SELECT 1, so the
        health probe can report an outage as `database_connected: false`
        instead of hanging or raising."""
        if self._pool is None:
            return False
        try:
            async with asyncio.timeout(timeout_s):
                async with self._pool.acquire() as conn:
                    await conn.execute("SELECT 1")
            return True
        except (asyncio.TimeoutError, OSError, asyncpg.PostgresError, RuntimeError):
            return False

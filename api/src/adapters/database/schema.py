#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/adapters/database/schema.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Applies db/init/*.sql on every start.

  The postgres entrypoint only runs that directory on an EMPTY data
  directory, so a database adopted from a compose volume, restored from a
  dump, or created by hand would silently lack whatever was added since.
  Every statement in those files is idempotent, so running them again is
  free, and it means "the schema the code expects" and "the schema the
  database has" cannot drift apart between deploys.

  Retention is applied here rather than in the SQL because it is a setting,
  not a schema: 0 keeps everything and REMOVES any policy a previous value
  installed.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from pathlib import Path

from ...core.config import settings
from ...core.logging import get_logger
from .connection import DatabasePool

logger = get_logger(__name__)


async def apply_schema(db: DatabasePool) -> None:
    schema_dir = Path(settings.schema_dir)
    files = sorted(schema_dir.glob("*.sql"))
    if not files:
        raise RuntimeError(f"no schema files in {schema_dir}")
    async with db.pool.acquire() as conn:
        for path in files:
            sql = path.read_text(encoding="utf-8")
            await conn.execute(sql)
            logger.info("[schema] applied %s", path.name)

        days = settings.events_retention_days
        if days > 0:
            await conn.execute(
                "SELECT add_retention_policy('book_events', $1::interval, if_not_exists => TRUE)",
                f"{days} days",
            )
            logger.warning("[schema] book_events retention: %d days — older diffs are DROPPED", days)
        else:
            await conn.execute(
                "SELECT remove_retention_policy('book_events', if_exists => TRUE)"
            )
            logger.info("[schema] book_events retention: keep everything")

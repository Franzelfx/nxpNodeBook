#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/main.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

NexPatch Book API (FastAPI)
───────────────────────────
Captures tick-level order books and trades from exchange WebSocket streams
and publishes derived, grid-aligned microstructure columns over HTTP. The
warehouse consumes it as an ordinary source, the way it consumes
nxpNodeOptions.

  GET /metrics/{name}      one derived column on the 5-min grid
  GET /metrics/catalog     what is published, and the read-side contract
  GET /book/{label}        the live in-memory book (operator view)
  GET /stream/book/{label} the same as a Server-Sent Events stream
  GET /coverage            per-stream capture coverage + last derive gates
  GET /capabilities        what each column means and how to read it
  GET /health              DB, per-stream capture freshness, grid freshness

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI

from .adapters.database.connection import DatabasePool
from .adapters.database.repositories.grid import GridRepository
from .adapters.database.repositories.raw import RawRepository
from .adapters.database.schema import apply_schema
from .api.routes.book import router as book_router
from .api.routes.capabilities import router as capabilities_router
from .api.routes.coverage import router as coverage_router
from .api.routes.health import router as health_router
from .api.routes.metrics import router as metrics_router
from .api.routes.root import router as root_router
from .collectors.manager import CollectorManager
from .core import dependencies
from .core.config import settings
from .core.logging import get_logger, setup_logging
from .derive.runner import DeriveRunner
from .scheduler.runner import SchedulerRunner
from .services.health import HealthService
from .services.metrics import MetricsService

setup_logging()
logger = get_logger(__name__)

db_pool: Optional[DatabasePool] = None
scheduler: Optional[SchedulerRunner] = None
scheduler_task: Optional[asyncio.Task] = None
collectors: Optional[CollectorManager] = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global db_pool, scheduler, scheduler_task, collectors

    logger.info("Starting NexPatch Book API — streams: %s", ", ".join(s.key for s in settings.streams))

    db_pool = DatabasePool()
    await db_pool.connect()
    await apply_schema(db_pool)
    logger.info("Database pool connected, schema applied")

    raw_repo = RawRepository(db_pool)
    grid_repo = GridRepository(db_pool)
    derive_runner = DeriveRunner(raw_repo, grid_repo)

    collectors = CollectorManager(raw_repo)
    health_service = HealthService(db_pool, grid_repo, collectors)

    dependencies.set_metrics_service(MetricsService(grid_repo))
    dependencies.set_health_service(health_service)
    dependencies.set_derive_runner(derive_runner)
    dependencies.set_collectors(collectors)

    if settings.collectors_enabled:
        await collectors.start()
    else:
        logger.warning(
            "Collectors disabled (BOOK_COLLECTORS_ENABLED=false) — NOTHING IS BEING "
            "CAPTURED. Every second in this state is history that cannot be recovered."
        )

    if settings.scheduler_enabled:
        scheduler = SchedulerRunner(
            derive_runner,
            derive_tail_seconds=settings.derive_tail_seconds,
            derive_full_seconds=settings.derive_full_seconds,
        )
        scheduler_task = asyncio.create_task(scheduler.run())
        health_service.set_scheduler_running(True)
    else:
        logger.warning("Scheduler disabled (BOOK_SCHEDULER_ENABLED=false) — the grid will not be derived.")

    yield

    logger.info("Shutting down NexPatch Book API")
    # Collectors first: closing their sessions cleanly is what turns "the
    # process stopped" into a declared gap in stream_runs, and it must not
    # wait behind a derive that happens to be mid-flight.
    if collectors is not None:
        await collectors.stop()
    if scheduler is not None:
        scheduler.stop()
        if scheduler_task is not None:
            try:
                await asyncio.wait_for(scheduler_task, timeout=5.0)
            except asyncio.TimeoutError:
                scheduler_task.cancel()
    await db_pool.disconnect()
    logger.info("Database pool disconnected")


app = FastAPI(
    openapi_tags=[
        {
            "name": "Contracts",
            "description": (
                "Three endpoints, three different questions:\n\n"
                "- **`/health`** — is this node writing *right now*? Per-stream capture "
                "freshness from collector memory; never optimistic.\n"
                "- **`/coverage`** — can I trust this column over *this window*? Sessions, "
                "resyncs and snapshot density per stream, next to the declaration.\n"
                "- **`/capabilities`** — what does this column *mean*, and how must it be read?"
            ),
        },
        {"name": "Metrics", "description": "The published columns themselves."},
        {"name": "Book", "description": "Live in-memory books — operator views, not a warehouse contract."},
    ],
    title=settings.api_title,
    version=settings.api_version,
    description=(
        "**Tick-level order books and trades for the NexPatch warehouse.**\n\n"
        "Every depth message and every print from the configured venues is stored "
        "as received; everything served here is derived from that and fully "
        "recomputable. The raw layer is capture-or-lose: no venue serves a past book.\n\n"
        "### Read-side contract\n"
        "- Pin `fill_mode=\"none\"`. A missing bucket is one in which the collector was "
        "not connected; carrying a value across it would present a disconnect as a calm market.\n"
        "- A value at `ts` summarises the bucket `[ts - 5min, ts)`.\n"
        "- Depth and flow columns are **USD notional** on every venue; `spread_bps` is basis "
        "points of mid; `imbalance_*` is a ratio in [-1, 1].\n"
        "- Assert on `/coverage` before running a screen; nothing predates the first capture.\n"
    ),
    lifespan=lifespan,
)

app.include_router(root_router)
app.include_router(health_router)
app.include_router(coverage_router)
app.include_router(capabilities_router)
app.include_router(book_router)
app.include_router(metrics_router)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port, log_level="info")

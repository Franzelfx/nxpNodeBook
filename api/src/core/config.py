#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/core/config.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Configuration for nxpNodeBook. Environment variables with defaults that
  work inside docker-compose.

  THE ONE SETTING THAT MATTERS is BOOK_STREAMS: which venue/symbol books are
  captured. Everything captured is history that cannot be bought back;
  everything not captured never existed. The default is the three books the
  BTC positioning work needs — Binance spot, Binance USDⓈ-M perpetual and
  the Deribit perpetual — under the labels the derived columns are served
  with.

CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict

VENUES = ("binance_spot", "binance_futures", "deribit")


@dataclass(frozen=True, slots=True)
class StreamSpec:
    """One captured book: a venue, its symbol, and the label it is served under.

    `label` is the middle of every derived column name — BOOK.<label>.spread_bps
    — so it is the research-side identity of the stream and must be stable.
    The venue symbol can be renamed by the exchange; the label cannot.
    """

    venue: str
    symbol: str
    label: str

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.symbol}"


def parse_streams(raw: str) -> List[StreamSpec]:
    """Parse `venue:SYMBOL:label,venue:SYMBOL:label`.

    Fails loudly on a bad entry rather than skipping it: a typo that silently
    dropped a stream would lose history for as long as nobody noticed.
    """
    out: List[StreamSpec] = []
    seen_labels = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) != 3:
            raise ValueError(f"BOOK_STREAMS entry must be venue:SYMBOL:label, got {item!r}")
        venue, symbol, label = (p.strip() for p in parts)
        if venue not in VENUES:
            raise ValueError(f"unknown venue {venue!r} in BOOK_STREAMS; known: {', '.join(VENUES)}")
        if not symbol or not label:
            raise ValueError(f"BOOK_STREAMS entry has an empty symbol or label: {item!r}")
        if label in seen_labels:
            raise ValueError(f"BOOK_STREAMS label {label!r} used twice; labels are column names")
        seen_labels.add(label)
        out.append(StreamSpec(venue=venue, symbol=symbol, label=label))
    if not out:
        raise ValueError("BOOK_STREAMS is empty — nothing would be captured")
    return out


DEFAULT_STREAMS = (
    "binance_spot:BTCUSDT:btc.spot,"
    "binance_futures:BTCUSDT:btc.perp,"
    "deribit:BTC-PERPETUAL:btc.dperp"
)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).parent.parent.parent.parent / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── API ──────────────────────────────────────────────────────────────
    api_title: str = "NexPatch Book API"
    api_version: str = "0.1.0"
    api_host: str = "0.0.0.0"
    api_port: int = 9130

    # Node stream identifier, mirroring the other nodes so the warehouse
    # adapter can pass node="nxp-book:5m" or omit it.
    node_id: str = os.environ.get("NODE_ID", "nxp-book")

    # ── Database ─────────────────────────────────────────────────────────
    book_db_host: str = os.environ.get("BOOK_DB_HOST", "book-db")
    book_db_port: int = int(os.environ.get("BOOK_DB_PORT", "5432"))
    book_db_database: str = os.environ.get("BOOK_DB_DATABASE", "book")
    book_db_user: str = os.environ.get("BOOK_DB_USER", "nxp_book")
    book_db_password: str = os.environ.get("BOOK_DB_PASSWORD", "nxp_book_pw_change_me")
    book_db_min_pool_size: int = int(os.environ.get("BOOK_DB_MIN_POOL_SIZE", "2"))
    book_db_max_pool_size: int = int(os.environ.get("BOOK_DB_MAX_POOL_SIZE", "10"))
    # Applied on every start; every statement in it is idempotent.
    schema_dir: str = os.environ.get(
        "BOOK_SCHEMA_DIR", str(Path(__file__).parent.parent.parent.parent / "db" / "init")
    )

    # ── Streams ──────────────────────────────────────────────────────────
    streams_raw: str = os.environ.get("BOOK_STREAMS", DEFAULT_STREAMS)
    user_agent: str = os.environ.get(
        "BOOK_USER_AGENT",
        "nxpNodeBook/0.1 (+mailto:fabian@nexpatch.ai) NexPatch AI research data collector",
    )
    binance_spot_rest: str = os.environ.get("BOOK_BINANCE_SPOT_REST", "https://api.binance.com")
    binance_spot_ws: str = os.environ.get("BOOK_BINANCE_SPOT_WS", "wss://stream.binance.com:9443")
    binance_futures_rest: str = os.environ.get("BOOK_BINANCE_FUTURES_REST", "https://fapi.binance.com")
    binance_futures_ws: str = os.environ.get("BOOK_BINANCE_FUTURES_WS", "wss://fstream.binance.com")
    deribit_ws: str = os.environ.get("BOOK_DERIBIT_WS", "wss://www.deribit.com/ws/api/v2")
    # Binance diff-depth update speed. 100ms is the finest the venue offers.
    binance_depth_speed: str = os.environ.get("BOOK_BINANCE_DEPTH_SPEED", "100ms")
    # `trade` (every print) or `aggTrade` (venue-side conflation). Futures
    # delivers nothing on aggTrade over the combined stream, so trade it is.
    binance_trade_stream: str = os.environ.get("BOOK_BINANCE_TRADE_STREAM", "trade")
    # REST snapshot depth used to sync the diff stream. 5000 is the spot
    # maximum; futures caps at 1000. Deeper is better: levels outside the
    # snapshot only appear in the in-memory book once the diff stream touches
    # them, so a shallow snapshot understates depth for a while after sync.
    binance_snapshot_limit_spot: int = int(os.environ.get("BOOK_BINANCE_SNAPSHOT_LIMIT_SPOT", "5000"))
    binance_snapshot_limit_futures: int = int(os.environ.get("BOOK_BINANCE_SNAPSHOT_LIMIT_FUTURES", "1000"))
    # Deribit's raw book channel interval. "100ms" delivers every change
    # batched at 100 ms; "raw" is unbatched and several times the message rate.
    deribit_book_interval: str = os.environ.get("BOOK_DERIBIT_BOOK_INTERVAL", "100ms")

    # ── Capture ──────────────────────────────────────────────────────────
    # How often the whole in-memory book is written as a replay anchor.
    snapshot_interval_seconds: int = int(os.environ.get("BOOK_SNAPSHOT_INTERVAL_SECONDS", "300"))
    # Levels per side kept in a periodic snapshot. 0 = the whole book. A
    # Binance spot book carries tens of thousands of resting levels, most of
    # them far from the touch; 2,000 per side comfortably covers the ±1 %
    # bands the derived columns use, at a tenth of the storage.
    snapshot_levels: int = int(os.environ.get("BOOK_SNAPSHOT_LEVELS", "2000"))
    # Writer batching. Rows are flushed when either bound is reached.
    write_batch_rows: int = int(os.environ.get("BOOK_WRITE_BATCH_ROWS", "500"))
    write_flush_seconds: float = float(os.environ.get("BOOK_WRITE_FLUSH_SECONDS", "1.0"))
    # Reconnect back-off bounds, seconds.
    reconnect_min_seconds: float = float(os.environ.get("BOOK_RECONNECT_MIN_SECONDS", "1.0"))
    reconnect_max_seconds: float = float(os.environ.get("BOOK_RECONNECT_MAX_SECONDS", "60.0"))
    # Raw retention. 0 keeps everything. The diff layer is the large one (see
    # README for measured rates); snapshots and trades are always kept.
    events_retention_days: int = int(os.environ.get("BOOK_EVENTS_RETENTION_DAYS", "0"))

    # ── Grid / derive ────────────────────────────────────────────────────
    grid_step_seconds: int = int(os.environ.get("BOOK_GRID_STEP_SECONDS", "300"))
    derive_tail_hours: int = int(os.environ.get("BOOK_DERIVE_TAIL_HOURS", "6"))
    # Depth bands, in basis points from mid, that the depth/imbalance columns
    # are measured over. Serving several is cheap; each is one catalog entry.
    depth_bands_bps: str = os.environ.get("BOOK_DEPTH_BANDS_BPS", "10,50")

    # ── Scheduler ────────────────────────────────────────────────────────
    scheduler_enabled: bool = os.environ.get("BOOK_SCHEDULER_ENABLED", "true").lower() == "true"
    collectors_enabled: bool = os.environ.get("BOOK_COLLECTORS_ENABLED", "true").lower() == "true"
    derive_tail_seconds: int = int(os.environ.get("BOOK_DERIVE_TAIL_SECONDS", "300"))
    derive_full_seconds: int = int(os.environ.get("BOOK_DERIVE_FULL_SECONDS", str(24 * 3600)))

    # ── Health ───────────────────────────────────────────────────────────
    max_grid_lag_seconds: int = int(os.environ.get("BOOK_MAX_GRID_LAG_SECONDS", "1200"))
    # A live book that has not produced a message for this long is not quiet,
    # it is disconnected: BTCUSDT on Binance updates several times a second
    # around the clock.
    max_stream_lag_seconds: int = int(os.environ.get("BOOK_MAX_STREAM_LAG_SECONDS", "60"))

    @property
    def streams(self) -> List[StreamSpec]:
        return parse_streams(self.streams_raw)

    @property
    def depth_bands(self) -> List[int]:
        return sorted({int(x) for x in self.depth_bands_bps.split(",") if x.strip()})

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.book_db_user}:{self.book_db_password}"
            f"@{self.book_db_host}:{self.book_db_port}/{self.book_db_database}"
        )


settings = Settings()

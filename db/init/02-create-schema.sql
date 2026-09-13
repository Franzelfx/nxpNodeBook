-- ─────────────────────────────────────────────────────────────────────────────
-- File:        db/init/02-create-schema.sql
-- Author:      Fabian Franz
-- Company:     NexPatch AI
-- Created:     2026-09-13
--
-- Description:
--   Schema for nxpNodeBook — tick-level order books and trades from exchange
--   WebSocket streams, and the derived 5-min grid.
--
--   Every statement here is idempotent. The API applies this file on every
--   start (see adapters/database/schema.py), so a database that was created
--   by hand, restored from a dump, or adopted from a compose volume ends up
--   with the same tables as one initialised by the postgres entrypoint.
--
--   Four raw layers, one direction:
--
--     book_events     append-only. Every depth message a venue sent, exactly
--                     as sent, plus the top of book AFTER the message was
--                     applied to the in-memory book. THIS IS THE LAYER THAT
--                     CANNOT BE REBUILT: no venue serves a past order book,
--                     and no free archive of L2 diffs exists. A message missed
--                     is a message gone.
--     book_snapshots  the full in-memory book at a sync instant and on a fixed
--                     cadence. Together with book_events this makes the book
--                     replayable from any snapshot forward — the
--                     recomputability property the research side asked for.
--     trades          append-only. Every trade print the venue streamed.
--     stream_runs     one row per WebSocket session. A gap between two rows is
--                     a DECLARED gap; /coverage reports it instead of letting a
--                     reconnect look like a quiet market.
--
--     grid_features   fully recomputable from the raw layers plus the derive
--                     spec. Published only after every QA gate passes.
--                     Truncatable at any time.
--
--   The warehouse never reads the raw layers; it reads derived series over
--   HTTP exactly as it does from nxpNodeOptions.
--
-- CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
-- © 2026 NexPatch AI. All rights reserved.
-- ─────────────────────────────────────────────────────────────────────────────

-- ─────────────────────────────────────────────────────────────────────────────
-- book_events — the capture-or-lose layer
--
-- One row per MESSAGE, not per price level. A Binance depth@100ms message
-- carries every level that changed in the last 100 ms — often hundreds — and
-- exploding those into rows multiplies the row count by two orders of
-- magnitude for no analytical gain: every consumer of this table replays it in
-- order anyway. `bids`/`asks` are jsonb arrays of [price, qty] as strings,
-- exactly as the venue quoted them; a qty of "0" means the level was removed.
--
-- `ts` is the VENUE's event time, because that is the instant the book looked
-- like this. `received_at` is when this node saw it; the difference is the
-- transport latency and is kept so it can be measured, never assumed.
--
-- `update_id` / `prev_update_id` are the venue's sequence numbers. They are
-- what makes a gap detectable after the fact: a replay that finds
-- prev_update_id != last update_id knows the book is no longer trustworthy
-- until the next snapshot, and says so.
--
-- `best_bid`… are the top of book AFTER applying this message. They are
-- redundant with a replay and stored anyway, because spread, mid and order
-- flow imbalance are then one SQL window function instead of a Python replay
-- of a day of jsonb. `applied` is false for messages received before the
-- first snapshot sync (buffered per the venue protocol) or after a detected
-- gap; those rows have no top-of-book columns because the book was not
-- trustworthy at that instant.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS book_events (
    ts               TIMESTAMPTZ      NOT NULL,   -- venue event time
    venue            TEXT             NOT NULL,   -- binance_spot | binance_futures | deribit
    symbol           TEXT             NOT NULL,   -- BTCUSDT | BTC-PERPETUAL
    update_id        BIGINT           NOT NULL,   -- venue sequence: u (Binance), change_id (Deribit)
    first_update_id  BIGINT           NULL,       -- Binance U; null on Deribit
    prev_update_id   BIGINT           NULL,       -- Binance pu (futures) / Deribit prev_change_id
    kind             TEXT             NOT NULL,   -- diff | snapshot
    applied          BOOLEAN          NOT NULL DEFAULT FALSE,
    bids             JSONB            NOT NULL,   -- [[price, qty], ...] as quoted
    asks             JSONB            NOT NULL,
    best_bid         DOUBLE PRECISION NULL,       -- after applying, when applied
    best_bid_qty     DOUBLE PRECISION NULL,
    best_ask         DOUBLE PRECISION NULL,
    best_ask_qty     DOUBLE PRECISION NULL,
    received_at      TIMESTAMPTZ      NOT NULL,
    CONSTRAINT book_events_pkey PRIMARY KEY (ts, venue, symbol, update_id)
);

SELECT create_hypertable('book_events', 'ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_book_events_stream ON book_events (venue, symbol, ts DESC);

COMMENT ON TABLE  book_events IS 'Append-only depth messages. Irreplaceable: no venue serves a past book and no free L2 archive exists.';
COMMENT ON COLUMN book_events.ts IS 'Venue event time. The ONLY timestamp a screen may condition on.';
COMMENT ON COLUMN book_events.applied IS 'False before the first snapshot sync or after a sequence gap: the book was not trustworthy at this instant.';

-- Compression. Segmenting by stream keeps one venue/symbol contiguous, which
-- is what every replay and every derive walk. Two days uncompressed is enough
-- for the tail derive to read hot chunks; after that the row is history.
DO $$
BEGIN
    EXECUTE 'ALTER TABLE book_events SET ('
            'timescaledb.compress, '
            'timescaledb.compress_segmentby = ''venue, symbol'', '
            'timescaledb.compress_orderby = ''ts DESC, update_id DESC'')';
    BEGIN
        PERFORM add_compression_policy('book_events', INTERVAL '2 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'compression policy not added: %', SQLERRM;
    END;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'compression not enabled (community feature unavailable): %', SQLERRM;
END $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- book_snapshots — the replay anchors
--
-- The whole in-memory book at an instant: at every sync (initial, and after
-- every detected gap) and on a fixed cadence in between. A replay starts at
-- the newest snapshot before its window and applies book_events forward. The
-- cadence is a trade-off between snapshot storage and replay length; five
-- minutes means no replay ever has to apply more than ~3,000 messages.
--
-- `reason` says why this snapshot exists. `sync` and `resync` rows are the
-- ones a coverage reader cares about: each resync is a point where the raw
-- diff stream stopped being trustworthy for a moment.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS book_snapshots (
    ts          TIMESTAMPTZ NOT NULL,   -- node time when the snapshot was taken
    venue       TEXT        NOT NULL,
    symbol      TEXT        NOT NULL,
    update_id   BIGINT      NOT NULL,   -- book state as of this venue sequence
    reason      TEXT        NOT NULL,   -- sync | resync | periodic
    bid_levels  INTEGER     NOT NULL,
    ask_levels  INTEGER     NOT NULL,
    bids        JSONB       NOT NULL,   -- [[price, qty], ...] best first
    asks        JSONB       NOT NULL,
    CONSTRAINT book_snapshots_pkey PRIMARY KEY (ts, venue, symbol)
);

SELECT create_hypertable('book_snapshots', 'ts', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_book_snapshots_stream ON book_snapshots (venue, symbol, ts DESC);

COMMENT ON TABLE book_snapshots IS 'Full in-memory book at sync instants and on a fixed cadence. A replay starts here and applies book_events forward.';

DO $$
BEGIN
    EXECUTE 'ALTER TABLE book_snapshots SET ('
            'timescaledb.compress, '
            'timescaledb.compress_segmentby = ''venue, symbol'', '
            'timescaledb.compress_orderby = ''ts DESC'')';
    BEGIN
        PERFORM add_compression_policy('book_snapshots', INTERVAL '7 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'compression policy not added: %', SQLERRM;
    END;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'compression not enabled: %', SQLERRM;
END $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- trades — every print
--
-- `qty` is in the venue's own unit (BTC on Binance, USD contracts on Deribit
-- inverse perpetuals); `notional_usd` is the one unit every stream shares and
-- is what the derived flow columns are built from. Storing both keeps the raw
-- layer a faithful copy of the source while sparing every reader the venue
-- lookup.
--
-- `is_buyer_maker` true means the aggressor SOLD (hit a resting bid). That is
-- the Binance convention and is normalised to it for the other venues.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trades (
    ts              TIMESTAMPTZ      NOT NULL,   -- venue trade time
    venue           TEXT             NOT NULL,
    symbol          TEXT             NOT NULL,
    trade_id        BIGINT           NOT NULL,   -- venue id: aggTrade id / Deribit trade_seq
    price           DOUBLE PRECISION NOT NULL,
    qty             DOUBLE PRECISION NOT NULL,   -- venue unit
    notional_usd    DOUBLE PRECISION NOT NULL,
    is_buyer_maker  BOOLEAN          NOT NULL,   -- true = aggressor sold
    received_at     TIMESTAMPTZ      NOT NULL,
    CONSTRAINT trades_pkey PRIMARY KEY (ts, venue, symbol, trade_id)
);

SELECT create_hypertable('trades', 'ts', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_trades_stream ON trades (venue, symbol, ts DESC);

COMMENT ON TABLE  trades IS 'Append-only trade prints from the venue stream. Irreplaceable in the same sense as book_events.';
COMMENT ON COLUMN trades.is_buyer_maker IS 'True when the aggressor sold into a resting bid (Binance convention).';

DO $$
BEGIN
    EXECUTE 'ALTER TABLE trades SET ('
            'timescaledb.compress, '
            'timescaledb.compress_segmentby = ''venue, symbol'', '
            'timescaledb.compress_orderby = ''ts DESC, trade_id DESC'')';
    BEGIN
        PERFORM add_compression_policy('trades', INTERVAL '2 days', if_not_exists => TRUE);
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'compression policy not added: %', SQLERRM;
    END;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'compression not enabled: %', SQLERRM;
END $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- stream_runs — one row per WebSocket session
--
-- Exists so "was the collector connected at 12:05?" is an index lookup and a
-- reconnect is a declared gap rather than a quiet market. `resyncs` counts the
-- snapshot re-fetches inside ONE session — every one of them marks a sequence
-- gap the venue protocol told us about.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stream_runs (
    id               BIGSERIAL   PRIMARY KEY,
    venue            TEXT        NOT NULL,
    symbol           TEXT        NOT NULL,
    connected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    disconnected_at  TIMESTAMPTZ NULL,
    first_update_id  BIGINT      NULL,
    last_update_id   BIGINT      NULL,
    events           BIGINT      NOT NULL DEFAULT 0,
    trades           BIGINT      NOT NULL DEFAULT 0,
    resyncs          INTEGER     NOT NULL DEFAULT 0,
    reason           TEXT        NULL,       -- why the session ended
    detail           JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_stream_runs_recent ON stream_runs (venue, symbol, connected_at DESC);

COMMENT ON TABLE stream_runs IS 'One row per WebSocket session. The space between two rows is a DECLARED gap.';

-- ─────────────────────────────────────────────────────────────────────────────
-- grid_features — derived, recomputable, published only behind the QA gates
--
-- Served as `asset || '.' || metric`, e.g. BOOK.btc.spot.spread_bps. A missing
-- (ts, asset, metric) row means NaN — never zero. The read side must pin
-- fill_mode="none" (see README).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS grid_features (
    ts      TIMESTAMPTZ      NOT NULL,
    asset   VARCHAR(16)      NOT NULL,   -- BOOK | MICRO
    metric  VARCHAR(64)      NOT NULL,   -- btc.spot.spread_bps | …
    value   DOUBLE PRECISION NOT NULL,
    CONSTRAINT grid_features_pkey PRIMARY KEY (ts, asset, metric)
);

SELECT create_hypertable('grid_features', 'ts', chunk_time_interval => INTERVAL '90 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_grid_features_series ON grid_features (asset, metric, ts DESC);

COMMENT ON TABLE grid_features IS 'Derived 5-min grid. Fully recomputable from the raw layers; safe to truncate.';

CREATE TABLE IF NOT EXISTS grid_features_staging (
    ts      TIMESTAMPTZ      NOT NULL,
    asset   VARCHAR(16)      NOT NULL,
    metric  VARCHAR(64)      NOT NULL,
    value   DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_grid_staging_series ON grid_features_staging (asset, metric, ts);

-- ─────────────────────────────────────────────────────────────────────────────
-- Run bookkeeping — feeds /coverage and /health
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS derive_runs (
    id           BIGSERIAL   PRIMARY KEY,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ NULL,
    mode         TEXT        NOT NULL,   -- full | tail
    grid_start   TIMESTAMPTZ NULL,
    grid_end     TIMESTAMPTZ NULL,
    rows_written INTEGER     NOT NULL DEFAULT 0,
    status       TEXT        NOT NULL DEFAULT 'running',  -- running | published | aborted | failed
    gates        JSONB       NOT NULL DEFAULT '[]'::jsonb,
    detail       JSONB       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_derive_runs_recent ON derive_runs (started_at DESC);

COMMENT ON TABLE derive_runs IS 'One row per derive attempt. status=aborted means a QA gate refused to publish — grid_features is unchanged.';

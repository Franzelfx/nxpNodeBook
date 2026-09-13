"""The collector's buffer → snapshot → replay → live path, against fake sources.

No network, no database: the source is a scripted async iterator and the
repository/writer record what would have been written.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple

import pytest

from src.adapters.sources.base import Trade, VenueSource
from src.collectors.stream import StreamCollector
from src.core.config import StreamSpec
from src.domain.book import DiffEvent, Snapshot


class FakeWriter:
    def __init__(self) -> None:
        self.rows: Dict[str, List[Tuple[Any, ...]]] = {"book_events": [], "book_snapshots": [], "trades": []}

    def put(self, table: str, row: Tuple[Any, ...]) -> None:
        self.rows[table].append(row)

    @property
    def backlog(self) -> int:
        return 0


class FakeRepo:
    def __init__(self) -> None:
        self.runs: List[Dict[str, Any]] = []

    async def start_run(self, venue, symbol, detail=None) -> int:
        self.runs.append({"venue": venue, "symbol": symbol, "reason": None})
        return len(self.runs)

    async def update_run(self, run_id, **kw) -> None:
        self.runs[run_id - 1].update(kw)

    async def finish_run(self, run_id, reason, **counts) -> None:
        self.runs[run_id - 1]["reason"] = reason
        self.runs[run_id - 1].update(counts)


class ScriptedSource(VenueSource):
    """Yields a script of messages, then ends the session."""

    venue = "binance_spot"
    symbol = "BTCUSDT"

    def __init__(self, script, snapshots: List[Snapshot]) -> None:
        self._script = script
        self._snapshots = list(snapshots)
        self.snapshot_calls = 0

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def messages(self):
        for m in self._script:
            await asyncio.sleep(0)  # let the snapshot task run
            yield m

    async def fetch_snapshot(self) -> Optional[Snapshot]:
        self.snapshot_calls += 1
        return self._snapshots.pop(0)


def diff(U, u, bid=("100", "1"), ask=("101", "1")) -> DiffEvent:
    return DiffEvent(ts_ms=1_700_000_000_000 + u, update_id=u, first_update_id=U, prev_update_id=None,
                     bids=[list(bid)], asks=[list(ask)])


SPEC = StreamSpec(venue="binance_spot", symbol="BTCUSDT", label="btc.spot")


async def run_once(source: ScriptedSource) -> Tuple[FakeWriter, FakeRepo, StreamCollector]:
    writer, repo = FakeWriter(), FakeRepo()
    c = StreamCollector(SPEC, source, repo, writer)  # type: ignore[arg-type]
    await c._open_session()
    await c._session()
    await c._close_session("end")
    return writer, repo, c


@pytest.mark.asyncio
async def test_buffered_events_are_replayed_against_the_snapshot() -> None:
    snap = Snapshot(update_id=100, bids=[["100", "1"]], asks=[["101", "1"]])
    script = [diff(95, 99), diff(100, 102), diff(103, 105, bid=("100.5", "2")), diff(106, 108)]
    writer, repo, c = await run_once(ScriptedSource(script, [snap]))

    events = writer.rows["book_events"]
    by_id = {r[3]: r for r in events}
    # snapshot row
    assert any(r[6] == "snapshot" for r in events)
    # 99 predates the snapshot: stored, not applied
    assert by_id[99][7] is False and by_id[99][10] is None
    # 102 brackets 101 → applied; 105 and 108 contiguous → applied with top-of-book
    assert by_id[102][7] is True
    assert by_id[105][7] is True and by_id[105][10] == 100.5
    assert by_id[108][7] is True
    assert c.book.update_id == 108
    assert repo.runs[0]["reason"] == "end"
    assert repo.runs[0]["events"] == 4
    # one sync snapshot written
    assert [r[4] for r in writer.rows["book_snapshots"]] == ["sync"]


@pytest.mark.asyncio
async def test_sequence_gap_triggers_resync_and_marks_events_unapplied() -> None:
    snap1 = Snapshot(update_id=100, bids=[["100", "1"]], asks=[["101", "1"]])
    snap2 = Snapshot(update_id=120, bids=[["100", "1"]], asks=[["101", "1"]])
    # 102 ok, 110 skips 103..109 → gap → resync at 120; 121 brackets → ok
    script = [diff(101, 102), diff(110, 115), diff(116, 119), diff(121, 125), diff(126, 130)]
    writer, repo, c = await run_once(ScriptedSource(script, [snap1, snap2]))

    by_id = {r[3]: r for r in writer.rows["book_events"] if r[6] == "diff"}
    assert by_id[102][7] is True
    assert by_id[115][7] is False   # broke the chain, superseded by snap2
    assert by_id[119][7] is False   # before snap2 → dropped
    assert by_id[125][7] is True
    assert by_id[130][7] is True
    assert [r[4] for r in writer.rows["book_snapshots"]] == ["sync", "resync"]
    assert c.state.resyncs_session == 1
    assert c.book.update_id == 130


@pytest.mark.asyncio
async def test_crossed_book_after_apply_is_a_gap() -> None:
    snap1 = Snapshot(update_id=100, bids=[["100", "1"]], asks=[["101", "1"]])
    snap2 = Snapshot(update_id=200, bids=[["100", "1"]], asks=[["101", "1"]])
    script = [diff(101, 102, bid=("101", "1")), diff(201, 203)]
    writer, _, c = await run_once(ScriptedSource(script, [snap1, snap2]))
    by_id = {r[3]: r for r in writer.rows["book_events"] if r[6] == "diff"}
    assert by_id[102][7] is False
    assert by_id[203][7] is True
    assert c.state.resyncs_session == 1


@pytest.mark.asyncio
async def test_trades_are_written_with_notional() -> None:
    snap = Snapshot(update_id=100, bids=[["100", "1"]], asks=[["101", "1"]])
    script = [Trade(ts_ms=1, trade_id=7, price=100.0, qty=0.5, notional_usd=50.0, is_buyer_maker=True), diff(101, 102)]
    writer, repo, _ = await run_once(ScriptedSource(script, [snap]))
    (row,) = writer.rows["trades"]
    assert row[3] == 7 and row[6] == 50.0 and row[7] is True
    assert repo.runs[0]["trades"] == 1


@pytest.mark.asyncio
async def test_raw_rows_store_levels_exactly_as_quoted() -> None:
    snap = Snapshot(update_id=100, bids=[["100.00000000", "1.00000000"]], asks=[["101.00000000", "1.00000000"]])
    script = [diff(101, 102, bid=("99.99000000", "0.00100000"))]
    writer, _, _ = await run_once(ScriptedSource(script, [snap]))
    row = next(r for r in writer.rows["book_events"] if r[3] == 102)
    assert json.loads(row[8]) == [["99.99000000", "0.00100000"]]

from pathlib import Path

from beacon.storage import StorageEngine
from beacon.uplink.buffer import OutboundBuffer
from beacon.uplink.records import OutboundRecord, RecordState


async def _started_engine(path: str | Path) -> StorageEngine:
    # OutboundRecord is framework-internal, so it never enters the registry;
    # the uplink hands it to its engine explicitly, exactly as done here
    engine = StorageEngine(path, tables=[OutboundRecord])
    await engine.start()
    return engine


async def _buffer(tmp_path: Path, *, max_records: int = 1000) -> tuple[OutboundBuffer, StorageEngine]:
    engine = await _started_engine(tmp_path / "buffer.db")
    return OutboundBuffer(engine, max_records=max_records), engine


async def _count(engine: StorageEngine, state: RecordState) -> int:
    return await engine.count(OutboundRecord, {"state": state})


# ------------------------------------------------------------ enqueue


async def test_enqueue_persists_and_returns_record(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        record = await buf.enqueue("telemetry", '{"t": 1}')

        assert record.seq is not None
        assert record.record_id
        assert record.stream == "telemetry"
        assert record.payload == '{"t": 1}'
        assert record.state is RecordState.PENDING
        assert await buf.pending_count() == 1
    finally:
        await engine.stop()


async def test_enqueue_assigns_ascending_seq(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        first = await buf.enqueue("s", "1")
        second = await buf.enqueue("s", "2")
        assert first.seq is not None
        assert second.seq is not None
        assert second.seq > first.seq
    finally:
        await engine.stop()


# ------------------------------------------------------------ claim


async def test_claim_marks_oldest_pending_inflight_fifo(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        r1 = await buf.enqueue("s", "1")
        r2 = await buf.enqueue("s", "2")
        await buf.enqueue("s", "3")

        claimed = await buf.claim(2)

        assert [r.seq for r in claimed] == [r1.seq, r2.seq]
        assert all(r.state is RecordState.INFLIGHT for r in claimed)
        assert await _count(engine, RecordState.INFLIGHT) == 2
        assert await buf.pending_count() == 1
    finally:
        await engine.stop()


async def test_claim_returns_empty_when_no_pending(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        assert await buf.claim(10) == []
    finally:
        await engine.stop()


async def test_claim_does_not_reclaim_inflight_rows(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        first = await buf.claim(10)
        second = await buf.claim(10)
        assert len(first) == 1
        assert second == []
    finally:
        await engine.stop()


# ------------------------------------------------------------ ack / nack / bury


async def test_ack_deletes_rows(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        claimed = await buf.claim(1)
        await buf.ack(claimed)

        assert await buf.pending_count() == 0
        assert await _count(engine, RecordState.INFLIGHT) == 0
        assert await engine.count(OutboundRecord, {}) == 0
    finally:
        await engine.stop()


async def test_nack_returns_to_pending_and_records_error(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        claimed = await buf.claim(1)
        await buf.nack(claimed, "boom")

        assert await buf.pending_count() == 1
        [row] = await engine.fetch(OutboundRecord, {})
        assert row.state is RecordState.PENDING
        assert row.attempts == 1
        assert row.last_error == "boom"
        # nack updates the in-hand instances too
        assert claimed[0].attempts == 1
        assert claimed[0].last_error == "boom"
    finally:
        await engine.stop()


async def test_bury_marks_dead_and_never_reclaimed(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        claimed = await buf.claim(1)
        await buf.bury(claimed, "poison")

        assert await buf.pending_count() == 0
        assert await _count(engine, RecordState.DEAD) == 1
        assert await buf.claim(10) == []
        [row] = await engine.fetch(OutboundRecord, {})
        assert row.attempts == 1
        assert row.last_error == "poison"
    finally:
        await engine.stop()


async def test_ack_nack_bury_ignore_empty_batches(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        # must not raise or touch the db
        await buf.ack([])
        await buf.nack([], "e")
        await buf.bury([], "e")
    finally:
        await engine.stop()


# ------------------------------------------------------------ recover


async def test_recover_returns_inflight_to_pending(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        await buf.enqueue("s", "2")
        await buf.claim(10)
        assert await _count(engine, RecordState.INFLIGHT) == 2

        recovered = await buf.recover()

        assert recovered == 2
        assert await buf.pending_count() == 2
        assert await _count(engine, RecordState.INFLIGHT) == 0
    finally:
        await engine.stop()


async def test_recover_leaves_dead_rows_alone(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        await buf.bury(await buf.claim(1), "poison")

        assert await buf.recover() == 0
        assert await _count(engine, RecordState.DEAD) == 1
    finally:
        await engine.stop()


# ------------------------------------------------------------ drop-oldest cap


async def test_enqueue_drops_oldest_pending_past_cap(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path, max_records=3)
    try:
        records = [await buf.enqueue("s", str(i)) for i in range(5)]

        assert await buf.pending_count() == 3
        assert buf.dropped_records == 2
        # the two oldest are gone; the three newest survive
        surviving = {r.seq for r in await engine.fetch(OutboundRecord, {})}
        assert surviving == {r.seq for r in records[2:]}
    finally:
        await engine.stop()


async def test_cap_never_drops_inflight(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path, max_records=2)
    try:
        held = await buf.enqueue("s", "held")
        await buf.claim(1)  # 'held' is now inflight
        for i in range(4):
            await buf.enqueue("s", str(i))

        # the inflight row is never dropped even while pending overflows
        assert await _count(engine, RecordState.INFLIGHT) == 1
        assert held.seq in {r.seq for r in await engine.fetch(OutboundRecord, {})}
    finally:
        await engine.stop()


# ------------------------------------------------------------ dead rows and the cap (review fixes)


async def test_dead_rows_do_not_evict_live_data(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path, max_records=3)
    try:
        # fill the cap's worth of buried poison first
        for _ in range(3):
            r = await buf.enqueue("s", "dead")
            await buf.bury([r], "poison")
        assert await _count(engine, RecordState.DEAD) == 3

        # live pending rows fit under the cap despite the dead backlog
        live = [await buf.enqueue("s", str(i)) for i in range(3)]

        assert await buf.pending_count() == 3
        assert buf.dropped_records == 0
        assert {r.seq for r in live} <= {r.seq for r in await engine.fetch(OutboundRecord, {})}
    finally:
        await engine.stop()


async def test_bury_bounds_dead_rows_at_cap(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path, max_records=3)
    try:
        # enqueue-claim-bury one at a time so the pending cap is never the limiter
        for _ in range(6):
            await buf.enqueue("s", "x")
            await buf.bury(await buf.claim(1), "poison")

        assert await _count(engine, RecordState.DEAD) == 3
    finally:
        await engine.stop()

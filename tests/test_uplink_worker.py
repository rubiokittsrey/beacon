import asyncio
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path

from beacon.core.config import UplinkBufferConfig, UplinkRetryConfig
from beacon.storage import StorageEngine
from beacon.uplink.buffer import OutboundBuffer
from beacon.uplink.records import OutboundRecord, RecordState, SendResult
from beacon.uplink.worker import UplinkWorker


class FakeTransport:
    """Scripted transport: returns queued results, or raises a queued error."""

    def __init__(self, results: Sequence[SendResult | Exception] | None = None) -> None:
        self._results = list(results or [])
        self.default = SendResult(ok=True)
        self.calls = 0
        self.batches: list[list[str]] = []

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def send(self, records: Sequence[OutboundRecord]) -> SendResult:
        self.calls += 1
        self.batches.append([r.payload for r in records])
        result = self._results.pop(0) if self._results else self.default
        if isinstance(result, Exception):
            raise result
        return result


async def _started_engine(path: str | Path) -> StorageEngine:
    engine = StorageEngine(path, tables=[OutboundRecord])
    await engine.start()
    return engine


async def _buffer(tmp_path: Path, *, max_records: int = 1000) -> tuple[OutboundBuffer, StorageEngine]:
    engine = await _started_engine(tmp_path / "worker.db")
    return OutboundBuffer(engine, max_records=max_records), engine


def _config(**retry: float) -> UplinkBufferConfig:
    # fast + deterministic: no real backoff, quick idle polling
    defaults: dict[str, float | int] = {"min_seconds": 0.0, "max_seconds": 0.0, "max_attempts": 0}
    defaults.update(retry)
    return UplinkBufferConfig(flush_interval=0.01, retry=UplinkRetryConfig(**defaults))


async def _run_until(
    worker: UplinkWorker,
    predicate: Callable[[], Awaitable[bool]],
    *,
    deadline: float = 2.0,
) -> None:
    """Run the worker loop until `predicate` holds, then stop it cleanly."""
    shutdown = asyncio.Event()
    task = asyncio.create_task(worker.run(shutdown))
    try:

        async def _poll() -> None:
            while not await predicate():
                await asyncio.sleep(0.005)

        await asyncio.wait_for(_poll(), deadline)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=deadline)


async def _drained(buf: OutboundBuffer, engine: StorageEngine) -> bool:
    inflight = await engine.count(OutboundRecord, {"state": RecordState.INFLIGHT})
    return await buf.pending_count() == 0 and inflight == 0


# ------------------------------------------------------------ _resolve outcomes


async def test_success_acks_batch(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        worker = UplinkWorker(buf, FakeTransport(), _config())
        backoff = await worker._resolve(await buf.claim(1), SendResult(ok=True))

        assert backoff == 0.0
        assert await engine.count(OutboundRecord, {}) == 0
    finally:
        await engine.stop()


async def test_retryable_failure_nacks_and_backs_off(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        worker = UplinkWorker(buf, FakeTransport(), _config(min_seconds=1.0, max_seconds=60.0))
        claimed = await buf.claim(1)
        backoff = await worker._resolve(claimed, SendResult(ok=False, retryable=True, detail="503"))

        assert backoff == 1.0  # first failure: min_seconds * 2**0
        assert await buf.pending_count() == 1
        [row] = await engine.fetch(OutboundRecord, {})
        assert row.attempts == 1
        assert row.last_error == "503"
    finally:
        await engine.stop()


async def test_poison_failure_buries(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        worker = UplinkWorker(buf, FakeTransport(), _config())
        backoff = await worker._resolve(
            await buf.claim(1), SendResult(ok=False, retryable=False, detail="400")
        )

        assert backoff == 0.0
        assert await engine.count(OutboundRecord, {"state": RecordState.DEAD}) == 1
    finally:
        await engine.stop()


async def test_retryable_batch_buried_once_attempts_exhausted(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        record = await buf.enqueue("s", "1")
        # two prior failed sends already recorded
        await buf.nack([record], "e")
        await buf.nack([record], "e")
        assert record.attempts == 2

        worker = UplinkWorker(buf, FakeTransport(), _config(max_attempts=3))
        backoff = await worker._resolve([record], SendResult(ok=False, retryable=True, detail="503"))

        assert backoff == 0.0
        assert await engine.count(OutboundRecord, {"state": RecordState.DEAD}) == 1
        assert await buf.pending_count() == 0
    finally:
        await engine.stop()


async def test_outage_never_spends_attempts(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        record = await buf.enqueue("s", "1")
        # the endpoint never answered; however long that lasts it must not
        # push the record toward burial, so attempts stays put
        worker = UplinkWorker(buf, FakeTransport(), _config(max_attempts=2))
        offline = SendResult(ok=False, retryable=True, reached_server=False, detail="offline")

        for _ in range(10):
            await worker._resolve(await buf.claim(1), offline)

        assert await buf.pending_count() == 1
        assert await engine.count(OutboundRecord, {"state": RecordState.DEAD}) == 0
        [row] = await engine.fetch(OutboundRecord, {})
        assert row.attempts == 0
        assert row.last_error == "offline"  # still recorded, just not charged
        assert record.seq == row.seq
    finally:
        await engine.stop()


async def test_only_exhausted_records_are_buried(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        old = await buf.enqueue("s", "old")
        await buf.nack([old], "e")  # one server-answered failure already spent
        fresh = await buf.enqueue("s", "fresh")

        # both are claimed together, but only `old` has spent its budget —
        # `fresh` has never been tried and must survive the batch's burial
        worker = UplinkWorker(buf, FakeTransport(), _config(max_attempts=2))
        claimed = await buf.claim(10)
        assert len(claimed) == 2
        backoff = await worker._resolve(claimed, SendResult(ok=False, retryable=True, detail="503"))

        assert backoff == 0.0  # min_seconds is 0.0 in the test config
        dead = await engine.fetch(OutboundRecord, {"state": RecordState.DEAD})
        assert [row.payload for row in dead] == ["old"]
        pending = await engine.fetch(OutboundRecord, {"state": RecordState.PENDING})
        assert [row.payload for row in pending] == ["fresh"]
        assert old.seq != fresh.seq
    finally:
        await engine.stop()


# ------------------------------------------------------------ _is_exhausted / _backoff units


async def test_exhausted_respects_ceiling(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        worker = UplinkWorker(buf, FakeTransport(), _config(max_attempts=3))
        assert worker._is_exhausted(OutboundRecord(stream="s", payload="p", attempts=1)) is False
        assert worker._is_exhausted(OutboundRecord(stream="s", payload="p", attempts=2)) is True
        # each record is judged on its own count, not the batch's highest
        batch = [
            OutboundRecord(stream="s", payload="p", attempts=0),
            OutboundRecord(stream="s", payload="p", attempts=2),
        ]
        exhausted, remaining = worker._partition_exhausted(batch)
        assert [r.attempts for r in exhausted] == [2]
        assert [r.attempts for r in remaining] == [0]
    finally:
        await engine.stop()


async def test_max_attempts_zero_never_exhausts(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        worker = UplinkWorker(buf, FakeTransport(), _config(max_attempts=0))
        record = OutboundRecord(stream="s", payload="p", attempts=9999)
        assert worker._is_exhausted(record) is False
    finally:
        await engine.stop()


async def test_backoff_grows_exponentially_and_caps(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        worker = UplinkWorker(buf, FakeTransport(), _config(min_seconds=1.0, max_seconds=10.0))
        worker._failures = 1
        assert worker._backoff() == 1.0
        worker._failures = 3
        assert worker._backoff() == 4.0
        worker._failures = 10
        assert worker._backoff() == 10.0  # capped at max_seconds
    finally:
        await engine.stop()


# ------------------------------------------------------------ run() integration


async def test_run_drains_all_pending(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        for i in range(5):
            await buf.enqueue("s", str(i))
        transport = FakeTransport()
        worker = UplinkWorker(buf, transport, _config())

        await _run_until(worker, lambda: _drained(buf, engine))

        assert transport.calls >= 1
        assert await engine.count(OutboundRecord, {}) == 0
    finally:
        await engine.stop()


async def test_run_survives_unexpected_transport_error(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        # first send raises something the transport contract says it never should;
        # the loop must log, release the batch, and redeliver rather than die
        transport = FakeTransport([RuntimeError("boom")])
        worker = UplinkWorker(buf, transport, _config())

        await _run_until(worker, lambda: _drained(buf, engine))

        assert transport.calls >= 2
        assert await engine.count(OutboundRecord, {}) == 0
    finally:
        await engine.stop()


async def test_run_buries_poison_then_keeps_draining(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("poison", "bad")
        await buf.enqueue("good", "ok")
        # first batch (both rows, whole-batch semantics) is rejected as poison,
        # so both are buried; nothing should remain pending afterward
        transport = FakeTransport([SendResult(ok=False, retryable=False, detail="400")])
        worker = UplinkWorker(buf, transport, _config())

        async def _no_pending() -> bool:
            return await buf.pending_count() == 0

        await _run_until(worker, _no_pending)

        assert await engine.count(OutboundRecord, {"state": RecordState.DEAD}) == 2
    finally:
        await engine.stop()


async def test_run_survives_sustained_outage(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        for i in range(3):
            await buf.enqueue("s", str(i))
        # the link stays down for many more cycles than max_attempts allows;
        # every record must still be waiting for the link to come back
        transport = FakeTransport()
        transport.default = SendResult(
            ok=False, retryable=True, reached_server=False, detail="offline"
        )
        worker = UplinkWorker(buf, transport, _config(max_attempts=2))

        async def _retried_often() -> bool:
            return transport.calls >= 8

        await _run_until(worker, _retried_often)

        assert await buf.pending_count() == 3
        assert await engine.count(OutboundRecord, {"state": RecordState.DEAD}) == 0
    finally:
        await engine.stop()


async def test_run_buries_after_max_attempts(tmp_path: Path) -> None:
    buf, engine = await _buffer(tmp_path)
    try:
        await buf.enqueue("s", "1")
        # every send fails retryably; with max_attempts=2 the record is buried
        # instead of blocking the queue forever
        transport = FakeTransport()
        transport.default = SendResult(ok=False, retryable=True, detail="503")
        worker = UplinkWorker(buf, transport, _config(max_attempts=2))

        async def _buried() -> bool:
            return await engine.count(OutboundRecord, {"state": RecordState.DEAD}) == 1

        await _run_until(worker, _buried)

        assert await buf.pending_count() == 0
        assert transport.calls == 2
    finally:
        await engine.stop()

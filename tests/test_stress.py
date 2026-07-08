"""Load/concurrency stress tests for the burst-handling machinery.

These push the backpressure chain and the storage group-commit far past the
scale the unit tests cover, and assert the invariants the design promises:
the handler cap is never exceeded, drop-oldest loses only the oldest and
never double-counts, and a burst of concurrent writes coalesces without
losing, duplicating, or hanging a single row. The group-commit cases also
act as race probes — overlapping bursts with a tiny window, and stop() racing
an in-flight flush, run many iterations to shake out scheduling races in the
one-cycle-at-a-time commit state.

All cases are marked `stress` and can be skipped with `-m "not stress"`.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from beacon.core.app import Beacon
from beacon.mqtt.client import BeaconMQTTClient
from beacon.mqtt.decorators import SubscriptionSpec
from beacon.mqtt.messages import Message
from beacon.storage import StorageEngine, Table, field

pytestmark = pytest.mark.stress


def _message_item(topic: str, seq: int) -> dict[str, Any]:
    # the seq rides in the payload so a handler can record exactly which
    # messages it saw, and drop-oldest can be checked against ordering
    return {"type": "message", "topic": topic, "payload": str(seq), "timestamp": 0.0}


def _reading_table() -> type[Table]:
    class Reading(Table):
        id: int | None = field(pk=True, auto=True)
        sensor_id: str = field(index=True)
        celsius: float

    return Reading


def _count_commits(engine: StorageEngine) -> list[int]:
    # wrap the connection's commit with a counter so a test can prove a
    # burst of writes shared far fewer commits than there were writes
    assert engine._conn is not None
    conn = engine._conn
    real_commit = conn.commit
    commits = [0]

    async def counting_commit() -> None:
        commits[0] += 1
        await real_commit()

    conn.commit = counting_commit  # type: ignore[method-assign]
    return commits


# ------------------------------------------------------ handler backpressure


async def test_concurrency_cap_is_never_exceeded_under_load() -> None:
    # gate every handler on one event: the loop fills exactly `cap` slots and
    # then blocks on acquire(), so if it could ever exceed the cap this pins
    # the violation. Releasing drains the whole backlog through the same cap.
    cap = 16
    total = 500
    app = Beacon(name="stress")
    app.mqtt_message_queue = asyncio.Queue()  # unbounded: isolate the cap, no drops
    app._handler_semaphore = asyncio.Semaphore(cap)

    running = 0
    peak = 0
    processed: list[int] = []
    release = asyncio.Event()

    async def handler(msg: Message[Any]) -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await release.wait()
        running -= 1
        processed.append(int(msg.payload))

    app._mqtt_subscriptions["a/b"] = [SubscriptionSpec(topic="a/b", qos=0, handler=handler)]
    for seq in range(total):
        app.mqtt_message_queue.put_nowait(_message_item("a/b", seq))

    processor = asyncio.create_task(app._process_mqtt_messages())

    # wait until the cap is saturated: `cap` handlers in flight, the loop
    # parked on acquire() unable to start a (cap+1)th
    for _ in range(200):
        if running >= cap:
            break
        await asyncio.sleep(0.005)
    assert running == cap
    assert peak == cap  # the cap held exactly; nothing slipped past it

    release.set()
    for _ in range(500):
        if len(processed) == total:
            break
        await asyncio.sleep(0.005)
    app._shutdown_event.set()
    await asyncio.gather(processor, return_exceptions=True)

    assert peak == cap  # never exceeded across the whole drain
    assert sorted(processed) == list(range(total))  # saturation delays, never loses


async def test_drop_oldest_sheds_only_oldest_at_scale(
    command_queue: asyncio.Queue[Any],
) -> None:
    # push far more than fits into a bounded queue with no consumer draining;
    # exactly the newest `maxsize` must survive and the rest are counted drops
    maxsize = 64
    total = 10_000
    queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)
    client = BeaconMQTTClient(
        pw=None,
        uname=None,
        command_queue=command_queue,
        message_queue=queue,
        host="localhost",
        port=1883,
        keepalive=60,
    )
    client.client = MagicMock()

    for seq in range(total):
        client._enqueue_message(_message_item("t", seq))

    assert client.dropped_messages == total - maxsize
    survivors = [int(queue.get_nowait()["payload"]) for _ in range(maxsize)]
    assert survivors == list(range(total - maxsize, total))  # newest telemetry won


async def test_no_message_is_both_processed_and_dropped(
    command_queue: asyncio.Queue[Any],
) -> None:
    # produce faster than a slow handler drains, through a small bounded queue,
    # so shedding actually happens while the processor runs concurrently. The
    # conservation invariant: every produced message is either handled exactly
    # once or counted as a drop — never lost silently, never double-counted.
    total = 3_000
    maxsize = 32
    app = Beacon(name="stress")
    app.mqtt_message_queue = asyncio.Queue(maxsize=maxsize)
    app._handler_semaphore = asyncio.Semaphore(8)

    processed: list[int] = []

    async def handler(msg: Message[Any]) -> None:
        await asyncio.sleep(0.0005)  # slow enough that the queue backs up
        processed.append(int(msg.payload))

    app._mqtt_subscriptions["t"] = [SubscriptionSpec(topic="t", qos=0, handler=handler)]

    client = BeaconMQTTClient(
        pw=None,
        uname=None,
        command_queue=command_queue,
        message_queue=app.mqtt_message_queue,
        host="localhost",
        port=1883,
        keepalive=60,
    )
    client.client = MagicMock()

    processor = asyncio.create_task(app._process_mqtt_messages())

    for seq in range(total):
        client._enqueue_message(_message_item("t", seq))
        if seq % 200 == 0:
            await asyncio.sleep(0)  # yield so the processor can make progress

    # let the processor and all in-flight handlers fully catch up
    for _ in range(2000):
        if app.mqtt_message_queue.empty() and not app._handler_tasks:
            break
        await asyncio.sleep(0.005)
    app._shutdown_event.set()
    await asyncio.gather(processor, return_exceptions=True)

    assert app.mqtt_message_queue.empty()
    assert len(set(processed)) == len(processed)  # nothing handled twice
    assert len(processed) + client.dropped_messages == total  # nothing lost
    assert client.dropped_messages > 0  # the burst really did overflow


# --------------------------------------------------------- group commit


async def test_large_burst_of_saves_coalesces_commits(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    engine = StorageEngine(tmp_path / "test.db", commit_delay=0.05)
    await engine.start()
    try:
        commits = _count_commits(engine)

        total = 500
        rows = [reading_cls(sensor_id=f"s{i}", celsius=float(i)) for i in range(total)]
        await asyncio.gather(*(row.save() for row in rows))

        assert await reading_cls.count() == total
        assert sorted(row.id for row in rows) == list(range(1, total + 1))  # every pk backfilled
        assert commits[0] <= total // 10  # >=10x coalescing, not one fsync per write
    finally:
        await engine.stop()


async def test_overlapping_bursts_with_tiny_window_lose_nothing(tmp_path: Path) -> None:
    # race probe: a tiny commit window means the flush fires mid-burst over and
    # over, so many group-commit cycles open and close while writes are still
    # arriving. Every row must still land with a unique, contiguous pk.
    reading_cls = _reading_table()
    engine = StorageEngine(tmp_path / "test.db", commit_delay=0.002)
    await engine.start()
    try:
        per_burst = 40
        bursts = 25

        async def burst(base: int) -> list[Table]:
            rows = [
                reading_cls(sensor_id=f"s{base + i}", celsius=float(i)) for i in range(per_burst)
            ]
            await asyncio.gather(*(row.save() for row in rows))
            return rows

        results = await asyncio.gather(*(burst(b * per_burst) for b in range(bursts)))

        total = per_burst * bursts
        ids = sorted(row.id for group in results for row in group)
        assert ids == list(range(1, total + 1))  # unique, contiguous, none dropped
        assert await reading_cls.count() == total
    finally:
        await engine.stop()


async def test_stop_racing_inflight_flush_always_commits(tmp_path: Path) -> None:
    # race probe: with a long window a save's commit only lands if stop() wakes
    # the pending flush. Run the start/save/stop handoff many times over fresh
    # engines; every save must return promptly and its row must survive reopen.
    reading_cls = _reading_table()

    for i in range(20):
        db = tmp_path / f"db{i}.db"
        engine = StorageEngine(db, commit_delay=30.0)
        await engine.start()

        save_task = asyncio.create_task(reading_cls(sensor_id="s", celsius=float(i)).save())
        await asyncio.sleep(0.01)  # let the INSERT run and the flush cycle open

        started = time.monotonic()
        await engine.stop()
        await asyncio.wait_for(save_task, timeout=1.0)  # must not hang on the 30s window
        assert time.monotonic() - started < 5.0

        reopened = StorageEngine(db)
        await reopened.start()
        try:
            assert await reading_cls.count() == 1  # the pending write was flushed
        finally:
            await reopened.stop()

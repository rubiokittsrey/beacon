from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from beacon.core.app import Beacon
from beacon.core.config import BeaconConfig, StorageConfig
from beacon.mqtt.decorators import PublisherSpec, SubscriptionSpec
from beacon.mqtt.messages import Message
from beacon.storage import Table, field


class Reading(BaseModel):
    sensor_id: str
    value: float


@pytest.fixture()
def app() -> Beacon:
    return Beacon(name="test-beacon")


def _message_item(topic: str, payload: str) -> dict[str, Any]:
    return {"type": "message", "topic": topic, "payload": payload, "timestamp": 123.0}


async def _run_processor_until(app: Beacon, received: list[Any]) -> None:
    processor = asyncio.create_task(app._process_mqtt_messages())

    # wait until the handler has run
    for _ in range(100):
        if received:
            break
        await asyncio.sleep(0.01)

    app._shutdown_event.set()
    await asyncio.gather(processor, return_exceptions=True)


class TestRegisterSubscriptions:
    def test_populates_routing_table_and_enqueues_commands(self, app: Beacon) -> None:
        async def handler(_msg: Message[Any]) -> None:
            return None

        app.bindings.subscribe("sensors/temp", qos=1)(handler)
        app._register_mqtt_subscriptions()

        assert app._mqtt_subscriptions["sensors/temp"][0].handler is handler

        cmd = app.mqtt_command_queue.get_nowait()
        assert cmd == {"type": "subscribe", "topic": "sensors/temp", "qos": 1}

    def test_same_filter_keeps_all_handlers_and_subscribes_once_at_max_qos(
        self, app: Beacon
    ) -> None:
        async def first(_msg: Message[Any]) -> None:
            return None

        async def second(_msg: Message[Any]) -> None:
            return None

        app.bindings.subscribe("sensors/temp", qos=1)(first)
        app.bindings.subscribe("sensors/temp", qos=0)(second)
        app._register_mqtt_subscriptions()

        handlers = [spec.handler for spec in app._mqtt_subscriptions["sensors/temp"]]
        assert handlers == [first, second]

        # a single subscribe command, at the max qos among the bindings
        cmd = app.mqtt_command_queue.get_nowait()
        assert cmd == {"type": "subscribe", "topic": "sensors/temp", "qos": 1}
        assert app.mqtt_command_queue.empty()


class TestPruneDoneTasks:
    async def test_done_tasks_are_removed(self, app: Beacon) -> None:
        async def quick() -> None:
            return None

        async def slow() -> None:
            await asyncio.sleep(10)

        done = asyncio.create_task(quick())
        pending = asyncio.create_task(slow())
        await done

        app._tasks = [done, pending]
        app._prune_done_tasks()

        assert app._tasks == [pending]

        pending.cancel()


class TestLoadConfig:
    async def test_missing_file_uses_defaults(self, app: Beacon) -> None:
        app.config_path = Path("definitely-missing.yaml")
        await app._load_config()
        assert app._config is not None
        assert app._config.mqtt.host == "localhost"


class TestOutgoingMessageProcessor:
    async def test_dispatches_message_to_registered_handler(self, app: Beacon) -> None:
        received: list[Message[Any]] = []

        async def handler(msg: Message[Any]) -> None:
            received.append(msg)

        app._mqtt_subscriptions["a/b"] = [SubscriptionSpec(topic="a/b", qos=0, handler=handler)]
        app.mqtt_message_queue.put_nowait(_message_item("a/b", json.dumps({"v": 1})))

        await _run_processor_until(app, received)

        assert len(received) == 1
        msg = received[0]
        assert msg.topic == "a/b"
        assert msg.timestamp == 123.0
        assert msg.json() == {"v": 1}
        assert msg.data is None

    async def test_wildcard_subscription_routes_concrete_topics(self, app: Beacon) -> None:
        received: list[Message[Any]] = []

        async def handler(msg: Message[Any]) -> None:
            received.append(msg)

        app._mqtt_subscriptions["sensors/+/temp"] = [
            SubscriptionSpec(topic="sensors/+/temp", qos=0, handler=handler)
        ]
        app.mqtt_message_queue.put_nowait(_message_item("sensors/kitchen/temp", "21.5"))

        await _run_processor_until(app, received)

        assert len(received) == 1
        assert received[0].topic == "sensors/kitchen/temp"

    async def test_overlapping_filters_each_receive_the_message(self, app: Beacon) -> None:
        hits: list[str] = []

        def make_handler(name: str):
            async def handler(_msg: Message[Any]) -> None:
                hits.append(name)

            return handler

        app._mqtt_subscriptions["sensors/+/temp"] = [
            SubscriptionSpec(topic="sensors/+/temp", qos=0, handler=make_handler("wildcard"))
        ]
        app._mqtt_subscriptions["sensors/kitchen/#"] = [
            SubscriptionSpec(topic="sensors/kitchen/#", qos=0, handler=make_handler("hash"))
        ]
        app.mqtt_message_queue.put_nowait(_message_item("sensors/kitchen/temp", "{}"))

        processor = asyncio.create_task(app._process_mqtt_messages())
        for _ in range(100):
            if len(hits) == 2:
                break
            await asyncio.sleep(0.01)
        app._shutdown_event.set()
        await asyncio.gather(processor, return_exceptions=True)

        assert sorted(hits) == ["hash", "wildcard"]

    async def test_multiple_handlers_on_same_filter_each_receive_the_message(
        self, app: Beacon
    ) -> None:
        hits: list[str] = []

        def make_handler(name: str):
            async def handler(_msg: Message[Any]) -> None:
                hits.append(name)

            return handler

        app.bindings.subscribe("sensors/temp")(make_handler("first"))
        app.bindings.subscribe("sensors/temp")(make_handler("second"))
        app._register_mqtt_subscriptions()
        app.mqtt_message_queue.put_nowait(_message_item("sensors/temp", "{}"))

        processor = asyncio.create_task(app._process_mqtt_messages())
        for _ in range(100):
            if len(hits) == 2:
                break
            await asyncio.sleep(0.01)
        app._shutdown_event.set()
        await asyncio.gather(processor, return_exceptions=True)

        assert sorted(hits) == ["first", "second"]

    async def test_message_without_handler_is_ignored(self, app: Beacon) -> None:
        app.mqtt_message_queue.put_nowait(
            {"type": "message", "topic": "no/handler", "payload": "x"}
        )

        processor = asyncio.create_task(app._process_mqtt_messages())
        await asyncio.sleep(0.05)
        app._shutdown_event.set()
        await asyncio.gather(processor, return_exceptions=True)

        # no handler tasks should have been tracked
        assert app._tasks == []


class TestInboundValidation:
    async def test_valid_payload_is_exposed_as_model_instance(self, app: Beacon) -> None:
        received: list[Message[Reading]] = []

        async def handler(msg: Message[Reading]) -> None:
            received.append(msg)

        app._mqtt_subscriptions["sensors/temp"] = [
            SubscriptionSpec(topic="sensors/temp", qos=0, handler=handler, model=Reading)
        ]
        app.mqtt_message_queue.put_nowait(
            _message_item("sensors/temp", json.dumps({"sensor_id": "s1", "value": 21.5}))
        )

        await _run_processor_until(app, received)

        assert len(received) == 1
        assert received[0].data == Reading(sensor_id="s1", value=21.5)

    async def test_invalid_payload_is_dropped_and_loop_continues(self, app: Beacon) -> None:
        received: list[Message[Reading]] = []

        async def handler(msg: Message[Reading]) -> None:
            received.append(msg)

        app._mqtt_subscriptions["sensors/temp"] = [
            SubscriptionSpec(topic="sensors/temp", qos=0, handler=handler, model=Reading)
        ]
        # malformed json, wrong shape, then a valid payload; only the valid
        # one may reach the handler, proving drop without killing the loop
        app.mqtt_message_queue.put_nowait(_message_item("sensors/temp", "not-json"))
        app.mqtt_message_queue.put_nowait(_message_item("sensors/temp", json.dumps({"v": 1})))
        app.mqtt_message_queue.put_nowait(
            _message_item("sensors/temp", json.dumps({"sensor_id": "s1", "value": 1.0}))
        )

        await _run_processor_until(app, received)

        assert len(received) == 1
        assert received[0].data == Reading(sensor_id="s1", value=1.0)


class TestRunPublisher:
    async def test_publishes_payload_to_command_queue(self, app: Beacon) -> None:
        async def make_payload() -> dict[str, Any]:
            return {"status": "online"}

        spec = PublisherSpec(
            topic="devices/heartbeat",
            qos=0,
            retain=False,
            every_s=0.01,
            fn=make_payload,
        )

        task = asyncio.create_task(app._run_publisher(spec))

        cmd = await asyncio.wait_for(app.mqtt_command_queue.get(), timeout=1.0)

        app._shutdown_event.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        assert cmd["type"] == "publish"
        assert cmd["topic"] == "devices/heartbeat"
        assert json.loads(cmd["payload"]) == {"status": "online"}

    async def test_model_publisher_validates_and_serializes(self, app: Beacon) -> None:
        async def make_payload() -> dict[str, Any]:
            return {"sensor_id": "s1", "value": 2}

        spec = PublisherSpec(
            topic="sensors/temp",
            qos=0,
            retain=False,
            every_s=0.01,
            fn=make_payload,
            model=Reading,
        )

        task = asyncio.create_task(app._run_publisher(spec))
        cmd = await asyncio.wait_for(app.mqtt_command_queue.get(), timeout=1.0)

        app._shutdown_event.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        # value coerced to float by the model on the way out
        assert json.loads(cmd["payload"]) == {"sensor_id": "s1", "value": 2.0}

    async def test_bare_model_return_is_serialized_without_declared_model(
        self, app: Beacon
    ) -> None:
        async def make_payload() -> Reading:
            return Reading(sensor_id="s1", value=3.0)

        spec = PublisherSpec(
            topic="sensors/temp",
            qos=0,
            retain=False,
            every_s=0.01,
            fn=make_payload,
        )

        task = asyncio.create_task(app._run_publisher(spec))
        cmd = await asyncio.wait_for(app.mqtt_command_queue.get(), timeout=1.0)

        app._shutdown_event.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        assert json.loads(cmd["payload"]) == {"sensor_id": "s1", "value": 3.0}

    async def test_invalid_model_return_is_not_published(self, app: Beacon) -> None:
        async def make_payload() -> dict[str, Any]:
            return {"wrong": "shape"}

        spec = PublisherSpec(
            topic="sensors/temp",
            qos=0,
            retain=False,
            every_s=0.01,
            fn=make_payload,
            model=Reading,
        )

        task = asyncio.create_task(app._run_publisher(spec))

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(app.mqtt_command_queue.get(), timeout=0.1)

        app._shutdown_event.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class TestStorageLifecycle:
    def _configure(self, app: Beacon) -> None:
        app._config = BeaconConfig(storage=StorageConfig(path=":memory:"))

    async def test_skipped_when_no_tables_registered(self, app: Beacon) -> None:
        self._configure(app)
        await app._start_storage()
        assert app.storage is None

    async def test_engine_starts_and_binds_when_tables_registered(self, app: Beacon) -> None:
        class Thing(Table):
            id: int | None = field(pk=True, auto=True)
            name: str

        self._configure(app)
        await app._start_storage()
        try:
            assert app.storage is not None
            assert Table._engine is app.storage
        finally:
            await app.storage.stop()  # type: ignore[union-attr]

    async def test_shutdown_stops_storage_and_unbinds(self, app: Beacon) -> None:
        class Thing(Table):
            id: int | None = field(pk=True, auto=True)
            name: str

        self._configure(app)
        await app._start_storage()
        await app._shutdown()

        assert Table._engine is None
        assert app.storage is not None
        assert app.storage._conn is None

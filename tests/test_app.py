from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from beacon.core.app import Beacon
from beacon.mqtt.decorators import PublisherSpec


@pytest.fixture()
def app() -> Beacon:
    return Beacon(name="test-beacon")


class TestRegisterSubscriptions:
    def test_populates_handlers_and_enqueues_commands(self, app: Beacon) -> None:
        async def handler(_msg: dict[str, Any]) -> None:
            return None

        app.bindings.subscribe("sensors/temp", qos=1)(handler)
        app._register_mqtt_subscriptions()

        assert app._mqtt_handlers["sensors/temp"] is handler

        cmd = app.mqtt_incoming_queue.get_nowait()
        assert cmd == {"type": "subscribe", "topic": "sensors/temp", "qos": 1}


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
        received: list[dict[str, Any]] = []

        async def handler(msg: dict[str, Any]) -> None:
            received.append(msg)

        app._mqtt_handlers["a/b"] = handler
        app.mqtt_outgoing_queue.put_nowait(
            {
                "type": "message",
                "topic": "a/b",
                "payload": json.dumps({"v": 1}),
                "timestamp": 123.0,
            }
        )

        processor = asyncio.create_task(app._process_mqtt_outgoing_messages())

        # wait until the handler has run
        for _ in range(100):
            if received:
                break
            await asyncio.sleep(0.01)

        app._shutdown_event.set()
        await asyncio.gather(processor, return_exceptions=True)

        assert len(received) == 1
        msg = received[0]
        assert msg["topic"] == "a/b"
        assert msg["json"]() == {"v": 1}

    async def test_message_without_handler_is_ignored(self, app: Beacon) -> None:
        app.mqtt_outgoing_queue.put_nowait(
            {"type": "message", "topic": "no/handler", "payload": "x"}
        )

        processor = asyncio.create_task(app._process_mqtt_outgoing_messages())
        await asyncio.sleep(0.05)
        app._shutdown_event.set()
        await asyncio.gather(processor, return_exceptions=True)

        # no handler tasks should have been tracked
        assert app._tasks == []


class TestRunPublisher:
    async def test_publishes_payload_to_incoming_queue(self, app: Beacon) -> None:
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

        cmd = await asyncio.wait_for(app.mqtt_incoming_queue.get(), timeout=1.0)

        app._shutdown_event.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        assert cmd["type"] == "publish"
        assert cmd["topic"] == "devices/heartbeat"
        assert json.loads(cmd["payload"]) == {"status": "online"}

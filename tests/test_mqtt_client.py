from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from beacon.mqtt.client import BeaconMQTTClient


@pytest.fixture()
def client(
    command_queue: asyncio.Queue[Any],
    message_queue: asyncio.Queue[Any],
) -> BeaconMQTTClient:
    c = BeaconMQTTClient(
        pw=None,
        uname=None,
        command_queue=command_queue,
        message_queue=message_queue,
        host="localhost",
        port=1883,
        keepalive=60,
    )
    # replace the real paho client with a mock so nothing touches the network
    c.client = MagicMock()
    return c


class _FakeMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


class TestOnMessage:
    async def test_decoded_message_is_enqueued(
        self, client: BeaconMQTTClient, message_queue: asyncio.Queue[Any]
    ) -> None:
        client._loop = asyncio.get_running_loop()
        client._on_message(None, None, _FakeMessage("sensors/temp", b'{"v": 1}'))

        # the put is scheduled on the loop via call_soon_threadsafe
        item = await asyncio.wait_for(message_queue.get(), timeout=1.0)
        assert item["type"] == "message"
        assert item["topic"] == "sensors/temp"
        assert item["payload"] == '{"v": 1}'
        assert isinstance(item["timestamp"], float)

    async def test_invalid_utf8_is_replaced_not_raised(
        self, client: BeaconMQTTClient, message_queue: asyncio.Queue[Any]
    ) -> None:
        client._loop = asyncio.get_running_loop()
        client._on_message(None, None, _FakeMessage("topic", b"\xff\xfe"))
        item = await asyncio.wait_for(message_queue.get(), timeout=1.0)
        assert isinstance(item["payload"], str)

    def test_dropped_without_event_loop(
        self, client: BeaconMQTTClient, message_queue: asyncio.Queue[Any]
    ) -> None:
        # before start() captures the loop, messages are dropped, not crashed on
        client._on_message(None, None, _FakeMessage("topic", b"x"))
        assert message_queue.empty()


class TestConnect:
    def test_uses_async_connect_so_paho_retries_initial_connection(
        self, client: BeaconMQTTClient
    ) -> None:
        client._connect()

        client.client.reconnect_delay_set.assert_called_once_with(min_delay=1, max_delay=60)
        client.client.connect_async.assert_called_once_with("localhost", 1883, 60)
        client.client.loop_start.assert_called_once()
        # the blocking variant would raise on an unreachable broker at startup
        client.client.connect.assert_not_called()


class TestHandleSubscribe:
    def test_ignores_bad_topic(self, client: BeaconMQTTClient) -> None:
        client._handle_subscribe({"topic": "", "qos": 0})
        client._handle_subscribe({"qos": 0})
        assert client._retained_subs == {}
        client.client.subscribe.assert_not_called()

    def test_records_desired_sub_when_disconnected(self, client: BeaconMQTTClient) -> None:
        client.client.is_connected.return_value = False
        client._handle_subscribe({"topic": "a/b", "qos": 1})

        assert client._retained_subs == {"a/b": 1}
        client.client.subscribe.assert_not_called()

    def test_subscribes_when_connected(self, client: BeaconMQTTClient) -> None:
        client.client.is_connected.return_value = True
        client._handle_subscribe({"topic": "a/b", "qos": 2})

        assert client._retained_subs == {"a/b": 2}
        client.client.subscribe.assert_called_once_with("a/b", qos=2)


class TestHandlePublish:
    def test_ignores_bad_topic(self, client: BeaconMQTTClient) -> None:
        client._handle_publish({"topic": "", "payload": "x"})
        client.client.publish.assert_not_called()

    def test_dropped_when_disconnected(self, client: BeaconMQTTClient) -> None:
        client.client.is_connected.return_value = False
        client._handle_publish({"topic": "a/b", "payload": "x"})
        client.client.publish.assert_not_called()

    def test_publishes_when_connected(self, client: BeaconMQTTClient) -> None:
        client.client.is_connected.return_value = True
        client._handle_publish(
            {"topic": "a/b", "payload": "hi", "qos": 1, "retain": True}
        )
        client.client.publish.assert_called_once_with(
            "a/b", payload="hi", qos=1, retain=True
        )


class TestStop:
    async def test_shutdown_only_runs_once(self, client: BeaconMQTTClient) -> None:
        # stop() and start()'s cleanup both call _shutdown; the second is a no-op
        await client.stop()
        await client.stop()

        client.client.loop_stop.assert_called_once()
        client.client.disconnect.assert_called_once()


class TestProcessCommands:
    async def test_routes_subscribe_and_publish_then_stops(
        self,
        client: BeaconMQTTClient,
        command_queue: asyncio.Queue[Any],
    ) -> None:
        client.client.is_connected.return_value = True
        client._running = True

        command_queue.put_nowait({"type": "subscribe", "topic": "a/b", "qos": 0})
        command_queue.put_nowait(
            {"type": "publish", "topic": "a/b", "payload": "p"}
        )

        async def stop_soon() -> None:
            while command_queue.qsize() > 0:
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.05)
            client._running = False

        await asyncio.gather(client._process_commands(), stop_soon())

        client.client.subscribe.assert_called_once_with("a/b", qos=0)
        client.client.publish.assert_called_once()

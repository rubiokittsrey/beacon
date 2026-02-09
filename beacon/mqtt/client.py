from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from paho.mqtt import client as paho_mqtt
from paho.mqtt import enums


class BeaconMQTTClient:
    """
    Async MQTT client using paho-mqtt.

    Responsibilities:
    - Maintain a paho mqtt connection (with reconnect)
    - Accept commands from the app via incoming_queue (subscribe/publish)
    - Emit events/messages back to the app via outgoing_queue
    - Remember desired subscriptions and resubscribe after reconnect
    """

    def __init__(
        self,
        *,
        pw: str | None,
        uname: str | None,
        id: str = "beacon-mqtt-client",
        outgoing_queue: asyncio.Queue[Any],
        incoming_queue: asyncio.Queue[Any],
        host: str,
        port: int,
        keepalive: int,
    ) -> None:
        # identity/auth
        self.id = id
        self.uname = uname
        self.pw = pw

        # async queues for communication
        self._outgoing_queue = outgoing_queue
        self._incoming_queue = incoming_queue

        # connection config
        self.host = host
        self.port = port
        self.keepalive = keepalive

        # logging
        self._logger = logging.getLogger(__name__)

        # desired subscriptions (topic -> qos) used for reconnect > resubscribe
        self._retained_subs: dict[str, int] = {}

        # shutdown flag
        self._running = False

        # paho client
        self.client = paho_mqtt.Client(
            callback_api_version=enums.CallbackAPIVersion.VERSION2,
            client_id=self.id,
            protocol=paho_mqtt.MQTTv311,
        )

        # callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    # ----------#
    # lifecycle #
    # ----------#

    async def start(self) -> None:
        """Start the MQTT client and begin processing commands."""
        self._logger.info("mqtt client starting id=%s", self.id)
        self._running = True

        # configure auth before connect
        if self.uname and self.pw:
            self.client.username_pw_set(self.uname, self.pw)

        # connect and start network loop thread
        await self._connect()

        # start command processing task
        try:
            await self._process_commands()
        finally:
            await self._shutdown()

    async def stop(self) -> None:
        # signal client to stop
        self._running = False

    async def _connect(self) -> None:

        # runs connect in an executor
        # to initiate non-blocking connection to mqtt broker

        loop = asyncio.get_running_loop()

        try:
            self.client.reconnect_delay_set(min_delay=1, max_delay=60)
            await loop.run_in_executor(
                None, lambda: self.client.connect(self.host, self.port, self.keepalive)
            )
            self.client.loop_start()
            self._logger.info("mqtt client connected to %s:%s", self.host, self.port)
        except Exception:
            self._logger.exception(
                "could not connect to broker host=%s port=%s", self.host, self.port
            )

    async def _shutdown(self) -> None:
        # shutdown the mqtt client gracefully

        self._logger.info("mqtt client shutting down")
        try:
            self.client.loop_stop()
        except Exception:
            self._logger.exception("error stopping mqtt loop")

        try:
            self.client.disconnect()
        except Exception:
            self._logger.exception("error disconnecting mqtt client")

    # ----------------#
    # paho callbacks  #
    # ----------------#

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code != 0:
            self._logger.error("failed to connect reason_code=%s", reason_code)
            return

        self._logger.info("connected to broker")

        # resubscribe on reconnect using desired subscriptions
        for topic, qos in list(self._retained_subs.items()):
            try:
                client.subscribe(topic, qos=qos)
                self._logger.info("resubscribed topic=%s qos=%s", topic, qos)
            except Exception:
                self._logger.exception("resubscribe failed topic=%s", topic)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:
        self._logger.warning("disconnected from broker reason_code=%s", reason_code)

    def _on_message(self, client, userdata, message: paho_mqtt.MQTTMessage) -> None:
        # handles incoming mqtt messages
        # runs in paho's netowrk thread
        try:
            payload = message.payload.decode(errors="replace")
        except Exception:  # noqa: BLE001
            payload = repr(message.payload)

        # message into queue (thread safe)
        try:
            self._outgoing_queue.put_nowait(
                {
                    "type": "message",
                    "topic": message.topic,
                    "payload": payload,
                    "timestamp": time.time(),
                }
            )
        except asyncio.QueueFull:
            self._logger.warning(
                "outgoing queue full, dropping message from topic=%s", message.topic
            )

    # -------------------#
    # command processing #
    # -------------------#

    async def _process_commands(self) -> None:
        while self._running:
            try:
                # wait for command with timeout to allow checking _running flag
                cmd = await asyncio.wait_for(self._incoming_queue.get(), timeout=0.1)

                if not isinstance(cmd, dict):
                    continue

                cmd_type = cmd.get("type")
                if cmd_type == "subscribe":
                    self._handle_subscribe(cmd)
                elif cmd_type == "publish":
                    self._handle_publish(cmd)
                else:
                    self._logger.debug("unknown cmd: %r", cmd)

            except TimeoutError:
                # no command received, continue loop
                continue

            except Exception:
                self._logger.exception("error processing command")

    def _handle_subscribe(self, cmd: dict[str, Any]) -> None:
        """Handle subscribe command."""
        topic = cmd.get("topic")
        if not isinstance(topic, str) or not topic:
            self._logger.debug("subscribe ignored (bad topic): %r", cmd)
            return

        qos = int(cmd.get("qos", 0))
        self._retained_subs[topic] = qos

        if not self.client.is_connected():
            # keep desired subs so connect callback can resubscribe
            self._logger.debug("subscription queued (not connected) topic=%s qos=%s", topic, qos)
            return

        try:
            self.client.subscribe(topic, qos=qos)
            self._logger.info("subscribed topic=%s qos=%s", topic, qos)
        except Exception:
            self._logger.exception("subscribe failed topic=%s", topic)

    def _handle_publish(self, cmd: dict[str, Any]) -> None:
        """Handle publish command."""
        topic = cmd.get("topic")
        if not isinstance(topic, str) or not topic:
            self._logger.debug("publish ignored (bad topic): %r", cmd)
            return

        payload = cmd.get("payload", "")
        qos = int(cmd.get("qos", 0))
        retain = bool(cmd.get("retain", False))

        if not self.client.is_connected():
            self._logger.debug("publish dropped (not connected) topic=%s", topic)
            return

        try:
            self.client.publish(topic, payload=payload, qos=qos, retain=retain)
        except Exception:
            self._logger.exception("publish failed topic=%s", topic)

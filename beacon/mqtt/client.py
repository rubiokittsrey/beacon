import asyncio
import logging
import time
from typing import Any

from paho.mqtt import client as paho_mqtt
from paho.mqtt import enums

# rate limit for the drop warnings (inbound queue-full, outbound while
# disconnected) so a sustained burst logs a heartbeat with a running total
# instead of one line per drop
_DROP_LOG_INTERVAL_S = 5.0


class BeaconMQTTClient:
    """Async wrapper over the paho MQTT client.

    Consumes subscribe/publish commands from `command_queue` and forwards
    inbound broker messages onto `message_queue` for the app to dispatch.
    """

    def __init__(
        self,
        *,
        pw: str | None,
        uname: str | None,
        id: str = "beacon-mqtt-client",
        command_queue: asyncio.Queue[Any],
        message_queue: asyncio.Queue[Any],
        host: str,
        port: int,
        keepalive: int,
    ) -> None:
        # identity/auth
        self.id = id
        self.uname = uname
        self.pw = pw

        # async queues for communication:
        # commands (subscribe/publish) flow in from the app,
        # broker messages flow out to the app's handlers
        self._command_queue = command_queue
        self._message_queue = message_queue

        # connection config
        self.host = host
        self.port = port
        self.keepalive = keepalive

        self._logger = logging.getLogger(__name__)

        # desired subscriptions (topic -> qos) used for reconnect > resubscribe
        self._retained_subs: dict[str, int] = {}

        self._running = False
        self._shutdown_done = False

        # messages shed when the (bounded) message queue is full
        self._dropped_total = 0
        self._last_drop_log = 0.0

        # publishes dropped because the broker link was down
        self._dropped_publishes = 0
        self._last_publish_drop_log = 0.0

        # event loop captured in start(); paho callbacks run on the network
        # thread and need it to hand messages back to the loop thread
        self._loop: asyncio.AbstractEventLoop | None = None

        self.client = paho_mqtt.Client(
            callback_api_version=enums.CallbackAPIVersion.VERSION2,
            client_id=self.id,
            protocol=paho_mqtt.MQTTv311,
        )

        # callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message


    async def start(self) -> None:
        """Connect to the broker and process commands until stopped."""
        self._logger.info("mqtt client starting id=%s", self.id)
        self._running = True
        self._shutdown_done = False
        self._loop = asyncio.get_running_loop()

        if self.uname and self.pw:
            self.client.username_pw_set(self.uname, self.pw)

        self._connect()

        try:
            await self._process_commands()
        finally:
            await self._shutdown()

    async def stop(self) -> None:
        """Signal the command loop to stop and shut the client down."""
        self._running = False
        await self._shutdown()

    def _connect(self) -> None:
        # connect_async + loop_start hands the initial connection to paho's
        # network thread, which retries it with this backoff (loop_start runs
        # loop_forever with retry_first_connection=True) - so a broker that is
        # down at startup is picked up once reachable; _on_connect logs success
        self.client.reconnect_delay_set(min_delay=1, max_delay=60)
        self.client.connect_async(self.host, self.port, self.keepalive)
        self.client.loop_start()
        self._logger.info("mqtt client connecting to %s:%s", self.host, self.port)

    async def _shutdown(self) -> None:
        # stop() and start()'s cleanup can both land here; only run once
        if self._shutdown_done:
            return
        self._shutdown_done = True

        self._logger.info("mqtt client shutting down")
        try:
            self.client.loop_stop()
        except Exception:
            self._logger.exception("error stopping mqtt loop")

        try:
            self.client.disconnect()
        except Exception:
            self._logger.exception("error disconnecting mqtt client")

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:  # noqa: ARG002
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

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:  # noqa: ARG002
        self._logger.warning("disconnected from broker reason_code=%s", reason_code)

    def _on_message(self, client, userdata, message: paho_mqtt.MQTTMessage) -> None:  # noqa: ARG002

        try:
            payload = message.payload.decode(errors="replace")
        except Exception:  # noqa: BLE001
            payload = repr(message.payload)

        item = {
            "type": "message",
            "topic": message.topic,
            "payload": payload,
            "timestamp": time.time(),
        }

        # this callback runs on paho's network thread and asyncio queues are
        # not thread-safe, so the put must be scheduled on the loop thread
        if self._loop is None:
            self._logger.warning("no event loop, dropping message from topic=%s", message.topic)
            return

        try:
            self._loop.call_soon_threadsafe(self._enqueue_message, item)
        except RuntimeError:
            # loop already closed mid-shutdown
            self._logger.warning(
                "event loop closed, dropping message from topic=%s", message.topic
            )

    @property
    def dropped_messages(self) -> int:
        """How many inbound messages were shed because the queue was full."""
        return self._dropped_total

    # runs on the loop thread (via call_soon_threadsafe). When the bounded
    # queue is full, the oldest message is evicted so the newest telemetry
    # wins; drops are counted and warned about at most every few seconds —
    # blocking paho's network thread instead would stall keepalives
    def _enqueue_message(self, item: dict[str, Any]) -> None:
        dropped = 0
        while True:
            try:
                self._message_queue.put_nowait(item)
                break
            except asyncio.QueueFull:
                try:
                    self._message_queue.get_nowait()
                    dropped += 1
                except asyncio.QueueEmpty:
                    continue

        if dropped:
            self._dropped_total += dropped
            now = time.monotonic()
            if now - self._last_drop_log >= _DROP_LOG_INTERVAL_S:
                self._last_drop_log = now
                self._logger.warning(
                    "message queue full; dropping oldest (%d dropped since start)",
                    self._dropped_total,
                )

    async def _process_commands(self) -> None:
        while self._running:
            try:
                cmd = await asyncio.wait_for(self._command_queue.get(), timeout=0.1)

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
                continue

            except Exception:
                self._logger.exception("error processing command")

    def _handle_subscribe(self, cmd: dict[str, Any]) -> None:
        topic = cmd.get("topic")
        if not isinstance(topic, str) or not topic:
            self._logger.debug("subscribe ignored (bad topic): %r", cmd)
            return

        qos = int(cmd.get("qos", 0))
        self._retained_subs[topic] = qos

        if not self.client.is_connected():
            self._logger.debug("subscription queued (not connected) topic=%s qos=%s", topic, qos)
            return

        try:
            self.client.subscribe(topic, qos=qos)
            self._logger.info("subscribed topic=%s qos=%s", topic, qos)
        except Exception:
            self._logger.exception("subscribe failed topic=%s", topic)

    def _handle_publish(self, cmd: dict[str, Any]) -> None:
        topic = cmd.get("topic")
        if not isinstance(topic, str) or not topic:
            self._logger.debug("publish ignored (bad topic): %r", cmd)
            return

        payload = cmd.get("payload", "")
        qos = int(cmd.get("qos", 0))
        retain = bool(cmd.get("retain", False))

        if not self.client.is_connected():
            self._drop_publish(topic)
            return

        try:
            self.client.publish(topic, payload=payload, qos=qos, retain=retain)
        except Exception:
            self._logger.exception("publish failed topic=%s", topic)

    @property
    def dropped_publishes(self) -> int:
        """How many outbound publishes were dropped because the broker was unreachable."""
        return self._dropped_publishes

    # a publish issued while the link is down is gone: paho only queues QoS>0
    # messages for a session it has already established, and holding them here
    # would grow unbounded. Counting and warning on a rate limit at least makes
    # the loss visible, like the inbound shed path; data that must survive an
    # outage belongs in the uplink
    def _drop_publish(self, topic: str) -> None:
        self._dropped_publishes += 1
        now = time.monotonic()
        if now - self._last_publish_drop_log >= _DROP_LOG_INTERVAL_S:
            self._last_publish_drop_log = now
            self._logger.warning(
                "not connected; dropping publish topic=%s (%d dropped since start)",
                topic,
                self._dropped_publishes,
            )

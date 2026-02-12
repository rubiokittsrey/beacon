from __future__ import annotations

import asyncio
import json
import logging
import signal
from pathlib import Path
from typing import Any

from beacon.mqtt import BeaconMQTTClient, Handler, MQTTBindings, PublisherSpec
from beacon.utils.logging_conf import AsyncLogging, LoggingConfig, new_run_log_dir


class Beacon:
    def __init__(self, name: str, config_path: Path | None = None):
        self.name = name
        self.config_path = config_path or Path("beacon.yaml")

        self._shutdown_event = asyncio.Event()

        # async runtime state
        self._tasks: list[asyncio.Task[Any]] = []

        # app clients
        self._mqtt_client: BeaconMQTTClient | None = None

        # mqtt dsl bindings + handler routing table
        self.mqtt = MQTTBindings()
        self._mqtt_handlers: dict[str, Handler] = {}

        # asyncio queues for clients communication
        self.mqtt_incoming_queue: asyncio.Queue[Any] = asyncio.Queue()
        self.mqtt_outgoing_queue: asyncio.Queue[Any] = asyncio.Queue()

        # logging
        self.logger = logging.getLogger(__name__)
        self._async_logging: AsyncLogging | None = None

    async def _load_config(self) -> None:
        # TODO: setup config + handling
        self.logger.warning("config not setup")

    # ----------------#
    # signal handling #
    # ----------------#

    # sets up handler for SIGINT and SIGTERM
    def _setup_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def _request_shutdown(sig: int) -> None:
            sig_name = signal.Signals(sig).name
            self.logger.info("received shutdown signal: %s", sig_name)

            # schedule shutdown as a task
            # adding this task to the tracked tasks (self._tasks) will cause a maximum recursion depth error
            asyncio.create_task(self._shutdown())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: _request_shutdown(s))
            except NotImplementedError:
                # windows fallback
                signal.signal(sig, lambda *_args, sig=sig: _request_shutdown(sig))

    # ---------------#
    # shutdown logic #
    # ---------------#

    async def _shutdown(self) -> None:
        # idempotent shutdown guard
        if self._shutdown_event.is_set():
            return

        self.logger.info("shutting down")
        self._shutdown_event.set()

        # stop mqtt client
        if self._mqtt_client:
            await self._mqtt_client.stop()

        # stop logging
        if self._async_logging:
            self._async_logging.stop()

        # cancel all tasks
        await self._cancel_tasks()

    async def _cancel_tasks(self) -> None:
        # cancel tracked tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    # -------------#
    # main runtime #
    # -------------#

    async def start(self) -> None:
        self._setup_logging()

        self.logger.info("starting %s", self.name)
        await self._load_config()

        try:
            self._setup_signal_handlers()

            # start clients
            await self._start_mqtt_client()

            # start mqtt asyncio background tasks
            # register mqtt dsl bindings (including periodic publishers) > start outgoing message processing
            # TODO: allow connection attemptt to complete first before registering subscriptions
            self._register_mqtt_subscriptions()
            self._start_mqtt_periodic_publisher()
            self._start_mqtt_outgoing_message_processoor()

            # keep main process alive until shutdown is requested
            await self._shutdown_event.wait()

        except Exception:
            self.logger.exception("fatal error in beacon")
            raise

        finally:
            if not self._shutdown_event.is_set():
                await self._shutdown()

    # -----------------------------------------------------#
    #   MQTT client methods, processors, background tasks  #
    # -----------------------------------------------------#

    # initialize and start the mqtt client
    # starts the client in an asyncio background task
    async def _start_mqtt_client(self) -> None:
        """Create and start the MQTT client."""
        self._mqtt_client = BeaconMQTTClient(
            id=f"{self.name}-mqtt-client",
            uname=None,
            pw=None,
            incoming_queue=self.mqtt_incoming_queue,
            outgoing_queue=self.mqtt_outgoing_queue,
            host="localhost",
            port=1883,
            keepalive=60,
        )

        mqtt_task = asyncio.create_task(self._mqtt_client.start())
        self._tasks.append(mqtt_task)

    # register mqtt subscriptions
    # puts subcriptions passed from the mqtt handler binding into the mqtt incoming queue
    # beacon-mqtt-client receives sub commands and subscribes the paho client
    def _register_mqtt_subscriptions(self) -> None:
        for sub in self.mqtt.subscriptions:
            self._mqtt_handlers[sub.topic] = sub.handler
            self.mqtt_incoming_queue.put_nowait(
                {"type": "subscribe", "topic": sub.topic, "qos": sub.qos}
            )

    # spawns individual background tasks for every periodic publisher in this main thread
    # creates a _run_publisher task and appends to tracked tasks
    def _start_mqtt_periodic_publisher(self) -> None:
        for pub in self.mqtt.publishers:
            if pub.every_s is None:
                continue
            self._tasks.append(asyncio.create_task(self._run_publisher(pub)))

    # called from _start_mqtt_periodic_publisher method
    # running a single background task for one periodic publisher
    async def _run_publisher(self, pub: PublisherSpec) -> None:
        assert pub.every_s is not None
        self.logger.info("starting publisher topic=%s every=%ss", pub.topic, pub.every_s)

        while not self._shutdown_event.is_set():
            try:
                payload_obj = await pub.fn()
                self.mqtt_incoming_queue.put_nowait(
                    {
                        "type": "publish",
                        "topic": pub.topic,
                        "payload": json.dumps(payload_obj),
                        "qos": pub.qos,
                        "retain": pub.retain,
                    }
                )
            except asyncio.CancelledError:
                break
            except Exception:
                self.logger.exception("publisher error topic=%s", pub.topic)

            try:
                await asyncio.sleep(pub.every_s)
            except (asyncio.CancelledError, TimeoutError):
                break

    # starts the mqtt outgoing (from mqtt client) messages processor
    # appends task to tracked tasks
    def _start_mqtt_outgoing_message_processoor(self) -> None:
        self._tasks.append(asyncio.create_task(self._process_mqtt_outgoing_messages()))

    # processes messages coming from the mqtt client
    # must be run as an asyncio background task
    async def _process_mqtt_outgoing_messages(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                # timeout of 0.1s to for non-blocking get
                # allows checking of shutdown event every 0.1s
                item = await asyncio.wait_for(self.mqtt_outgoing_queue.get(), timeout=0.1)

                if not isinstance(item, dict):
                    self.logger.warning("outgoing unexpected item: %r", item)
                    continue

                msg_type = item.get("type")
                if msg_type != "message":
                    self.logger.info("outgoing: %r", item)
                    continue

                topic = item.get("topic")
                payload = item.get("payload")
                timestamp = item.get("timestamp")

                handler = self._mqtt_handlers.get(topic)
                if not handler:
                    self.logger.debug("no handler registered for topic=%r", topic)
                    continue

                # create a message object for the handler
                # and then run the handler as a task (added to tracked tasks)
                msg = {
                    "topic": topic,
                    "payload": payload,
                    "timestamp": timestamp,
                    "json": lambda p=payload: json.loads(p) if p else None,
                }
                handler_task = asyncio.create_task(handler(msg))
                self._tasks.append(handler_task)

            except TimeoutError:
                continue

            except asyncio.CancelledError:
                break

            except Exception:
                self.logger.exception("error processing outgoing message")

    # async logging setup then call start on asynclogging object
    def _setup_logging(self):
        log_dir = new_run_log_dir(Path("logs"))
        log_file = log_dir / "beacon.log"
        self._async_logging = AsyncLogging(
            LoggingConfig(
                log_file=log_file,
            )
        )
        self._async_logging.start()

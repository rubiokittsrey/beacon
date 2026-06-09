from __future__ import annotations

import asyncio
import json
import logging
import signal
from pathlib import Path
from typing import Any

from beacon.core.config import BeaconConfig, load_config
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
        self.bindings = MQTTBindings()
        self._mqtt_handlers: dict[str, Handler] = {}

        # asyncio queues for mqtt communication:
        # commands (subscribe/publish) flow to the client via mqtt_command_queue,
        # broker messages flow back to handlers via mqtt_message_queue
        self.mqtt_command_queue: asyncio.Queue[Any] = asyncio.Queue()
        self.mqtt_message_queue: asyncio.Queue[Any] = asyncio.Queue()

        # config
        self._config: BeaconConfig | None = None

        # logging
        self.logger = logging.getLogger(__name__)
        self._async_logging: AsyncLogging | None = None

    async def _load_config(self) -> None:
        self._config = load_config(self.config_path)
        self.logger.info("config loaded from %s", self.config_path)

    # sets up handler for SIGINT and SIGTERM
    def _setup_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def _request_shutdown(sig: int) -> None:
            sig_name = signal.Signals(sig).name
            self.logger.info("received shutdown signal: %s", sig_name)

            # dont add to the tracked tasks (self._tasks)
            # will cause a maximum recursion depth error
            asyncio.create_task(self._shutdown())  # noqa: RUF006

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown, sig)
            except NotImplementedError:
                # windows fallback
                signal.signal(sig, lambda *_args, sig=sig: _request_shutdown(sig))

    async def _shutdown(self) -> None:
        # idempotent shutdown guard
        if self._shutdown_event.is_set():
            return

        self.logger.info("shutting down")
        self._shutdown_event.set()

        if self._mqtt_client:
            await self._mqtt_client.stop()

        if self._async_logging:
            self._async_logging.stop()

        await self._cancel_tasks()

    def _prune_done_tasks(self) -> None:
        self._tasks = [t for t in self._tasks if not t.done()]

    async def _cancel_tasks(self) -> None:
        for task in self._tasks:
            if not task.done():
                task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def start(self) -> None:
        await self._load_config()
        self._setup_logging()

        self.logger.info("starting %s", self.name)

        try:
            self._setup_signal_handlers()

            # start clients here
            await self._start_mqtt_client()

            # TODO: allow connection attemptt to complete first before registering subscriptions
            self._register_mqtt_subscriptions()
            self._start_mqtt_periodic_publisher()
            self._start_mqtt_message_processor()

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

    async def _start_mqtt_client(self) -> None:
        assert self._config is not None
        mqtt_cfg = self._config.mqtt

        self._mqtt_client = BeaconMQTTClient(
            id=f"{self.name}-mqtt-client",
            uname=mqtt_cfg.auth.username,
            pw=mqtt_cfg.auth.password,
            command_queue=self.mqtt_command_queue,
            message_queue=self.mqtt_message_queue,
            host=mqtt_cfg.host,
            port=mqtt_cfg.port,
            keepalive=mqtt_cfg.keepalive,
        )

        mqtt_task = asyncio.create_task(self._mqtt_client.start())
        self._tasks.append(mqtt_task)

    # register mqtt subscriptions
    # puts subcriptions passed from the mqtt handler binding into the mqtt command queue
    # beacon-mqtt-client receives sub commands and subscribes with the paho client
    def _register_mqtt_subscriptions(self) -> None:
        for sub in self.bindings.subscriptions:
            self._mqtt_handlers[sub.topic] = sub.handler
            self.mqtt_command_queue.put_nowait(
                {"type": "subscribe", "topic": sub.topic, "qos": sub.qos}
            )

    def _start_mqtt_periodic_publisher(self) -> None:
        for pub in self.bindings.publishers:
            if pub.every_s is None:
                continue
            self._tasks.append(asyncio.create_task(self._run_publisher(pub)))

    async def _run_publisher(self, pub: PublisherSpec) -> None:
        assert pub.every_s is not None
        self.logger.info("starting publisher topic=%s every=%ss", pub.topic, pub.every_s)

        while not self._shutdown_event.is_set():
            try:
                payload_obj = await pub.fn()
                self.mqtt_command_queue.put_nowait(
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

    def _start_mqtt_message_processor(self) -> None:
        self._tasks.append(asyncio.create_task(self._process_mqtt_messages()))

    async def _process_mqtt_messages(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                # timeout of 0.1s to for non-blocking get
                # allows checking of shutdown event every 0.1s
                item = await asyncio.wait_for(self.mqtt_message_queue.get(), timeout=0.1)

                if not isinstance(item, dict):
                    self.logger.warning("message queue unexpected item: %r", item)
                    continue

                msg_type = item.get("type")
                if msg_type != "message":
                    self.logger.info("ignoring non-message item: %r", item)
                    continue

                topic = item.get("topic")
                payload = item.get("payload")
                timestamp = item.get("timestamp")

                if not isinstance(topic, str):
                    self.logger.warning("message missing topic: %r", item)
                    continue

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
                self._prune_done_tasks()
                handler_task = asyncio.create_task(handler(msg))
                self._tasks.append(handler_task)

            except TimeoutError:
                continue

            except asyncio.CancelledError:
                break

            except Exception:
                self.logger.exception("error processing mqtt message")

    def _setup_logging(self) -> None:
        log_cfg = self._config.logging if self._config else None
        log_dir = new_run_log_dir(Path("logs"))
        log_file = log_dir / "beacon.log"
        self._async_logging = AsyncLogging(
            LoggingConfig(
                log_file=log_file,
                level=log_cfg.log_level if log_cfg else logging.DEBUG,
                console=log_cfg.console if log_cfg else True,
                max_bytes=log_cfg.max_bytes if log_cfg else 10 * 1024 * 1024,
                backup_count=log_cfg.backup_count if log_cfg else 5,
            )
        )
        self._async_logging.start()

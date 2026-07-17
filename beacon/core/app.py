from __future__ import annotations

import asyncio
import logging
import signal
from functools import partial
from pathlib import Path
from typing import Any

from paho.mqtt.client import topic_matches_sub
from pydantic import ValidationError

from beacon.core.config import BeaconConfig, MQTTConfig, load_config
from beacon.mqtt import BeaconMQTTClient, Message, MQTTBindings, PublisherSpec, SubscriptionSpec
from beacon.storage import StorageEngine, registry
from beacon.uplink import Uplink
from beacon.utils.logging_conf import AsyncLogging, LoggingConfig, new_run_log_dir
from beacon.utils.serialization import encode_json


class Beacon:
    """The application: owns config, logging, and the MQTT client lifecycle.

    Wires decorator bindings to the broker, routes inbound messages to their
    handlers, and runs periodic publishers until a shutdown signal arrives.
    """

    def __init__(self, name: str, config_path: Path | None = None):
        self.name = name
        self.config_path = config_path or Path("beacon.yaml")

        self._shutdown_event = asyncio.Event()

        # async runtime state: long-lived tasks (mqtt client, publishers,
        # message processor) and the ephemeral per-message handler tasks,
        # whose count is capped by the semaphore
        self._tasks: list[asyncio.Task[Any]] = []
        self._handler_tasks: set[asyncio.Task[Any]] = set()
        self._handler_semaphore = asyncio.Semaphore(MQTTConfig().max_concurrent_handlers)

        # app clients
        self._mqtt_client: BeaconMQTTClient | None = None
        self.storage: StorageEngine | None = None
        self.uplink: Uplink | None = None

        # mqtt dsl bindings + subscription routing table (topic filter -> specs;
        # a list so multiple handlers can bind to the same filter)
        self.bindings = MQTTBindings()
        self._mqtt_subscriptions: dict[str, list[SubscriptionSpec]] = {}

        # asyncio queues for mqtt communication:
        # commands (subscribe/publish) flow to the client via mqtt_command_queue,
        # broker messages flow back to handlers via mqtt_message_queue (bounded;
        # the client drops oldest when it fills); the message queue and handler
        # semaphore are rebuilt from config in start(), the defaults here cover
        # use before/without start()
        self.mqtt_command_queue: asyncio.Queue[Any] = asyncio.Queue()
        self.mqtt_message_queue: asyncio.Queue[Any] = asyncio.Queue(
            maxsize=MQTTConfig().message_queue_size
        )

        # config
        self._config: BeaconConfig | None = None

        # logging
        self.logger = logging.getLogger(__name__)
        self._async_logging: AsyncLogging | None = None

    async def _load_config(self) -> None:
        self._config = load_config(self.config_path)
        self.logger.info("config loaded from %s", self.config_path)

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

        # uplink stops before storage: it drains through the same engine
        if self.uplink:
            await self.uplink.stop()

        if self.storage:
            await self.storage.stop()

        if self._async_logging:
            self._async_logging.stop()

        await self._cancel_tasks()

    async def _cancel_tasks(self) -> None:
        tasks = [*self._tasks, *self._handler_tasks]
        for task in tasks:
            if not task.done():
                task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def start(self) -> None:
        """Load config, start clients and background tasks, and run until shutdown."""
        await self._load_config()
        self._setup_logging()

        self.logger.info("starting %s", self.name)

        try:
            self._setup_signal_handlers()

            # storage comes up before subscriptions so handlers can save from
            # the very first message; the uplink rides that same engine
            await self._start_storage()
            await self._start_uplink()
            await self._start_mqtt_client()

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

    # brings up the storage engine only when tables are declared; an app with
    # no Table subclasses imported skips storage entirely (zero behavior change)
    async def _start_storage(self) -> None:
        if not registry.tables:
            self.logger.debug("no tables registered; skipping storage engine")
            return

        assert self._config is not None
        storage_cfg = self._config.storage
        self.storage = StorageEngine(storage_cfg.path, commit_delay=storage_cfg.commit_delay)
        await self.storage.start()

    # the uplink rides the shared storage engine (started just before this), so
    # it is always constructed — even when disabled — so enqueue() raises
    # UplinkNotEnabledError instead of an AttributeError on a None uplink
    async def _start_uplink(self) -> None:
        assert self._config is not None
        assert self.storage is not None

        self.uplink = Uplink(self.storage, self._config.uplink)
        if self._config.uplink.enabled:
            await self.uplink.start()

    async def _start_mqtt_client(self) -> None:
        assert self._config is not None
        mqtt_cfg = self._config.mqtt

        # apply configured flow limits before the client gets the queue;
        # nothing has enqueued messages yet at this point
        self.mqtt_message_queue = asyncio.Queue(maxsize=mqtt_cfg.message_queue_size)
        self._handler_semaphore = asyncio.Semaphore(mqtt_cfg.max_concurrent_handlers)

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

    # queues one subscribe command per topic filter; the client subscribes with paho
    def _register_mqtt_subscriptions(self) -> None:
        for sub in self.bindings.subscriptions:
            self._mqtt_subscriptions.setdefault(sub.topic, []).append(sub)

        # one subscribe per topic filter; the max qos among its specs so a
        # duplicate low-qos binding never downgrades another on the same filter
        for topic, specs in self._mqtt_subscriptions.items():
            qos = max(spec.qos for spec in specs)
            self.mqtt_command_queue.put_nowait({"type": "subscribe", "topic": topic, "qos": qos})

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
                        "payload": self._encode_payload(pub, payload_obj),
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

    # serializes a publisher's return value
    # model declared -> validate + model_dump_json (a ValidationError propagates
    # to _run_publisher, which logs it and skips the publish);
    # bare BaseModel return -> model_dump_json; anything else -> json.dumps
    def _encode_payload(self, pub: PublisherSpec, payload_obj: Any) -> str:
        if pub.model is not None:
            if not isinstance(payload_obj, pub.model):
                payload_obj = pub.model.model_validate(payload_obj)
            return payload_obj.model_dump_json()

        return encode_json(payload_obj)

    def _start_mqtt_message_processor(self) -> None:
        self._tasks.append(asyncio.create_task(self._process_mqtt_messages()))

    async def _process_mqtt_messages(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                # short timeout so the shutdown event is checked ~10x/second
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

                # match against subscription filters so wildcard topics (+, #)
                # route; overlapping filters each get the message, as does
                # every handler bound to the same filter
                matched = [
                    spec
                    for filt, specs in self._mqtt_subscriptions.items()
                    if topic_matches_sub(filt, topic)
                    for spec in specs
                ]
                if not matched:
                    self.logger.debug("no handler registered for topic=%r", topic)
                    continue

                # build a message per matched spec (validating against its
                # model) and run each handler as a task; the semaphore caps
                # in-flight handlers, and while saturated this loop stops
                # draining the queue, which fills, and the client sheds
                # oldest at the edge — the backpressure chain under burst
                for spec in matched:
                    msg = self._build_message(spec, topic, payload, timestamp)
                    if msg is None:
                        continue
                    await self._handler_semaphore.acquire()
                    handler_task = asyncio.create_task(spec.handler(msg))
                    self._handler_tasks.add(handler_task)
                    handler_task.add_done_callback(
                        partial(self._on_handler_done, topic=topic)
                    )

            except TimeoutError:
                continue

            except asyncio.CancelledError:
                break

            except Exception:
                self.logger.exception("error processing mqtt message")

    # releases the handler's semaphore slot and surfaces its exception, if
    # any; runs for every handler task, including cancelled ones
    def _on_handler_done(self, task: asyncio.Task[Any], *, topic: str) -> None:
        self._handler_tasks.discard(task)
        self._handler_semaphore.release()
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.logger.error("handler error topic=%r", topic, exc_info=exc)

    # builds the Message handed to a subscription handler, validating the
    # payload against the subscription's model when one is declared
    # policy: payloads failing validation are logged and dropped, never raised
    def _build_message(
        self,
        spec: SubscriptionSpec,
        topic: str,
        payload: str | None,
        timestamp: float | None,
    ) -> Message[Any] | None:
        data: Any = None
        if spec.model is not None:
            try:
                data = spec.model.model_validate_json(payload or "")
            except ValidationError as e:
                self.logger.warning(
                    "dropping message topic=%r: failed %s validation: %s",
                    topic,
                    spec.model.__name__,
                    e,
                )
                return None

        return Message(topic=topic, payload=payload, timestamp=timestamp, data=data)

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

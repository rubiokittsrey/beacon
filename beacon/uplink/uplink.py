import asyncio
import logging
from typing import Any

from beacon.core.config import UplinkConfig
from beacon.core.exceptions import UplinkNotEnabledError, UplinkNotReadyError
from beacon.storage import StorageEngine
from beacon.uplink.buffer import OutboundBuffer
from beacon.uplink.http import HTTPUplinkTransport
from beacon.uplink.records import OutboundRecord
from beacon.uplink.transport import UplinkTransport
from beacon.uplink.worker import UplinkWorker
from beacon.utils.serialization import encode_json


class Uplink:
    """Store-and-forward facade owning the buffer, transport, and drain worker.

    `enqueue()` persists outbound data durably and returns; the worker
    delivers it in the background. The uplink must be `enabled` in config to
    accept enqueues and run the worker — a disabled uplink raises rather
    than silently dropping data.
    """

    def __init__(self, engine: StorageEngine | None, config: UplinkConfig) -> None:
        self._enabled = config.enabled
        self._transport = _build_transport(config)
        # a disabled uplink in an app that declares no table has no engine to
        # buffer into; it is still constructed so enqueue() answers with
        # UplinkNotEnabledError, which is the mistake actually being made
        self._buffer = (
            OutboundBuffer(engine, max_records=config.buffer.max_records)
            if engine is not None
            else None
        )
        self._worker = (
            UplinkWorker(self._buffer, self._transport, config.buffer)
            if self._buffer is not None
            else None
        )
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        """Open the transport, recover interrupted records, and run the drain worker.

        Raises:
            UplinkNotReadyError: If the uplink has no storage engine to buffer
                into; an enabled uplink always brings one up.
        """
        if self._buffer is None or self._worker is None:
            what = "no storage engine; the outbound buffer has nowhere to persist"
            raise UplinkNotReadyError(what)

        self._shutdown.clear()
        await self._transport.start()
        await self._buffer.recover()
        self._task = asyncio.create_task(self._worker.run(self._shutdown))
        self._logger.info("uplink started")

    async def stop(self) -> None:
        """Stop the worker and close the transport; inflight records stay durable."""
        self._shutdown.set()
        if self._task is not None:
            await self._task
            self._task = None
        await self._transport.stop()

    async def enqueue(self, stream: str, data: Any) -> OutboundRecord:
        """Durably buffer one message for delivery; returns once it is persisted.

        Raises:
            UplinkNotEnabledError: If the uplink is disabled in config.
        """
        if not self._enabled or self._buffer is None:
            raise UplinkNotEnabledError(stream)
        return await self._buffer.enqueue(stream, encode_json(data))


def _build_transport(config: UplinkConfig) -> UplinkTransport:
    # http is the only transport today; config.transport (a Literal) is the
    # seam for future ones (e.g. an mqtt uplink)
    return HTTPUplinkTransport(config.http)

import asyncio
import logging
from typing import Any

from beacon.core.config import UplinkConfig
from beacon.core.exceptions import UplinkNotEnabledError
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

    def __init__(self, engine: StorageEngine, config: UplinkConfig) -> None:
        self._enabled = config.enabled
        self._buffer = OutboundBuffer(engine, max_records=config.buffer.max_records)
        self._transport = _build_transport(config)
        self._worker = UplinkWorker(self._buffer, self._transport, config.buffer)
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        """Open the transport, recover interrupted records, and run the drain worker."""
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
        if not self._enabled:
            raise UplinkNotEnabledError(stream)
        return await self._buffer.enqueue(stream, encode_json(data))


def _build_transport(config: UplinkConfig) -> UplinkTransport:
    # http is the only transport today; config.transport (a Literal) is the
    # seam for future ones (e.g. an mqtt uplink)
    return HTTPUplinkTransport(config.http)

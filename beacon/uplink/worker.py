import asyncio
import contextlib
import logging
from collections.abc import Sequence

from beacon.core.config import UplinkBufferConfig
from beacon.uplink.buffer import OutboundBuffer
from beacon.uplink.records import OutboundRecord, SendResult
from beacon.uplink.transport import UplinkTransport


class UplinkWorker:
    """Drains the outbound buffer through the transport, one batch at a time.

    Each cycle claims up to `batch_size` pending records, sends them, and
    resolves the batch by outcome: acked on success, nacked for retry on a
    retryable failure, buried as poison otherwise. Retryable failures back
    the whole worker off exponentially between `min_seconds` and
    `max_seconds` — with a single claimer, a failure to reach the endpoint
    means retrying sooner only burns it. When idle the worker polls every
    `flush_interval`. Records left inflight at shutdown stay durable and are
    recovered to pending on the next start.

    `max_attempts` is spent per record and only on failures the server
    answered for, so an offline stretch of any length costs a record
    nothing: burial is reserved for a batch the server keeps rejecting,
    which is the only case that can block the queue behind it.
    """

    def __init__(
        self,
        buffer: OutboundBuffer,
        transport: UplinkTransport,
        config: UplinkBufferConfig,
    ) -> None:
        self._buffer = buffer
        self._transport = transport
        self._batch_size = config.batch_size
        self._flush_interval = config.flush_interval
        self._retry = config.retry
        self._failures = 0
        self._logger = logging.getLogger(__name__)

    async def run(self, shutdown: asyncio.Event) -> None:
        """Loop claim -> send -> resolve until `shutdown` is set.

        Unexpected errors (a storage hiccup, a transport that raises instead
        of returning a `SendResult`) back the worker off and let it retry
        rather than killing the drain task — a silently dead worker would let
        the durable buffer fill and start dropping data.
        """
        while not shutdown.is_set():
            batch: Sequence[OutboundRecord] = ()
            try:
                batch = await self._buffer.claim(self._batch_size)
                if not batch:
                    await self._wait(shutdown, self._flush_interval)
                    continue

                result = await self._transport.send(batch)
                backoff = await self._resolve(batch, result)
                if backoff:
                    await self._wait(shutdown, backoff)
            except Exception:
                self._logger.exception("uplink worker cycle failed")
                self._failures += 1
                await self._release(batch)
                await self._wait(shutdown, self._backoff())

    async def _resolve(self, batch: Sequence[OutboundRecord], result: SendResult) -> float:
        """Apply the batch outcome; return how long to back off before the next claim."""
        detail = result.detail or "send failed"
        if result.ok:
            await self._buffer.ack(batch)
            self._failures = 0
            return 0.0

        if not result.retryable:
            # poison: rejected outright, not a transport failure — bury and keep draining
            await self._buffer.bury(batch, detail)
            self._logger.warning("uplink buried %d poison record(s): %s", len(batch), detail)
            self._failures = 0
            return 0.0

        # the endpoint never answered, so the failure is about the link, not
        # about these records — retry them without spending their attempts.
        # charging an outage would bury good data once the backoff ladder ran
        # long enough, which is precisely what the buffer exists to prevent
        if not result.reached_server:
            await self._buffer.nack(batch, detail, count_attempt=False)
            self._failures += 1
            return self._backoff()

        exhausted, remaining = self._partition_exhausted(batch)
        if exhausted:
            reason = f"buried after {self._retry.max_attempts} attempts: {detail}"
            await self._buffer.bury(exhausted, reason)
            self._logger.warning(
                "uplink buried %d exhausted record(s): %s", len(exhausted), detail
            )
        if remaining:
            await self._buffer.nack(remaining, detail)
            self._failures += 1
            return self._backoff()
        self._failures = 0
        return 0.0

    def _partition_exhausted(
        self, batch: Sequence[OutboundRecord]
    ) -> tuple[list[OutboundRecord], list[OutboundRecord]]:
        """Split a failed batch into records out of attempts and records still owed a retry.

        Resolving per record rather than per batch keeps a record claimed
        alongside an older one from being buried on attempts it never spent.
        """
        exhausted: list[OutboundRecord] = []
        remaining: list[OutboundRecord] = []
        for record in batch:
            target = exhausted if self._is_exhausted(record) else remaining
            target.append(record)
        return exhausted, remaining

    def _is_exhausted(self, record: OutboundRecord) -> bool:
        # attempts counts prior sends the server answered for; this send failed
        # too, so a nack would push the record to attempts+1 — bury at the ceiling
        if self._retry.max_attempts <= 0:
            return False
        return record.attempts + 1 >= self._retry.max_attempts

    # returns a batch left in hand by a failed cycle to pending so it is not
    # stranded inflight until the next process restart; best-effort
    async def _release(self, batch: Sequence[OutboundRecord]) -> None:
        if not batch:
            return
        with contextlib.suppress(Exception):
            await self._buffer.nack(batch, "worker cycle error")

    def _backoff(self) -> float:
        # exponential in the consecutive-failure count, capped at max_seconds
        delay = self._retry.min_seconds * 2 ** (self._failures - 1)
        return min(delay, self._retry.max_seconds)

    # sleeps for `seconds`, but wakes immediately once shutdown is requested
    async def _wait(self, shutdown: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(shutdown.wait(), timeout=seconds)

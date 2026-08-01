import logging
import time
from collections.abc import Sequence

from beacon.storage import StorageEngine
from beacon.uplink.records import OutboundRecord, RecordState

# rate limit for the buffer-full warning so sustained overflow logs a
# heartbeat with a running total instead of one line per drop
_DROP_LOG_INTERVAL_S = 5.0

_TABLE = OutboundRecord.__tablename__


class OutboundBuffer:
    """Durable FIFO of outbound records, stored in the app's storage engine.

    `enqueue()` returns only after its commit lands, so a returned record
    survives a crash. `claim()` hands the oldest pending rows to the worker
    as `inflight`; `ack()` deletes them, `nack()` returns them to `pending`
    for retry, and `bury()` keeps poison rows as `dead`. State transitions
    assume a single claiming worker; `recover()` returns rows a previous
    run left `inflight` to `pending`.
    """

    def __init__(self, engine: StorageEngine, *, max_records: int) -> None:
        self._engine = engine
        self.max_records = max_records
        self._logger = logging.getLogger(__name__)
        self._dropped_total = 0
        self._last_drop_log = 0.0

    @property
    def dropped_records(self) -> int:
        """How many pending records were dropped because the buffer was full."""
        return self._dropped_total

    async def recover(self) -> int:
        """Return `inflight` rows from a previous run to `pending`; count returned."""
        cursor = await self._engine.execute(
            f'UPDATE "{_TABLE}" SET "state" = ? WHERE "state" = ?',  # noqa: S608
            [RecordState.PENDING.value, RecordState.INFLIGHT.value],
        )
        if cursor.rowcount:
            self._logger.info(
                "uplink buffer: recovered %d inflight records to pending", cursor.rowcount
            )
        return cursor.rowcount

    async def enqueue(self, stream: str, payload: str) -> OutboundRecord:
        """Persist one outbound record and return it once durable.

        Past `max_records` undelivered rows, the oldest pending rows are
        dropped to make room — the newest data wins.
        """
        record = OutboundRecord(stream=stream, payload=payload)
        # the insert skips its own commit so it shares one group commit
        # with the prune below; enqueue returns only after that lands
        await self._engine.save(record, commit=False)
        await self._prune_overflow()
        return record

    async def claim(self, limit: int) -> list[OutboundRecord]:
        """Mark up to `limit` of the oldest pending rows `inflight` and return them, FIFO."""
        # read-then-flip is safe under the single-claimer assumption: no
        # one else can claim rows read as pending before the update lands
        records = await self._engine.fetch(
            OutboundRecord,
            {"state": RecordState.PENDING},
            order_by="seq",
            limit=limit,
        )
        if not records:
            return []

        await self._engine.execute(
            f'UPDATE "{_TABLE}" SET "state" = ? WHERE "seq" IN ({_placeholders(records)})',  # noqa: S608
            [RecordState.INFLIGHT.value, *_seqs(records)],
        )
        for record in records:
            record.state = RecordState.INFLIGHT
        return records

    async def ack(self, records: Sequence[OutboundRecord]) -> None:
        """Delete delivered rows; the batch is gone once this returns."""
        if not records:
            return
        await self._engine.execute(
            f'DELETE FROM "{_TABLE}" WHERE "seq" IN ({_placeholders(records)})',  # noqa: S608
            _seqs(records),
        )

    async def nack(
        self,
        records: Sequence[OutboundRecord],
        error: str,
        *,
        count_attempt: bool = True,
    ) -> None:
        """Return failed rows to `pending` for retry, recording the error.

        `count_attempt=False` retries without charging the rows an attempt —
        used when the send never reached the server, so `attempts` counts
        only the failures the server actually answered for.
        """
        await self._resolve(records, RecordState.PENDING, error, count_attempt=count_attempt)

    async def bury(self, records: Sequence[OutboundRecord], error: str) -> None:
        """Mark rejected rows `dead`; they are kept for inspection, never resent."""
        await self._resolve(records, RecordState.DEAD, error)
        await self._trim_dead()

    async def pending_count(self) -> int:
        """How many rows await delivery."""
        return await self._engine.count(OutboundRecord, {"state": RecordState.PENDING})

    async def _resolve(
        self,
        records: Sequence[OutboundRecord],
        state: RecordState,
        error: str,
        *,
        count_attempt: bool = True,
    ) -> None:
        if not records:
            return
        attempts = '"attempts" + 1' if count_attempt else '"attempts"'
        await self._engine.execute(
            f'UPDATE "{_TABLE}" SET "state" = ?, "attempts" = {attempts}, '  # noqa: S608
            f'"last_error" = ? WHERE "seq" IN ({_placeholders(records)})',
            [state.value, error, *_seqs(records)],
        )
        for record in records:
            record.state = state
            if count_attempt:
                record.attempts += 1
            record.last_error = error

    # deletes the oldest pending rows past max_records (newest data wins);
    # rows the worker holds inflight are never dropped, and buried `dead`
    # rows are excluded from the count so poison can't evict live data
    async def _prune_overflow(self) -> None:
        cursor = await self._engine.execute(
            f'DELETE FROM "{_TABLE}" WHERE "seq" IN ('  # noqa: S608
            f'SELECT "seq" FROM "{_TABLE}" WHERE "state" = ? ORDER BY "seq" '
            f'LIMIT MAX((SELECT COUNT(*) FROM "{_TABLE}" WHERE "state" != ?) - ?, 0))',
            [RecordState.PENDING.value, RecordState.DEAD.value, self.max_records],
        )
        dropped = cursor.rowcount
        if dropped <= 0:
            return

        self._dropped_total += dropped
        now = time.monotonic()
        if now - self._last_drop_log >= _DROP_LOG_INTERVAL_S:
            self._last_drop_log = now
            self._logger.warning(
                "uplink buffer full; dropping oldest (%d dropped since start)",
                self._dropped_total,
            )

    # keeps buried poison for inspection but bounds it at max_records so a run
    # that meets recurring poison can't grow the db without limit
    async def _trim_dead(self) -> None:
        await self._engine.execute(
            f'DELETE FROM "{_TABLE}" WHERE "seq" IN ('  # noqa: S608
            f'SELECT "seq" FROM "{_TABLE}" WHERE "state" = ? ORDER BY "seq" '
            f'LIMIT MAX((SELECT COUNT(*) FROM "{_TABLE}" WHERE "state" = ?) - ?, 0))',
            [RecordState.DEAD.value, RecordState.DEAD.value, self.max_records],
        )


def _seqs(records: Sequence[OutboundRecord]) -> list[int | None]:
    return [record.seq for record in records]


def _placeholders(records: Sequence[OutboundRecord]) -> str:
    return ", ".join("?" for _ in records)

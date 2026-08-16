from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel

from beacon.storage import Table, field


class RecordState(StrEnum):
    """Delivery state of a buffered record.

    `pending` awaits send, `inflight` is claimed by the worker, and
    `dead` is buried poison kept for inspection.
    """

    PENDING = "pending"
    INFLIGHT = "inflight"
    DEAD = "dead"


def _new_record_id() -> str:
    return uuid4().hex


def _utcnow() -> datetime:
    return datetime.now(UTC)


class OutboundRecord(Table):
    """One durable outbound message awaiting uplink delivery.

    `seq` gives strict FIFO order; `record_id` is the idempotency key the
    ingest server dedupes on, so a batch resent after a crash or an
    ambiguous failure is applied at most once. `payload` holds the
    JSON-encoded body exactly as it will be sent.
    """

    __tablename__ = "outbound"
    # the uplink's own table, not the app's: it is created only when the
    # uplink is enabled, so importing beacon does not put it in every database
    __internal__ = True

    seq: int | None = field(pk=True, auto=True)
    record_id: str = field(unique=True, default_factory=_new_record_id)
    stream: str
    payload: str
    created_at: datetime = field(default_factory=_utcnow)
    attempts: int = 0
    state: RecordState = field(index=True, default=RecordState.PENDING)
    last_error: str | None = None


class SendResult(BaseModel, frozen=True):
    """Outcome of one transport send.

    `ok=True` acks the whole batch; `ok=False, retryable=True` nacks it
    for backoff and retry; `ok=False, retryable=False` buries it as poison.

    `reached_server` reports whether the endpoint answered at all. A
    retryable failure that never reached it is an outage — it says nothing
    about the records, so it costs them no attempts. Transports set it
    `False` only for that case; everything else means a verdict came back.
    """

    ok: bool
    retryable: bool = False
    reached_server: bool = True
    detail: str | None = None

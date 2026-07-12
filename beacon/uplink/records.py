from __future__ import annotations

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
    """

    ok: bool
    retryable: bool = False
    detail: str | None = None

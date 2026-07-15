from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

    from beacon.uplink.records import OutboundRecord, SendResult


class UplinkTransport(Protocol):
    """The wire seam the uplink worker drains batches through.

    Implementations own their connections: `start()`/`stop()` bracket the
    lifecycle, and `send()` reports the whole batch's outcome as a
    `SendResult` rather than raising for delivery failures.
    """

    async def start(self) -> None:
        """Open the transport's connections."""

    async def stop(self) -> None:
        """Close the transport (idempotent)."""

    async def send(self, records: Sequence[OutboundRecord]) -> SendResult:
        """Attempt delivery of one batch and report its outcome."""
        ...

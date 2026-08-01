from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING

import aiohttp

from beacon.core.exceptions import UplinkNotReadyError
from beacon.uplink.records import SendResult

if TYPE_CHECKING:
    from collections.abc import Sequence

    from beacon.core.config import UplinkHTTPConfig
    from beacon.uplink.records import OutboundRecord

# response bodies are truncated to this many chars in SendResult.detail
_DETAIL_LIMIT = 200


class HTTPUplinkTransport:
    """POSTs record batches to an ingest endpoint over one shared session.

    The batch shares one outcome: 2xx acks it, 4xx buries it as poison
    (except 429, which retries), and 5xx, timeouts, and connection errors
    retry. `payload` strings pass through opaque; the server dedupes
    resent batches on `record_id`.

    Raises:
        UplinkNotReadyError: If `send()` runs before `start()`.
    """

    def __init__(self, config: UplinkHTTPConfig) -> None:
        self._config = config
        self._url = config.base_url.rstrip("/") + "/" + config.endpoint.lstrip("/")
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        """Open the shared HTTP session."""
        if self._session is not None:
            return
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._config.timeout),
            headers=self._config.headers,
        )

    async def stop(self) -> None:
        """Close the session (idempotent)."""
        if self._session is None:
            return
        await self._session.close()
        self._session = None

    async def send(self, records: Sequence[OutboundRecord]) -> SendResult:
        """POST one batch; the response status maps to the batch outcome."""
        if self._session is None:
            what = f"transport not started; cannot send to {self._url}"
            raise UplinkNotReadyError(what)

        body = {
            "records": [
                {
                    "record_id": record.record_id,
                    "stream": record.stream,
                    "payload": record.payload,
                    "created_at": record.created_at.isoformat(),
                }
                for record in records
            ],
        }

        try:
            async with self._session.post(self._url, json=body) as resp:
                if HTTPStatus.OK <= resp.status < HTTPStatus.MULTIPLE_CHOICES:
                    return SendResult(ok=True)
                text = (await resp.text())[:_DETAIL_LIMIT]
                # 429 is the one 4xx that means try again later, not poison
                retryable = (
                    resp.status >= HTTPStatus.INTERNAL_SERVER_ERROR
                    or resp.status == HTTPStatus.TOO_MANY_REQUESTS
                )
                return SendResult(
                    ok=False, retryable=retryable, detail=f"http {resp.status}: {text}"
                )
        except (aiohttp.ClientError, TimeoutError) as exc:
            # no response at all: the link is down, which is no verdict on these
            # records — reached_server=False keeps it off their attempt budget
            return SendResult(
                ok=False,
                retryable=True,
                reached_server=False,
                detail=f"{type(exc).__name__}: {exc}",
            )

import asyncio
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from beacon.core.config import UplinkHTTPConfig
from beacon.core.exceptions import UplinkNotReadyError
from beacon.uplink.http import HTTPUplinkTransport
from beacon.uplink.records import OutboundRecord

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


@asynccontextmanager
async def _server(handler: Handler, path: str = "/ingest") -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_post(path, handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    try:
        yield f"http://{host}:{port}"
    finally:
        await runner.cleanup()


def _closed_base_url() -> str:
    # grab a port, close it, and hand back a URL that will refuse connections
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return f"http://127.0.0.1:{port}"


def _record() -> OutboundRecord:
    return OutboundRecord(stream="telemetry", payload='{"t": 1}')


@asynccontextmanager
async def _transport(base_url: str, **kwargs: object) -> AsyncIterator[HTTPUplinkTransport]:
    transport = HTTPUplinkTransport(UplinkHTTPConfig(base_url=base_url, endpoint="/ingest", **kwargs))
    await transport.start()
    try:
        yield transport
    finally:
        await transport.stop()


# ------------------------------------------------------------ status -> outcome


async def test_2xx_acks_batch() -> None:
    async def handler(_: web.Request) -> web.StreamResponse:
        return web.json_response({"accepted": True})

    async with _server(handler) as base, _transport(base) as transport:
        result = await transport.send([_record()])
        assert result.ok is True
        assert result.retryable is False


async def test_4xx_is_poison_not_retryable() -> None:
    async def handler(_: web.Request) -> web.StreamResponse:
        return web.Response(status=400, text="bad request")

    async with _server(handler) as base, _transport(base) as transport:
        result = await transport.send([_record()])
        assert result.ok is False
        assert result.retryable is False
        assert "400" in (result.detail or "")


async def test_429_is_retryable() -> None:
    async def handler(_: web.Request) -> web.StreamResponse:
        return web.Response(status=429, text="slow down")

    async with _server(handler) as base, _transport(base) as transport:
        result = await transport.send([_record()])
        assert result.ok is False
        assert result.retryable is True


async def test_5xx_is_retryable() -> None:
    async def handler(_: web.Request) -> web.StreamResponse:
        return web.Response(status=503, text="unavailable")

    async with _server(handler) as base, _transport(base) as transport:
        result = await transport.send([_record()])
        assert result.ok is False
        assert result.retryable is True
        # the server answered, so this failure counts against the records
        assert result.reached_server is True


async def test_connection_error_is_retryable() -> None:
    async with _transport(_closed_base_url()) as transport:
        result = await transport.send([_record()])
        assert result.ok is False
        assert result.retryable is True
        # nothing answered: an outage, which costs the records no attempts
        assert result.reached_server is False


async def test_timeout_is_retryable() -> None:
    async def slow(_: web.Request) -> web.StreamResponse:
        # never responds within the transport's timeout
        await asyncio.sleep(1.0)
        return web.Response()

    async with _server(slow) as base, _transport(base, timeout=0.05) as transport:
        result = await transport.send([_record()])
        assert result.ok is False
        assert result.retryable is True
        assert result.reached_server is False


# ------------------------------------------------------------ request shape


async def test_send_posts_record_envelope() -> None:
    captured: dict[str, object] = {}

    async def handler(request: web.Request) -> web.StreamResponse:
        captured["body"] = await request.json()
        return web.json_response({})

    record = _record()
    async with _server(handler) as base, _transport(base) as transport:
        await transport.send([record])

    body = captured["body"]
    assert isinstance(body, dict)
    [sent] = body["records"]
    assert sent["record_id"] == record.record_id
    assert sent["stream"] == "telemetry"
    assert sent["payload"] == '{"t": 1}'
    assert sent["created_at"] == record.created_at.isoformat()


async def test_configured_headers_are_sent() -> None:
    captured: dict[str, str] = {}

    async def handler(request: web.Request) -> web.StreamResponse:
        captured["auth"] = request.headers.get("Authorization", "")
        return web.json_response({})

    async with _server(handler) as base:
        transport = HTTPUplinkTransport(
            UplinkHTTPConfig(base_url=base, endpoint="/ingest", headers={"Authorization": "Bearer k"})
        )
        await transport.start()
        try:
            await transport.send([_record()])
        finally:
            await transport.stop()

    assert captured["auth"] == "Bearer k"


# ------------------------------------------------------------ lifecycle


async def test_send_before_start_raises() -> None:
    transport = HTTPUplinkTransport(UplinkHTTPConfig())
    with pytest.raises(UplinkNotReadyError):
        await transport.send([_record()])


async def test_start_and_stop_are_idempotent() -> None:
    async def handler(_: web.Request) -> web.StreamResponse:
        return web.json_response({})

    async with _server(handler) as base:
        transport = HTTPUplinkTransport(UplinkHTTPConfig(base_url=base, endpoint="/ingest"))
        await transport.start()
        await transport.start()  # second start is a no-op
        await transport.send([_record()])
        await transport.stop()
        await transport.stop()  # second stop is a no-op

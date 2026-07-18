from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import BaseModel

from beacon.core.config import UplinkConfig
from beacon.core.exceptions import UplinkNotEnabledError
from beacon.storage import StorageEngine, registry
from beacon.uplink import Uplink
from beacon.uplink.records import OutboundRecord


async def _started_engine(path: str | Path) -> StorageEngine:
    if registry.get(OutboundRecord.__tablename__) is None:
        registry.register(OutboundRecord)
    engine = StorageEngine(path)
    await engine.start()
    return engine


async def test_enqueue_while_disabled_raises(tmp_path: Path) -> None:
    engine = await _started_engine(tmp_path / "u.db")
    try:
        uplink = Uplink(engine, UplinkConfig(enabled=False))
        with pytest.raises(UplinkNotEnabledError):
            await uplink.enqueue("telemetry", {"t": 1})
        # nothing was persisted
        assert await engine.count(OutboundRecord, {}) == 0
    finally:
        await engine.stop()


async def test_enqueue_persists_serialized_payload(tmp_path: Path) -> None:
    engine = await _started_engine(tmp_path / "u.db")
    try:
        uplink = Uplink(engine, UplinkConfig(enabled=True))
        record = await uplink.enqueue("telemetry", {"t": 1})

        assert record.payload == '{"t": 1}'
        [stored] = await engine.fetch(OutboundRecord, {})
        assert stored.record_id == record.record_id
        assert stored.stream == "telemetry"
    finally:
        await engine.stop()


async def test_enqueue_serializes_pydantic_models(tmp_path: Path) -> None:
    class Reading(BaseModel):
        sensor: str
        value: int

    engine = await _started_engine(tmp_path / "u.db")
    try:
        uplink = Uplink(engine, UplinkConfig(enabled=True))
        record = await uplink.enqueue("telemetry", Reading(sensor="a", value=3))
        assert record.payload == '{"sensor":"a","value":3}'
    finally:
        await engine.stop()


async def test_restart_clears_shutdown_and_runs_worker(tmp_path: Path) -> None:
    engine = await _started_engine(tmp_path / "u.db")
    uplink = Uplink(engine, UplinkConfig(enabled=True))
    try:
        await uplink.start()
        await uplink.stop()  # sets the shutdown event

        # a second start must clear the event so the new worker keeps running
        await uplink.start()
        assert uplink._task is not None
        assert not uplink._task.done()
    finally:
        await uplink.stop()
        await engine.stop()

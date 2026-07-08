from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import pytest
from pydantic import BaseModel

from beacon.storage import StorageEngine, Table, field, registry


class Color(StrEnum):
    RED = "red"
    GREEN = "green"


class Geo(BaseModel):
    lat: float
    lon: float


async def _started_engine(path: str | Path) -> StorageEngine:
    engine = StorageEngine(path)
    await engine.start()
    return engine


# ------------------------------------------------------------- constraints


async def test_unique_violation_raises(tmp_path: Path) -> None:
    class Device(Table):
        id: int | None = field(pk=True, auto=True)
        serial: str = field(unique=True)

    engine = await _started_engine(tmp_path / "test.db")
    try:
        await Device(serial="abc").save()
        with pytest.raises(sqlite3.IntegrityError):
            await Device(serial="abc").save()  # second row, same unique value
    finally:
        await engine.stop()


# -------------------------------------------------------------- concurrency


async def test_concurrent_saves_get_distinct_pks(tmp_path: Path) -> None:
    class Sample(Table):
        id: int | None = field(pk=True, auto=True)
        n: int

    engine = await _started_engine(tmp_path / "test.db")
    try:
        rows = [Sample(n=i) for i in range(20)]
        # the engine's single aiosqlite connection serializes these on its
        # worker thread, so every insert gets its own rowid back
        await asyncio.gather(*(row.save() for row in rows))

        pks = [row.id for row in rows]
        assert None not in pks
        assert sorted(pks) == list(range(1, 21))  # distinct, gap-free rowids
        assert await Sample.count() == 20
    finally:
        await engine.stop()


# -------------------------------------------------------------- persistence


async def test_persistence_across_reopen_through_active_record(tmp_path: Path) -> None:
    db = tmp_path / "test.db"

    class Reading(Table):
        id: int | None = field(pk=True, auto=True)
        sensor_id: str
        celsius: float

    engine = await _started_engine(db)
    await Reading(sensor_id="s1", celsius=9.0).save()
    await engine.stop()

    # a fresh engine over the same file rebinds the (still-registered) model
    engine = await _started_engine(db)
    try:
        rows = await Reading.all()
        assert len(rows) == 1
        assert rows[0].celsius == 9.0
    finally:
        await engine.stop()


async def test_additive_migration_visible_through_active_record(tmp_path: Path) -> None:
    db = tmp_path / "test.db"

    class Device(Table):
        id: int | None = field(pk=True, auto=True)
        name: str

    engine = await _started_engine(db)
    await Device(name="pump-1").save()
    await engine.stop()

    # the model evolves: a new optional column appears
    registry.clear()

    class DeviceV2(Table):
        __tablename__ = "device"

        id: int | None = field(pk=True, auto=True)
        name: str
        firmware: str | None = None

    engine = await _started_engine(db)
    try:
        old = await DeviceV2.get(name="pump-1")
        assert old is not None
        assert old.firmware is None  # migrated column reads as NULL on old rows

        await DeviceV2(name="pump-2", firmware="1.2.0").save()
        migrated = await DeviceV2.get(name="pump-2")
        assert migrated is not None
        assert migrated.firmware == "1.2.0"
    finally:
        await engine.stop()


# ------------------------------------------------------------- round-trips


async def test_typed_roundtrip_through_save_and_get(tmp_path: Path) -> None:
    class Event(Table):
        id: int | None = field(pk=True, auto=True)
        when: datetime
        where: Geo
        color: Color
        ok: bool

    engine = await _started_engine(tmp_path / "test.db")
    try:
        event = Event(
            when=datetime(2026, 7, 8, 12, 0, tzinfo=UTC),
            where=Geo(lat=7.19, lon=125.45),
            color=Color.GREEN,
            ok=False,
        )
        await event.save()

        got = await Event.get(id=event.id)
        assert got is not None
        assert got.when == event.when
        assert got.where == Geo(lat=7.19, lon=125.45)
        assert got.color is Color.GREEN
        assert got.ok is False
    finally:
        await engine.stop()


# ----------------------------------------------------------------- queries


async def _seed_readings(reading: type[Table]) -> None:
    await reading(sensor_id="s1", celsius=10.0).save()
    await reading(sensor_id="s1", celsius=40.0).save()
    await reading(sensor_id="s2", celsius=40.0).save()


def _reading_table() -> type[Table]:
    class Reading(Table):
        id: int | None = field(pk=True, auto=True)
        sensor_id: str
        celsius: float

    return Reading


async def test_multiple_lookups_are_anded(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed_readings(reading)
        rows = await reading.filter(sensor_id="s1", celsius__gt=20)
        assert [row.celsius for row in rows] == [40.0]  # only the hot s1 reading
    finally:
        await engine.stop()


async def test_order_by_multiple_fields(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed_readings(reading)
        rows = await reading.filter(order_by=["sensor_id", "-celsius"])
        assert [(row.sensor_id, row.celsius) for row in rows] == [
            ("s1", 40.0),
            ("s1", 10.0),
            ("s2", 40.0),
        ]
    finally:
        await engine.stop()


async def test_upsert_updates_all_nonpk_columns(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        row = reading(sensor_id="s1", celsius=10.0)
        await row.save()

        row.sensor_id = "s1-renamed"
        row.celsius = 99.0
        await row.save()  # same pk -> updates every non-pk column

        assert await reading.count() == 1
        got = await reading.get(id=row.id)
        assert got is not None
        assert got.sensor_id == "s1-renamed"
        assert got.celsius == 99.0
    finally:
        await engine.stop()


async def test_delete_where_no_match_returns_zero(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed_readings(reading)
        assert await reading.delete_where(sensor_id="nope") == 0
        assert await reading.count() == 3
    finally:
        await engine.stop()

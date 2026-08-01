from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import pytest
from pydantic import BaseModel

from beacon.core.exceptions import StorageNotReadyError, UnknownLookupError
from beacon.storage import StorageEngine, Table, field


class Color(StrEnum):
    RED = "red"
    BLUE = "blue"


class Location(BaseModel):
    lat: float
    lon: float


def _reading_table() -> type[Table]:
    class Reading(Table):
        id: int | None = field(pk=True, auto=True)
        sensor_id: str = field(index=True)
        celsius: float
        ok: bool = True
        ts: datetime | None = None
        location: Location | None = None
        color: Color = Color.RED

    return Reading


async def _started_engine(path: str | Path) -> StorageEngine:
    engine = StorageEngine(path)
    await engine.start()
    return engine


# --------------------------------------------------------------- insert/save


async def test_save_assigns_autoincrement_pk(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        row = reading(sensor_id="s1", celsius=21.5)
        assert row.id is None
        await row.save()
        assert row.id == 1  # written back from lastrowid

        second = reading(sensor_id="s2", celsius=9.0)
        await second.save()
        assert second.id == 2
    finally:
        await engine.stop()


async def test_save_upserts_when_pk_set(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        row = reading(sensor_id="s1", celsius=21.5)
        await row.save()

        row.celsius = 30.0
        await row.save()  # same pk -> UPDATE, not a duplicate

        assert await reading.count() == 1
        fetched = await reading.get(id=row.id)
        assert fetched is not None
        assert fetched.celsius == 30.0
    finally:
        await engine.stop()


async def test_save_with_explicit_pk_inserts(tmp_path: Path) -> None:
    class Device(Table):
        code: str = field(pk=True)
        name: str

    engine = await _started_engine(tmp_path / "test.db")
    try:
        await Device(code="abc", name="pump").save()
        fetched = await Device.get(code="abc")
        assert fetched is not None
        assert fetched.name == "pump"
    finally:
        await engine.stop()


# ------------------------------------------------------------------ queries


async def _seed(reading: type[Table]) -> None:
    await reading(sensor_id="s1", celsius=10.0, ts=datetime(2026, 7, 1, tzinfo=UTC)).save()
    await reading(sensor_id="s1", celsius=35.0, ts=datetime(2026, 7, 2, tzinfo=UTC)).save()
    await reading(sensor_id="s2", celsius=40.0, ts=datetime(2026, 7, 3, tzinfo=UTC)).save()


async def test_get_returns_none_when_absent(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        assert await reading.get(sensor_id="nope") is None
    finally:
        await engine.stop()


async def test_all_returns_every_row(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed(reading)
        assert len(await reading.all()) == 3
    finally:
        await engine.stop()


async def test_filter_equality(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed(reading)
        rows = await reading.filter(sensor_id="s1")
        assert {row.celsius for row in rows} == {10.0, 35.0}
    finally:
        await engine.stop()


async def test_filter_comparison_lookups(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed(reading)
        hot = await reading.filter(celsius__gt=30)
        assert {row.celsius for row in hot} == {35.0, 40.0}
        assert len(await reading.filter(celsius__gte=35.0)) == 2
        assert len(await reading.filter(celsius__lt=35.0)) == 1
        assert len(await reading.filter(celsius__ne=40.0)) == 2
    finally:
        await engine.stop()


async def test_filter_in_and_like(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed(reading)
        assert len(await reading.filter(sensor_id__in=["s1", "s2"])) == 3
        assert len(await reading.filter(sensor_id__in=["s2"])) == 1
        assert len(await reading.filter(sensor_id__in=[])) == 0  # empty -> no rows
        assert len(await reading.filter(sensor_id__like="s%")) == 3
    finally:
        await engine.stop()


async def test_filter_datetime_lookup_encodes_value(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed(reading)
        recent = await reading.filter(ts__gt=datetime(2026, 7, 1, 12, tzinfo=UTC))
        assert len(recent) == 2
    finally:
        await engine.stop()


async def test_filter_none_lookup_uses_is_null(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await reading(sensor_id="s1", celsius=1.0).save()  # ts defaults to None
        await reading(sensor_id="s2", celsius=2.0, ts=datetime(2026, 7, 1, tzinfo=UTC)).save()
        assert len(await reading.filter(ts=None)) == 1
        assert len(await reading.filter(ts__ne=None)) == 1
    finally:
        await engine.stop()


async def test_filter_order_by_and_limit(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed(reading)
        top = await reading.filter(order_by="-celsius", limit=2)
        assert [row.celsius for row in top] == [40.0, 35.0]

        ascending = await reading.filter(order_by="celsius")
        assert [row.celsius for row in ascending] == [10.0, 35.0, 40.0]
    finally:
        await engine.stop()


async def test_count_with_and_without_lookups(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed(reading)
        assert await reading.count() == 3
        assert await reading.count(sensor_id="s1") == 2
        assert await reading.count(celsius__gt=100) == 0
    finally:
        await engine.stop()


# ------------------------------------------------------------------ deletes


async def test_delete_instance(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        row = reading(sensor_id="s1", celsius=1.0)
        await row.save()
        await row.delete()
        assert await reading.count() == 0
    finally:
        await engine.stop()


async def test_delete_where_returns_count(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await _seed(reading)
        removed = await reading.delete_where(sensor_id="s1")
        assert removed == 2
        assert await reading.count() == 1
    finally:
        await engine.stop()


# ------------------------------------------------------------- error paths


async def test_unknown_lookup_raises(tmp_path: Path) -> None:
    reading = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        with pytest.raises(UnknownLookupError, match="celsius__between"):
            await reading.filter(celsius__between=1)
        with pytest.raises(UnknownLookupError, match="missing"):
            await reading.filter(missing=1)
        with pytest.raises(UnknownLookupError, match="ranking"):
            await reading.filter(order_by="ranking")
    finally:
        await engine.stop()


async def test_query_without_engine_raises() -> None:
    reading = _reading_table()
    with pytest.raises(StorageNotReadyError, match="not bound"):
        await reading.all()
    with pytest.raises(StorageNotReadyError, match="not bound"):
        await reading(sensor_id="s1", celsius=1.0).save()

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path

import pytest
from pydantic import BaseModel

from beacon.core.exceptions import StorageNotReadyError
from beacon.storage import StorageEngine, Table, field, registry


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
        tags: list[str] = field(default_factory=list)
        color: Color = Color.RED

    return Reading


async def _started_engine(path: str | Path) -> StorageEngine:
    engine = StorageEngine(path)
    await engine.start()
    return engine


# ------------------------------------------------------------ lifecycle


async def test_start_creates_registered_tables(tmp_path: Path) -> None:
    _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        rows = await engine.fetchall(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", ("reading",)
        )
        assert len(rows) == 1
        indexes = await engine.fetchall(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("idx_reading_sensor_id",),
        )
        assert len(indexes) == 1
    finally:
        await engine.stop()


async def test_wal_mode_enabled(tmp_path: Path) -> None:
    _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        row = await engine.fetchone("PRAGMA journal_mode")
        assert row is not None
        assert row[0] == "wal"
    finally:
        await engine.stop()


async def test_start_is_idempotent(tmp_path: Path) -> None:
    engine = await _started_engine(tmp_path / "test.db")
    try:
        await engine.start()  # second start is a no-op, not an error
    finally:
        await engine.stop()


async def test_stop_is_idempotent(tmp_path: Path) -> None:
    engine = await _started_engine(tmp_path / "test.db")
    await engine.stop()
    await engine.stop()

    never_started = StorageEngine(tmp_path / "other.db")
    await never_started.stop()


async def test_memory_database(tmp_path: Path) -> None:
    _reading_table()
    engine = await _started_engine(":memory:")
    try:
        rows = await engine.fetchall(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
        assert [row["name"] for row in rows] == ["reading"]
    finally:
        await engine.stop()


async def test_engine_binds_and_unbinds_on_table(tmp_path: Path) -> None:
    engine = await _started_engine(tmp_path / "test.db")
    assert Table._engine is engine

    other = StorageEngine(tmp_path / "other.db")
    await other.stop()  # stopping a non-bound engine must not unbind the active one
    assert Table._engine is engine

    await engine.stop()
    assert Table._engine is None


async def test_raw_access_before_start_raises(tmp_path: Path) -> None:
    engine = StorageEngine(tmp_path / "test.db")
    with pytest.raises(StorageNotReadyError, match="not started"):
        await engine.fetchall("SELECT 1")


# --------------------------------------------------------------- codecs


async def test_row_roundtrip(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        original = reading_cls(
            sensor_id="s1",
            celsius=21.5,
            ok=False,
            ts=datetime(2026, 7, 7, 12, 0, tzinfo=UTC),
            location=Location(lat=7.19, lon=125.45),
            tags=["field", "north"],
            color=Color.BLUE,
        )
        encoded = engine.encode_row(original)
        columns = ", ".join(f'"{name}"' for name in encoded)
        placeholders = ", ".join("?" for _ in encoded)
        await engine.execute(
            f'INSERT INTO "reading" ({columns}) VALUES ({placeholders})',  # noqa: S608
            list(encoded.values()),
        )

        row = await engine.fetchone('SELECT * FROM "reading"')
        assert row is not None
        decoded = engine.decode_row(reading_cls, row)

        assert decoded.id == 1  # assigned by AUTOINCREMENT
        assert decoded.sensor_id == original.sensor_id
        assert decoded.celsius == original.celsius
        assert decoded.ok is False
        assert decoded.ts == original.ts
        assert decoded.location == original.location
        assert decoded.tags == original.tags
        assert decoded.color is Color.BLUE
    finally:
        await engine.stop()


async def test_aware_datetime_normalized_to_utc(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        manila = timezone(timedelta(hours=8))
        original = reading_cls(
            sensor_id="s1",
            celsius=1.0,
            ts=datetime(2026, 7, 7, 20, 0, tzinfo=manila),
        )
        encoded = engine.encode_row(original)
        assert encoded["ts"] == "2026-07-07T12:00:00+00:00"
    finally:
        await engine.stop()


async def test_none_values_roundtrip(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        encoded = engine.encode_row(reading_cls(sensor_id="s1", celsius=1.0))
        assert encoded["ts"] is None
        assert encoded["location"] is None
        assert encoded["id"] is None  # NULL pk lets AUTOINCREMENT assign
    finally:
        await engine.stop()


# ------------------------------------------------------------ migration


async def test_additive_migration_adds_new_column(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db = tmp_path / "test.db"

    class Device(Table):
        id: int | None = field(pk=True, auto=True)
        name: str

    engine = await _started_engine(db)
    await engine.execute('INSERT INTO "device" ("name") VALUES (?)', ("pump-1",))
    await engine.stop()

    # same table evolves: a new optional column appears on the model
    registry.clear()

    class DeviceV2(Table):
        __tablename__ = "device"

        id: int | None = field(pk=True, auto=True)
        name: str
        firmware: str | None = None

    with caplog.at_level(logging.INFO):
        engine = await _started_engine(db)
    try:
        assert "added column device.firmware" in caplog.text

        row = await engine.fetchone('SELECT * FROM "device"')
        assert row is not None
        old = engine.decode_row(DeviceV2, row)
        assert old.name == "pump-1"
        assert old.firmware is None
    finally:
        await engine.stop()


async def test_migration_warns_on_removed_and_retyped_columns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db = tmp_path / "test.db"

    class Device(Table):
        id: int | None = field(pk=True, auto=True)
        name: str
        rssi: int | None = None

    engine = await _started_engine(db)
    await engine.stop()

    registry.clear()

    class DeviceV2(Table):
        __tablename__ = "device"

        id: int | None = field(pk=True, auto=True)
        name: float  # retyped: TEXT in db, maps to REAL now

    with caplog.at_level(logging.WARNING):
        engine = await _started_engine(db)
    await engine.stop()

    assert "device.name is TEXT in the database but maps to REAL" in caplog.text
    assert "column device.rssi exists in the database but not on the model" in caplog.text



# --------------------------------------------------------- group commit


async def test_synchronous_normal_enabled(tmp_path: Path) -> None:
    engine = await _started_engine(tmp_path / "test.db")
    try:
        row = await engine.fetchone("PRAGMA synchronous")
        assert row is not None
        assert row[0] == 1  # NORMAL
    finally:
        await engine.stop()


# wraps the connection's commit with a counter; returns the counter cell
def _count_commits(engine: StorageEngine) -> list[int]:
    assert engine._conn is not None
    conn = engine._conn
    real_commit = conn.commit
    commits = [0]

    async def counting_commit() -> None:
        commits[0] += 1
        await real_commit()

    conn.commit = counting_commit  # type: ignore[method-assign]
    return commits


async def test_burst_of_saves_shares_one_commit(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    engine = StorageEngine(tmp_path / "test.db", commit_delay=0.05)
    await engine.start()
    try:
        commits = _count_commits(engine)

        rows = [reading_cls(sensor_id=f"s{i}", celsius=float(i)) for i in range(10)]
        await asyncio.gather(*(row.save() for row in rows))

        assert commits[0] == 1  # the whole burst coalesced
        assert sorted(row.id for row in rows) == list(range(1, 11))
        assert await reading_cls.count() == 10
    finally:
        await engine.stop()


async def test_stop_flushes_pending_commit_early(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    db = tmp_path / "test.db"
    engine = StorageEngine(db, commit_delay=30.0)
    await engine.start()

    save_task = asyncio.create_task(reading_cls(sensor_id="s1", celsius=1.0).save())
    await asyncio.sleep(0.05)  # let the INSERT execute and the flush cycle start

    started = time.monotonic()
    await engine.stop()
    await asyncio.wait_for(save_task, timeout=1.0)
    assert time.monotonic() - started < 5.0  # did not wait out commit_delay

    engine = await _started_engine(db)
    try:
        assert await reading_cls.count() == 1
    finally:
        await engine.stop()


async def test_save_many_shares_one_commit_and_backfills_ids(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        commits = _count_commits(engine)

        rows = [reading_cls(sensor_id=f"s{i}", celsius=float(i)) for i in range(5)]
        await reading_cls.save_many(rows)

        assert commits[0] == 1
        assert [row.id for row in rows] == [1, 2, 3, 4, 5]

        await reading_cls.save_many([])  # empty batch is a no-op
        assert commits[0] == 1
    finally:
        await engine.stop()


async def test_commit_failure_propagates_to_awaiting_writers(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    engine = await _started_engine(tmp_path / "test.db")
    try:
        assert engine._conn is not None

        async def failing_commit() -> None:
            what = "disk I/O error"
            raise RuntimeError(what)

        real_commit = engine._conn.commit
        engine._conn.commit = failing_commit  # type: ignore[method-assign]
        try:
            with pytest.raises(RuntimeError, match="disk I/O error"):
                await reading_cls(sensor_id="s1", celsius=1.0).save()
        finally:
            engine._conn.commit = real_commit  # type: ignore[method-assign]
    finally:
        await engine.stop()


async def test_data_persists_across_reopen(tmp_path: Path) -> None:
    reading_cls = _reading_table()
    db = tmp_path / "test.db"

    engine = await _started_engine(db)
    encoded = engine.encode_row(reading_cls(sensor_id="s1", celsius=3.0))
    await engine.execute(
        'INSERT INTO "reading" ("sensor_id", "celsius", "ok", "tags", "color") '
        "VALUES (?, ?, ?, ?, ?)",
        [encoded["sensor_id"], encoded["celsius"], encoded["ok"], encoded["tags"], encoded["color"]],
    )
    await engine.stop()

    engine = await _started_engine(db)
    try:
        rows = await engine.fetchall('SELECT * FROM "reading"')
        assert len(rows) == 1
        assert engine.decode_row(reading_cls, rows[0]).celsius == 3.0
    finally:
        await engine.stop()

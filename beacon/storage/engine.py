from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

import aiosqlite
from pydantic import BaseModel

from beacon.core.exceptions import StorageNotReadyError
from beacon.storage.ddl import columns_for, create_index_sql, create_table_sql
from beacon.storage.table import Table, registry

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

    from beacon.storage.ddl import ColumnSpec


# TEXT columns that don't hold plain strings (or ISO datetimes, or str
# subclasses like StrEnum) are stored as JSON
def _needs_json(spec: ColumnSpec) -> bool:
    if spec.sql_type != "TEXT":
        return False
    py_type = spec.py_type
    return not (isinstance(py_type, type) and issubclass(py_type, (str, datetime)))


# python value -> sqlite parameter for one column
# aware datetimes are normalized to UTC ISO 8601; naive ones are stored as-is
def encode_value(spec: ColumnSpec, value: Any) -> Any:
    if value is None:
        return None

    if _needs_json(spec):
        if isinstance(value, BaseModel):
            return value.model_dump_json()
        if isinstance(value, Enum):
            value = value.value
        return json.dumps(value, default=str)

    if isinstance(value, Enum):
        value = value.value

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.isoformat()

    return value


# sqlite value -> something pydantic can validate for one column
# (scalars pass through; model_validate coerces bools, datetimes, enums)
def decode_value(spec: ColumnSpec, value: Any) -> Any:
    if value is None:
        return None
    if _needs_json(spec) and isinstance(value, (str, bytes)):
        return json.loads(value)
    return value


class StorageEngine:
    """Owns the aiosqlite connection and the schema of every registered table.

    `start()` opens the database (WAL mode), creates missing tables and
    indexes, applies additive column migrations, and binds itself onto
    `Table` so the active-record API works. `stop()` is idempotent.
    Reads are validated through the table model - the same
    validation-at-the-edge policy inbound MQTT payloads get.
    """

    def __init__(self, path: str | Path = "beacon.db") -> None:
        self.path = str(path)
        self._conn: aiosqlite.Connection | None = None
        self._started = False
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        if self._started:
            return

        self._logger.info("storage engine starting path=%s", self.path)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row

        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._create_tables()
        await self._conn.commit()

        Table.bind_engine(self)
        self._started = True
        self._logger.info("storage engine ready (%d tables)", len(registry))

    async def stop(self) -> None:
        if self._conn is None:
            return

        self._logger.info("storage engine stopping")
        Table.unbind_engine(self)
        await self._conn.close()
        self._conn = None
        self._started = False

    # ------------------------------------------------------------------
    # schema
    # ------------------------------------------------------------------

    async def _create_tables(self) -> None:
        assert self._conn is not None
        for table_cls in registry.tables:
            tablename = table_cls.__tablename__
            specs = columns_for(table_cls)

            await self._conn.execute(create_table_sql(tablename, specs))
            for statement in create_index_sql(tablename, specs):
                await self._conn.execute(statement)
            await self._migrate_additive(tablename, specs)

    async def _migrate_additive(self, tablename: str, specs: list[ColumnSpec]) -> None:
        assert self._conn is not None
        cursor = await self._conn.execute(f'PRAGMA table_info("{tablename}")')
        rows = await cursor.fetchall()
        existing: dict[str, str] = {row["name"]: row["type"] for row in rows}

        for spec in specs:
            if spec.name not in existing:
                await self._conn.execute(
                    f'ALTER TABLE "{tablename}" ADD COLUMN "{spec.name}" {spec.sql_type}'
                )
                self._logger.info(
                    "storage migration: added column %s.%s (%s)",
                    tablename,
                    spec.name,
                    spec.sql_type,
                )
                if not spec.nullable and not spec.has_default:
                    self._logger.warning(
                        "storage migration: %s.%s is required on the model but existing "
                        "rows have no value; reads of old rows will fail validation",
                        tablename,
                        spec.name,
                    )
            elif existing[spec.name] != spec.sql_type:
                self._logger.warning(
                    "storage migration: %s.%s is %s in the database but maps to %s "
                    "on the model; column left untouched",
                    tablename,
                    spec.name,
                    existing[spec.name],
                    spec.sql_type,
                )

        model_columns = {spec.name for spec in specs}
        for name in existing:
            if name not in model_columns:
                self._logger.warning(
                    "storage migration: column %s.%s exists in the database but not "
                    "on the model; column left untouched",
                    tablename,
                    name,
                )

    # ------------------------------------------------------------------
    # row codecs
    # ------------------------------------------------------------------

    def encode_row(self, instance: Table) -> dict[str, Any]:
        return {
            spec.name: encode_value(spec, getattr(instance, spec.name))
            for spec in columns_for(type(instance))
        }

    def decode_row[T: Table](self, table_cls: type[T], row: Mapping[str, Any]) -> T:
        keys = row.keys()
        data = {
            spec.name: decode_value(spec, row[spec.name])
            for spec in columns_for(table_cls)
            if spec.name in keys
        }
        return table_cls.model_validate(data)

    # ------------------------------------------------------------------
    # raw escape hatch (parameterized sql only)
    # ------------------------------------------------------------------

    async def execute(
        self,
        sql: str,
        params: Sequence[Any] = (),
        *,
        commit: bool = True,
    ) -> aiosqlite.Cursor:
        conn = self._require_conn()
        cursor = await conn.execute(sql, params)
        if commit:
            await conn.commit()
        return cursor

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[aiosqlite.Row]:
        conn = self._require_conn()
        cursor = await conn.execute(sql, params)
        return list(await cursor.fetchall())

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> aiosqlite.Row | None:
        conn = self._require_conn()
        cursor = await conn.execute(sql, params)
        return await cursor.fetchone()

    def _require_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            what = "engine not started; call start() first"
            raise StorageNotReadyError(what)
        return self._conn

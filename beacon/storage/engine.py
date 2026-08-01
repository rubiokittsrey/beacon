import asyncio
import contextlib
import json
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, cast

import aiosqlite
from pydantic import BaseModel

from beacon.core.exceptions import StorageNotReadyError
from beacon.storage.ddl import ColumnSpec, columns_for, create_index_sql, create_table_sql
from beacon.storage.query import build_order_by, build_where
from beacon.storage.table import Table, registry


# TEXT columns that don't hold plain strings (or ISO datetimes, or str
# subclasses like StrEnum) are stored as JSON
def _needs_json(spec: ColumnSpec) -> bool:
    if spec.sql_type != "TEXT":
        return False
    py_type = spec.py_type
    return not (isinstance(py_type, type) and issubclass(py_type, (str, datetime)))


def encode_value(spec: ColumnSpec, value: Any) -> Any:
    """Encode a python value into the sqlite parameter for one column.

    Aware datetimes are normalized to UTC ISO 8601 and naive ones stored
    as-is; non-string TEXT columns are serialized to JSON.
    """
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


def decode_value(spec: ColumnSpec, value: Any) -> Any:
    """Decode a sqlite value into something the table model can validate.

    JSON TEXT columns are parsed back to python; scalars pass through and
    are coerced by `model_validate` on the way in.
    """
    if value is None:
        return None
    if _needs_json(spec) and isinstance(value, (str, bytes)):
        return json.loads(value)
    return value


def _columns_by_name(table_cls: type[Table]) -> dict[str, ColumnSpec]:
    return {spec.name: spec for spec in columns_for(table_cls)}


def _insert_sql(tablename: str, values: Mapping[str, Any]) -> tuple[str, list[Any]]:
    if not values:
        return f'INSERT INTO "{tablename}" DEFAULT VALUES', []
    columns = ", ".join(f'"{name}"' for name in values)
    placeholders = ", ".join("?" for _ in values)
    sql = f'INSERT INTO "{tablename}" ({columns}) VALUES ({placeholders})'  # noqa: S608
    return sql, list(values.values())


def _upsert_sql(tablename: str, values: Mapping[str, Any], pk: str) -> tuple[str, list[Any]]:
    insert, params = _insert_sql(tablename, values)
    updates = ", ".join(f'"{name}" = excluded."{name}"' for name in values if name != pk)
    action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"
    return f'{insert} ON CONFLICT("{pk}") {action}', params


class StorageEngine:
    """Owns the aiosqlite connection and the schema of every registered table.

    `start()` opens the database (WAL mode), creates missing tables and
    indexes, applies additive column migrations, and binds itself onto
    `Table`; `stop()` is idempotent. Reads are validated through the table
    model, the same validation-at-the-edge policy inbound payloads get.

    Writes are group-committed: a statement run with `commit=True` awaits a
    shared commit scheduled `commit_delay` seconds out, so concurrent writes
    (a burst of handler `save()` calls) share one fsync instead of paying one
    each. `await save()` still returns only after its commit has landed.

    Raises:
        StorageNotReadyError: If a raw query method runs before `start()`.
    """

    def __init__(self, path: str | Path = "beacon.db", *, commit_delay: float = 0.01) -> None:
        self.path = str(path)
        self.commit_delay = commit_delay
        self._conn: aiosqlite.Connection | None = None
        self._started = False
        self._logger = logging.getLogger(__name__)

        # group-commit state, one "cycle" at a time: the pending future
        # resolves when the shared commit lands, the wakeup event lets
        # stop() flush early instead of waiting out the delay
        self._commit_pending: asyncio.Future[None] | None = None
        self._flush_task: asyncio.Task[None] | None = None
        self._flush_wakeup: asyncio.Event | None = None

    async def start(self) -> None:
        """Open the database, create/migrate schema, and bind the active-record API."""
        if self._started:
            return

        self._logger.info("storage engine starting path=%s", self.path)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row

        await self._conn.execute("PRAGMA journal_mode=WAL")
        # WAL + NORMAL skips the fsync-per-commit of FULL; worst case on
        # power loss is the last few transactions, never corruption
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._create_tables()
        await self._conn.commit()

        Table.bind_engine(self)
        self._started = True
        self._logger.info("storage engine ready (%d tables)", len(registry))

    async def stop(self) -> None:
        """Flush any pending commit, close, and unbind the active-record API (idempotent)."""
        if self._conn is None:
            return

        self._logger.info("storage engine stopping")
        Table.unbind_engine(self)

        # wake the pending flush (if any) so in-flight writes commit now
        # instead of waiting out the delay, then close behind it
        if self._flush_task is not None:
            if self._flush_wakeup is not None:
                self._flush_wakeup.set()
            await self._flush_task
            self._flush_task = None
            self._flush_wakeup = None

        await self._conn.close()
        self._conn = None
        self._started = False

    async def _commit_soon(self) -> None:
        # join the current group-commit cycle, starting one if none is
        # pending; resolves once the shared commit has landed
        if self._commit_pending is None:
            self._commit_pending = asyncio.get_running_loop().create_future()
            self._flush_wakeup = asyncio.Event()
            self._flush_task = asyncio.create_task(self._delayed_flush(self._flush_wakeup))
        await self._commit_pending

    async def _delayed_flush(self, wakeup: asyncio.Event) -> None:
        # wait out the coalescing window (or an early wakeup from stop()),
        # then commit once for every write that joined this cycle
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(wakeup.wait(), self.commit_delay)

        pending, self._commit_pending = self._commit_pending, None
        if pending is None:
            return
        try:
            await self._require_conn().commit()
        except Exception as exc:  # noqa: BLE001 - delivered to every awaiting writer
            pending.set_exception(exc)
        else:
            pending.set_result(None)

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

    def encode_row(self, instance: Table) -> dict[str, Any]:
        """Encode a table instance to a column-name -> sqlite-value mapping."""
        return {
            spec.name: encode_value(spec, getattr(instance, spec.name))
            for spec in columns_for(type(instance))
        }

    def decode_row[T: Table](self, table_cls: type[T], row: Mapping[str, Any]) -> T:
        """Rebuild a validated table instance from a database row."""
        keys = row.keys()
        data = {
            spec.name: decode_value(spec, row[spec.name])
            for spec in columns_for(table_cls)
            if spec.name in keys
        }
        return table_cls.model_validate(data)

    async def fetch[T: Table](
        self,
        table_cls: type[T],
        lookups: Mapping[str, Any],
        *,
        order_by: str | list[str] | None = None,
        limit: int | None = None,
    ) -> list[T]:
        """Run a SELECT for `table_cls` and return validated instances."""
        tablename = table_cls.__tablename__
        columns = _columns_by_name(table_cls)
        where, params = build_where(tablename, columns, encode_value, lookups)

        sql = f'SELECT * FROM "{tablename}"'  # noqa: S608
        if where:
            sql += f" WHERE {where}"
        if order_by is not None:
            sql += f" ORDER BY {build_order_by(tablename, columns, order_by)}"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))

        rows = await self.fetchall(sql, params)
        return [self.decode_row(table_cls, cast("Mapping[str, Any]", row)) for row in rows]

    async def count(self, table_cls: type[Table], lookups: Mapping[str, Any]) -> int:
        """Return how many rows of `table_cls` match `lookups`."""
        tablename = table_cls.__tablename__
        columns = _columns_by_name(table_cls)
        where, params = build_where(tablename, columns, encode_value, lookups)

        sql = f'SELECT COUNT(*) FROM "{tablename}"'  # noqa: S608
        if where:
            sql += f" WHERE {where}"

        row = await self.fetchone(sql, params)
        return int(row[0]) if row is not None else 0

    async def delete_where(self, table_cls: type[Table], lookups: Mapping[str, Any]) -> int:
        """Delete every row of `table_cls` matching `lookups`; return the count."""
        tablename = table_cls.__tablename__
        columns = _columns_by_name(table_cls)
        where, params = build_where(tablename, columns, encode_value, lookups)

        sql = f'DELETE FROM "{tablename}"'  # noqa: S608
        if where:
            sql += f" WHERE {where}"

        cursor = await self.execute(sql, params)
        return cursor.rowcount

    async def save(self, instance: Table, *, commit: bool = True) -> None:
        """Insert `instance`, or upsert it when its primary key is set.

        An unset autoincrement pk takes the INSERT path and the generated
        rowid is written back onto the instance; any other pk value upserts
        via `ON CONFLICT` so re-saving a loaded row updates it in place.
        """
        specs = columns_for(type(instance))
        pk = next(spec for spec in specs if spec.pk)
        encoded = self.encode_row(instance)
        tablename = instance.__tablename__

        if pk.auto and encoded[pk.name] is None:
            insert = {name: value for name, value in encoded.items() if name != pk.name}
            cursor = await self.execute(*_insert_sql(tablename, insert), commit=commit)
            setattr(instance, pk.name, cursor.lastrowid)
            return

        await self.execute(*_upsert_sql(tablename, encoded, pk.name), commit=commit)

    async def save_many(self, instances: Sequence[Table]) -> None:
        """Save every instance, sharing a single commit across the batch.

        Autoincrement pks are written back per row, same as `save()`.
        """
        for instance in instances:
            await self.save(instance, commit=False)
        if instances:
            await self._commit_soon()

    async def delete(self, instance: Table) -> None:
        """Delete the row identified by `instance`'s primary key."""
        specs = columns_for(type(instance))
        pk = next(spec for spec in specs if spec.pk)
        value = encode_value(pk, getattr(instance, pk.name))
        await self.execute(
            f'DELETE FROM "{instance.__tablename__}" WHERE "{pk.name}" = ?',  # noqa: S608
            [value],
        )

    async def execute(
        self,
        sql: str,
        params: Sequence[Any] = (),
        *,
        commit: bool = True,
    ) -> aiosqlite.Cursor:
        """Run a parameterized statement, joining a group commit by default."""
        conn = self._require_conn()
        cursor = await conn.execute(sql, params)
        if commit:
            await self._commit_soon()
        return cursor

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[aiosqlite.Row]:
        """Run a parameterized query and return all rows."""
        conn = self._require_conn()
        cursor = await conn.execute(sql, params)
        return list(await cursor.fetchall())

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> aiosqlite.Row | None:
        """Run a parameterized query and return the first row, or None."""
        conn = self._require_conn()
        cursor = await conn.execute(sql, params)
        return await cursor.fetchone()

    def _require_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            what = "engine not started; call start() first"
            raise StorageNotReadyError(what)
        return self._conn

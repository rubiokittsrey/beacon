from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, ClassVar, Self

from pydantic import BaseModel

from beacon.core.exceptions import StorageNotReadyError, TableDefinitionError
from beacon.storage.ddl import columns_for

if TYPE_CHECKING:
    from collections.abc import Sequence

    from beacon.storage.ddl import ColumnSpec
    from beacon.storage.engine import StorageEngine

# CamelCase -> snake_case, keeping acronyms together (HTTPServerLog -> http_server_log)
_SNAKE_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def _snake_case(name: str) -> str:
    return _SNAKE_RE.sub("_", name).lower()


class TableRegistry:
    """Registry of declared tables; subclassing `Table` registers (import = declaration)."""

    def __init__(self) -> None:
        self._tables: dict[str, type[Table]] = {}

    def register(self, table_cls: type[Table]) -> None:
        name = table_cls.__tablename__
        existing = self._tables.get(name)
        if existing is not None and existing is not table_cls:
            reason = f"table name already registered by {existing.__qualname__}"
            raise TableDefinitionError(name, reason)
        self._tables[name] = table_cls

    def clear(self) -> None:
        self._tables.clear()

    def get(self, name: str) -> type[Table] | None:
        return self._tables.get(name)

    @property
    def tables(self) -> list[type[Table]]:
        return list(self._tables.values())

    def __len__(self) -> int:
        return len(self._tables)

    def __contains__(self, name: object) -> bool:
        return name in self._tables


# module-level registry; the storage engine creates every registered
# table's schema on start
registry = TableRegistry()


class Table(BaseModel):
    """Declarative storage model: the pydantic model is the table definition.

    Subclassing declares and registers a table. Column metadata comes from
    `field()`; the table name defaults to the snake_case class name unless
    `__tablename__` is set. A `Table` is a plain `BaseModel`, so the same
    class doubles as a `model=` validator on MQTT bindings.
    """

    __tablename__: ClassVar[str]
    _engine: ClassVar[StorageEngine | None] = None

    @classmethod
    def bind_engine(cls, engine: StorageEngine) -> None:
        Table._engine = engine

    @classmethod
    def unbind_engine(cls, engine: StorageEngine) -> None:
        if Table._engine is engine:
            Table._engine = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)

        if "__tablename__" not in cls.__dict__:
            cls.__tablename__ = _snake_case(cls.__name__)

        specs = columns_for(cls)
        _validate_definition(cls.__tablename__, specs)
        registry.register(cls)

    @classmethod
    def columns(cls) -> list[ColumnSpec]:
        return columns_for(cls)

    @classmethod
    def _require_engine(cls) -> StorageEngine:
        if cls._engine is None:
            what = f"{cls.__name__} is not bound to a running storage engine"
            raise StorageNotReadyError(what)
        return cls._engine

    @classmethod
    async def get(cls, **lookups: Any) -> Self | None:
        """Return the first row matching `lookups`, or None if there is none."""
        results = await cls._require_engine().fetch(cls, lookups, limit=1)
        return results[0] if results else None

    @classmethod
    async def filter(
        cls,
        *,
        order_by: str | list[str] | None = None,
        limit: int | None = None,
        **lookups: Any,
    ) -> list[Self]:
        """Return rows matching `lookups`, optionally ordered and capped.

        Lookups are Django-style: `celsius__gt=30`, `sensor_id__in=[...]`,
        `name__like="pump-%"`; `order_by="-ts"` sorts descending.
        """
        return await cls._require_engine().fetch(
            cls, lookups, order_by=order_by, limit=limit
        )

    @classmethod
    async def all(cls) -> list[Self]:
        """Return every row of the table."""
        return await cls._require_engine().fetch(cls, {})

    @classmethod
    async def count(cls, **lookups: Any) -> int:
        """Return how many rows match `lookups` (every row if none given)."""
        return await cls._require_engine().count(cls, lookups)

    @classmethod
    async def delete_where(cls, **lookups: Any) -> int:
        """Delete rows matching `lookups`; return how many were removed."""
        return await cls._require_engine().delete_where(cls, lookups)

    @classmethod
    async def save_many(cls, instances: Sequence[Self]) -> None:
        """Save every instance in `instances`, sharing a single commit."""
        await cls._require_engine().save_many(instances)

    async def save(self) -> None:
        """Insert this row, or update it when its primary key is already set."""
        await self._require_engine().save(self)

    async def delete(self) -> None:
        """Delete the row identified by this instance's primary key."""
        await self._require_engine().delete(self)


def _validate_definition(tablename: str, specs: list[ColumnSpec]) -> None:
    pks = [spec for spec in specs if spec.pk]

    if not pks:
        reason = "a primary key is required, e.g. id: int | None = field(pk=True, auto=True)"
        raise TableDefinitionError(tablename, reason)

    if len(pks) > 1:
        names = ", ".join(spec.name for spec in pks)
        reason = f"multiple primary keys declared ({names}); exactly one is supported"
        raise TableDefinitionError(tablename, reason)

    pk = pks[0]
    if pk.auto and pk.py_type is not int:
        reason = f"auto=True requires an int primary key, got {pk.py_type!r} on '{pk.name}'"
        raise TableDefinitionError(tablename, reason)

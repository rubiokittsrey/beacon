from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel

from beacon.core.exceptions import TableDefinitionError
from beacon.storage.ddl import columns_for

if TYPE_CHECKING:
    from beacon.storage.ddl import ColumnSpec

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

    Subclassing declares (and registers) a table. Column metadata comes
    from `beacon.storage.field()`; plain pydantic fields become plain
    columns. The table name defaults to the snake_case class name and can
    be overridden with `__tablename__`. Because a `Table` is a plain
    pydantic `BaseModel`, the same class works as `model=` on MQTT
    bindings - one model is both the wire validator and the schema.
    """

    __tablename__: ClassVar[str]

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

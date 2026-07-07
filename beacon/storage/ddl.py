from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import NoneType, UnionType
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin

from beacon.storage.fields import COLUMN_METADATA_KEY

if TYPE_CHECKING:
    from pydantic import BaseModel

# python type -> sqlite storage class; order matters (bool before int,
# since bool subclasses int) and subclasses map through issubclass
# (StrEnum -> TEXT, IntEnum -> INTEGER). Anything unmapped is stored as
# TEXT and encoded as JSON by the engine.
_TYPE_MAP: tuple[tuple[type, str], ...] = (
    (bool, "INTEGER"),
    (int, "INTEGER"),
    (float, "REAL"),
    (str, "TEXT"),
    (bytes, "BLOB"),
    (datetime, "TEXT"),
)


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One column derived from a pydantic model field."""

    name: str
    py_type: Any
    sql_type: str
    nullable: bool
    pk: bool
    auto: bool
    index: bool
    unique: bool
    has_default: bool


# strips `| None` from an annotation
# returns the remaining type and whether None was allowed; unions of
# multiple non-None types fall through to Any (stored as JSON TEXT)
def unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    origin = get_origin(annotation)
    if origin is Union or origin is UnionType:
        args = get_args(annotation)
        non_none = [a for a in args if a is not NoneType]
        nullable = len(non_none) < len(args)
        if len(non_none) == 1:
            return non_none[0], nullable
        return Any, nullable
    return annotation, False


def sqlite_type_for(py_type: Any) -> str:
    if isinstance(py_type, type):
        for base, sql_type in _TYPE_MAP:
            if issubclass(py_type, base):
                return sql_type
    return "TEXT"


def columns_for(model_cls: type[BaseModel]) -> list[ColumnSpec]:
    """Derive column specs from a pydantic model's fields.

    Storage metadata recorded by `field()` is read off each field; plain
    pydantic fields become plain columns.
    """
    specs: list[ColumnSpec] = []
    for name, info in model_cls.model_fields.items():
        extra = info.json_schema_extra
        meta: dict[str, Any] = {}
        if isinstance(extra, dict):
            found = extra.get(COLUMN_METADATA_KEY)
            if isinstance(found, dict):
                meta = found

        py_type, nullable = unwrap_optional(info.annotation)
        specs.append(
            ColumnSpec(
                name=name,
                py_type=py_type,
                sql_type=sqlite_type_for(py_type),
                nullable=nullable,
                pk=bool(meta.get("pk", False)),
                auto=bool(meta.get("auto", False)),
                index=bool(meta.get("index", False)),
                unique=bool(meta.get("unique", False)),
                has_default=not info.is_required(),
            )
        )
    return specs


def _column_sql(spec: ColumnSpec) -> str:
    parts = [f'"{spec.name}"', spec.sql_type]
    if spec.pk:
        parts.append("PRIMARY KEY")
        if spec.auto:
            parts.append("AUTOINCREMENT")
    elif not spec.nullable:
        parts.append("NOT NULL")
    if spec.unique and not spec.pk:
        parts.append("UNIQUE")
    return " ".join(parts)


def create_table_sql(tablename: str, specs: list[ColumnSpec]) -> str:
    """Build the `CREATE TABLE IF NOT EXISTS` statement for `specs`."""
    columns = ", ".join(_column_sql(spec) for spec in specs)
    return f'CREATE TABLE IF NOT EXISTS "{tablename}" ({columns})'


def create_index_sql(tablename: str, specs: list[ColumnSpec]) -> list[str]:
    """Build `CREATE INDEX` statements for the `index=True` columns.

    Unique and primary-key columns are skipped; they already get an index
    from their own constraint.
    """
    return [
        f'CREATE INDEX IF NOT EXISTS "idx_{tablename}_{spec.name}" '
        f'ON "{tablename}" ("{spec.name}")'
        for spec in specs
        if spec.index and not spec.unique and not spec.pk
    ]

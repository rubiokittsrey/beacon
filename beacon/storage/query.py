from collections.abc import Callable, Iterable, Mapping
from typing import Any

from beacon.core.exceptions import UnknownLookupError
from beacon.storage.ddl import ColumnSpec

# Django-style lookup suffix -> SQL comparison operator. A bare field name
# (no `__suffix`) is equality; `field=None` becomes `IS NULL` on `eq`/`ne`.
_OPERATORS: dict[str, str] = {
    "ne": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "in": "IN",
    "like": "LIKE",
}


def _resolve(table: str, columns: Mapping[str, ColumnSpec], key: str) -> tuple[str, str]:
    # a bare column name wins outright, so snake_case names are never
    # mistaken for a `field__suffix` split
    if key in columns:
        return key, "="

    field, sep, suffix = key.rpartition("__")
    if sep and field in columns and suffix in _OPERATORS:
        return field, _OPERATORS[suffix]

    raise UnknownLookupError(table, key)


def build_where(
    table: str,
    columns: Mapping[str, ColumnSpec],
    encode: Callable[[ColumnSpec, Any], Any],
    lookups: Mapping[str, Any],
) -> tuple[str, list[Any]]:
    """Build a parameterized WHERE body (no leading `WHERE`) from lookups.

    Values are encoded through `encode` with their column spec, so a
    `ts__gt=datetime(...)` filter compares against the same ISO string the
    row was stored as. Returns the clause text and its ordered parameters.

    Raises:
        UnknownLookupError: If a key names no column or an unknown suffix.
    """
    clauses: list[str] = []
    params: list[Any] = []

    for key, value in lookups.items():
        column, op = _resolve(table, columns, key)
        spec = columns[column]

        if op == "IN":
            scalar = isinstance(value, (str, bytes)) or not isinstance(value, Iterable)
            values = [value] if scalar else list(value)
            if not values:
                clauses.append("0")  # `IN ()` is invalid sql; match nothing
                continue
            placeholders = ", ".join("?" for _ in values)
            clauses.append(f'"{column}" IN ({placeholders})')
            params.extend(encode(spec, item) for item in values)
        elif value is None and op in ("=", "!="):
            negate = "NOT " if op == "!=" else ""
            clauses.append(f'"{column}" IS {negate}NULL')
        else:
            clauses.append(f'"{column}" {op} ?')
            params.append(encode(spec, value))

    return " AND ".join(clauses), params


def build_order_by(
    table: str,
    columns: Mapping[str, ColumnSpec],
    order_by: str | list[str],
) -> str:
    """Build an ORDER BY body (no leading `ORDER BY`) from field names.

    A leading `-` on a field sorts it descending (`order_by="-ts"`).

    Raises:
        UnknownLookupError: If an entry names no column.
    """
    fields = [order_by] if isinstance(order_by, str) else list(order_by)
    parts: list[str] = []
    for entry in fields:
        descending = entry.startswith("-")
        name = entry[1:] if descending else entry
        if name not in columns:
            raise UnknownLookupError(table, entry)
        parts.append(f'"{name}" {"DESC" if descending else "ASC"}')
    return ", ".join(parts)

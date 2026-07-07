from __future__ import annotations

from typing import Any

from pydantic import Field
from pydantic_core import PydanticUndefined

# key under FieldInfo.json_schema_extra where column metadata is stored
COLUMN_METADATA_KEY = "beacon_column"


def field(
    *,
    pk: bool = False,
    auto: bool = False,
    index: bool = False,
    unique: bool = False,
    default: Any = PydanticUndefined,
    default_factory: Any = None,
) -> Any:
    """Declare column metadata on a `Table` field.

    A thin wrapper over `pydantic.Field` that records storage metadata
    without changing how the model validates.

    Args:
        pk: Mark the column as the table's primary key.
        auto: Autoincrementing integer pk; implies `pk=True` and defaults
            the field to `None`, so annotate such fields as `int | None`.
        index: Create a secondary index on the column.
        unique: Add a `UNIQUE` constraint on the column.
        default: Default value; omit for a required column.
        default_factory: Zero-arg callable producing the default.

    Returns:
        A pydantic `FieldInfo` carrying the column metadata.
    """
    if auto:
        pk = True
        if default is PydanticUndefined and default_factory is None:
            default = None

    metadata: dict[str, Any] = {
        COLUMN_METADATA_KEY: {"pk": pk, "auto": auto, "index": index, "unique": unique},
    }

    if default_factory is not None:
        return Field(default_factory=default_factory, json_schema_extra=metadata)
    return Field(default=default, json_schema_extra=metadata)

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
    (primary key, autoincrement, index, unique) without changing how the
    model validates. `auto=True` implies `pk=True` and defaults the field
    to `None` so instances can be constructed before the database assigns
    the id; annotate such fields as `int | None`.
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

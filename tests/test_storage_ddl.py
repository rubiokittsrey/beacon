from datetime import datetime
from enum import IntEnum, StrEnum
from typing import Any

import pytest
from pydantic import BaseModel

from beacon.core.exceptions import TableDefinitionError
from beacon.storage import (
    Table,
    columns_for,
    create_index_sql,
    create_table_sql,
    field,
    registry,
)


def _spec_map(table_cls: type[Table]) -> dict[str, Any]:
    return {spec.name: spec for spec in columns_for(table_cls)}


# ------------------------------------------------------ field metadata


def test_field_records_column_metadata() -> None:
    class Reading(Table):
        id: int | None = field(pk=True, auto=True)
        sensor_id: str = field(index=True)
        tag: str = field(unique=True)
        celsius: float

    specs = _spec_map(Reading)
    assert specs["id"].pk
    assert specs["id"].auto
    assert specs["sensor_id"].index
    assert not specs["sensor_id"].pk
    assert specs["tag"].unique
    assert not specs["celsius"].pk
    assert not specs["celsius"].index


def test_auto_implies_pk_and_defaults_to_none() -> None:
    class Reading(Table):
        id: int | None = field(auto=True)
        celsius: float

    specs = _spec_map(Reading)
    assert specs["id"].pk
    assert Reading(celsius=1.0).id is None


def test_field_default_passthrough() -> None:
    class Device(Table):
        id: int | None = field(pk=True, auto=True)
        name: str = field(default="unnamed")
        tags: list[str] = field(default_factory=list)

    device = Device()
    assert device.name == "unnamed"
    assert device.tags == []


# ------------------------------------------------------- registration


def test_subclass_registers_with_snake_case_tablename() -> None:
    class SoilMoistureReading(Table):
        id: int | None = field(pk=True, auto=True)

    assert SoilMoistureReading.__tablename__ == "soil_moisture_reading"
    assert "soil_moisture_reading" in registry
    assert registry.get("soil_moisture_reading") is SoilMoistureReading


def test_snake_case_keeps_acronyms_together() -> None:
    class HTTPServerLog(Table):
        id: int | None = field(pk=True, auto=True)

    assert HTTPServerLog.__tablename__ == "http_server_log"


def test_tablename_override() -> None:
    class TempReading(Table):
        __tablename__ = "temp_readings"

        id: int | None = field(pk=True, auto=True)

    assert TempReading.__tablename__ == "temp_readings"
    assert "temp_readings" in registry
    assert len(registry) == 1


def test_duplicate_tablename_raises() -> None:
    class Reading(Table):
        id: int | None = field(pk=True, auto=True)

    with pytest.raises(TableDefinitionError, match="already registered"):

        class Duplicate(Table):
            __tablename__ = "reading"

            id: int | None = field(pk=True, auto=True)


def test_table_is_a_pydantic_model() -> None:
    class Reading(Table):
        id: int | None = field(pk=True, auto=True)
        celsius: float

    # the same class must be usable as a payload model on MQTT bindings
    parsed = Reading.model_validate_json('{"celsius": 21.5}')
    assert parsed.celsius == 21.5


# ---------------------------------------------------- definition rules


def test_missing_pk_raises() -> None:
    with pytest.raises(TableDefinitionError, match="primary key is required"):

        class NoPk(Table):
            celsius: float


def test_multiple_pks_raise() -> None:
    with pytest.raises(TableDefinitionError, match="multiple primary keys"):

        class TwoPks(Table):
            a: int = field(pk=True)
            b: int = field(pk=True)


def test_auto_requires_int_pk() -> None:
    with pytest.raises(TableDefinitionError, match="auto=True requires an int"):

        class StrAuto(Table):
            id: str | None = field(auto=True)


# ------------------------------------------------------- type mapping


class Nested(BaseModel):
    x: int


class Color(StrEnum):
    RED = "red"


class Level(IntEnum):
    LOW = 1


@pytest.mark.parametrize(
    ("annotation", "sql_type", "nullable"),
    [
        (int, "INTEGER", False),
        (bool, "INTEGER", False),
        (float, "REAL", False),
        (str, "TEXT", False),
        (bytes, "BLOB", False),
        (datetime, "TEXT", False),
        (str | None, "TEXT", True),
        (int | None, "INTEGER", True),
        (Nested, "TEXT", False),
        (dict[str, Any], "TEXT", False),
        (list[str], "TEXT", False),
        (Color, "TEXT", False),
        (Level, "INTEGER", False),
        (int | str, "TEXT", False),
    ],
)
def test_sqlite_type_mapping(annotation: Any, sql_type: str, nullable: bool) -> None:
    table_cls = type(
        "MappingCase",
        (Table,),
        {"__annotations__": {"id": int | None, "value": annotation}, "id": field(auto=True)},
    )
    specs = {spec.name: spec for spec in columns_for(table_cls)}
    assert specs["value"].sql_type == sql_type
    assert specs["value"].nullable is nullable


# ---------------------------------------------------------------- ddl


def test_create_table_sql() -> None:
    class TempReading(Table):
        id: int | None = field(pk=True, auto=True)
        sensor_id: str = field(index=True)
        tag: str = field(unique=True)
        celsius: float
        note: str | None = None

    sql = create_table_sql(TempReading.__tablename__, TempReading.columns())
    assert sql.startswith('CREATE TABLE IF NOT EXISTS "temp_reading" (')
    assert '"id" INTEGER PRIMARY KEY AUTOINCREMENT' in sql
    assert '"sensor_id" TEXT NOT NULL' in sql
    assert '"tag" TEXT NOT NULL UNIQUE' in sql
    assert '"celsius" REAL NOT NULL' in sql
    assert '"note" TEXT,' in sql or sql.endswith('"note" TEXT)')
    assert "NOT NULL UNIQUE" not in sql.split('"tag"')[0]  # unique only on tag


def test_non_auto_pk_has_no_autoincrement() -> None:
    class Device(Table):
        device_id: str = field(pk=True)
        name: str

    sql = create_table_sql(Device.__tablename__, Device.columns())
    assert '"device_id" TEXT PRIMARY KEY' in sql
    assert "AUTOINCREMENT" not in sql


def test_create_index_sql_only_for_plain_indexes() -> None:
    class Reading(Table):
        id: int | None = field(pk=True, auto=True)
        sensor_id: str = field(index=True)
        tag: str = field(unique=True)
        celsius: float

    statements = create_index_sql(Reading.__tablename__, Reading.columns())
    assert statements == [
        'CREATE INDEX IF NOT EXISTS "idx_reading_sensor_id" ON "reading" ("sensor_id")'
    ]

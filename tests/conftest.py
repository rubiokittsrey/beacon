import asyncio
from typing import Any

import pytest

from beacon.storage import registry


@pytest.fixture(autouse=True)
def clean_table_registry():
    # table declaration is import-time (subclassing registers), so tests
    # defining tables inline need a fresh registry each time
    registry.clear()
    yield
    registry.clear()


@pytest.fixture()
def command_queue() -> asyncio.Queue[Any]:
    return asyncio.Queue()


@pytest.fixture()
def message_queue() -> asyncio.Queue[Any]:
    return asyncio.Queue()

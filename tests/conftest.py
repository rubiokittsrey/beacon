from __future__ import annotations

import asyncio
from typing import Any

import pytest


@pytest.fixture()
def command_queue() -> asyncio.Queue[Any]:
    return asyncio.Queue()


@pytest.fixture()
def message_queue() -> asyncio.Queue[Any]:
    return asyncio.Queue()

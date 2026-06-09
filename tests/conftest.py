from __future__ import annotations

import asyncio
from typing import Any

import pytest


@pytest.fixture()
def incoming_queue() -> asyncio.Queue[Any]:
    return asyncio.Queue()


@pytest.fixture()
def outgoing_queue() -> asyncio.Queue[Any]:
    return asyncio.Queue()

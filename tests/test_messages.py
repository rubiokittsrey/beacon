from __future__ import annotations

import dataclasses

import pytest

from beacon.mqtt.messages import Message


class TestMessage:
    def test_json_parses_payload(self) -> None:
        msg = Message(topic="a/b", payload='{"v": 1}', timestamp=1.0, data=None)
        assert msg.json() == {"v": 1}

    @pytest.mark.parametrize("empty", [None, ""])
    def test_json_returns_none_for_empty_payload(self, empty: str | None) -> None:
        msg = Message(topic="a/b", payload=empty, timestamp=None, data=None)
        assert msg.json() is None

    def test_is_immutable(self) -> None:
        msg = Message(topic="a/b", payload="x", timestamp=None, data=None)
        with pytest.raises(dataclasses.FrozenInstanceError):
            msg.topic = "c/d"  # type: ignore[misc]

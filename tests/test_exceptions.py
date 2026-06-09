from __future__ import annotations

from beacon.core.exceptions import UnsupportedIntervalError


def test_is_a_value_error() -> None:
    assert issubclass(UnsupportedIntervalError, ValueError)


def test_message_includes_offending_value() -> None:
    err = UnsupportedIntervalError(-1.0)
    assert "-1.0" in str(err)

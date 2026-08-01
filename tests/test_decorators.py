from typing import Any

import pytest
from pydantic import BaseModel

from beacon.core.exceptions import UnsupportedIntervalError
from beacon.mqtt.decorators import MQTTBindings, PublisherSpec, SubscriptionSpec
from beacon.mqtt.messages import Message


class _Payload(BaseModel):
    value: float


async def _noop_handler(_msg: Message[Any]) -> None:
    return None


async def _noop_publisher() -> dict[str, Any]:
    return {}


class TestSubscribe:
    def test_registers_subscription_spec(self) -> None:
        bindings = MQTTBindings()

        returned = bindings.subscribe("sensors/temp", qos=1)(_noop_handler)

        assert returned is _noop_handler
        assert len(bindings.subscriptions) == 1
        spec = bindings.subscriptions[0]
        assert isinstance(spec, SubscriptionSpec)
        assert spec.topic == "sensors/temp"
        assert spec.qos == 1
        assert spec.handler is _noop_handler

    def test_default_qos_is_zero(self) -> None:
        bindings = MQTTBindings()
        bindings.subscribe("a/b")(_noop_handler)
        assert bindings.subscriptions[0].qos == 0

    def test_default_model_is_none(self) -> None:
        bindings = MQTTBindings()
        bindings.subscribe("a/b")(_noop_handler)
        assert bindings.subscriptions[0].model is None

    def test_model_is_stored_on_spec(self) -> None:
        bindings = MQTTBindings()
        bindings.subscribe("a/b", model=_Payload)(_noop_handler)
        assert bindings.subscriptions[0].model is _Payload

    def test_multiple_subscriptions_preserve_order(self) -> None:
        bindings = MQTTBindings()
        bindings.subscribe("first")(_noop_handler)
        bindings.subscribe("second")(_noop_handler)
        assert [s.topic for s in bindings.subscriptions] == ["first", "second"]


class TestPublisher:
    def test_registers_publisher_spec(self) -> None:
        bindings = MQTTBindings()

        returned = bindings.publisher("devices/heartbeat", qos=2, retain=True, every=5.0)(
            _noop_publisher
        )

        assert returned is _noop_publisher
        assert len(bindings.publishers) == 1
        spec = bindings.publishers[0]
        assert isinstance(spec, PublisherSpec)
        assert spec.topic == "devices/heartbeat"
        assert spec.qos == 2
        assert spec.retain is True
        assert spec.every_s == 5.0
        assert spec.fn is _noop_publisher

    def test_defaults(self) -> None:
        bindings = MQTTBindings()
        bindings.publisher("topic")(_noop_publisher)
        spec = bindings.publishers[0]
        assert spec.qos == 0
        assert spec.retain is False
        assert spec.every_s is None
        assert spec.model is None

    def test_model_is_stored_on_spec(self) -> None:
        bindings = MQTTBindings()
        bindings.publisher("topic", model=_Payload)(_noop_publisher)
        assert bindings.publishers[0].model is _Payload


class TestParseEvery:
    def test_none_stays_none(self) -> None:
        bindings = MQTTBindings()
        bindings.publisher("topic", every=None)(_noop_publisher)
        assert bindings.publishers[0].every_s is None

    def test_int_is_coerced_to_float(self) -> None:
        bindings = MQTTBindings()
        bindings.publisher("topic", every=3)(_noop_publisher)
        assert bindings.publishers[0].every_s == 3.0
        assert isinstance(bindings.publishers[0].every_s, float)

    @pytest.mark.parametrize("bad", [0, 0.0, -1, -2.5])
    def test_non_positive_raises(self, bad: float) -> None:
        bindings = MQTTBindings()
        with pytest.raises(UnsupportedIntervalError):
            bindings.publisher("topic", every=bad)(_noop_publisher)

    def test_non_number_raises(self) -> None:
        bindings = MQTTBindings()
        with pytest.raises(UnsupportedIntervalError):
            bindings.publisher("topic", every="fast")(_noop_publisher)  # type: ignore[arg-type]

    def test_failed_parse_does_not_register(self) -> None:
        bindings = MQTTBindings()
        with pytest.raises(UnsupportedIntervalError):
            bindings.publisher("topic", every=-1)(_noop_publisher)
        assert bindings.publishers == []


class TestPropertiesReturnCopies:
    def test_subscriptions_property_is_a_copy(self) -> None:
        bindings = MQTTBindings()
        bindings.subscribe("a")(_noop_handler)
        snapshot = bindings.subscriptions
        snapshot.clear()
        assert len(bindings.subscriptions) == 1

    def test_publishers_property_is_a_copy(self) -> None:
        bindings = MQTTBindings()
        bindings.publisher("a")(_noop_publisher)
        snapshot = bindings.publishers
        snapshot.clear()
        assert len(bindings.publishers) == 1

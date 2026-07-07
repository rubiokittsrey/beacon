from __future__ import annotations

from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from numbers import Real
from typing import TYPE_CHECKING, Any

from beacon.core.exceptions import UnsupportedIntervalError
from beacon.mqtt.messages import Message

if TYPE_CHECKING:
    from pydantic import BaseModel

# note: keep these aliases narrow and explicit for type checkers
Handler = Callable[[Message[Any]], Coroutine[Any, Any, None]]
PublisherFn = Callable[[], Awaitable[Any]]


@dataclass(frozen=True)
class SubscriptionSpec:
    """A topic subscription bound to a handler.

    `topic` is an mqtt filter and may contain wildcards (`+`, `#`); `model`,
    when set, validates inbound payloads before dispatch.
    """

    topic: str
    qos: int
    handler: Handler
    model: type[BaseModel] | None = None


@dataclass(frozen=True)
class PublisherSpec:
    """A topic publisher binding, optionally periodic via `every_s`.

    `model`, when set, validates and serializes the publisher's return value.
    """

    topic: str
    qos: int
    retain: bool
    every_s: float | None
    fn: PublisherFn
    model: type[BaseModel] | None = None


# normalizes the publisher `every` interval; rejects non-numbers and n <= 0
def _parse_every(every: float | None) -> float | None:
    if every is None:
        return None

    if not isinstance(every, Real):
        raise UnsupportedIntervalError(every)

    seconds = float(every)

    if seconds <= 0:
        raise UnsupportedIntervalError(every)

    return seconds


class MQTTBindings:
    """Registry of MQTT subscription and publisher bindings declared via decorators."""

    def __init__(self) -> None:
        self._subs: list[SubscriptionSpec] = []
        self._pubs: list[PublisherSpec] = []

    @property
    def subscriptions(self) -> list[SubscriptionSpec]:
        return list(self._subs)

    @property
    def publishers(self) -> list[PublisherSpec]:
        return list(self._pubs)

    def subscribe(
        self,
        topic: str,
        *,
        qos: int = 0,
        model: type[BaseModel] | None = None,
    ) -> Callable[[Handler], Handler]:
        """Register a handler for inbound messages on `topic`.

        Args:
            topic: MQTT topic filter; may contain wildcards (`+`, `#`).
            qos: Subscription QoS.
            model: Optional pydantic model; inbound payloads are validated
                against it and exposed as `msg.data`. Payloads that fail
                validation are logged and dropped.
        """

        def decorator(fn: Handler) -> Handler:
            self._subs.append(SubscriptionSpec(topic=topic, qos=qos, handler=fn, model=model))
            return fn

        return decorator

    def publisher(
        self,
        topic: str,
        *,
        qos: int = 0,
        retain: bool = False,
        every: float | None = None,
        model: type[BaseModel] | None = None,
    ) -> Callable[[PublisherFn], PublisherFn]:
        """Register a publisher for `topic`, run every `every` seconds when set.

        Args:
            topic: MQTT topic to publish to.
            qos: Publish QoS.
            retain: Set the MQTT retain flag on published messages.
            every: Interval in seconds for periodic publishing; `None` for a
                non-periodic binding.
            model: Optional pydantic model; the return value is validated
                against it and serialized with `model_dump_json()`. Values
                that fail validation are logged and not published.
        """

        every_s = _parse_every(every)

        def decorator(fn: PublisherFn) -> PublisherFn:
            self._pubs.append(
                PublisherSpec(
                    topic=topic,
                    qos=qos,
                    retain=retain,
                    every_s=every_s,
                    fn=fn,
                    model=model,
                )
            )
            return fn

        return decorator

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


# describes a topic subscription + handler binding
# `topic` is an mqtt topic filter and may contain wildcards (+, #)
# `model` (optional pydantic model) validates inbound payloads before dispatch
@dataclass(frozen=True)
class SubscriptionSpec:
    topic: str
    qos: int
    handler: Handler
    model: type[BaseModel] | None = None


# describes a topic publisher binding (optionally periodic)
# `model` (optional pydantic model) validates + serializes the return value
@dataclass(frozen=True)
class PublisherSpec:
    topic: str
    qos: int
    retain: bool
    every_s: float | None
    fn: PublisherFn
    model: type[BaseModel] | None = None


# parses `every` parameter of the publish decorator
# raises an UnsupportedIntervalError if the provider is not a number and n < 0
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
        """@bindings.subscribe("topic") -> registers a handler for inbound messages

        `topic` may contain mqtt wildcards (+, #). When `model` is given,
        inbound payloads are validated against it and exposed as `msg.data`;
        payloads that fail validation are logged and dropped.
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
        """@bindings.publisher("topic", every=1.0) -> registers a periodic publisher

        When `model` is given, the return value is validated against it and
        serialized with `model_dump_json()`; values that fail validation are
        logged and not published.
        """

        every_s = _parse_every(every)

        # records publisher metadata and returns the function unchanged
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

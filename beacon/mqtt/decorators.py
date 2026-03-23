from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from numbers import Real
from typing import Any

from beacon.core.exceptions import UnsupportedIntervalError

# note: keep these aliases narrow and explicit for type checkers
Handler = Callable[[dict[str, Any]], Awaitable[None]]
PublisherFn = Callable[[], Awaitable[Any]]

# describes a topic subscription + handler binding
@dataclass(frozen=True)
class SubscriptionSpec:
    topic: str
    qos: int
    handler: Handler

# describes a topic publisher binding (optionally periodic)
@dataclass(frozen=True)
class PublisherSpec:
    topic: str
    qos: int
    retain: bool
    every_s: float | None
    fn: PublisherFn

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

    def subscribe(self, topic: str, *, qos: int = 0) -> Callable[[Handler], Handler]:
        """@bindings.subscribe("topic") -> registers a handler for inbound messages"""

        def decorator(fn: Handler) -> Handler:
            self._subs.append(SubscriptionSpec(topic=topic, qos=qos, handler=fn))
            return fn

        return decorator

    def publisher(
        self,
        topic: str,
        *,
        qos: int = 0,
        retain: bool = False,
        every: float | None = None,
    ) -> Callable[[PublisherFn], PublisherFn]:
        """@bindings.publisher("topic", every=1.0) -> registers a periodic publisher"""

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
                )
            )
            return fn

        return decorator

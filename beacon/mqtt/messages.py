from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Message[T]:
    """Inbound mqtt message delivered to a subscription handler.

    `data` is the validated model instance when the subscription declared
    `model=`, otherwise `None`; use `json()` or `payload` for raw access.
    """

    topic: str
    payload: str | None
    timestamp: float | None
    data: T

    def json(self) -> Any:
        """Parse `payload` as JSON, or return `None` when it is empty."""
        return json.loads(self.payload) if self.payload else None

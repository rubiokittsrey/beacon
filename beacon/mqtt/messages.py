from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


# inbound mqtt message as delivered to subscription handlers
# `data` carries the validated model instance when the subscription declared
# `model=`, otherwise None - use `json()` or `payload` for raw access
@dataclass(frozen=True, slots=True)
class Message[T]:
    topic: str
    payload: str | None
    timestamp: float | None
    data: T

    def json(self) -> Any:
        return json.loads(self.payload) if self.payload else None

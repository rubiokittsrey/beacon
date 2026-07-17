from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel


def encode_json(obj: Any) -> str:
    """Serialize a value to a JSON string.

    Pydantic models go through `model_dump_json`; everything else through
    `json.dumps`.
    """
    if isinstance(obj, BaseModel):
        return obj.model_dump_json()
    return json.dumps(obj)

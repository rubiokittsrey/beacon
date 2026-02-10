from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class MQTTSubscribeCmd:
    type: Literal["subscribe"]
    topic: str
    qos: int = 0


@dataclass(frozen=True, slots=True)
class MQTTPublishCmd:
    type: Literal["publish"]
    topic: str
    payload: str
    qos: int = 0
    retain: bool = False


MQTTCmd = MQTTSubscribeCmd | MQTTPublishCmd
MQTT_QOS_MIN = 0
MQTT_QOS_MID = 1
MQTT_QOS_MAX = 2


class MQTTQueueProtocol:
    @staticmethod
    def _as_int(value: Any, *, default: int) -> int:
        if value is None:
            return default

        err = f"Expected int, got {type(value).__name__}"

        if isinstance(value, bool):
            raise TypeError(err)

        if isinstance(value, int):
            return value

        if isinstance(value, str):
            s = value.strip()
            if s.isdigit():
                return int(s)

        raise TypeError(err)

    @staticmethod
    def _as_bool(value: Any, *, default: bool) -> bool:
        if value is None:
            return default

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            v = value.strip().lower()
            if v in {"true", "1", "yes", "y", "on"}:
                return True
            if v in {"false", "0", "no", "n", "off"}:
                return False

        err = f"Expected bool, got {type(value).__name__}"
        raise TypeError(err)

    @staticmethod
    def parse_cmd(obj: Any) -> MQTTCmd | None:
        if not isinstance(obj, dict):
            return None

        cmd_type = obj.get("type")

        if cmd_type == "subscribe":
            topic = obj.get("topic")
            if not isinstance(topic, str) or not topic:
                return None

            try:
                qos = MQTTQueueProtocol._as_int(obj.get("qos"), default=0)
            except TypeError:
                return None

            if qos < 0 or qos > MQTT_QOS_MAX:
                return None

            return MQTTSubscribeCmd(type="subscribe", topic=topic, qos=qos)

        if cmd_type == "publish":
            topic = obj.get("topic")
            if not isinstance(topic, str) or not topic:
                return None

            payload = obj.get("payload", "")
            if payload is None:
                payload = ""
            if not isinstance(payload, str):
                payload = str(payload)

            try:
                qos = MQTTQueueProtocol._as_int(obj.get("qos"), default=0)
                retain = MQTTQueueProtocol._as_bool(obj.get("retain"), default=False)
            except TypeError:
                return None

            if qos < 0 or qos > MQTT_QOS_MAX:
                return None

            return MQTTPublishCmd(
                type="publish",
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
            )

        return None

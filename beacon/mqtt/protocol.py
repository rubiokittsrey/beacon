from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

MQTTCmdType = Literal["subscribe", "publish"]
MQTT_QOS_MIN = 0
MQTT_QOS_MID = 1
MQTT_QOS_MAX = 2


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


class MQTTQueueProtocol:
    @staticmethod
    def _validate_cmd_fields(obj: Any) -> tuple[bool, str, MQTTCmdType]:
        if not isinstance(obj, dict):
            return (False, "", "subscribe")

        cmd_type = obj.get("type")
        cmd_topic = obj.get("topic")

        if cmd_type not in ("subscribe", "publish"):
            return (False, "", "subscribe")

        if not isinstance(cmd_topic, str) or not cmd_topic:
            return (False, "", "subscribe")

        return (True, cmd_topic, cmd_type)

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
            try:
                return int(s)
            except ValueError as e:
                raise TypeError(err) from e

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
        valid, cmd_topic, cmd_type = MQTTQueueProtocol._validate_cmd_fields(obj)
        if not valid:
            return None

        qos = MQTTQueueProtocol._as_int(obj.get("qos"), default=0)
        if not (MQTT_QOS_MIN <= qos <= MQTT_QOS_MAX):
            return None

        if cmd_type == "subscribe":
            return MQTTSubscribeCmd(type="subscribe", topic=cmd_topic, qos=qos)

        if cmd_type == "publish":
            payload = obj.get("payload") or ""
            if not isinstance(payload, str):
                payload = str(payload)

        retain = MQTTQueueProtocol._as_bool(obj.get("retain"), default=False)

        return MQTTPublishCmd(
            type="publish",
            topic=cmd_topic,
            payload=payload,
            qos=qos,
            retain=retain,
        )

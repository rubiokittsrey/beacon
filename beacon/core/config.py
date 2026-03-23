from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import yaml
from pydantic import BaseModel

if TYPE_CHECKING:
    from pathlib import Path


class MQTTAuthConfig(BaseModel):
    username: str | None = None
    password: str | None = None


class MQTTConfig(BaseModel):
    host: str = "localhost"
    port: int = 1883
    keepalive: int = 60
    auth: MQTTAuthConfig = MQTTAuthConfig()


class LoggingConfig(BaseModel):
    level: str = "DEBUG"
    console: bool = True
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5

    @property
    def log_level(self) -> int:
        return getattr(logging, self.level.upper(), logging.DEBUG)


class BeaconConfig(BaseModel):
    mqtt: MQTTConfig = MQTTConfig()
    logging: LoggingConfig = LoggingConfig()


def load_config(path: Path) -> BeaconConfig:
    if not path.exists():
        return BeaconConfig()

    raw = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw)

    if not isinstance(data, dict):
        return BeaconConfig()

    return BeaconConfig.model_validate(data)

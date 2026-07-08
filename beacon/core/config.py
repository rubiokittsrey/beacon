from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import yaml
from pydantic import BaseModel

if TYPE_CHECKING:
    from pathlib import Path


class MQTTAuthConfig(BaseModel):
    """MQTT username and password; both optional."""

    username: str | None = None
    password: str | None = None


class MQTTConfig(BaseModel):
    """MQTT broker connection settings."""

    host: str = "localhost"
    port: int = 1883
    keepalive: int = 60
    auth: MQTTAuthConfig = MQTTAuthConfig()


class LoggingConfig(BaseModel):
    """Logging configuration: level, file rotation, and console output."""

    level: str = "DEBUG"
    console: bool = True
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5

    @property
    def log_level(self) -> int:
        return getattr(logging, self.level.upper(), logging.DEBUG)


class StorageConfig(BaseModel):
    """SQLite storage settings; `path` may be `:memory:` for an ephemeral db."""

    path: str = "beacon.db"


class BeaconConfig(BaseModel):
    """Top-level Beacon configuration loaded from YAML."""

    mqtt: MQTTConfig = MQTTConfig()
    logging: LoggingConfig = LoggingConfig()
    storage: StorageConfig = StorageConfig()


def load_config(path: Path) -> BeaconConfig:
    """Load and validate `BeaconConfig` from a YAML file, or defaults if absent."""
    if not path.exists():
        return BeaconConfig()

    raw = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw)

    if not isinstance(data, dict):
        return BeaconConfig()

    return BeaconConfig.model_validate(data)

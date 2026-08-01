import logging
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel


class MQTTAuthConfig(BaseModel):
    """MQTT username and password; both optional."""

    username: str | None = None
    password: str | None = None


class MQTTConfig(BaseModel):
    """MQTT broker connection settings and inbound flow limits.

    `message_queue_size` bounds the inbound message backlog (oldest are
    dropped once full); `max_concurrent_handlers` caps how many handler
    tasks run at once.
    """

    host: str = "localhost"
    port: int = 1883
    keepalive: int = 60
    auth: MQTTAuthConfig = MQTTAuthConfig()
    message_queue_size: int = 1000
    max_concurrent_handlers: int = 64


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
    """SQLite storage settings; `path` may be `:memory:` for an ephemeral db.

    `commit_delay` is the group-commit coalescing window in seconds: writes
    landing within it share one commit.
    """

    path: str = "beacon.db"
    commit_delay: float = 0.01


class UplinkHTTPConfig(BaseModel):
    """HTTP transport settings: where batches are POSTed and how long to wait."""

    base_url: str = "http://localhost:8000"
    endpoint: str = "/ingest"
    headers: dict[str, str] = {}
    timeout: float = 10.0


class UplinkRetryConfig(BaseModel):
    """Retry policy for failed send attempts.

    `min_seconds`/`max_seconds` bound the exponential backoff between
    attempts; `max_attempts` caps how many times a record the server keeps
    rejecting is retried before it is buried, so one persistently failing
    batch cannot block every record behind it forever (0 retries without
    limit). Sends that never reach the server cost no attempts, so an
    outage never buries anything however long it lasts.
    """

    min_seconds: float = 1.0
    max_seconds: float = 60.0
    max_attempts: int = 10


class UplinkBufferConfig(BaseModel):
    """Outbound buffer flow limits.

    `batch_size` records are claimed per send; `flush_interval` is how often
    the worker polls for work when idle; past `max_records` buffered rows,
    the oldest pending records are dropped to make room.
    """

    batch_size: int = 50
    flush_interval: float = 1.0
    max_records: int = 10_000
    retry: UplinkRetryConfig = UplinkRetryConfig()


class UplinkConfig(BaseModel):
    """Store-and-forward uplink settings; disabled by default."""

    enabled: bool = False
    transport: Literal["http"] = "http"
    http: UplinkHTTPConfig = UplinkHTTPConfig()
    buffer: UplinkBufferConfig = UplinkBufferConfig()


class BeaconConfig(BaseModel):
    """Top-level Beacon configuration loaded from YAML."""

    mqtt: MQTTConfig = MQTTConfig()
    logging: LoggingConfig = LoggingConfig()
    storage: StorageConfig = StorageConfig()
    uplink: UplinkConfig = UplinkConfig()


def load_config(path: Path) -> BeaconConfig:
    """Load and validate `BeaconConfig` from a YAML file, or defaults if absent."""
    if not path.exists():
        return BeaconConfig()

    raw = path.read_text(encoding="utf-8")
    data = yaml.safe_load(raw)

    if not isinstance(data, dict):
        return BeaconConfig()

    return BeaconConfig.model_validate(data)

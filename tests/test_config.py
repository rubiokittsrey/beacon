import logging
from pathlib import Path

import pytest
from pydantic import ValidationError

from beacon.core.config import (
    BeaconConfig,
    LoggingConfig,
    MQTTAuthConfig,
    MQTTConfig,
    StorageConfig,
    UplinkConfig,
    load_config,
)


class TestDefaults:
    def test_beacon_config_defaults(self) -> None:
        cfg = BeaconConfig()
        assert isinstance(cfg.mqtt, MQTTConfig)
        assert isinstance(cfg.logging, LoggingConfig)
        assert isinstance(cfg.storage, StorageConfig)

    def test_storage_config_defaults(self) -> None:
        assert StorageConfig().path == "beacon.db"

    def test_uplink_config_defaults(self) -> None:
        uplink = UplinkConfig()
        assert uplink.enabled is False
        assert uplink.transport == "http"
        assert uplink.http.base_url == "http://localhost:8000"
        assert uplink.http.endpoint == "/ingest"
        assert uplink.http.timeout == 10.0
        assert uplink.buffer.batch_size == 50
        assert uplink.buffer.flush_interval == 1.0
        assert uplink.buffer.max_records == 10_000
        assert uplink.buffer.retry.min_seconds == 1.0
        assert uplink.buffer.retry.max_seconds == 60.0
        assert uplink.buffer.retry.max_attempts == 10

    def test_mqtt_config_defaults(self) -> None:
        mqtt = MQTTConfig()
        assert mqtt.host == "localhost"
        assert mqtt.port == 1883
        assert mqtt.keepalive == 60
        assert mqtt.auth.username is None
        assert mqtt.auth.password is None

    def test_logging_config_defaults(self) -> None:
        log = LoggingConfig()
        assert log.level == "DEBUG"
        assert log.console is True
        assert log.max_bytes == 10 * 1024 * 1024
        assert log.backup_count == 5


class TestLogLevelProperty:
    def test_known_level_resolves(self) -> None:
        assert LoggingConfig(level="INFO").log_level == logging.INFO

    def test_level_is_case_insensitive(self) -> None:
        assert LoggingConfig(level="warning").log_level == logging.WARNING

    def test_unknown_level_falls_back_to_debug(self) -> None:
        assert LoggingConfig(level="NOPE").log_level == logging.DEBUG


class TestLoadConfig:
    def test_missing_file_returns_defaults(self, tmp_path: Path) -> None:
        cfg = load_config(tmp_path / "does-not-exist.yaml")
        assert cfg == BeaconConfig()

    def test_valid_yaml_is_parsed(self, tmp_path: Path) -> None:
        path = tmp_path / "beacon.yaml"
        path.write_text(
            "mqtt:\n"
            "  host: broker.local\n"
            "  port: 8883\n"
            "  auth:\n"
            "    username: bob\n"
            "    password: secret\n"
            "logging:\n"
            "  level: INFO\n"
            "  console: false\n"
            "storage:\n"
            "  path: /var/lib/beacon.db\n",
            encoding="utf-8",
        )

        cfg = load_config(path)

        assert cfg.mqtt.host == "broker.local"
        assert cfg.mqtt.port == 8883
        assert cfg.mqtt.auth == MQTTAuthConfig(username="bob", password="secret")
        assert cfg.logging.level == "INFO"
        assert cfg.logging.console is False
        assert cfg.storage.path == "/var/lib/beacon.db"

    def test_partial_yaml_keeps_defaults_for_missing_keys(self, tmp_path: Path) -> None:
        path = tmp_path / "beacon.yaml"
        path.write_text("mqtt:\n  host: only-host\n", encoding="utf-8")

        cfg = load_config(path)

        assert cfg.mqtt.host == "only-host"
        assert cfg.mqtt.port == 1883
        assert cfg.logging == LoggingConfig()

    def test_empty_file_returns_defaults(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.yaml"
        path.write_text("", encoding="utf-8")
        assert load_config(path) == BeaconConfig()

    def test_non_mapping_yaml_returns_defaults(self, tmp_path: Path) -> None:
        path = tmp_path / "list.yaml"
        path.write_text("- a\n- b\n", encoding="utf-8")
        assert load_config(path) == BeaconConfig()


class TestUplinkConfig:
    def test_transport_rejects_unknown_value(self) -> None:
        with pytest.raises(ValidationError):
            UplinkConfig(transport="mqtt")

    def test_uplink_yaml_is_parsed(self, tmp_path: Path) -> None:
        path = tmp_path / "beacon.yaml"
        path.write_text(
            "uplink:\n"
            "  enabled: true\n"
            "  http:\n"
            "    base_url: https://ingest.example\n"
            "    endpoint: /v1/records\n"
            "    timeout: 3.5\n"
            "  buffer:\n"
            "    batch_size: 25\n"
            "    max_records: 500\n"
            "    retry:\n"
            "      min_seconds: 2\n"
            "      max_seconds: 30\n"
            "      max_attempts: 4\n",
            encoding="utf-8",
        )

        uplink = load_config(path).uplink

        assert uplink.enabled is True
        assert uplink.http.base_url == "https://ingest.example"
        assert uplink.http.endpoint == "/v1/records"
        assert uplink.http.timeout == 3.5
        assert uplink.buffer.batch_size == 25
        assert uplink.buffer.max_records == 500
        assert uplink.buffer.retry.min_seconds == 2.0
        assert uplink.buffer.retry.max_seconds == 30.0
        assert uplink.buffer.retry.max_attempts == 4

    def test_uplink_defaults_when_absent(self, tmp_path: Path) -> None:
        path = tmp_path / "beacon.yaml"
        path.write_text("mqtt:\n  host: only-host\n", encoding="utf-8")
        assert load_config(path).uplink == UplinkConfig()

from __future__ import annotations

import logging
from pathlib import Path

from beacon.core.config import (
    BeaconConfig,
    LoggingConfig,
    MQTTAuthConfig,
    MQTTConfig,
    load_config,
)


class TestDefaults:
    def test_beacon_config_defaults(self) -> None:
        cfg = BeaconConfig()
        assert isinstance(cfg.mqtt, MQTTConfig)
        assert isinstance(cfg.logging, LoggingConfig)

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
            "  console: false\n",
            encoding="utf-8",
        )

        cfg = load_config(path)

        assert cfg.mqtt.host == "broker.local"
        assert cfg.mqtt.port == 8883
        assert cfg.mqtt.auth == MQTTAuthConfig(username="bob", password="secret")
        assert cfg.logging.level == "INFO"
        assert cfg.logging.console is False

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

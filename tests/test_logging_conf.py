from __future__ import annotations

import logging
from pathlib import Path

from beacon.utils.logging_conf import AsyncLogging, LoggingConfig, new_run_log_dir


class TestNewRunLogDir:
    def test_creates_directory(self, tmp_path: Path) -> None:
        run_dir = new_run_log_dir(tmp_path)
        assert run_dir.exists()
        assert run_dir.parent == tmp_path

    def test_collisions_get_suffixed(self, tmp_path: Path) -> None:
        first = new_run_log_dir(tmp_path)
        second = new_run_log_dir(tmp_path)
        third = new_run_log_dir(tmp_path)

        dirs = {first, second, third}
        assert len(dirs) == 3
        assert second.name == f"{first.name}_1"
        assert third.name == f"{first.name}_2"


class TestAsyncLogging:
    def test_start_creates_log_file_parent_and_writes(self, tmp_path: Path) -> None:
        log_file = tmp_path / "nested" / "beacon.log"
        async_logging = AsyncLogging(
            LoggingConfig(log_file=log_file, console=False, level=logging.INFO)
        )

        async_logging.start()
        try:
            logging.getLogger("beacon.test").info("hello world")
        finally:
            async_logging.stop()

        assert log_file.exists()
        assert "hello world" in log_file.read_text(encoding="utf-8")

    def test_stop_is_idempotent(self, tmp_path: Path) -> None:
        async_logging = AsyncLogging(
            LoggingConfig(log_file=tmp_path / "beacon.log", console=False)
        )
        async_logging.start()
        async_logging.stop()
        async_logging.stop()  # should not raise

    def test_context_manager(self, tmp_path: Path) -> None:
        log_file = tmp_path / "ctx.log"
        with AsyncLogging(LoggingConfig(log_file=log_file, console=False)) as al:
            assert al._listener is not None
        assert al._listener is None

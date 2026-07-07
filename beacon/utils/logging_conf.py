import logging
import logging.handlers
import queue
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Self


@dataclass(slots=True)
class LoggingConfig:
    """Configuration for `AsyncLogging`: level, format, and file rotation."""

    log_file: Path
    level: int = logging.DEBUG
    fmt: str = "%(asctime)s - %(levelname)s - %(name)s:%(funcName)s - %(message)s"
    datefmt: str = "%Y-%m-%d %H:%M:%S"
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5
    console: bool = True


class AsyncLogging:
    """Routes logging through a background queue listener so calls don't block."""

    def __init__(self, config: LoggingConfig):
        self.config = config
        self._queue: queue.Queue[logging.LogRecord] = queue.Queue(-1)
        self._listener: logging.handlers.QueueListener | None = None

    def start(self) -> None:
        """Install the queue handler and start the background listener."""
        root = logging.getLogger()
        root.setLevel(self.config.level)
        root.handlers.clear()

        formatter = logging.Formatter(
            self.config.fmt,
            datefmt=self.config.datefmt,
        )

        root.addHandler(logging.handlers.QueueHandler(self._queue))

        self.config.log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            self.config.log_file,
            maxBytes=self.config.max_bytes,
            backupCount=self.config.backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)

        handlers: list[logging.Handler] = [file_handler]

        if self.config.console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            handlers.append(console_handler)

        self._listener = logging.handlers.QueueListener(
            self._queue,
            *handlers,
            respect_handler_level=True,
        )
        self._listener.start()

    def stop(self) -> None:
        """Stop the background listener (idempotent)."""
        if self._listener is not None:
            self._listener.stop()
            self._listener = None

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


def new_run_log_dir(base: Path) -> Path:
    """Create and return a unique per-run log directory under `base`."""
    ts = datetime.now(UTC).strftime("%Y-%m-%d")
    run_dir = base / ts

    counter = 1
    while run_dir.exists():
        run_dir = base / f"{ts}_{counter}"
        counter += 1

    run_dir.mkdir(parents=True)
    return run_dir

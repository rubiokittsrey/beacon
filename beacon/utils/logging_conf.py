import logging
import logging.handlers
import queue
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Self


# the asynclogging config dataclass
@dataclass(slots=True)
class LoggingConfig:
    log_file: Path
    level: int = logging.DEBUG
    fmt: str = "%(asctime)s - %(levelname)s - %(name)s:%(funcName)s - %(message)s"
    datefmt: str = "%Y-%m-%d %H:%M:%S"
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5
    console: bool = True


class AsyncLogging:
    """
    Async-safe logging setup using QueueHandler/Listener.

    - Configures the root logger and funnels all log records into a thread-safe queue
    - Run I/O handlers (file / console) behind a QueueListener
    """

    def __init__(self, config: LoggingConfig):
        self.config = config
        self._queue: queue.Queue[logging.LogRecord] = queue.Queue(-1)
        self._listener: logging.handlers.QueueListener | None = None

    def start(self) -> None:
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
        if self._listener is not None:
            self._listener.stop()
            self._listener = None

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()


# generate unique log directories per run
def new_run_log_dir(base: Path) -> Path:
    ts = datetime.now(UTC).strftime("%Y-%m-%d")
    run_dir = base / ts

    counter = 1
    while run_dir.exists():
        run_dir = base / f"{ts}_{counter}"
        counter += 1

    run_dir.mkdir(parents=True)
    return run_dir

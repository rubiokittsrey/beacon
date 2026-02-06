import logging
import logging.handlers
from multiprocessing import Queue
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "beacon.log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FORMATTER = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(processName)s (%(process)d) - %(name)s:%(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
"""
OUTPUT:
2025-02-06 14:23:45 - INFO - MainProcess (12345) - module.submodule:function - Message...
"""


# setup logging for main proc
def setup_logging(queue: Queue = None):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    if queue is not None:
        # if using multiprocessing, route main process logs through queue too
        queue_handler = logging.handlers.QueueHandler(queue)
        root.addHandler(queue_handler)
    else:
        # single proc mode, log directly to file and console
        file_handler = logging.FileHandler(LOG_FILE)
        console_handler = logging.StreamHandler()

        file_handler.setFormatter(LOG_FORMATTER)
        console_handler.setFormatter(LOG_FORMATTER)

        root.addHandler(file_handler)
        root.addHandler(console_handler)


# called in each worker process to route logs through the queue
def setup_worker_logger(queue: Queue):
    root = logging.getLogger()
    root.handlers.clear()

    handler = logging.handlers.QueueHandler(queue)
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)


# call in main process to listen for logs from all processes
def setup_listener(queue: Queue):
    file_handler = logging.FileHandler(LOG_FILE)
    console_handler = logging.StreamHandler()

    file_handler.setFormatter(LOG_FORMATTER)
    console_handler.setFormatter(LOG_FORMATTER)

    listener = logging.handlers.QueueListener(
        queue, file_handler, console_handler, respect_handler_level=True
    )
    listener.start()
    return listener

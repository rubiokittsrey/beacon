import asyncio
import logging
import multiprocessing
import signal
from collections.abc import Callable
from pathlib import Path

from beacon.mqtt.client import MQTTClient
from beacon.utils.logging_config import setup_listener, setup_logging

# temporary config vars


class Beacon:
    """
    Main Beacon application class.

    Example:
        app = Beacon("my-gateway")
        app.config.MQTT_BROKER = "localhost"

        @app.mqtt.subscribe("/sensors/+/data")
        async def handle_sensor(message):
            await app.storage.save(message.json())
        app.run()
    """

    def __init__(self, name: str, config_path: Path | None = None):
        self.name = name
        self.config_path = config_path or Path("beacon.yaml")

        # core components

        # clients
        # every client has its own sub process
        self.mqtt: MQTTClient | None = None
        self.mqtt_process: multiprocessing.Process | None = None

        # a dictionary of the client processes
        self.client_processes: dict[str, multiprocessing.Process] = {
            "MQTTClient": self.mqtt_process
        }

        self._running = False
        self._tasks: list[asyncio.Task] = []

        # logging
        self.logger = logging.getLogger(__name__)
        self._log_queue = multiprocessing.Queue()

    async def _load_config(self):
        self.logger.warning("Config not setup")

    async def start(self):
        # setup logging first
        logs_listener = setup_listener(self._log_queue)
        setup_logging(self._log_queue)

        self.logger.info("Starting %s", self.name)

        # load configs
        await self._load_config()

        # initialize components
        await self._init_mqtt()

        # run sub processes
        for proc_key in self.client_processes:
            self.client_processes[proc_key] = multiprocessing.Process(
                target=self.mqtt.start, kwargs={"log_queue": self._log_queue}, name="MQTTClient"
            )
            self.client_processes[proc_key].start()

        # setup signal handler
        def signal_handler(signum, frame):
            self.logger.info("Received shutdown signal")

            # shutdown clients here
            self.mqtt.shutdown()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while self.mqtt_process.is_alive():
                await asyncio.sleep(1)

                if not self.mqtt.data_queue.empty():
                    message = self.mqtt.data_queue.get()
                    self.logger.info("Received message from MQTT: %s", message)

            self.logger.warning("MQTT Process terminated")

        except KeyboardInterrupt:
            self.logger.info("Shutting down gracefully")
            self.mqtt.shutdown()

        finally:
            # Wait for clean shutdown
            for proc_key in self.client_processes:
                self.client_processes[proc_key].join(timeout=5)

                if self.client_processes[proc_key].is_alive():
                    self.logger.warning(
                        "%s process did not terminate, forcing shutdown",
                        self.client_processes[proc_key].name,
                    )
                    self.client_processes[proc_key].terminate()
                    self.client_processes[proc_key].join()

            # stop logging listener
            logs_listener.stop()

            self.logger.info("Shutdown complete")

    # initialize MQTTClient object
    async def _init_mqtt(self):
        self.mqtt = MQTTClient(
            pw=None,
            uname=None,
            id=f"{self.name}-mqtt-client",  # Fixed f-string syntax
        )
        self.logger.info("MQTT client initialized")


if __name__ == "__main__":
    beacon_app = Beacon(
        "beacon-test",
    )

    asyncio.run(beacon_app.start())

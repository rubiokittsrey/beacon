import logging
import multiprocessing
import time
from typing import Any

from paho.mqtt import client as paho_mqtt
from paho.mqtt import enums

from beacon.utils.logging_config import setup_worker_logger


class MQTTClient:
    def __init__(self, pw: str | None, uname: str | None, id: str = "beacon-mqtt-client"):
        self.id = id
        self.uname = uname
        self.pw = pw

        # queues
        self.command_queue = multiprocessing.Queue()
        self.data_queue = multiprocessing.Queue()

        # shutdown event for clean termination
        self._shutdown_event = multiprocessing.Event()

        self.client = paho_mqtt.Client(
            callback_api_version=enums.CallbackAPIVersion.VERSION2,
            client_id=id,
            protocol=paho_mqtt.MQTTv311,
        )

        # set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

        self.log_queue: multiprocessing.Queue | None = None
        self.logger = logging.getLogger(__name__)

    def start(self, log_queue: multiprocessing.Queue):
        # setup logger for this sub process
        self.log_queue = log_queue
        setup_worker_logger(self.log_queue)

        self.logger.info("MQTT Client starting with ID: %s", self.id)

        if self.pw and self.uname:
            self.client.username_pw_set(self.uname, self.pw)

        self._connect_loop()

        # keep process alive until shutdown signal
        try:
            while not self._shutdown_event.is_set():
                time.sleep(0.1)
        except KeyboardInterrupt:
            self.logger.info("MQTT Client received KeyboardInterrupt")
        finally:
            self.logger.info("MQTT Client shutting down")
            self.client.disconnect()
            time.sleep(0.2)
            self.client.disconnect()

    def shutdown(self):
        self._shutdown_event.set()

    def _connect_loop(self):
        try:
            self.client.reconnect_delay_set(min_delay=1, max_delay=60)
            self.client.connect("localhost", 1883, 60)
            self.client.loop_start()
        except Exception:
            self.logger.exception("Could not connect to broker")

    def _temp_handler(
        self, client: paho_mqtt.Client, userdata: Any, message: paho_mqtt.MQTTMessage
    ):
        self.logger.debug(
            "Message received on topic %s: %s", message.topic, message.payload.decode()
        )

        # example: put message in queue for other processes
        self.data_queue.put(
            {"topic": message.topic, "payload": message.payload.decode(), "timestamp": time.time()}
        )

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.logger.info("Connected to the broker")

            client.message_callback_add("test", self._temp_handler)
            client.subscribe("test")

        else:
            self.logger.error("Failed to connect: %s", reason_code)

    # TODO: handle disconnects by reconnecting the client
    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.logger.warning("Disconnected from broker: %s", reason_code)

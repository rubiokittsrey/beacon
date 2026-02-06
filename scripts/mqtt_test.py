import logging
import time

from paho.mqtt import client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC = "test"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def on_connect(client: mqtt.Client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to broker")
        client.subscribe(TOPIC)
        client.publish(TOPIC, "hello from paho")
    else:
        logger.error("Connection failed with code %s", rc)


def on_message(client: mqtt.Client, userdata, msg):
    logger.info("Received: topic=%s payload=%s", msg.topic, msg.payload.decode())


def main():
    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT, keepalive=60)

    # network loop
    client.loop_start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

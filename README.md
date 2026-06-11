# Beacon

An async Python framework for building MQTT-connected services. Beacon handles the runtime plumbing — connection management, message routing, periodic publishing, logging, and graceful shutdown — so an app is just a config file and a few decorated functions.

## Features

- **Decorator DSL** — bind handlers to topics with `@app.bindings.subscribe(...)` and register periodic publishers with `@app.bindings.publisher(..., every=...)`
- **Resilient MQTT client** — built on paho-mqtt with background connection retry and backoff; starting before the broker is up is fine
- **YAML configuration** — Pydantic-validated, with sane defaults when the file is missing
- **Async logging** — rotating file logs in a fresh per-run directory under `logs/`, plus optional console output
- **Graceful lifecycle** — SIGINT/SIGTERM handlers, tracked asyncio tasks, idempotent shutdown

## Requirements

- Python >= 3.14
- An MQTT broker (e.g. [Mosquitto](https://mosquitto.org/)) reachable from the app

## Installation

```bash
poetry install
```

## Quickstart

Create a `beacon.yaml` (see [`beacon.example.yaml`](beacon.example.yaml)):

```yaml
mqtt:
  host: localhost
  port: 1883
  keepalive: 60
  auth:
    username: null
    password: null

logging:
  level: DEBUG
  console: true
```

Then define your app:

```python
import asyncio
from datetime import UTC, datetime
from typing import Any

from beacon.core.app import Beacon

app = Beacon(name="my-device")


# handle inbound messages on a topic
@app.bindings.subscribe("sensors/temperature", qos=1)
async def on_temperature(msg: dict[str, Any]) -> None:
    # msg keys: topic, payload (str), timestamp (float), json (callable)
    reading = msg["json"]()
    print(f"temperature: {reading}")


# publish a JSON payload every 5 seconds
@app.bindings.publisher("devices/heartbeat", every=5.0)
async def heartbeat() -> dict[str, Any]:
    return {
        "device": app.name,
        "status": "online",
        "timestamp": datetime.now(UTC).isoformat(),
    }


if __name__ == "__main__":
    asyncio.run(app.start())  # blocks until SIGINT/SIGTERM
```

`start()` loads the config, sets up logging and signal handlers, connects to the broker, registers subscriptions, and runs periodic publishers until shutdown is requested. Ctrl-C exits cleanly.

## How it works

Two asyncio queues connect the app to the MQTT client, which runs alongside paho's network thread:

- `app.mqtt_command_queue` — subscribe/publish commands flowing **to** the client
- `app.mqtt_message_queue` — broker messages flowing **back** to your handlers

Each inbound message is dispatched to its topic's handler as its own asyncio task, so a slow handler never blocks the message loop.

## Project layout

```
beacon/
├── core/
│   ├── app.py          # Beacon orchestrator: lifecycle, routing, tasks
│   ├── config.py       # Pydantic models + YAML loading
│   └── exceptions.py   # framework exceptions
├── mqtt/
│   ├── client.py       # paho-mqtt wrapper with retry/backoff
│   └── decorators.py   # subscribe/publisher binding DSL
└── utils/
    └── logging_conf.py # async logging with per-run log dirs
```

A runnable tour of the full API lives in [`scripts/usage_guide.py`](scripts/usage_guide.py):

```bash
poetry run python scripts/usage_guide.py
```

## Development

```bash
poetry run pytest          # run the test suite
poetry run ruff check .    # lint
poetry run mypy beacon     # type check
```

## License

MIT

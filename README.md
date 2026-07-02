# Beacon

An async Python framework for building MQTT-connected services. Beacon handles the runtime plumbing — connection management, message routing, periodic publishing, logging, and graceful shutdown — so an app is just a config file and a few decorated functions.

## Features

- **Decorator DSL** — bind handlers to topic filters (wildcards `+`/`#` supported) with `@app.bindings.subscribe(...)` and register periodic publishers with `@app.bindings.publisher(..., every=...)`
- **Typed payloads** — declare a Pydantic model per binding with `model=`; inbound payloads are validated before your handler runs (invalid ones are logged and dropped), outbound ones are serialized with `model_dump_json()`
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

from pydantic import BaseModel

from beacon.core.app import Beacon
from beacon.mqtt import Message

app = Beacon(name="my-device")


class TempReading(BaseModel):
    sensor_id: str
    celsius: float


# typed subscription: payloads are validated against the model before your
# handler runs; invalid ones are logged and dropped. Wildcards route.
@app.bindings.subscribe("sensors/+/temperature", qos=1, model=TempReading)
async def on_temperature(msg: Message[TempReading]) -> None:
    print(f"{msg.data.sensor_id}: {msg.data.celsius}C (topic={msg.topic})")


# untyped subscription: msg.data is None, use msg.json() or msg.payload
@app.bindings.subscribe("devices/announce")
async def on_announce(msg: Message[None]) -> None:
    print(f"announce: {msg.json()}")


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

Publishers can also declare `model=` to validate and serialize their return value via `model_dump_json()` — which handles `datetime`, enums, and nested models. A bare `BaseModel` return is serialized the same way even without a declared model.

## How it works

Two asyncio queues connect the app to the MQTT client, which runs alongside paho's network thread:

- `app.mqtt_command_queue` — subscribe/publish commands flowing **to** the client
- `app.mqtt_message_queue` — broker messages flowing **back** to your handlers

Inbound messages are matched against every registered topic filter (so `sensors/+/temperature` receives `sensors/kitchen/temperature`, and overlapping filters each fire). Multiple handlers may bind to the same filter — each receives the message, and the broker subscription uses the highest qos among them. Each handler runs as its own asyncio task, so a slow handler never blocks the message loop. Payloads that fail model validation never reach a handler — they are logged at WARNING and dropped.

## Project layout

```
beacon/
├── core/
│   ├── app.py          # Beacon orchestrator: lifecycle, routing, tasks
│   ├── config.py       # Pydantic models + YAML loading
│   └── exceptions.py   # framework exceptions
├── mqtt/
│   ├── client.py       # paho-mqtt wrapper with retry/backoff
│   ├── decorators.py   # subscribe/publisher binding DSL
│   └── messages.py     # Message[T] delivered to handlers
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

# Beacon

An async Python framework for building MQTT-connected services. Beacon handles the runtime plumbing — connection management, message routing, periodic publishing, logging, and graceful shutdown — so an app is just a config file and a few decorated functions.

## Features

- **Decorator DSL** — bind handlers to topic filters (wildcards `+`/`#` supported) with `@app.bindings.subscribe(...)` and register publishers with `@app.bindings.publisher(...)`, periodic via `every=` or ad-hoc via `await app.publish(...)`
- **Typed payloads** — declare a Pydantic model per binding with `model=`; inbound payloads are validated before your handler runs (invalid ones are logged and dropped), outbound ones are serialized with `model_dump_json()`
- **Pydantic-native storage** — subclass `Table` and the same model validates payloads *and* defines a SQLite schema; an async active-record API (`save`/`get`/`filter`) with Django-style lookups over aiosqlite (WAL, additive migrations)
- **Store-and-forward uplink** — `await app.uplink.enqueue(...)` buffers a message durably and returns; a background worker POSTs batches to your ingest endpoint, retries with exponential backoff while the network is down, and buries poison instead of blocking behind it
- **Resilient MQTT client** — built on paho-mqtt with background connection retry and backoff; starting before the broker is up is fine
- **Bounded under burst** — capped concurrent handlers, a bounded inbound queue, and drop-oldest shedding at the edge; storage group-commits so a burst of writes shares one fsync
- **YAML configuration** — Pydantic-validated, with sane defaults when the file is missing
- **Async logging** — rotating file logs in a fresh per-run directory under `logs/`, plus optional console output
- **Graceful lifecycle** — SIGINT/SIGTERM handlers, tracked asyncio tasks, idempotent shutdown

## Requirements

- Python >= 3.14
- An MQTT broker (e.g. [Mosquitto](https://mosquitto.org/)) reachable from the app

## Installation

```bash
uv sync
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

storage:
  path: beacon.db  # only used when a Table is declared; ":memory:" also works
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

To publish outside a schedule — from a handler, on startup, in response to anything — call `app.publish()`:

```python
@app.bindings.subscribe("commands/reboot")
async def on_reboot(msg: Message[None]) -> None:
    await app.publish("devices/status", {"device": app.name, "state": "rebooting"}, qos=1)
```

Or bind the topic once by leaving `every` off, and publish by calling the function:

```python
class DeviceStatus(BaseModel):
    device: str
    state: str


# no `every`: nothing schedules this, calling it publishes
@app.bindings.publisher("devices/status", qos=1, model=DeviceStatus)
async def announce() -> DeviceStatus:
    return DeviceStatus(device=app.name, state="online")


await announce()  # published with the topic, qos, retain, and model above
```

Either way the call returns once the message is queued for the client, not once the broker has it — and a publish issued while the connection is down is dropped (counted, and logged at WARNING on a rate limit). For data that must survive an outage, use the uplink.

## Storage

Beacon includes an optional Pydantic-native storage layer: subclass `Table` and the same model both validates MQTT payloads and defines a SQLite table — declared once, used at both edges.

```python
from datetime import datetime

from beacon.mqtt import Message
from beacon.storage import Table, field

app = Beacon(name="my-device")


class TempReading(Table):
    id: int | None = field(pk=True, auto=True)  # autoincrement primary key
    sensor_id: str = field(index=True)          # secondary index
    celsius: float
    ts: datetime | None = None


# a Table is a BaseModel, so it doubles as the subscription model; the
# validated payload is a row you persist in one line
@app.bindings.subscribe("sensors/+/temperature", model=TempReading)
async def store(msg: Message[TempReading]) -> None:
    await msg.data.save()


# query with the async active-record API and Django-style lookups
hot = await TempReading.filter(celsius__gt=30, order_by="-ts", limit=10)
kitchen = await TempReading.get(sensor_id="kitchen")
count = await TempReading.count(sensor_id="kitchen")
```

The engine starts automatically inside `app.start()` whenever any `Table` is declared or the uplink is enabled, then closes on shutdown. It is skipped entirely otherwise — an app that stores nothing and forwards nothing opens no database file at all. Point it at a file or an in-memory database via config:

```yaml
storage:
  path: beacon.db  # sqlite file; ":memory:" for an ephemeral database
```

- **Lookups** — bare `field=` is equality (`None` becomes `IS NULL`), plus `__ne`, `__gt`, `__gte`, `__lt`, `__lte`, `__in`, `__like`; `order_by="-field"` sorts descending.
- **API** — instances have `save()` (insert, or upsert once the primary key is set) and `delete()`; the class has `get`/`filter`/`all`/`count`/`delete_where`, plus `save_many()` to write a batch under a single commit.
- **Schema** — created on start with `CREATE TABLE IF NOT EXISTS`; new model fields become `ALTER TABLE ... ADD COLUMN` (additive migrations only), and every value is parameterized. v1 is flat tables — no relations or joins.
- **Commits** — writes are group-committed: saves landing within `storage.commit_delay` (default 10ms) share one commit, so a burst of handler saves pays one fsync instead of one each. `await save()` still returns only after its commit is durable.

## Uplink

An edge device is offline sooner or later, and telemetry dropped during the outage is gone for good. The uplink is Beacon's answer: `enqueue()` writes the message to a durable buffer and returns as soon as that commit lands, and a background worker delivers it whenever the network allows. Nothing is forwarded implicitly — data enters by explicit call.

```python
@app.bindings.subscribe("telemetry/air/+", qos=1, model=AirReading)
async def on_air(msg: Message[AirReading]) -> None:
    await app.uplink.enqueue("air", msg.data)  # durable once this returns
```

Enable it and point it at your ingest endpoint:

```yaml
uplink:
  enabled: true
  http:
    base_url: https://ingest.example.com
    endpoint: /v1/telemetry
    headers:
      Authorization: Bearer ...
```

The worker claims up to `batch_size` records in FIFO order and POSTs them as one batch:

```json
{"records": [{"record_id": "...", "stream": "air", "payload": "{...}", "created_at": "..."}]}
```

- **Batch outcomes** — the whole batch shares the response's fate: 2xx acks it (rows deleted), 5xx / timeouts / connection errors nack it for retry, and 4xx buries it as poison, kept in the buffer as `dead` for inspection but never resent (429 is the exception — it retries).
- **Backoff** — a failed send backs the worker off exponentially from `retry.min_seconds` to `retry.max_seconds`. With a single claimer, retrying an unreachable endpoint sooner only burns it.
- **Outages are free** — a send that never reaches the endpoint costs a record no attempts, so being offline for a minute and being offline for a week cost the same: nothing. `retry.max_attempts` is spent only on failures the server answered for, and per record rather than per batch, so a record claimed alongside an older one is never buried on attempts it didn't spend. Burial is reserved for the one case that can actually block the queue: a batch the server keeps rejecting.
- **Exactly-once, server-side** — `record_id` is a UUID idempotency key. A batch resent after an ambiguous failure (the send landed, the response didn't) arrives with the same ids, so an ingest server that dedupes on them applies it once.
- **Bounded** — past `buffer.max_records` undelivered rows the oldest *pending* records are dropped, newest data wins; rows the worker holds inflight are never dropped, and buried rows don't count against the cap. Drops are counted and logged on a rate limit.
- **Crash-safe** — the buffer lives in the app's SQLite database, so a record that `enqueue()` returned survives a kill. Records left inflight by a crash or a mid-send SIGINT are recovered to pending on the next start; shutdown does no flush heroics because it doesn't need any.
- **Disabled by default** — enqueuing while `uplink.enabled` is false raises `UplinkNotEnabledError` rather than silently dropping the message.

`scripts/sim_ingest_server.py` is a local stand-in for the cloud: it accepts batches, dedupes on `record_id`, and can misbehave on demand.

```bash
uv run python scripts/sim_ingest_server.py --state seen.json   # terminal 1
uv run python scripts/collector.py                             # terminal 2 (uplink.enabled: true)
uv run python scripts/sim_air_sensor.py                        # terminal 3
```

Kill the ingest server and the collector keeps accepting readings — they pile up in the `outbound` table while the worker backs off. Start it again and the backlog drains, with `GET /stats` reporting duplicates as 0. `--fail-rate 0.5` shows retry under a flaky link, and `--reject air` shows a poison batch being buried.

## How it works

Two asyncio queues connect the app to the MQTT client, which runs alongside paho's network thread:

- `app.mqtt_command_queue` — subscribe/publish commands flowing **to** the client
- `app.mqtt_message_queue` — broker messages flowing **back** to your handlers

Inbound messages are matched against every registered topic filter (so `sensors/+/temperature` receives `sensors/kitchen/temperature`, and overlapping filters each fire). Multiple handlers may bind to the same filter — each receives the message, and the broker subscription uses the highest qos among them. Each handler runs as its own asyncio task, so a slow handler never blocks the message loop. Payloads that fail model validation never reach a handler — they are logged at WARNING and dropped.

Under burst, every stage has a bound and a policy: at most `mqtt.max_concurrent_handlers` handler tasks run at once; while they are saturated the dispatcher stops draining the message queue, which holds up to `mqtt.message_queue_size` messages; once the queue is full the client sheds the *oldest* message (newest telemetry wins) — counted, and logged at WARNING on a rate limit. paho's network thread is never blocked, so keepalives keep flowing while the app catches up.

Outbound, a publish that arrives while the broker link is down is dropped the same way — counted on `dropped_publishes` and warned about on the same rate limit, rather than queued forever. Publishing is best-effort by design; durability is the uplink's job.

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
├── storage/
│   ├── fields.py       # field() column metadata on Table models
│   ├── table.py        # Table base: registry + active-record API
│   ├── query.py        # Django-style lookup + WHERE/ORDER BY builders
│   ├── ddl.py          # model -> column specs + DDL generation
│   └── engine.py       # aiosqlite engine: lifecycle, codecs, execution
├── uplink/
│   ├── records.py      # OutboundRecord row + SendResult
│   ├── buffer.py       # durable FIFO: enqueue/claim/ack/nack/bury
│   ├── transport.py    # UplinkTransport protocol (seam for other transports)
│   ├── http.py         # aiohttp POST transport; status -> batch outcome
│   ├── worker.py       # claim -> send -> resolve drain loop with backoff
│   └── uplink.py       # Uplink facade owning buffer + transport + worker
└── utils/
    ├── logging_conf.py # async logging with per-run log dirs
    └── serialization.py # JSON encoding shared by publishers and the uplink
```

A runnable tour of the full API lives in [`scripts/usage_guide.py`](scripts/usage_guide.py):

```bash
uv run python scripts/usage_guide.py
```

## Development

```bash
uv run pytest          # run the test suite
uv run ruff check .    # lint
uv run mypy beacon     # type check
```

To see what a live app's tasks are doing — a stuck handler, the uplink worker mid-backoff — Python 3.14's asyncio introspection CLI attaches to the running process by pid:

```bash
uv run python -m asyncio ps <pid>      # flat table of tasks and what they await
uv run python -m asyncio pstree <pid>  # the same as an await tree
```

## License

MIT

"""Beacon — runnable usage guide.

A top-to-bottom tour of the framework's public surface; each section prints
what it did. Run with:

    uv run python scripts/usage_guide.py

Sections 1-7 need no broker. The final `app.start()` wants an MQTT broker
per `beacon.yaml` (default localhost:1883) but retries in the background,
so starting without one is fine.
"""

# ruff: noqa: T201, E402, INP001

import asyncio
import logging
import platform
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# -----------------------------------------------------#
# 1. Configuration  (beacon.core.config)               #
# -----------------------------------------------------#
# Pydantic-modeled, YAML-driven; defaults apply when the file is missing.
from beacon.core.config import (
    BeaconConfig,
    LoggingConfig,
    MQTTAuthConfig,
    MQTTConfig,
    load_config,
)

cfg = BeaconConfig(
    mqtt=MQTTConfig(
        host="localhost",
        port=1883,
        keepalive=60,
        auth=MQTTAuthConfig(username=None, password=None),
    ),
    logging=LoggingConfig(level="INFO", console=True),
)
print("[1] programmatic cfg log level int:", cfg.logging.log_level)

disk_cfg = load_config(Path("beacon.yaml"))
print("[1] disk cfg mqtt host:", disk_cfg.mqtt.host)


# -----------------------------------------------------#
# 2. App construction  (beacon.core.app.Beacon)        #
# -----------------------------------------------------#
# The orchestrator: owns the MQTT client, bindings, logging, storage,
# uplink, signals, and shutdown.
from beacon.core.app import Beacon

app = Beacon(name="usage-guide", config_path=Path("beacon.yaml"))


# -----------------------------------------------------#
# 3. MQTT decorator bindings  (beacon.mqtt.decorators) #
# -----------------------------------------------------#
# Topics are MQTT filters — wildcards (+, #) route.
from pydantic import BaseModel

from beacon.mqtt import Message


@app.bindings.subscribe("sensors/temperature", qos=1)
async def on_temperature(msg: Message[None]) -> None:
    # untyped: .topic, .payload (str), .timestamp; .json() parses, .data is None
    print(f"[3] temperature handler got: {msg.json()}")


class TempReading(BaseModel):
    sensor_id: str
    celsius: float


# typed: `model=` validates inbound payloads before the handler runs;
# invalid ones are logged and dropped, valid ones arrive as `msg.data`
@app.bindings.subscribe("sensors/+/reading", qos=1, model=TempReading)
async def on_reading(msg: Message[TempReading]) -> None:
    print(f"[3] {msg.data.sensor_id} reads {msg.data.celsius}C (from {msg.topic})")


# return value is JSON-encoded and published every `every` seconds; with
# `model=` (or a BaseModel return) it goes through model_dump_json()
@app.bindings.publisher("devices/heartbeat", every=5.0, qos=0, retain=False)
async def publish_heartbeat() -> dict[str, Any]:
    return {
        "device": app.name,
        "status": "online",
        "platform": platform.system(),
        "timestamp": datetime.now(UTC).isoformat(),
    }


print("[3] subscriptions:", [s.topic for s in app.bindings.subscriptions])
print("[3] publishers:", [p.topic for p in app.bindings.publishers])


# -----------------------------------------------------#
# 4. Exceptions  (beacon.core.exceptions)              #
# -----------------------------------------------------#
from beacon.core.exceptions import UnsupportedIntervalError

try:
    app.bindings.publisher("bad", every=-1)  # `every` must be positive
except UnsupportedIntervalError as e:
    print("[4] bad interval rejected:", e)


# -----------------------------------------------------#
# 5. Async logging  (beacon.utils.logging_conf)        #
# -----------------------------------------------------#
# `app.start()` configures this for you; the primitives also work standalone.
from beacon.utils.logging_conf import AsyncLogging, new_run_log_dir
from beacon.utils.logging_conf import LoggingConfig as LogFileCfg

run_dir = new_run_log_dir(Path("logs"))
print("[5] new run log dir:", run_dir)

with AsyncLogging(LogFileCfg(log_file=run_dir / "demo.log", console=False)):
    logging.getLogger("beacon.usage_guide").info("hello from the usage guide")
print("[5] wrote one log line to", run_dir / "demo.log")


# -----------------------------------------------------#
# 6. Storage  (beacon.storage)                         #
# -----------------------------------------------------#
# A Table is a Pydantic model that is also a SQLite table: one class
# validates payloads and defines the schema.
from beacon.storage import StorageEngine, Table, field


class TempRow(Table):
    id: int | None = field(pk=True, auto=True)
    sensor_id: str = field(index=True)
    celsius: float
    ts: datetime | None = None


# a Table drops straight into a subscription as its model; app.start()
# brings up the engine automatically when any Table is declared
@app.bindings.subscribe("sensors/+/db", qos=1, model=TempRow)
async def store_reading(msg: Message[TempRow]) -> None:
    await msg.data.save()
    print(f"[6] stored reading from {msg.data.sensor_id}")


# the query surface, driven on an ephemeral in-memory engine
async def _storage_demo() -> None:
    engine = StorageEngine(":memory:")
    await engine.start()
    try:
        await TempRow(sensor_id="s1", celsius=19.5, ts=datetime.now(UTC)).save()
        await TempRow(sensor_id="s1", celsius=31.0, ts=datetime.now(UTC)).save()
        await TempRow(sensor_id="s2", celsius=42.0, ts=datetime.now(UTC)).save()

        # Django-style lookups: __gt/__gte/__lt/__lte/__ne/__in/__like;
        # order_by ("-field" for DESC) and limit
        hot = await TempRow.filter(celsius__gt=30, order_by="-celsius", limit=5)
        print("[6] hot readings:", [(r.sensor_id, r.celsius) for r in hot])
        print("[6] s1 count:", await TempRow.count(sensor_id="s1"))

        one = await TempRow.get(sensor_id="s2")
        print("[6] got s2:", one and (one.id, one.celsius))
    finally:
        await engine.stop()


asyncio.run(_storage_demo())


# -----------------------------------------------------#
# 7. Uplink  (beacon.uplink)                           #
# -----------------------------------------------------#
# Store-and-forward: `enqueue()` returns once the record is durable; a
# background worker POSTs batches whenever the network allows. Data enters
# by explicit call only.
from beacon.core.config import UplinkBufferConfig, UplinkConfig, UplinkHTTPConfig
from beacon.core.exceptions import UplinkNotEnabledError
from beacon.uplink import OutboundRecord, RecordState, Uplink

uplink_cfg = UplinkConfig(
    enabled=True,
    http=UplinkHTTPConfig(base_url="http://localhost:8000", endpoint="/ingest"),
    buffer=UplinkBufferConfig(batch_size=50, flush_interval=1.0, max_records=10_000),
)


# in a real app `app.start()` builds this as `app.uplink`; handlers just enqueue
@app.bindings.subscribe("sensors/+/cloud", qos=1, model=TempReading)
async def forward_reading(msg: Message[TempReading]) -> None:
    assert app.uplink is not None
    await app.uplink.enqueue("temperature", msg.data)  # durable on return


# the "store" half without a server: no worker runs, so the record stays
# buffered — exactly what an offline device looks like
async def _uplink_demo() -> None:
    engine = StorageEngine(":memory:")
    await engine.start()
    try:
        uplink = Uplink(engine, uplink_cfg)
        record = await uplink.enqueue("temperature", {"sensor_id": "s1", "celsius": 21.5})
        # `seq` is FIFO order, `record_id` the idempotency key servers dedupe on
        print("[7] buffered:", record.record_id, record.state, record.payload)
        print("[7] pending:", await OutboundRecord.count(state=RecordState.PENDING))

        # a disabled uplink raises instead of silently dropping data
        off = Uplink(engine, UplinkConfig(enabled=False))
        try:
            await off.enqueue("temperature", {"celsius": 21.5})
        except UplinkNotEnabledError as exc:
            print("[7] disabled uplink refused:", exc)
    finally:
        await engine.stop()


asyncio.run(_uplink_demo())


# -----------------------------------------------------#
# 8. Run the app                                       #
# -----------------------------------------------------#
# Brings up logging, storage, uplink, the MQTT client, and publishers, then
# blocks until SIGINT/SIGTERM. Ctrl-C exits cleanly.
if __name__ == "__main__":
    print("\n[8] starting app... (Ctrl-C to exit)")
    asyncio.run(app.start())

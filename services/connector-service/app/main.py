"""connector-service: pulls work from external systems into the pipeline.

Exists as its own service rather than as part of ingestion-service because the
two do different jobs. ingestion-service turns raw logs into `logs-topic`
events. A connector pulls things that are already incidents and puts them on
`incidents-topic`, skipping detection and correlation because the grouping
decision was made in the originating system.

Today it hosts one source, ServiceNow. The loop is generic, so a second source
is a registration rather than a rewrite.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from airs_shared.dlq import build_dlq_payload
from airs_shared.kafka import produce_json
from airs_shared.models import Incident
from airs_shared.monitoring import metrics_response
from airs_shared.settings import get_settings
from airs_shared.sinks import Sink, SinkPayload, fan_out
from airs_shared.sources import Source, SourceCursor, SourceKind
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram
from servicenow import ServiceNowSource, ServiceNowWorkNoteSink

settings = get_settings()
app = FastAPI(title="AIRS Connector Service")
logger = logging.getLogger("connector-service")

producer: AIOKafkaProducer | None = None
consumer: AIOKafkaConsumer | None = None
worker_tasks: list[asyncio.Task[None]] = []
sources: dict[str, Source] = {}
sinks: list[Sink] = []
cursors: dict[str, SourceCursor] = {}

RECORDS_FETCHED_TOTAL = Counter(
    "airs_connector_records_fetched_total",
    "Records pulled from an external source",
    labelnames=("source",),
)
RECORDS_PUBLISHED_TOTAL = Counter(
    "airs_connector_records_published_total",
    "Records published into the pipeline",
    labelnames=("source", "topic"),
)
POLLS_TOTAL = Counter(
    "airs_connector_polls_total",
    "Poll attempts by outcome",
    labelnames=("source", "status"),
)
POLL_DURATION_SECONDS = Histogram(
    "airs_connector_poll_duration_seconds",
    "Time to complete one poll",
    labelnames=("source",),
)
DLQ_PUBLISHED_TOTAL = Counter(
    "airs_dlq_published_total",
    "Count of DLQ events published by source topic",
    labelnames=("source_topic",),
)
SOURCE_UP = Gauge(
    "airs_connector_source_up",
    "Whether the external source answered its last health check",
    labelnames=("source",),
)
SINK_DELIVERIES_TOTAL = Counter(
    "airs_connector_sink_deliveries_total",
    "Outbound RCA deliveries by sink and outcome",
    labelnames=("sink", "status"),
)


def topic_for(kind: SourceKind) -> str:
    """Where a record enters the pipeline.

    An incident from another system is already grouped, so re-running detection
    and correlation over it would produce a second, competing opinion about the
    same event.
    """
    if kind is SourceKind.incidents:
        return settings.kafka.topics.incidents
    return settings.kafka.topics.logs


async def publish_to_dlq(*, source_name: str, payload: dict[str, Any], error: Exception) -> None:
    if producer is None:
        return
    topic = settings.kafka.topics.dlq
    await produce_json(
        producer,
        topic,
        {
            **build_dlq_payload(
                source_topic=f"connector:{source_name}", payload=payload, error=error
            ),
            "connector": source_name,
        },
    )
    DLQ_PUBLISHED_TOTAL.labels(source_topic=f"connector:{source_name}").inc()


async def poll_once(source: Source) -> int:
    """One fetch cycle. Returns how many records were published."""
    cursor = cursors.get(source.name, SourceCursor())
    records, next_cursor = await source.fetch(cursor)
    RECORDS_FETCHED_TOTAL.labels(source=source.name).inc(len(records))

    published = 0
    for record in records:
        try:
            # Validate against the contract before publishing, so a mapping bug
            # in a connector cannot put a malformed incident on the topic.
            incident = Incident.model_validate(record.payload)
            await produce_json(
                producer,
                topic_for(record.kind),
                incident.model_dump(mode="json"),
            )
            RECORDS_PUBLISHED_TOTAL.labels(source=source.name, topic=topic_for(record.kind)).inc()
            published += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("Could not publish record from %s: %s", source.name, exc)
            await publish_to_dlq(source_name=source.name, payload=record.payload, error=exc)

    # Advance only after the batch is handled, so a crash mid-batch re-fetches
    # rather than skipping. Publishing is idempotent by incident id downstream.
    cursors[source.name] = next_cursor
    return published


async def poll_loop(source: Source, interval_seconds: int) -> None:
    logger.info("Starting %s poller, every %ss", source.name, interval_seconds)
    while True:
        started = datetime.now(UTC)
        try:
            published = await poll_once(source)
            POLLS_TOTAL.labels(source=source.name, status="ok").inc()
            if published:
                logger.info("Published %d record(s) from %s", published, source.name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            # A failed poll is normal: instances hibernate, tokens expire,
            # networks blip. Record it and try again next tick.
            POLLS_TOTAL.labels(source=source.name, status="error").inc()
            logger.warning("Poll of %s failed: %s", source.name, exc)
        finally:
            POLL_DURATION_SECONDS.labels(source=source.name).observe(
                (datetime.now(UTC) - started).total_seconds()
            )
        await asyncio.sleep(interval_seconds)


async def deliver_enriched(incident_payload: dict[str, Any]) -> None:
    """Fan one enriched incident out to every configured sink.

    Sinks are contracted not to raise, and fan_out contains any that break that
    contract, so one unreachable destination cannot deny the others or fail the
    incident. The RCA is already durable in OpenSearch by the time we see it.
    """
    if not sinks:
        return

    incident = Incident.model_validate(incident_payload)
    if incident.rca is None:
        return

    payload = SinkPayload.from_incident(incident)
    for result in await fan_out(sinks, payload):
        status = (
            "duplicate" if result.skipped_duplicate else ("ok" if result.delivered else "error")
        )
        SINK_DELIVERIES_TOTAL.labels(sink=result.sink.split(":")[0], status=status).inc()
        if not result.delivered:
            logger.warning("Sink %s did not deliver: %s", result.sink, result.detail)


async def delivery_loop() -> None:
    """Consume enriched incidents and hand them to the sinks."""
    assert consumer is not None
    while True:
        try:
            records = await consumer.getmany(timeout_ms=1000, max_records=50)
            for _tp, tp_records in records.items():
                for message in tp_records:
                    payload = json.loads(message.value.decode("utf-8"))
                    try:
                        await deliver_enriched(payload)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("Delivery failed: %s", exc)
                        await publish_to_dlq(source_name="delivery", payload=payload, error=exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Delivery loop error: %s", exc)
            await asyncio.sleep(1)


def build_sinks() -> list[Sink]:
    built: list[Sink] = []
    cfg = settings.connectors.servicenow

    if cfg.enabled and cfg.instance_url:
        password = os.getenv(cfg.password_env, "")
        if password:
            built.append(
                ServiceNowWorkNoteSink(
                    instance_url=cfg.instance_url,
                    username=cfg.username,
                    password=password,
                    dry_run=cfg.dry_run,
                )
            )
            if cfg.dry_run:
                logger.info(
                    "ServiceNow sink is in DRY RUN. Work notes will be logged, not written."
                )

    return built


def build_sources() -> dict[str, tuple[Source, int]]:
    """Instantiate every enabled source. Returns name to (source, interval)."""
    built: dict[str, tuple[Source, int]] = {}
    cfg = settings.connectors.servicenow

    if cfg.enabled:
        password = os.getenv(cfg.password_env, "")
        if not password:
            logger.error(
                "ServiceNow connector is enabled but %s is not set. Not starting it.",
                cfg.password_env,
            )
        elif not cfg.instance_url:
            logger.error("ServiceNow connector is enabled but instance_url is empty.")
        else:
            built["servicenow"] = (
                ServiceNowSource(
                    instance_url=cfg.instance_url,
                    username=cfg.username,
                    password=password,
                    tenant_id=cfg.tenant_id,
                    page_size=cfg.page_size,
                ),
                cfg.poll_interval_seconds,
            )

    return built


@app.on_event("startup")
async def startup() -> None:
    global producer, consumer

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka.bootstrap_servers)
    await producer.start()

    for name, (source, interval) in build_sources().items():
        sources[name] = source
        worker_tasks.append(asyncio.create_task(poll_loop(source, interval)))

    sinks.extend(build_sinks())
    if sinks:
        consumer = AIOKafkaConsumer(
            settings.kafka.topics.enriched_incidents,
            bootstrap_servers=settings.kafka.bootstrap_servers,
            group_id="airs-connector-delivery",
            enable_auto_commit=True,
            auto_offset_reset="latest",
        )
        await consumer.start()
        worker_tasks.append(asyncio.create_task(delivery_loop()))

    if not sources and not sinks:
        logger.info("No connectors enabled. Service is idle but healthy.")


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in worker_tasks:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    for source in sources.values():
        with suppress(Exception):
            await source.aclose()
    for sink in sinks:
        with suppress(Exception):
            await sink.aclose()

    if consumer is not None:
        await consumer.stop()
    if producer is not None:
        await producer.stop()


@app.get("/health")
async def health() -> dict[str, Any]:
    checks: dict[str, bool] = {}
    for name, source in sources.items():
        ok = await source.health()
        SOURCE_UP.labels(source=name).set(1 if ok else 0)
        checks[name] = ok

    return {
        # An unreachable external system is not this service being unhealthy.
        # It polls, it fails, it retries. Reporting otherwise would restart a
        # container that is working correctly.
        "status": "ok" if producer is not None else "degraded",
        "service": "connector-service",
        "connectors": checks or {"none_enabled": True},
    }


@app.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "ok", "service": "connector-service"}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    if producer is not None:
        return JSONResponse(
            status_code=200, content={"status": "ok", "service": "connector-service"}
        )
    return JSONResponse(
        status_code=503, content={"status": "degraded", "service": "connector-service"}
    )


@app.get("/metrics")
async def metrics() -> object:
    return metrics_response()


@app.post("/connectors/{name}/sync")
async def force_sync(name: str) -> dict[str, Any]:
    """Poll one source immediately instead of waiting for its next tick."""
    source = sources.get(name)
    if source is None:
        return {"error": f"connector '{name}' is not enabled", "published": 0}
    try:
        published = await poll_once(source)
    except Exception as exc:  # noqa: BLE001
        return {"connector": name, "status": "error", "error": str(exc)[:500]}
    return {"connector": name, "status": "ok", "published": published}

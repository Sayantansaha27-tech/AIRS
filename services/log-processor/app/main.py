from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import UTC, datetime
from time import perf_counter

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from airs_shared.consumer import build_consumer, commit_safely
from airs_shared.dlq import build_dlq_payload
from airs_shared.kafka import produce_json
from airs_shared.logging import configure_logging
from airs_shared.models import NormalizedLogEvent
from airs_shared.monitoring import metrics_response
from airs_shared.normalize import normalize_log
from airs_shared.opensearch import (
    async_ensure_index,
    build_async_client,
    build_client,
    bulk_index,
    ensure_retention_policy,
)
from airs_shared.settings import get_settings
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram

settings = get_settings()
app = FastAPI(title="AIRS Log Processor")
logger = configure_logging("log-processor")

consumer: AIOKafkaConsumer | None = None
producer: AIOKafkaProducer | None = None
worker_task: asyncio.Task[None] | None = None

os_client = build_client(settings.opensearch.url)
os_async = build_async_client(settings.opensearch.url)
LOGS_PROCESSED_TOTAL = Counter(
    "airs_logs_processed_total",
    "Count of processed log events",
    labelnames=("service",),
)
LOGS_PROCESSING_DURATION_SECONDS = Histogram(
    "airs_logs_processing_duration_seconds",
    "Log processing latency in seconds",
    labelnames=("service",),
)
DLQ_PUBLISHED_TOTAL = Counter(
    "airs_dlq_published_total",
    "Count of DLQ events published by source topic",
    labelnames=("source_topic",),
)
CONSUMER_BATCH_SIZE = Gauge(
    "airs_log_processor_batch_size",
    "Number of messages fetched in the latest batch",
)


def document_id(normalized: NormalizedLogEvent) -> str:
    """Stable per event, so re-ingesting the same log overwrites rather than
    duplicating. That idempotence is what makes at-least-once safe here."""
    return (
        f"{normalized.tenant_id}-{normalized.service}-"
        f"{normalized.timestamp.timestamp()}-{abs(hash(normalized.message))}"
    )


async def handle_batch(payloads: list[dict]) -> list[tuple[dict, Exception]]:
    """Normalize, index in one bulk request, then forward.

    Previously each event was indexed individually with refresh=True through
    the synchronous client, from inside this async loop. That paid a segment
    flush per document and blocked the event loop for the duration of every
    call, and it was the measured ceiling on the ingest path.

    Returns the events that could not be normalized, for dead-lettering. An
    indexing failure raises, because that is a dependency problem affecting the
    whole batch rather than a property of one event.
    """
    started_at = perf_counter()
    rejected: list[tuple[dict, Exception]] = []
    normalized_events: list[NormalizedLogEvent] = []

    for payload in payloads:
        try:
            normalized_events.append(normalize_log(payload))
        except Exception as exc:  # noqa: BLE001
            rejected.append((payload, exc))

    if not normalized_events:
        return rejected

    ingested_at = datetime.now(UTC).isoformat()
    await bulk_index(
        os_async,
        settings.opensearch.logs_index,
        [
            (
                document_id(event),
                {**event.model_dump(mode="json"), "ingested_at": ingested_at},
            )
            for event in normalized_events
        ],
    )

    if producer is None:
        return rejected

    for event in normalized_events:
        await produce_json(
            producer,
            settings.kafka.topics.processed_logs,
            event.model_dump(mode="json"),
        )
        LOGS_PROCESSED_TOTAL.labels(service=event.service).inc()

    elapsed = (perf_counter() - started_at) / len(normalized_events)
    for event in normalized_events:
        LOGS_PROCESSING_DURATION_SECONDS.labels(service=event.service).observe(elapsed)

    return rejected


async def publish_to_dlq(
    *,
    payload: dict,
    error: Exception,
    partition: int | None,
    offset: int | None,
) -> None:
    if producer is None:
        return
    await produce_json(
        producer,
        settings.kafka.topics.dlq,
        build_dlq_payload(
            source_topic=settings.kafka.topics.logs,
            payload=payload,
            error=error,
            partition=partition,
            offset=offset,
        ),
    )
    DLQ_PUBLISHED_TOTAL.labels(source_topic=settings.kafka.topics.logs).inc()


async def consume_loop() -> None:
    assert consumer is not None
    while True:
        try:
            records = await consumer.getmany(timeout_ms=1000, max_records=200)
            CONSUMER_BATCH_SIZE.set(sum(len(tp_records) for tp_records in records.values()))
            for tp, tp_records in records.items():
                payloads: list[dict] = []
                offsets: list[int] = []
                for message in tp_records:
                    payloads.append(json.loads(message.value.decode("utf-8")))
                    offsets.append(message.offset)

                try:
                    rejected = await handle_batch(payloads)
                except Exception as exc:  # noqa: BLE001
                    # A dependency failed for the whole batch, so every event in
                    # it is dead-lettered rather than silently lost.
                    logger.exception("Batch indexing failed: %s", exc)
                    for payload, offset in zip(payloads, offsets, strict=True):
                        await publish_to_dlq(
                            payload=payload,
                            error=exc,
                            partition=tp.partition,
                            offset=offset,
                        )
                    continue

                for payload, exc in rejected:
                    logger.warning("Rejecting unprocessable log event: %s", exc)
                    await publish_to_dlq(
                        payload=payload,
                        error=exc,
                        partition=tp.partition,
                        offset=None,
                    )
            if records:
                # Only now are these records genuinely handled. A crash before
                # this point replays them rather than skipping them.
                await commit_safely(consumer, where="log-processor")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Consumer loop error: %s", exc)
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup() -> None:
    global consumer, producer, worker_task

    await async_ensure_index(os_async, settings.opensearch.logs_index)
    ensure_retention_policy(
        os_client,
        index_name=settings.opensearch.logs_index,
        retention_days=settings.pipeline.log_retention_days,
    )

    consumer = build_consumer(
        settings.kafka.topics.logs,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id="airs-log-processor",
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka.bootstrap_servers)

    await consumer.start()
    await producer.start()

    worker_task = asyncio.create_task(consume_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    global worker_task

    if worker_task is not None:
        worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await worker_task

    if consumer is not None:
        await consumer.stop()
    if producer is not None:
        await producer.stop()
    await os_async.close()


@app.get("/health")
async def health() -> dict[str, str | bool]:
    return {
        "status": "ok",
        "service": "log-processor",
        "ready": await ready_check(),
    }


async def ready_check() -> bool:
    if consumer is None or producer is None:
        return False
    try:
        return bool(os_client.ping())
    except Exception:  # noqa: BLE001
        return False


@app.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "ok", "service": "log-processor"}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    if await ready_check():
        return JSONResponse(status_code=200, content={"status": "ok", "service": "log-processor"})
    return JSONResponse(status_code=503, content={"status": "degraded", "service": "log-processor"})


@app.get("/metrics")
async def metrics() -> object:
    return metrics_response()

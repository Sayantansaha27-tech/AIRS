from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from datetime import UTC, datetime
from time import perf_counter

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram

from airs_shared.dlq import build_dlq_payload
from airs_shared.kafka import produce_json
from airs_shared.monitoring import metrics_response
from airs_shared.normalize import normalize_log
from airs_shared.opensearch import (
    build_client,
    ensure_index,
    ensure_retention_policy,
    upsert_doc,
)
from airs_shared.settings import get_settings

settings = get_settings()
app = FastAPI(title="AIRS Log Processor")
logger = logging.getLogger("log-processor")

consumer: AIOKafkaConsumer | None = None
producer: AIOKafkaProducer | None = None
worker_task: asyncio.Task[None] | None = None

os_client = build_client(settings.opensearch.url)
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


async def handle_message(raw_payload: dict) -> None:
    normalized = normalize_log(raw_payload)
    started_at = perf_counter()

    upsert_doc(
        os_client,
        settings.opensearch.logs_index,
        (
            f"{normalized.tenant_id}-{normalized.service}-"
            f"{normalized.timestamp.timestamp()}-{abs(hash(normalized.message))}"
        ),
        {
            **normalized.model_dump(mode="json"),
            "ingested_at": datetime.now(UTC).isoformat(),
        },
    )

    if producer is None:
        return

    await produce_json(
        producer,
        settings.kafka.topics.processed_logs,
        normalized.model_dump(mode="json"),
    )
    LOGS_PROCESSED_TOTAL.labels(service=normalized.service).inc()
    LOGS_PROCESSING_DURATION_SECONDS.labels(service=normalized.service).observe(
        perf_counter() - started_at
    )


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
                for message in tp_records:
                    payload = json.loads(message.value.decode("utf-8"))
                    try:
                        await handle_message(payload)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("Failed to process log event: %s", exc)
                        await publish_to_dlq(
                            payload=payload,
                            error=exc,
                            partition=tp.partition,
                            offset=message.offset,
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Consumer loop error: %s", exc)
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup() -> None:
    global consumer, producer, worker_task

    ensure_index(os_client, settings.opensearch.logs_index)
    ensure_retention_policy(
        os_client,
        index_name=settings.opensearch.logs_index,
        retention_days=settings.pipeline.log_retention_days,
    )

    consumer = AIOKafkaConsumer(
        settings.kafka.topics.logs,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id="airs-log-processor",
        enable_auto_commit=True,
        auto_offset_reset="latest",
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

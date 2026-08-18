from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

import httpx
from aiokafka import AIOKafkaProducer
from airs_shared.dlq import build_dlq_payload
from airs_shared.kafka import produce_json
from airs_shared.logging import configure_logging
from airs_shared.models import DataSource, IngestRequest, SourceMethod
from airs_shared.monitoring import metrics_response
from airs_shared.normalize import normalize_log
from airs_shared.opensearch import build_client, ensure_index, upsert_doc
from airs_shared.settings import get_settings
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram

settings = get_settings()
app = FastAPI(title="AIRS Ingestion Service")
logger = configure_logging("ingestion-service")

producer: AIOKafkaProducer | None = None
source_poll_task: asyncio.Task[None] | None = None
source_http_client: httpx.AsyncClient | None = None
os_client = build_client(settings.opensearch.url)
LOGS_INGESTED_TOTAL = Counter(
    "airs_logs_ingested_total",
    "Count of logs accepted into the pipeline",
    labelnames=("source", "service"),
)
LOGS_REJECTED_TOTAL = Counter(
    "airs_logs_rejected_total",
    "Count of rejected logs",
    labelnames=("source", "reason"),
)
INGEST_DURATION_SECONDS = Histogram(
    "airs_ingest_duration_seconds",
    "Latency of ingestion publishing in seconds",
    labelnames=("source",),
)
SOURCE_POLLS_TOTAL = Counter(
    "airs_source_polls_total",
    "Count of external source poll attempts",
    labelnames=("status",),
)
DLQ_PUBLISHED_TOTAL = Counter(
    "airs_dlq_published_total",
    "Count of DLQ events published by source topic",
    labelnames=("source_topic",),
)


def payload_size(raw: dict[str, Any] | str) -> int:
    raw_json = raw if isinstance(raw, str) else json.dumps(raw, default=str)
    return len(raw_json.encode("utf-8"))


def apply_default_service(
    raw: dict[str, Any] | str,
    default_service: str | None,
    tenant_id: str | None,
) -> dict[str, Any] | str:
    normalized_tenant = (tenant_id or "default").strip().lower()
    if not default_service:
        if isinstance(raw, str):
            return {
                "timestamp": datetime.now(UTC).isoformat(),
                "service": "unknown-service",
                "level": "info",
                "message": raw,
                "tenant_id": normalized_tenant,
            }
        return {**raw, "tenant_id": raw.get("tenant_id", normalized_tenant)}
    if isinstance(raw, str):
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "service": default_service,
            "level": "info",
            "message": raw,
            "tenant_id": normalized_tenant,
        }
    if raw.get("service"):
        return {**raw, "tenant_id": raw.get("tenant_id", normalized_tenant)}
    return {**raw, "service": default_service, "tenant_id": normalized_tenant}


async def publish_to_dlq(
    *,
    payload: dict[str, Any] | str,
    error: Exception,
    ingest_source: str,
) -> None:
    """Park a log that could not be normalized or published.

    Ingestion is the head of the pipeline, so there is no upstream partition or
    offset to record. The DLQ entry is keyed on the topic the event failed to
    reach, and carries the ingest channel that produced it.
    """
    if producer is None:
        return
    body = payload if isinstance(payload, dict) else {"raw": payload}
    await produce_json(
        producer,
        settings.kafka.topics.dlq,
        {
            **build_dlq_payload(
                source_topic=settings.kafka.topics.logs,
                payload=body,
                error=error,
            ),
            "ingest_source": ingest_source,
        },
    )
    DLQ_PUBLISHED_TOTAL.labels(source_topic=settings.kafka.topics.logs).inc()


async def ingest_one(
    raw: dict[str, Any] | str,
    default_service: str | None = None,
    tenant_id: str | None = None,
    source: str = "api",
) -> bool:
    if producer is None:
        LOGS_REJECTED_TOTAL.labels(source=source, reason="producer_unavailable").inc()
        return False

    started_at = perf_counter()
    candidate = apply_default_service(raw, default_service, tenant_id)
    if payload_size(candidate) > settings.pipeline.max_log_payload_kb * 1024:
        LOGS_REJECTED_TOTAL.labels(source=source, reason="payload_too_large").inc()
        await publish_to_dlq(
            payload=candidate,
            error=ValueError("payload exceeds max_log_payload_kb"),
            ingest_source=source,
        )
        return False

    try:
        normalized = normalize_log(candidate)
        await produce_json(
            producer,
            settings.kafka.topics.logs,
            normalized.model_dump(mode="json"),
        )
    except Exception as exc:  # noqa: BLE001
        # A single malformed event must not abandon the rest of the batch.
        logger.warning("Rejecting unprocessable log event: %s", exc)
        LOGS_REJECTED_TOTAL.labels(source=source, reason="unprocessable").inc()
        await publish_to_dlq(payload=candidate, error=exc, ingest_source=source)
        return False

    LOGS_INGESTED_TOTAL.labels(source=source, service=normalized.service).inc()
    INGEST_DURATION_SECONDS.labels(source=source).observe(perf_counter() - started_at)
    return True


def resolve_response_path(payload: Any, path: str | None) -> Any:
    if not path:
        return payload

    current = payload
    for piece in path.split("."):
        if isinstance(current, dict):
            current = current.get(piece)
        elif isinstance(current, list) and piece.isdigit():
            index = int(piece)
            if index < 0 or index >= len(current):
                return None
            current = current[index]
        else:
            return None
    return current


def extract_logs(payload: Any, source: DataSource) -> list[dict[str, Any] | str]:
    raw_logs = resolve_response_path(payload, source.response_logs_field)
    if raw_logs is payload and isinstance(payload, dict) and "logs" in payload:
        raw_logs = payload["logs"]

    if isinstance(raw_logs, list):
        return [item for item in raw_logs if isinstance(item, (dict, str))]
    if isinstance(raw_logs, (dict, str)):
        return [raw_logs]
    return []


def fetch_enabled_sources() -> list[DataSource]:
    query = {
        "query": {"term": {"enabled": True}},
        "size": 200,
        "sort": [{"updated_at": {"order": "asc"}}],
    }
    result = os_client.search(index=settings.opensearch.sources_index, body=query)

    sources: list[DataSource] = []
    for hit in result.get("hits", {}).get("hits", []):
        try:
            source = DataSource.model_validate({"id": hit["_id"], **hit["_source"]})
        except Exception:  # noqa: BLE001
            continue
        sources.append(source)
    return sources


def get_source_or_404(source_id: str, tenant_id: str | None = None) -> DataSource:
    try:
        existing = os_client.get(index=settings.opensearch.sources_index, id=source_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Source not found") from exc
    source = DataSource.model_validate({"id": existing["_id"], **existing["_source"]})
    if tenant_id is not None and source.tenant_id != tenant_id:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


def save_source(source: DataSource) -> None:
    upsert_doc(
        os_client,
        settings.opensearch.sources_index,
        source.id,
        source.model_dump(mode="json"),
    )


def due_for_poll(source: DataSource, now: datetime) -> bool:
    if not source.enabled:
        return False
    if source.last_polled_at is None:
        return True
    elapsed = (now - source.last_polled_at).total_seconds()
    return elapsed >= source.poll_interval_seconds


async def fetch_logs_from_source(source: DataSource) -> list[dict[str, Any] | str]:
    assert source_http_client is not None

    headers = dict(source.headers)
    if source.auth_token:
        headers.setdefault("Authorization", f"Bearer {source.auth_token}")

    request_args: dict[str, Any] = {"headers": headers}
    if source.method == SourceMethod.post and source.body:
        request_args["json"] = source.body
    elif source.method == SourceMethod.get and source.body:
        request_args["params"] = source.body

    response = await source_http_client.request(
        source.method.value,
        source.endpoint,
        **request_args,
    )
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError:
        payload = [line for line in response.text.splitlines() if line.strip()]

    return extract_logs(payload, source)


async def poll_source(source: DataSource) -> dict[str, Any]:
    poll_at = datetime.now(UTC)
    ingested = 0

    try:
        source_logs = await fetch_logs_from_source(source)
        SOURCE_POLLS_TOTAL.labels(status="ok").inc()
        for raw in source_logs:
            accepted = await ingest_one(
                raw,
                default_service=source.default_service,
                tenant_id=source.tenant_id,
                source="poller",
            )
            ingested += int(accepted)

        source.last_status = "ok"
        source.last_error = None
        source.last_success_at = poll_at
        source.total_ingested += ingested
        return {
            "source_id": source.id,
            "status": source.last_status,
            "fetched": len(source_logs),
            "accepted": ingested,
            "polled_at": poll_at.isoformat(),
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed polling source %s", source.id)
        SOURCE_POLLS_TOTAL.labels(status="error").inc()
        source.last_status = "error"
        source.last_error = str(exc)[:500]
        return {
            "source_id": source.id,
            "status": source.last_status,
            "error": source.last_error,
            "accepted": 0,
            "polled_at": poll_at.isoformat(),
        }
    finally:
        source.last_polled_at = poll_at
        source.updated_at = poll_at
        save_source(source)


async def source_poll_loop() -> None:
    while True:
        try:
            now = datetime.now(UTC)
            for source in fetch_enabled_sources():
                if due_for_poll(source, now):
                    await poll_source(source)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Source polling loop failed: %s", exc)

        await asyncio.sleep(settings.pipeline.source_poll_tick_seconds)


@app.on_event("startup")
async def startup() -> None:
    global producer, source_poll_task, source_http_client

    ensure_index(os_client, settings.opensearch.sources_index)

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka.bootstrap_servers)
    await producer.start()
    source_http_client = httpx.AsyncClient(timeout=10.0)
    source_poll_task = asyncio.create_task(source_poll_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    global source_poll_task

    if source_poll_task is not None:
        source_poll_task.cancel()
        with suppress(asyncio.CancelledError):
            await source_poll_task

    if producer is not None:
        await producer.stop()
    if source_http_client is not None:
        await source_http_client.aclose()


@app.get("/health")
async def health() -> dict[str, Any]:
    ready = await ready_check()
    return {
        "status": "ok" if ready else "degraded",
        "service": "ingestion-service",
        "ready": ready,
        "source_poller_running": source_poll_task is not None,
    }


async def ready_check() -> bool:
    if producer is None or source_http_client is None or source_poll_task is None:
        return False
    try:
        return bool(os_client.ping())
    except Exception:  # noqa: BLE001
        return False


@app.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "ok", "service": "ingestion-service"}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    if await ready_check():
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "service": "ingestion-service"},
        )
    return JSONResponse(
        status_code=503,
        content={"status": "degraded", "service": "ingestion-service"},
    )


@app.get("/metrics")
async def metrics() -> object:
    return metrics_response()


@app.post("/ingest")
async def ingest(request: Request, payload: IngestRequest) -> dict[str, int]:
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")

    tenant_id = str(request.headers.get("x-tenant-id") or "default").strip().lower()
    accepted = 0
    for raw in payload.logs:
        accepted += int(await ingest_one(raw, tenant_id=tenant_id, source="api"))

    if accepted == 0:
        raise HTTPException(status_code=400, detail="No valid logs accepted")

    return {"accepted": accepted}


@app.post("/sources/{source_id}/pull")
async def pull_source(request: Request, source_id: str) -> dict[str, Any]:
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer not ready")
    tenant_id = str(request.headers.get("x-tenant-id") or "").strip().lower() or None
    source = get_source_or_404(source_id, tenant_id)
    result = await poll_source(source)
    if result.get("status") == "error":
        raise HTTPException(status_code=502, detail=result.get("error", "Source sync failed"))
    return result

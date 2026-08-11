from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
import os
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Any
from uuid import uuid4

import httpx
from ai_providers import BaseLLMProvider, OllamaProvider, OpenAIProvider
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from airs_shared.dlq import build_dlq_payload
from airs_shared.kafka import produce_json
from airs_shared.models import (
    AnalyzeRequest,
    Incident,
    LLMConfigRequest,
    RCAFeedbackRequest,
    RCARegenerateRequest,
    RCAResult,
)
from airs_shared.monitoring import metrics_response
from airs_shared.normalize import normalize_log
from airs_shared.opensearch import build_client, ensure_index, upsert_doc
from airs_shared.settings import AIRSSettings, get_settings
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram
from rca import build_prompt, deterministic_fallback, heuristic_confidence
from redis import asyncio as redis_async

settings = get_settings()
app = FastAPI(title="AIRS AI Service")
logger = logging.getLogger("ai-service")


@dataclass
class RuntimeLLMConfig:
    provider: str
    model: str


runtime_llm_config: RuntimeLLMConfig | None = None
consumer: AIOKafkaConsumer | None = None
producer: AIOKafkaProducer | None = None
worker_task: asyncio.Task[None] | None = None
redis_client: redis_async.Redis | None = None

os_client = build_client(settings.opensearch.url)
KEYWORDS = ["error", "exception", "timeout", "connection refused", "oom", "5xx"]
FEEDBACK_INDEX = "airs-rca-feedback"
TOPOLOGY_INDEX = "airs-topology"
SUPPRESSIONS_INDEX = "airs-suppressions"

RCA_SUCCESS_TOTAL = Counter(
    "airs_rca_success_total",
    "Count of successful RCA generations",
    labelnames=("service", "severity"),
)
RCA_FAILURE_TOTAL = Counter(
    "airs_rca_failure_total",
    "Count of failed RCA generations",
    labelnames=("service", "severity"),
)
RCA_GENERATION_DURATION_SECONDS = Histogram(
    "airs_rca_generation_duration_seconds",
    "RCA generation latency in seconds",
    labelnames=("model", "severity"),
)
RCA_ROUTED_MODEL_TOTAL = Counter(
    "airs_rca_routed_model_total",
    "Count of RCA requests routed by selected model",
    labelnames=("model", "severity"),
)
RCA_REGENERATE_TOTAL = Counter(
    "airs_rca_regenerate_total",
    "Count of RCA regenerate requests",
    labelnames=("status",),
)
RCA_FEEDBACK_TOTAL = Counter(
    "airs_rca_feedback_total",
    "Count of submitted RCA feedback items",
    labelnames=("rating",),
)
DLQ_PUBLISHED_TOTAL = Counter(
    "airs_dlq_published_total",
    "Count of DLQ events published by source topic",
    labelnames=("source_topic",),
)
CONSUMER_BATCH_SIZE = Gauge(
    "airs_ai_batch_size",
    "Number of messages fetched in the latest batch",
)


def active_provider_config() -> RuntimeLLMConfig:
    if runtime_llm_config is not None:
        return runtime_llm_config
    return RuntimeLLMConfig(provider=settings.llm.provider, model=settings.llm.model)


def build_provider(active: RuntimeLLMConfig, cfg: AIRSSettings) -> BaseLLMProvider:
    if active.provider == "ollama":
        return OllamaProvider(
            base_url=cfg.llm.ollama_base_url,
            model=active.model,
            timeout_seconds=cfg.llm.timeout_seconds,
        )

    if active.provider == "openai":
        api_key = os.getenv(cfg.llm.openai_api_key_env)
        if not api_key:
            raise RuntimeError(f"Missing environment variable: {cfg.llm.openai_api_key_env}")
        return OpenAIProvider(
            base_url=cfg.llm.openai_base_url,
            api_key=api_key,
            model=active.model,
            timeout_seconds=cfg.llm.timeout_seconds,
        )

    raise RuntimeError(f"Unsupported provider: {active.provider}")


def severity_name(value: Any) -> str:
    if hasattr(value, "value"):
        return str(value.value)
    return str(value or "warning").lower()


def tenant_scope_filter(tenant_id: str) -> dict[str, Any]:
    if tenant_id == "default":
        return {
            "bool": {
                "should": [
                    {"term": {"tenant_id": tenant_id}},
                    {"bool": {"must_not": [{"exists": {"field": "tenant_id"}}]}},
                ],
                "minimum_should_match": 1,
            }
        }
    return {"term": {"tenant_id": tenant_id}}


def cheaper_model_for(active: RuntimeLLMConfig) -> str:
    """The lower-cost tier for the provider that is currently active.

    settings.llm.fallback_model names an Ollama model. Using it verbatim after a
    runtime switch to OpenAI would send an Ollama model name to the OpenAI API,
    so it only applies while the active provider is the one it was configured
    for. For any other provider, staying on the active model is correct: the
    routing decision that still holds is the deterministic tier below.
    """
    if active.provider == settings.llm.provider:
        return settings.llm.fallback_model
    return active.model


def select_model_for_context(context: dict[str, Any], active: RuntimeLLMConfig) -> tuple[str, bool]:
    force_model = context.get("force_model")
    if isinstance(force_model, str) and force_model.strip():
        return force_model.strip(), True

    severity = severity_name(context.get("severity"))
    if severity == "critical":
        return active.model, True
    if severity == "warning":
        return cheaper_model_for(active), True

    # Low-severity contexts default to deterministic fallback for latency/cost control.
    return "deterministic", False


def enrich_with_hybrid_confidence(
    payload: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    llm_conf = payload.get("confidence", 0.5)
    try:
        llm_conf_value = float(llm_conf)
    except (TypeError, ValueError):
        llm_conf_value = 0.5

    heuristic = heuristic_confidence(context)
    hybrid = max(0.0, min(1.0, (llm_conf_value + heuristic) / 2))
    payload["confidence"] = round(hybrid, 2)

    if not payload.get("evidence"):
        payload["evidence"] = [
            {
                "timestamp": item.get("timestamp"),
                "message": item.get("message"),
                "service": item.get("service"),
            }
            for item in context.get("logs", [])[:5]
        ]
    return payload


async def generate_with_retries(
    provider: BaseLLMProvider,
    prompt: str,
    context: dict[str, Any],
    model: str,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(settings.llm.retries + 1):
        try:
            return await provider.generate(prompt, {**context, "model": model})
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == settings.llm.retries:
                break
            await asyncio.sleep(2**attempt)

    raise RuntimeError(f"LLM generate failed after retries: {last_error}")


def infer_anomalies(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    for item in logs:
        message = str(item.get("message", "")).lower()
        level = str(item.get("level", "info")).lower()
        matched = [kw for kw in KEYWORDS if kw in message]
        if matched or level in {"error", "critical", "fatal"}:
            anomalies.append(
                {
                    "service": item.get("service", "unknown-service"),
                    "timestamp": item.get("timestamp"),
                    "reasons": matched
                    + ([f"level={level}"] if level in {"error", "critical", "fatal"} else []),
                    "message": item.get("message", ""),
                }
            )
    return anomalies


def fetch_source_metadata(tenant_id: str, service: str) -> dict[str, Any]:
    query = {
        "query": {
            "bool": {
                "filter": [tenant_scope_filter(tenant_id)],
                "should": [
                    {"term": {"default_service.keyword": service}},
                    {"term": {"default_service": service}},
                ],
                "minimum_should_match": 1,
            }
        },
        "size": 1,
        "sort": [{"updated_at": {"order": "desc"}}],
    }
    try:
        result = os_client.search(index=settings.opensearch.sources_index, body=query)
        hits = result.get("hits", {}).get("hits", [])
        if not hits:
            return {}

        source = hits[0].get("_source", {})
        return {
            "name": source.get("name"),
            "default_service": source.get("default_service"),
            "method": source.get("method"),
            "poll_interval_seconds": source.get("poll_interval_seconds"),
            "window_duration_minutes": source.get("window_duration_minutes"),
            "min_signal_count": source.get("min_signal_count"),
            "endpoint": source.get("endpoint"),
        }
    except Exception:  # noqa: BLE001
        return {}


def fetch_recent_incidents(
    tenant_id: str,
    service: str,
    exclude_id: str | None = None,
) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = [
        tenant_scope_filter(tenant_id),
        {
            "bool": {
                "should": [
                    {"term": {"service.keyword": service}},
                    {"term": {"service": service}},
                ],
                "minimum_should_match": 1,
            }
        },
    ]

    query: dict[str, Any] = {
        "query": {"bool": {"filter": filters}},
        "size": 6,
        "sort": [{"created_at": {"order": "desc"}}],
    }

    try:
        result = os_client.search(index=settings.opensearch.incidents_index, body=query)
    except Exception:  # noqa: BLE001
        return []

    items: list[dict[str, Any]] = []
    for hit in result.get("hits", {}).get("hits", []):
        incident_id = hit.get("_id")
        if exclude_id and incident_id == exclude_id:
            continue
        source = hit.get("_source", {})
        items.append(
            {
                "id": incident_id,
                "created_at": source.get("created_at"),
                "severity": source.get("severity"),
                "status": source.get("status"),
                "summary": source.get("summary"),
                "root_cause": (source.get("rca") or {}).get("root_cause"),
            }
        )
        if len(items) >= 5:
            break

    return items


def fetch_active_suppressions(
    tenant_id: str,
    service: str,
    at_time: datetime,
) -> list[dict[str, Any]]:
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"enabled": True}},
                    tenant_scope_filter(tenant_id),
                ]
            }
        },
        "size": 200,
        "sort": [{"updated_at": {"order": "desc"}}],
    }
    try:
        result = os_client.search(index=SUPPRESSIONS_INDEX, body=query)
    except Exception:  # noqa: BLE001
        return []

    items: list[dict[str, Any]] = []
    for hit in result.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        starts_raw = source.get("starts_at")
        ends_raw = source.get("ends_at")
        pattern = str(source.get("service_pattern") or "")

        if not starts_raw or not ends_raw or not pattern:
            continue

        try:
            starts_at = datetime.fromisoformat(str(starts_raw).replace("Z", "+00:00"))
            ends_at = datetime.fromisoformat(str(ends_raw).replace("Z", "+00:00"))
        except ValueError:
            continue

        if starts_at.tzinfo is None:
            starts_at = starts_at.replace(tzinfo=UTC)
        if ends_at.tzinfo is None:
            ends_at = ends_at.replace(tzinfo=UTC)

        if not (starts_at <= at_time <= ends_at):
            continue
        if not fnmatch.fnmatch(service, pattern):
            continue

        items.append(
            {
                "id": hit.get("_id"),
                "service_pattern": pattern,
                "reason": source.get("reason"),
                "starts_at": starts_at.isoformat(),
                "ends_at": ends_at.isoformat(),
            }
        )

    return items


def fetch_topology(tenant_id: str, service: str) -> list[dict[str, Any]]:
    query = {
        "query": {
            "bool": {
                "filter": [tenant_scope_filter(tenant_id)],
                "should": [
                    {"term": {"upstream.keyword": service}},
                    {"term": {"downstream.keyword": service}},
                    {"term": {"upstream": service}},
                    {"term": {"downstream": service}},
                ],
                "minimum_should_match": 1,
            }
        },
        "size": 20,
    }
    try:
        result = os_client.search(index=TOPOLOGY_INDEX, body=query)
    except Exception:  # noqa: BLE001
        return []

    edges: list[dict[str, Any]] = []
    for hit in result.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        edges.append(
            {
                "upstream": source.get("upstream"),
                "downstream": source.get("downstream"),
                "dependency_type": source.get("dependency_type"),
            }
        )
    return edges


def normalize_optional_logs(logs: list[dict[str, Any] | str] | None) -> list[dict[str, Any]]:
    if not logs:
        return []
    return [normalize_log(item).model_dump(mode="json") for item in logs]


def build_incident_context(
    incident: Incident,
    *,
    extra_logs: list[dict[str, Any]] | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    logs = [
        {
            "timestamp": item.get("timestamp"),
            "service": incident.service,
            "message": item.get("message"),
            "level": "critical" if severity_name(incident.severity) == "critical" else "warning",
        }
        for item in incident.timeline
    ]

    if extra_logs:
        logs.extend(extra_logs)

    top_logs = sorted(
        logs,
        key=lambda item: str(item.get("level", "")).lower() in {"critical", "fatal"},
        reverse=True,
    )[:15]

    context: dict[str, Any] = {
        "incident_id": incident.id,
        "service": incident.service,
        "tenant_id": incident.tenant_id,
        "severity": severity_name(incident.severity),
        "status": severity_name(incident.status),
        "summary": incident.summary,
        "timeline": incident.timeline,
        "logs": logs,
        "top_logs": top_logs,
        "anomalies": [{"id": aid} for aid in incident.anomaly_ids],
        "source_metadata": fetch_source_metadata(incident.tenant_id, incident.service),
        "recent_incidents": fetch_recent_incidents(
            incident.tenant_id,
            incident.service,
            exclude_id=incident.id,
        ),
        "active_suppressions": fetch_active_suppressions(
            incident.tenant_id,
            incident.service,
            incident.updated_at,
        ),
        "topology": fetch_topology(incident.tenant_id, incident.service),
    }

    if notes:
        context["notes"] = notes

    return context


def build_manual_context(logs: list[dict[str, Any]], notes: str | None = None) -> dict[str, Any]:
    anomalies = infer_anomalies(logs)
    service = str(logs[0].get("service", "unknown-service")) if logs else "unknown-service"
    tenant_id = str(logs[0].get("tenant_id", "default")) if logs else "default"
    severity = "warning" if anomalies else "info"

    top_logs = sorted(
        logs,
        key=lambda item: str(item.get("level", "")).lower() in {"critical", "fatal"},
        reverse=True,
    )[:15]

    context: dict[str, Any] = {
        "service": service,
        "tenant_id": tenant_id,
        "severity": severity,
        "logs": logs,
        "top_logs": top_logs,
        "timeline": [{"timestamp": item["timestamp"], "message": item["message"]} for item in logs],
        "anomalies": anomalies,
        "source_metadata": fetch_source_metadata(tenant_id, service),
        "recent_incidents": fetch_recent_incidents(tenant_id, service),
        "active_suppressions": fetch_active_suppressions(tenant_id, service, datetime.now(UTC)),
        "topology": fetch_topology(tenant_id, service),
    }

    if notes:
        context["notes"] = notes

    return context


async def generate_rca(context: dict[str, Any]) -> tuple[RCAResult, str]:
    prompt = build_prompt(context)
    active = active_provider_config()
    severity = severity_name(context.get("severity"))
    model_name, use_llm = select_model_for_context(context, active)

    RCA_ROUTED_MODEL_TOTAL.labels(model=model_name, severity=severity).inc()

    if not use_llm:
        return deterministic_fallback(context), "deterministic"

    # Provider construction can fail on misconfiguration (for example, provider
    # set to openai with no API key present). That is a model-availability
    # problem like any other and must not deny the incident an RCA.
    try:
        provider = build_provider(active, settings)
    except Exception:  # noqa: BLE001
        logger.exception("LLM provider unavailable, using deterministic RCA")
        return deterministic_fallback(context), "deterministic"

    try:
        llm_payload = await generate_with_retries(provider, prompt, context, model_name)
        enriched = enrich_with_hybrid_confidence(llm_payload, context)
        return RCAResult.model_validate(enriched), model_name
    except Exception:  # noqa: BLE001
        pass

    fallback_model = settings.llm.fallback_model
    if fallback_model != model_name:
        try:
            fallback_payload = await generate_with_retries(
                provider,
                prompt,
                context,
                fallback_model,
            )
            enriched = enrich_with_hybrid_confidence(fallback_payload, context)
            return RCAResult.model_validate(enriched), fallback_model
        except Exception:  # noqa: BLE001
            pass

    return deterministic_fallback(context), "deterministic"


def get_incident_or_404(incident_id: str) -> Incident:
    try:
        result = os_client.get(index=settings.opensearch.incidents_index, id=incident_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Incident not found") from exc
    return Incident.model_validate({"id": result["_id"], **result["_source"]})


async def process_incident(incident_payload: dict[str, Any]) -> None:
    incident = Incident.model_validate(incident_payload)
    context = build_incident_context(incident)

    started_at = perf_counter()
    rca, model_used = await generate_rca(context)
    RCA_GENERATION_DURATION_SECONDS.labels(
        model=model_used,
        severity=incident.severity.value,
    ).observe(perf_counter() - started_at)

    incident.rca = rca
    incident.updated_at = datetime.now(UTC)

    upsert_doc(
        os_client,
        settings.opensearch.incidents_index,
        incident.id,
        incident.model_dump(mode="json"),
    )
    RCA_SUCCESS_TOTAL.labels(service=incident.service, severity=incident.severity.value).inc()


async def publish_to_dlq(
    *,
    payload: dict[str, Any],
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
            source_topic=settings.kafka.topics.incidents,
            payload=payload,
            error=error,
            partition=partition,
            offset=offset,
        ),
    )
    DLQ_PUBLISHED_TOTAL.labels(source_topic=settings.kafka.topics.incidents).inc()


async def consumer_loop() -> None:
    assert consumer is not None
    while True:
        try:
            records = await consumer.getmany(timeout_ms=1000, max_records=100)
            CONSUMER_BATCH_SIZE.set(sum(len(tp_records) for tp_records in records.values()))
            for tp, tp_records in records.items():
                for message in tp_records:
                    payload = json.loads(message.value.decode("utf-8"))
                    try:
                        await process_incident(payload)
                    except Exception as exc:  # noqa: BLE001
                        service = str(payload.get("service", "unknown-service"))
                        severity = str(payload.get("severity", "info"))
                        RCA_FAILURE_TOTAL.labels(service=service, severity=severity).inc()
                        await publish_to_dlq(
                            payload=payload,
                            error=exc,
                            partition=tp.partition,
                            offset=message.offset,
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("RCA consumer loop error: %s", exc)
            await asyncio.sleep(1)


async def llm_healthcheck() -> bool:
    active = active_provider_config()
    timeout = settings.llm.timeout_seconds

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            if active.provider == "ollama":
                resp = await client.get(f"{settings.llm.ollama_base_url.rstrip('/')}/api/tags")
            else:
                api_key = os.getenv(settings.llm.openai_api_key_env)
                if not api_key:
                    return False
                resp = await client.get(
                    f"{settings.llm.openai_base_url.rstrip('/')}/models",
                    headers={"Authorization": f"Bearer {api_key}"},
                )
        return resp.status_code < 400
    except Exception:  # noqa: BLE001
        return False


@app.on_event("startup")
async def startup() -> None:
    global consumer, producer, worker_task, redis_client

    ensure_index(os_client, settings.opensearch.incidents_index)
    ensure_index(os_client, FEEDBACK_INDEX)
    ensure_index(os_client, TOPOLOGY_INDEX)
    ensure_index(os_client, SUPPRESSIONS_INDEX)

    consumer = AIOKafkaConsumer(
        settings.kafka.topics.incidents,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id="airs-ai-service",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka.bootstrap_servers)
    await consumer.start()
    await producer.start()

    redis_client = redis_async.from_url(settings.redis.url)
    worker_task = asyncio.create_task(consumer_loop())


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
    if redis_client is not None:
        await redis_client.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    ready, deps = await readiness_dependencies()
    return {
        "status": "ok" if ready else "degraded",
        "service": "ai-service",
        "dependencies": deps,
    }


async def readiness_dependencies() -> tuple[bool, dict[str, bool]]:
    redis_ok = False
    if redis_client is not None:
        try:
            redis_ok = bool(await redis_client.ping())
        except Exception:  # noqa: BLE001
            redis_ok = False

    llm_ok = await llm_healthcheck()

    os_ok = False
    try:
        os_ok = bool(os_client.ping())
    except Exception:  # noqa: BLE001
        os_ok = False

    deps = {
        "kafka": consumer is not None and producer is not None,
        "redis": redis_ok,
        "llm": llm_ok,
        "opensearch": os_ok,
    }
    return all(deps.values()), deps


@app.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "ok", "service": "ai-service"}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    ready, deps = await readiness_dependencies()
    if ready:
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "service": "ai-service", "dependencies": deps},
        )
    return JSONResponse(
        status_code=503,
        content={"status": "degraded", "service": "ai-service", "dependencies": deps},
    )


@app.get("/metrics")
async def metrics() -> object:
    return metrics_response()


@app.post("/config")
async def update_llm_config(payload: LLMConfigRequest) -> dict[str, str]:
    global runtime_llm_config

    if payload.provider not in {"ollama", "openai"}:
        raise HTTPException(status_code=400, detail="Unsupported provider")

    runtime_llm_config = RuntimeLLMConfig(provider=payload.provider, model=payload.model)
    return {"provider": payload.provider, "model": payload.model}


@app.post("/analyze", response_model=RCAResult)
async def analyze(payload: AnalyzeRequest) -> RCAResult:
    serialized = [item if isinstance(item, str) else item for item in payload.logs]
    raw_bytes = len(json.dumps(serialized).encode("utf-8"))
    if raw_bytes > settings.pipeline.max_manual_analyze_kb * 1024:
        raise HTTPException(status_code=413, detail="Payload too large")

    logs = [normalize_log(item).model_dump(mode="json") for item in payload.logs]
    context = build_manual_context(logs)
    result, _ = await generate_rca(context)
    return result


@app.post("/incidents/{incident_id}/regenerate")
async def regenerate_incident_rca(
    incident_id: str,
    payload: RCARegenerateRequest,
) -> dict[str, Any]:
    incident = get_incident_or_404(incident_id)

    extra_logs = normalize_optional_logs(payload.logs)
    context = build_incident_context(incident, extra_logs=extra_logs, notes=payload.notes)

    started_at = perf_counter()
    try:
        rca, model_used = await generate_rca(context)
    except Exception as exc:  # noqa: BLE001
        RCA_REGENERATE_TOTAL.labels(status="error").inc()
        raise HTTPException(status_code=502, detail=f"RCA regenerate failed: {exc}") from exc

    RCA_REGENERATE_TOTAL.labels(status="ok").inc()
    RCA_GENERATION_DURATION_SECONDS.labels(
        model=model_used,
        severity=incident.severity.value,
    ).observe(perf_counter() - started_at)

    incident.rca = rca
    incident.updated_at = datetime.now(UTC)
    upsert_doc(
        os_client,
        settings.opensearch.incidents_index,
        incident.id,
        incident.model_dump(mode="json"),
    )

    return {"id": incident.id, **incident.model_dump(mode="json")}


@app.post("/incidents/{incident_id}/feedback")
async def submit_incident_rca_feedback(
    incident_id: str,
    payload: RCAFeedbackRequest,
) -> dict[str, Any]:
    incident = get_incident_or_404(incident_id)

    feedback_id = str(uuid4())
    feedback_doc = {
        "incident_id": incident_id,
        "tenant_id": incident.tenant_id,
        "service": incident.service,
        "severity": incident.severity.value,
        "rating": payload.rating,
        "correction": payload.correction,
        "submitted_at": datetime.now(UTC).isoformat(),
        "rca": incident.rca.model_dump(mode="json") if incident.rca is not None else None,
    }
    upsert_doc(os_client, FEEDBACK_INDEX, feedback_id, feedback_doc)
    RCA_FEEDBACK_TOTAL.labels(rating=payload.rating).inc()

    return {"id": feedback_id, **feedback_doc}

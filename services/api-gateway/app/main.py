from __future__ import annotations

import asyncio
import base64
import fnmatch
import hashlib
import hmac
import json
import re
from datetime import UTC, datetime
from typing import Any, AsyncIterator, Literal
from uuid import uuid4

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from airs_shared.kafka import produce_json
from airs_shared.monitoring import metrics_response
from airs_shared.models import (
    AnalyzeRequest,
    ChatOpsCommandRequest,
    ChatOpsConfig,
    ChatOpsConfigCreateRequest,
    DataSource,
    DataSourceCreateRequest,
    DataSourceView,
    DEFAULT_TENANT_ID,
    DetectionRule,
    DetectionRuleCreateRequest,
    IncidentStatus,
    LLMConfigRequest,
    RCAFeedbackRequest,
    RCARegenerateRequest,
    ReplayFilterRequest,
    ReplayRequest,
    RuleHistoryTestRequest,
    SimulateIngestRequest,
    SuppressionCreateRequest,
    SuppressionWindow,
    TopologyEdge,
    TopologyUpdateRequest,
    normalize_tenant,
)
from airs_shared.opensearch import build_client, ensure_index, upsert_doc
from airs_shared.schema_registry import topic_contracts
from airs_shared.settings import get_settings
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from prometheus_client import Counter
from pydantic import BaseModel, Field, field_validator
from redis import asyncio as redis_async

settings = get_settings()
app = FastAPI(title="AIRS API Gateway")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

os_client = build_client(settings.opensearch.url)
producer: AIOKafkaProducer | None = None
redis_client: redis_async.Redis | None = None
runtime_llm_config: LLMConfigRequest | None = None
AI_SERVICE_URL = "http://ai-service:8005"
INGESTION_SERVICE_URL = "http://ingestion-service:8001"
AUDIT_INDEX = "airs-audit-log"
WEBHOOKS_INDEX = "airs-webhooks"
RULES_INDEX = "airs-rules"
SUPPRESSIONS_INDEX = "airs-suppressions"
TOPOLOGY_INDEX = "airs-topology"
CHATOPS_INDEX = "airs-chatops"

INCIDENT_STATUS_UPDATES_TOTAL = Counter(
    "airs_incident_status_updates_total",
    "Count of incident status updates by target status",
    labelnames=("status",),
)
ANALYZE_REQUESTS_TOTAL = Counter(
    "airs_analyze_requests_total",
    "Count of manual analyze requests",
)
REPLAYED_MESSAGES_TOTAL = Counter(
    "airs_replayed_messages_total",
    "Count of replayed Kafka messages",
)
WEBHOOK_DELIVERIES_TOTAL = Counter(
    "airs_webhook_deliveries_total",
    "Count of outgoing webhook deliveries",
    labelnames=("event", "status"),
)
STREAM_EVENTS_TOTAL = Counter(
    "airs_stream_events_total",
    "Count of SSE events emitted",
    labelnames=("event",),
)
SIMULATION_MESSAGES_TOTAL = Counter(
    "airs_simulation_messages_total",
    "Count of synthetic logs generated via simulation endpoint",
    labelnames=("tenant_id", "service"),
)
CHATOPS_COMMANDS_TOTAL = Counter(
    "airs_chatops_commands_total",
    "Count of processed ChatOps commands by action",
    labelnames=("tenant_id", "action", "status"),
)


class IncidentStatusPatch(BaseModel):
    status: IncidentStatus


class WebhookSubscriptionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    url: str
    events: list[str] = Field(default_factory=lambda: ["incident.status_changed"])
    severity_filter: list[str] = Field(default_factory=list)
    secret: str | None = None
    enabled: bool = True
    max_attempts: int = Field(default=3, ge=1, le=10)
    timeout_seconds: int = Field(default=5, ge=1, le=30)

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        url = value.strip()
        if not url.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return url

    @field_validator("events")
    @classmethod
    def validate_events(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("events must contain at least one event")
        return cleaned


class WebhookSubscription(WebhookSubscriptionCreate):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str = DEFAULT_TENANT_ID
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_status: Literal["never", "ok", "error"] = "never"
    last_error: str | None = None
    last_sent_at: datetime | None = None


ALLOWED_STATUS_TRANSITIONS: dict[IncidentStatus, set[IncidentStatus]] = {
    IncidentStatus.open: {IncidentStatus.acknowledged, IncidentStatus.resolved},
    IncidentStatus.acknowledged: {IncidentStatus.resolved},
    IncidentStatus.resolved: {IncidentStatus.acknowledged},
}


def resolve_tenant_id(request: Request, explicit: str | None = None) -> str:
    candidate = explicit or request.headers.get("x-tenant-id") or DEFAULT_TENANT_ID
    return normalize_tenant(candidate)


def tenant_scope_filter(tenant_id: str) -> dict[str, Any]:
    if tenant_id == DEFAULT_TENANT_ID:
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


def assert_tenant_access(doc: dict[str, Any], tenant_id: str) -> None:
    doc_tenant = str(doc.get("tenant_id") or DEFAULT_TENANT_ID).strip().lower()
    if doc_tenant != tenant_id:
        raise HTTPException(status_code=404, detail="Resource not found")


def build_incident_query(
    tenant_id: str,
    severity: str | None,
    service: str | None,
    start_time: str | None,
    end_time: str | None,
) -> dict[str, Any]:
    filters: list[dict[str, Any]] = [tenant_scope_filter(tenant_id)]

    if severity:
        filters.append({"term": {"severity": severity}})
    if service:
        filters.append({"term": {"service": service}})
    if start_time or end_time:
        range_filter: dict[str, Any] = {}
        if start_time:
            range_filter["gte"] = start_time
        if end_time:
            range_filter["lte"] = end_time
        filters.append({"range": {"created_at": range_filter}})

    query: dict[str, Any] = {"match_all": {}}
    if filters:
        query = {"bool": {"filter": filters}}
    return query


def encode_cursor(sort_values: list[Any]) -> str | None:
    if not sort_values:
        return None
    raw = json.dumps(sort_values, default=str).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8")


def decode_cursor(cursor: str) -> list[Any]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("utf-8"))
        values = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc
    if not isinstance(values, list) or not values:
        raise HTTPException(status_code=400, detail="Invalid cursor")
    return values


def source_to_view(source: DataSource) -> DataSourceView:
    payload = source.model_dump(mode="python")
    payload.pop("auth_token", None)
    payload["has_auth_token"] = bool(source.auth_token)
    return DataSourceView.model_validate(payload)


def webhook_to_view(sub: WebhookSubscription) -> dict[str, Any]:
    payload = sub.model_dump(mode="json")
    payload.pop("secret", None)
    payload["has_secret"] = bool(sub.secret)
    return payload


def write_audit_event(
    *,
    tenant_id: str,
    action: str,
    entity_type: str,
    entity_id: str,
    details: dict[str, Any] | None = None,
) -> None:
    payload = {
        "id": str(uuid4()),
        "timestamp": datetime.now(UTC).isoformat(),
        "tenant_id": tenant_id,
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "details": details or {},
    }
    upsert_doc(os_client, AUDIT_INDEX, payload["id"], payload)


def get_source_or_404(source_id: str, tenant_id: str) -> DataSource:
    try:
        existing = os_client.get(index=settings.opensearch.sources_index, id=source_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Source not found") from exc
    source = DataSource.model_validate({"id": existing["_id"], **existing["_source"]})
    assert_tenant_access(source.model_dump(mode="python"), tenant_id)
    return source


def upsert_source(source: DataSource) -> DataSource:
    source.updated_at = datetime.now(UTC)
    upsert_doc(
        os_client,
        settings.opensearch.sources_index,
        source.id,
        source.model_dump(mode="json"),
    )
    return source


def get_webhook_or_404(webhook_id: str, tenant_id: str) -> WebhookSubscription:
    try:
        existing = os_client.get(index=WEBHOOKS_INDEX, id=webhook_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Webhook not found") from exc
    webhook = WebhookSubscription.model_validate({"id": existing["_id"], **existing["_source"]})
    assert_tenant_access(webhook.model_dump(mode="python"), tenant_id)
    return webhook


def get_rule_or_404(rule_id: str, tenant_id: str) -> DetectionRule:
    try:
        existing = os_client.get(index=RULES_INDEX, id=rule_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Rule not found") from exc
    rule = DetectionRule.model_validate({"id": existing["_id"], **existing["_source"]})
    assert_tenant_access(rule.model_dump(mode="python"), tenant_id)
    return rule


def get_suppression_or_404(suppression_id: str, tenant_id: str) -> SuppressionWindow:
    try:
        existing = os_client.get(index=SUPPRESSIONS_INDEX, id=suppression_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Suppression not found") from exc
    suppression = SuppressionWindow.model_validate({"id": existing["_id"], **existing["_source"]})
    assert_tenant_access(suppression.model_dump(mode="python"), tenant_id)
    return suppression


def upsert_webhook(subscription: WebhookSubscription) -> WebhookSubscription:
    subscription.updated_at = datetime.now(UTC)
    upsert_doc(
        os_client,
        WEBHOOKS_INDEX,
        subscription.id,
        subscription.model_dump(mode="json"),
    )
    return subscription


def upsert_rule(rule: DetectionRule) -> DetectionRule:
    rule.updated_at = datetime.now(UTC)
    upsert_doc(
        os_client,
        RULES_INDEX,
        rule.id,
        rule.model_dump(mode="json"),
    )
    return rule


def upsert_suppression(suppression: SuppressionWindow) -> SuppressionWindow:
    suppression.updated_at = datetime.now(UTC)
    upsert_doc(
        os_client,
        SUPPRESSIONS_INDEX,
        suppression.id,
        suppression.model_dump(mode="json"),
    )
    return suppression


def list_webhooks_internal(tenant_id: str) -> list[WebhookSubscription]:
    query = {
        "query": {"bool": {"filter": [tenant_scope_filter(tenant_id)]}},
        "size": 500,
        "sort": [{"created_at": {"order": "desc"}}],
    }
    result = os_client.search(index=WEBHOOKS_INDEX, body=query)

    items: list[WebhookSubscription] = []
    for hit in result.get("hits", {}).get("hits", []):
        try:
            items.append(WebhookSubscription.model_validate({"id": hit["_id"], **hit["_source"]}))
        except Exception:  # noqa: BLE001
            continue
    return items


def list_rules_internal(tenant_id: str) -> list[DetectionRule]:
    query = {
        "query": {"bool": {"filter": [tenant_scope_filter(tenant_id)]}},
        "size": 500,
        "sort": [{"updated_at": {"order": "desc"}}],
    }
    result = os_client.search(index=RULES_INDEX, body=query)

    items: list[DetectionRule] = []
    for hit in result.get("hits", {}).get("hits", []):
        try:
            items.append(DetectionRule.model_validate({"id": hit["_id"], **hit["_source"]}))
        except Exception:  # noqa: BLE001
            continue
    return items


def list_suppressions_internal(tenant_id: str) -> list[SuppressionWindow]:
    query = {
        "query": {"bool": {"filter": [tenant_scope_filter(tenant_id)]}},
        "size": 500,
        "sort": [{"updated_at": {"order": "desc"}}],
    }
    result = os_client.search(index=SUPPRESSIONS_INDEX, body=query)

    items: list[SuppressionWindow] = []
    for hit in result.get("hits", {}).get("hits", []):
        try:
            items.append(SuppressionWindow.model_validate({"id": hit["_id"], **hit["_source"]}))
        except Exception:  # noqa: BLE001
            continue
    return items


def list_topology_edges_internal(tenant_id: str, service: str | None = None) -> list[TopologyEdge]:
    filters: list[dict[str, Any]] = [tenant_scope_filter(tenant_id)]
    if service:
        filters.append(
            {
                "bool": {
                    "should": [
                        {"term": {"upstream.keyword": service}},
                        {"term": {"downstream.keyword": service}},
                        {"term": {"upstream": service}},
                        {"term": {"downstream": service}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        )

    query = {
        "query": {"bool": {"filter": filters}},
        "size": 2000,
        "sort": [{"updated_at": {"order": "desc"}}],
    }
    result = os_client.search(index=TOPOLOGY_INDEX, body=query)

    items: list[TopologyEdge] = []
    for hit in result.get("hits", {}).get("hits", []):
        try:
            items.append(TopologyEdge.model_validate({"id": hit["_id"], **hit["_source"]}))
        except Exception:  # noqa: BLE001
            continue
    return items


def replace_topology_edges(tenant_id: str, payload: TopologyUpdateRequest) -> list[TopologyEdge]:
    try:
        os_client.delete_by_query(
            index=TOPOLOGY_INDEX,
            body={"query": {"bool": {"filter": [tenant_scope_filter(tenant_id)]}}},
            refresh=True,
            conflicts="proceed",
        )
    except Exception:  # noqa: BLE001
        pass

    edges: list[TopologyEdge] = []
    for item in payload.edges:
        edge = TopologyEdge(
            tenant_id=tenant_id,
            upstream=item.upstream,
            downstream=item.downstream,
            dependency_type=item.dependency_type,
        )
        upsert_doc(os_client, TOPOLOGY_INDEX, edge.id, edge.model_dump(mode="json"))
        edges.append(edge)
    return edges


def get_chatops_config_internal(tenant_id: str) -> ChatOpsConfig | None:
    query = {
        "query": {"bool": {"filter": [tenant_scope_filter(tenant_id)]}},
        "size": 1,
        "sort": [{"updated_at": {"order": "desc"}}],
    }
    result = os_client.search(index=CHATOPS_INDEX, body=query)
    hits = result.get("hits", {}).get("hits", [])
    if not hits:
        return None
    return ChatOpsConfig.model_validate({"id": hits[0]["_id"], **hits[0]["_source"]})


def upsert_chatops_config(config: ChatOpsConfig) -> ChatOpsConfig:
    config.updated_at = datetime.now(UTC)
    upsert_doc(os_client, CHATOPS_INDEX, config.id, config.model_dump(mode="json"))
    return config


def chatops_to_view(config: ChatOpsConfig) -> dict[str, Any]:
    payload = config.model_dump(mode="json")
    payload.pop("signing_secret", None)
    payload["has_signing_secret"] = bool(config.signing_secret)
    return payload


async def deliver_chatops_message(
    tenant_id: str,
    text: str,
    *,
    context: dict[str, Any] | None = None,
) -> bool:
    config = get_chatops_config_internal(tenant_id)
    if config is None or not config.enabled:
        return False

    payload: dict[str, Any]
    if config.provider == "slack":
        payload = {"text": text, "metadata": context or {}}
    else:
        payload = {
            "text": text,
            "sections": [{"type": "TextBlock", "text": text}],
            "context": context or {},
        }

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.post(config.webhook_url, json=payload)
            response.raise_for_status()
        config.last_status = "ok"
        config.last_error = None
        config.last_sent_at = datetime.now(UTC)
        upsert_chatops_config(config)
        return True
    except Exception as exc:  # noqa: BLE001
        config.last_status = "error"
        config.last_error = str(exc)[:500]
        upsert_chatops_config(config)
        return False


def detect_rule_match(rule: DetectionRule, log_doc: dict[str, Any]) -> bool:
    message = str(log_doc.get("message", ""))
    lower_message = message.lower()
    service = str(log_doc.get("service", ""))
    level = str(log_doc.get("level", "info")).lower()

    if rule.service_pattern not in {"", "*"} and not re.match(
        fnmatch.translate(rule.service_pattern),
        service,
    ):
        return False

    if rule.match_type.value == "keyword":
        return rule.pattern.lower() in lower_message
    if rule.match_type.value == "regex":
        try:
            return re.search(rule.pattern, message, flags=re.IGNORECASE) is not None
        except re.error:
            return False
    if rule.match_type.value == "threshold":
        try:
            threshold = float(rule.pattern)
        except ValueError:
            return False
        score = 4.0 if level in {"critical", "fatal", "error"} else 1.0
        return score >= threshold
    if rule.match_type.value == "composite":
        clauses = [part.strip().lower() for part in rule.pattern.split("&&") if part.strip()]
        return bool(clauses) and all(clause in lower_message for clause in clauses)
    return False


async def deliver_webhook(
    subscription: WebhookSubscription,
    *,
    event_name: str,
    payload: dict[str, Any],
) -> None:
    envelope = {
        "event": event_name,
        "sent_at": datetime.now(UTC).isoformat(),
        "payload": payload,
    }
    body = json.dumps(envelope, default=str).encode("utf-8")

    headers = {
        "content-type": "application/json",
        "x-airs-event": event_name,
    }
    if subscription.secret:
        signature = hmac.new(
            subscription.secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).hexdigest()
        headers["x-airs-signature"] = f"sha256={signature}"

    last_error: Exception | None = None
    for attempt in range(subscription.max_attempts):
        try:
            async with httpx.AsyncClient(timeout=subscription.timeout_seconds) as client:
                response = await client.post(subscription.url, headers=headers, content=body)
                response.raise_for_status()

            subscription.last_status = "ok"
            subscription.last_error = None
            subscription.last_sent_at = datetime.now(UTC)
            upsert_webhook(subscription)
            WEBHOOK_DELIVERIES_TOTAL.labels(event=event_name, status="ok").inc()
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < subscription.max_attempts - 1:
                await asyncio.sleep(2**attempt)

    subscription.last_status = "error"
    subscription.last_error = str(last_error)[:500] if last_error else "unknown error"
    upsert_webhook(subscription)
    WEBHOOK_DELIVERIES_TOTAL.labels(event=event_name, status="error").inc()


async def dispatch_webhook_event(
    tenant_id: str,
    event_name: str,
    payload: dict[str, Any],
    *,
    severity: str | None = None,
) -> None:
    subscriptions = list_webhooks_internal(tenant_id)
    for subscription in subscriptions:
        if not subscription.enabled:
            continue
        if event_name not in subscription.events:
            continue
        if (
            severity
            and subscription.severity_filter
            and severity not in subscription.severity_filter
        ):
            continue
        await deliver_webhook(subscription, event_name=event_name, payload=payload)


async def apply_incident_status_transition(
    tenant_id: str,
    incident_id: str,
    target_status: IncidentStatus,
) -> dict[str, Any]:
    try:
        existing = os_client.get(index=settings.opensearch.incidents_index, id=incident_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Incident not found") from exc

    body = existing["_source"]
    assert_tenant_access(body, tenant_id)
    current_status_raw = str(body.get("status", IncidentStatus.open.value))
    try:
        current_status = IncidentStatus(current_status_raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"Unsupported current incident status: {current_status_raw}",
        ) from exc

    if target_status == current_status:
        return {"id": incident_id, **body}

    allowed = ALLOWED_STATUS_TRANSITIONS.get(current_status, set())
    if target_status not in allowed:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "Invalid status transition",
                "from": current_status.value,
                "to": target_status.value,
                "allowed": sorted(item.value for item in allowed),
            },
        )

    body["status"] = target_status.value
    body["updated_at"] = datetime.now(UTC).isoformat()
    upsert_doc(os_client, settings.opensearch.incidents_index, incident_id, body)

    INCIDENT_STATUS_UPDATES_TOTAL.labels(status=target_status.value).inc()
    write_audit_event(
        tenant_id=tenant_id,
        action="incident.status_changed",
        entity_type="incident",
        entity_id=incident_id,
        details={
            "from": current_status.value,
            "to": target_status.value,
            "severity": body.get("severity"),
            "service": body.get("service"),
        },
    )

    await dispatch_webhook_event(
        tenant_id,
        "incident.status_changed",
        {
            "incident_id": incident_id,
            "from": current_status.value,
            "to": target_status.value,
            "incident": {"id": incident_id, **body},
        },
        severity=str(body.get("severity")) if body.get("severity") else None,
    )
    if str(body.get("severity", "")).lower() == "critical":
        await deliver_chatops_message(
            tenant_id,
            (
                f"Incident {incident_id} is {target_status.value} "
                f"(service={body.get('service')}, severity=critical)"
            ),
            context={
                "incident_id": incident_id,
                "status": target_status.value,
                "service": body.get("service"),
                "severity": body.get("severity"),
            },
        )

    return {"id": incident_id, **body}


@app.on_event("startup")
async def startup() -> None:
    global producer, redis_client

    ensure_index(os_client, settings.opensearch.incidents_index)
    ensure_index(os_client, settings.opensearch.sources_index)
    ensure_index(os_client, AUDIT_INDEX)
    ensure_index(os_client, WEBHOOKS_INDEX)
    ensure_index(os_client, RULES_INDEX)
    ensure_index(os_client, SUPPRESSIONS_INDEX)
    ensure_index(os_client, TOPOLOGY_INDEX)
    ensure_index(os_client, CHATOPS_INDEX)

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka.bootstrap_servers)
    await producer.start()

    redis_client = redis_async.from_url(settings.redis.url)


@app.on_event("shutdown")
async def shutdown() -> None:
    if producer is not None:
        await producer.stop()
    if redis_client is not None:
        await redis_client.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    ready, deps = await readiness_dependencies()
    return {
        "status": "ok" if ready else "degraded",
        "service": "api-gateway",
        "dependencies": deps,
    }


async def readiness_dependencies() -> tuple[bool, dict[str, bool]]:
    redis_ok = False
    if redis_client is not None:
        try:
            redis_ok = bool(await redis_client.ping())
        except Exception:  # noqa: BLE001
            redis_ok = False

    os_ok = False
    try:
        os_ok = bool(os_client.ping())
    except Exception:  # noqa: BLE001
        os_ok = False

    llm_ok = False
    kafka_ok = producer is not None

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{AI_SERVICE_URL}/health")
            response.raise_for_status()
            ai_health = response.json()
            llm_ok = bool(ai_health.get("dependencies", {}).get("llm"))
    except Exception:  # noqa: BLE001
        llm_ok = False

    deps = {
        "kafka": kafka_ok,
        "redis": redis_ok,
        "llm": llm_ok,
        "opensearch": os_ok,
    }
    return all(deps.values()), deps


@app.get("/health/live")
async def health_live() -> dict[str, str]:
    return {"status": "ok", "service": "api-gateway"}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    ready, deps = await readiness_dependencies()
    if ready:
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "service": "api-gateway", "dependencies": deps},
        )
    return JSONResponse(
        status_code=503,
        content={"status": "degraded", "service": "api-gateway", "dependencies": deps},
    )


@app.get("/metrics")
async def metrics() -> object:
    return metrics_response()


@app.get("/incidents")
@app.get("/v1/incidents")
async def list_incidents(
    request: Request,
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=100),
    severity: str | None = None,
    service: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=100),
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    if cursor is not None or limit is not None:
        cursor_limit = limit or 20
        query: dict[str, Any] = {
            "query": build_incident_query(tenant_id, severity, service, start_time, end_time),
            "size": cursor_limit,
            "sort": [
                {"created_at": {"order": "desc"}},
                {"_id": {"order": "desc"}},
            ],
        }
        if cursor is not None:
            query["search_after"] = decode_cursor(cursor)

        result = os_client.search(index=settings.opensearch.incidents_index, body=query)
        hits = result.get("hits", {}).get("hits", [])
        items = [{"id": hit["_id"], **hit["_source"]} for hit in hits]

        next_cursor = None
        if len(hits) == cursor_limit:
            next_cursor = encode_cursor(hits[-1].get("sort", []))

        return {
            "items": items,
            "limit": cursor_limit,
            "cursor": cursor,
            "next_cursor": next_cursor,
        }

    query = {
        "query": build_incident_query(tenant_id, severity, service, start_time, end_time),
        "from": (page - 1) * size,
        "size": size,
        "sort": [{"created_at": {"order": "desc"}}],
    }

    result = os_client.search(index=settings.opensearch.incidents_index, body=query)
    items = [{"id": hit["_id"], **hit["_source"]} for hit in result.get("hits", {}).get("hits", [])]
    total = result.get("hits", {}).get("total", {}).get("value", 0)

    return {
        "items": items,
        "page": page,
        "size": size,
        "total": total,
    }


@app.get("/incidents/{incident_id}")
@app.get("/v1/incidents/{incident_id}")
async def get_incident(request: Request, incident_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    try:
        result = os_client.get(index=settings.opensearch.incidents_index, id=incident_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Incident not found") from exc

    assert_tenant_access(result["_source"], tenant_id)
    return {"id": result["_id"], **result["_source"]}


@app.patch("/incidents/{incident_id}/status")
@app.patch("/v1/incidents/{incident_id}/status")
async def patch_incident_status(
    request: Request,
    incident_id: str,
    payload: IncidentStatusPatch,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    return await apply_incident_status_transition(tenant_id, incident_id, payload.status)


@app.post("/incidents/{incident_id}/acknowledge")
@app.post("/v1/incidents/{incident_id}/acknowledge")
async def acknowledge_incident(request: Request, incident_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    return await apply_incident_status_transition(
        tenant_id,
        incident_id,
        IncidentStatus.acknowledged,
    )


@app.post("/incidents/{incident_id}/resolve")
@app.post("/v1/incidents/{incident_id}/resolve")
async def resolve_incident(request: Request, incident_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    return await apply_incident_status_transition(
        tenant_id,
        incident_id,
        IncidentStatus.resolved,
    )


@app.post("/incidents/{incident_id}/rca/feedback")
@app.post("/v1/incidents/{incident_id}/rca/feedback")
async def submit_incident_rca_feedback(
    request: Request,
    incident_id: str,
    payload: RCAFeedbackRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    try:
        incident_doc = os_client.get(index=settings.opensearch.incidents_index, id=incident_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Incident not found") from exc
    assert_tenant_access(incident_doc["_source"], tenant_id)

    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            f"{AI_SERVICE_URL}/incidents/{incident_id}/feedback",
            json=payload.model_dump(mode="json"),
        )
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        result = response.json()

    write_audit_event(
        tenant_id=tenant_id,
        action="incident.rca_feedback_submitted",
        entity_type="incident",
        entity_id=incident_id,
        details={"rating": payload.rating},
    )
    return result


@app.post("/incidents/{incident_id}/rca/regenerate")
@app.post("/v1/incidents/{incident_id}/rca/regenerate")
async def regenerate_incident_rca(
    request: Request,
    incident_id: str,
    payload: RCARegenerateRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    try:
        incident_doc = os_client.get(index=settings.opensearch.incidents_index, id=incident_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Incident not found") from exc
    assert_tenant_access(incident_doc["_source"], tenant_id)

    async with httpx.AsyncClient(timeout=settings.llm.timeout_seconds + 10) as client:
        response = await client.post(
            f"{AI_SERVICE_URL}/incidents/{incident_id}/regenerate",
            json=payload.model_dump(mode="json"),
        )
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        result = response.json()

    incident_severity = str(result.get("severity")) if result.get("severity") else None
    await dispatch_webhook_event(
        tenant_id,
        "rca.ready",
        {
            "incident_id": incident_id,
            "incident": result,
        },
        severity=incident_severity,
    )
    if (incident_severity or "").lower() == "critical":
        await deliver_chatops_message(
            tenant_id,
            f"RCA ready for critical incident {incident_id}",
            context={"incident_id": incident_id, "event": "rca.ready"},
        )
    write_audit_event(
        tenant_id=tenant_id,
        action="incident.rca_regenerated",
        entity_type="incident",
        entity_id=incident_id,
        details={"notes_present": bool(payload.notes)},
    )
    return result


@app.get("/sources")
@app.get("/v1/sources")
async def list_sources(request: Request) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    query = {
        "query": {"bool": {"filter": [tenant_scope_filter(tenant_id)]}},
        "size": 200,
        "sort": [{"created_at": {"order": "desc"}}],
    }
    result = os_client.search(index=settings.opensearch.sources_index, body=query)

    items: list[dict[str, Any]] = []
    for hit in result.get("hits", {}).get("hits", []):
        try:
            source = DataSource.model_validate({"id": hit["_id"], **hit["_source"]})
        except Exception:  # noqa: BLE001
            continue
        items.append(source_to_view(source).model_dump(mode="json"))

    total = result.get("hits", {}).get("total", {}).get("value", len(items))
    return {"items": items, "total": total}


@app.post("/sources")
@app.post("/v1/sources")
async def create_source(request: Request, payload: DataSourceCreateRequest) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    source = DataSource(tenant_id=tenant_id, **payload.model_dump())
    upsert_source(source)
    write_audit_event(
        tenant_id=tenant_id,
        action="source.created",
        entity_type="source",
        entity_id=source.id,
        details={"name": source.name, "endpoint": source.endpoint},
    )
    return source_to_view(source).model_dump(mode="json")


@app.put("/sources/{source_id}")
@app.put("/v1/sources/{source_id}")
async def update_source(
    request: Request,
    source_id: str,
    payload: DataSourceCreateRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    existing = get_source_or_404(source_id, tenant_id)
    update_data = payload.model_dump(exclude_unset=True)

    if update_data.get("auth_token") is None:
        update_data["auth_token"] = existing.auth_token

    source = DataSource(
        id=existing.id,
        created_at=existing.created_at,
        updated_at=existing.updated_at,
        last_polled_at=existing.last_polled_at,
        last_success_at=existing.last_success_at,
        last_status=existing.last_status,
        last_error=existing.last_error,
        total_ingested=existing.total_ingested,
        tenant_id=existing.tenant_id,
        **update_data,
    )
    upsert_source(source)
    write_audit_event(
        tenant_id=tenant_id,
        action="source.updated",
        entity_type="source",
        entity_id=source.id,
        details={"name": source.name, "endpoint": source.endpoint},
    )
    return source_to_view(source).model_dump(mode="json")


@app.post("/sources/{source_id}/enable")
@app.post("/v1/sources/{source_id}/enable")
async def enable_source(request: Request, source_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    source = get_source_or_404(source_id, tenant_id)
    source.enabled = True
    source.last_error = None
    upsert_source(source)
    write_audit_event(
        tenant_id=tenant_id,
        action="source.enabled",
        entity_type="source",
        entity_id=source.id,
        details={"name": source.name},
    )
    return source_to_view(source).model_dump(mode="json")


@app.post("/sources/{source_id}/disable")
@app.post("/v1/sources/{source_id}/disable")
async def disable_source(request: Request, source_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    source = get_source_or_404(source_id, tenant_id)
    source.enabled = False
    upsert_source(source)
    write_audit_event(
        tenant_id=tenant_id,
        action="source.disabled",
        entity_type="source",
        entity_id=source.id,
        details={"name": source.name},
    )
    return source_to_view(source).model_dump(mode="json")


@app.post("/sources/{source_id}/sync")
@app.post("/v1/sources/{source_id}/sync")
async def sync_source(request: Request, source_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    get_source_or_404(source_id, tenant_id)
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(
            f"{INGESTION_SERVICE_URL}/sources/{source_id}/pull",
            headers={"x-tenant-id": tenant_id},
        )
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        payload = response.json()
        write_audit_event(
            tenant_id=tenant_id,
            action="source.synced",
            entity_type="source",
            entity_id=source_id,
            details={"result": payload},
        )
        return payload


@app.delete("/sources/{source_id}")
@app.delete("/v1/sources/{source_id}")
async def delete_source(request: Request, source_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    source = get_source_or_404(source_id, tenant_id)
    os_client.delete(index=settings.opensearch.sources_index, id=source_id, refresh=True)
    write_audit_event(
        tenant_id=tenant_id,
        action="source.deleted",
        entity_type="source",
        entity_id=source_id,
        details={"name": source.name},
    )
    return {"deleted": source_id}


@app.post("/analyze")
@app.post("/v1/analyze")
async def analyze(request: Request, payload: AnalyzeRequest) -> dict[str, Any]:
    ANALYZE_REQUESTS_TOTAL.inc()
    tenant_id = resolve_tenant_id(request)
    logs_with_tenant: list[dict[str, Any] | str] = []
    for raw in payload.logs:
        if isinstance(raw, dict):
            logs_with_tenant.append({**raw, "tenant_id": raw.get("tenant_id", tenant_id)})
        else:
            logs_with_tenant.append(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "service": "unknown-service",
                    "level": "info",
                    "message": raw,
                    "tenant_id": tenant_id,
                }
            )

    normalized_payload = AnalyzeRequest(logs=logs_with_tenant)
    raw_size = len(json.dumps(normalized_payload.model_dump()).encode("utf-8"))
    if raw_size > settings.pipeline.max_manual_analyze_kb * 1024:
        raise HTTPException(status_code=413, detail="Payload exceeds 512KB")

    async with httpx.AsyncClient(timeout=settings.llm.timeout_seconds + 5) as client:
        response = await client.post(
            f"{AI_SERVICE_URL}/analyze",
            json=normalized_payload.model_dump(mode="json"),
        )
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()


@app.post("/llm/config")
@app.post("/v1/llm/config")
async def set_llm_config(request: Request, payload: LLMConfigRequest) -> dict[str, str]:
    global runtime_llm_config
    tenant_id = resolve_tenant_id(request)

    runtime_llm_config = payload
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(
            f"{AI_SERVICE_URL}/config",
            json=payload.model_dump(mode="json"),
        )
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail=response.text)

    write_audit_event(
        tenant_id=tenant_id,
        action="llm.config_updated",
        entity_type="llm",
        entity_id="runtime",
        details=payload.model_dump(mode="json"),
    )
    return payload.model_dump()


@app.post("/admin/replay")
@app.post("/v1/admin/replay")
async def replay_logs(request: Request, payload: ReplayRequest) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer unavailable")

    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        enable_auto_commit=False,
        group_id=None,
        auto_offset_reset="earliest",
    )

    replayed = 0
    topic_partition = TopicPartition(payload.topic, payload.partition)

    try:
        await consumer.start()
        consumer.assign([topic_partition])
        consumer.seek(topic_partition, payload.offset)

        while replayed < payload.max_messages:
            records = await consumer.getmany(timeout_ms=750, max_records=100)
            batch = records.get(topic_partition, [])
            if not batch:
                break

            for message in batch:
                await producer.send_and_wait(settings.kafka.topics.logs, message.value)
                replayed += 1
                REPLAYED_MESSAGES_TOTAL.inc()
                if replayed >= payload.max_messages:
                    break
    finally:
        await consumer.stop()

    write_audit_event(
        tenant_id=tenant_id,
        action="admin.replay",
        entity_type="kafka",
        entity_id=payload.topic,
        details={
            "requested": payload.max_messages,
            "replayed": replayed,
            "partition": payload.partition,
            "offset": payload.offset,
        },
    )
    return {
        "requested": payload.max_messages,
        "replayed": replayed,
        "source": {
            "topic": payload.topic,
            "partition": payload.partition,
            "offset": payload.offset,
        },
    }


@app.get("/webhooks")
@app.get("/v1/webhooks")
async def list_webhooks(request: Request) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    items = [webhook_to_view(item) for item in list_webhooks_internal(tenant_id)]
    return {"items": items, "total": len(items)}


@app.post("/webhooks")
@app.post("/v1/webhooks")
async def create_webhook(request: Request, payload: WebhookSubscriptionCreate) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    subscription = WebhookSubscription(tenant_id=tenant_id, **payload.model_dump())
    upsert_webhook(subscription)
    write_audit_event(
        tenant_id=tenant_id,
        action="webhook.created",
        entity_type="webhook",
        entity_id=subscription.id,
        details={"name": subscription.name, "url": subscription.url},
    )
    return webhook_to_view(subscription)


@app.put("/webhooks/{webhook_id}")
@app.put("/v1/webhooks/{webhook_id}")
async def update_webhook(
    request: Request,
    webhook_id: str,
    payload: WebhookSubscriptionCreate,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    existing = get_webhook_or_404(webhook_id, tenant_id)
    secret = payload.secret if payload.secret is not None else existing.secret

    updated = WebhookSubscription(
        id=existing.id,
        tenant_id=existing.tenant_id,
        created_at=existing.created_at,
        updated_at=existing.updated_at,
        last_status=existing.last_status,
        last_error=existing.last_error,
        last_sent_at=existing.last_sent_at,
        **{**payload.model_dump(), "secret": secret},
    )
    upsert_webhook(updated)
    write_audit_event(
        tenant_id=tenant_id,
        action="webhook.updated",
        entity_type="webhook",
        entity_id=updated.id,
        details={"name": updated.name, "url": updated.url},
    )
    return webhook_to_view(updated)


@app.delete("/webhooks/{webhook_id}")
@app.delete("/v1/webhooks/{webhook_id}")
async def delete_webhook(request: Request, webhook_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    sub = get_webhook_or_404(webhook_id, tenant_id)
    os_client.delete(index=WEBHOOKS_INDEX, id=webhook_id, refresh=True)
    write_audit_event(
        tenant_id=tenant_id,
        action="webhook.deleted",
        entity_type="webhook",
        entity_id=webhook_id,
        details={"name": sub.name, "url": sub.url},
    )
    return {"deleted": webhook_id}


@app.get("/rules")
@app.get("/v1/rules")
async def list_rules(request: Request) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    items = [item.model_dump(mode="json") for item in list_rules_internal(tenant_id)]
    return {"items": items, "total": len(items)}


@app.post("/rules")
@app.post("/v1/rules")
async def create_rule(request: Request, payload: DetectionRuleCreateRequest) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    rule = DetectionRule(tenant_id=tenant_id, **payload.model_dump())
    upsert_rule(rule)
    write_audit_event(
        tenant_id=tenant_id,
        action="rule.created",
        entity_type="rule",
        entity_id=rule.id,
        details={"name": rule.name, "service_pattern": rule.service_pattern},
    )
    return rule.model_dump(mode="json")


@app.put("/rules/{rule_id}")
@app.put("/v1/rules/{rule_id}")
async def update_rule(
    request: Request,
    rule_id: str,
    payload: DetectionRuleCreateRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    existing = get_rule_or_404(rule_id, tenant_id)
    updated = DetectionRule(
        id=existing.id,
        tenant_id=existing.tenant_id,
        created_at=existing.created_at,
        updated_at=existing.updated_at,
        match_count=existing.match_count,
        **payload.model_dump(),
    )
    upsert_rule(updated)
    write_audit_event(
        tenant_id=tenant_id,
        action="rule.updated",
        entity_type="rule",
        entity_id=updated.id,
        details={"name": updated.name, "service_pattern": updated.service_pattern},
    )
    return updated.model_dump(mode="json")


@app.delete("/rules/{rule_id}")
@app.delete("/v1/rules/{rule_id}")
async def delete_rule(request: Request, rule_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    existing = get_rule_or_404(rule_id, tenant_id)
    os_client.delete(index=RULES_INDEX, id=rule_id, refresh=True)
    write_audit_event(
        tenant_id=tenant_id,
        action="rule.deleted",
        entity_type="rule",
        entity_id=rule_id,
        details={"name": existing.name, "service_pattern": existing.service_pattern},
    )
    return {"deleted": rule_id}


@app.get("/suppressions")
@app.get("/v1/suppressions")
async def list_suppressions(
    request: Request,
    active_only: bool = Query(default=False),
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    items = list_suppressions_internal(tenant_id)
    if active_only:
        now = datetime.now(UTC)
        items = [
            item
            for item in items
            if item.enabled and item.starts_at <= now <= item.ends_at
        ]
    return {
        "items": [item.model_dump(mode="json") for item in items],
        "total": len(items),
    }


@app.post("/suppressions")
@app.post("/v1/suppressions")
async def create_suppression(
    request: Request,
    payload: SuppressionCreateRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    suppression = SuppressionWindow(tenant_id=tenant_id, **payload.model_dump())
    upsert_suppression(suppression)
    write_audit_event(
        tenant_id=tenant_id,
        action="suppression.created",
        entity_type="suppression",
        entity_id=suppression.id,
        details={
            "service_pattern": suppression.service_pattern,
            "starts_at": suppression.starts_at.isoformat(),
            "ends_at": suppression.ends_at.isoformat(),
        },
    )
    return suppression.model_dump(mode="json")


@app.put("/suppressions/{suppression_id}")
@app.put("/v1/suppressions/{suppression_id}")
async def update_suppression(
    request: Request,
    suppression_id: str,
    payload: SuppressionCreateRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    existing = get_suppression_or_404(suppression_id, tenant_id)
    updated = SuppressionWindow(
        id=existing.id,
        tenant_id=existing.tenant_id,
        created_at=existing.created_at,
        updated_at=existing.updated_at,
        **payload.model_dump(),
    )
    upsert_suppression(updated)
    write_audit_event(
        tenant_id=tenant_id,
        action="suppression.updated",
        entity_type="suppression",
        entity_id=updated.id,
        details={
            "service_pattern": updated.service_pattern,
            "starts_at": updated.starts_at.isoformat(),
            "ends_at": updated.ends_at.isoformat(),
        },
    )
    return updated.model_dump(mode="json")


@app.delete("/suppressions/{suppression_id}")
@app.delete("/v1/suppressions/{suppression_id}")
async def delete_suppression(request: Request, suppression_id: str) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    existing = get_suppression_or_404(suppression_id, tenant_id)
    os_client.delete(index=SUPPRESSIONS_INDEX, id=suppression_id, refresh=True)
    write_audit_event(
        tenant_id=tenant_id,
        action="suppression.deleted",
        entity_type="suppression",
        entity_id=suppression_id,
        details={"service_pattern": existing.service_pattern},
    )
    return {"deleted": suppression_id}


@app.get("/audit")
@app.get("/v1/audit")
async def list_audit_events(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    action: str | None = None,
    entity_type: str | None = None,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    filters: list[dict[str, Any]] = [tenant_scope_filter(tenant_id)]
    if action:
        filters.append({"term": {"action": action}})
    if entity_type:
        filters.append({"term": {"entity_type": entity_type}})

    query: dict[str, Any] = {
        "query": {"match_all": {}},
        "size": limit,
        "sort": [{"timestamp": {"order": "desc"}}],
    }
    if filters:
        query["query"] = {"bool": {"filter": filters}}

    result = os_client.search(index=AUDIT_INDEX, body=query)
    items = [{"id": hit["_id"], **hit["_source"]} for hit in result.get("hits", {}).get("hits", [])]
    return {"items": items, "total": len(items)}


@app.get("/stream")
@app.get("/v1/stream")
async def stream_incidents(request: Request) -> StreamingResponse:
    tenant_id = resolve_tenant_id(request)

    async def event_generator() -> AsyncIterator[str]:
        last_marker: str | None = None

        while True:
            try:
                query = {
                    "query": {"bool": {"filter": [tenant_scope_filter(tenant_id)]}},
                    "size": 10,
                    "sort": [
                        {"updated_at": {"order": "desc"}},
                        {"_id": {"order": "desc"}},
                    ],
                }
                result = os_client.search(index=settings.opensearch.incidents_index, body=query)
                hits = result.get("hits", {}).get("hits", [])
                latest_marker = None
                if hits:
                    latest = hits[0]["_source"]
                    latest_marker = f"{latest.get('updated_at')}:{hits[0]['_id']}"

                if latest_marker != last_marker:
                    payload = {
                        "timestamp": datetime.now(UTC).isoformat(),
                        "items": [{"id": hit["_id"], **hit["_source"]} for hit in hits],
                    }
                    STREAM_EVENTS_TOTAL.labels(event="incidents").inc()
                    yield f"event: incidents\ndata: {json.dumps(payload, default=str)}\n\n"
                    last_marker = latest_marker
                else:
                    heartbeat = {"timestamp": datetime.now(UTC).isoformat()}
                    STREAM_EVENTS_TOTAL.labels(event="heartbeat").inc()
                    yield f"event: heartbeat\ndata: {json.dumps(heartbeat)}\n\n"
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                error_payload = {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "error": str(exc),
                }
                STREAM_EVENTS_TOTAL.labels(event="error").inc()
                yield f"event: error\ndata: {json.dumps(error_payload)}\n\n"

            await asyncio.sleep(2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/schema-registry")
@app.get("/v1/schema-registry")
async def list_schema_registry() -> dict[str, Any]:
    contracts = [item.model_dump(mode="json") for item in topic_contracts(settings)]
    return {"contracts": contracts, "total": len(contracts)}


@app.get("/schema-registry/{topic_name}")
@app.get("/v1/schema-registry/{topic_name}")
async def get_schema_registry_topic(topic_name: str) -> dict[str, Any]:
    contracts = topic_contracts(settings)
    for contract in contracts:
        if contract.topic == topic_name:
            return contract.model_dump(mode="json")
    raise HTTPException(status_code=404, detail="Topic schema not found")


@app.get("/topology")
@app.get("/v1/topology")
async def list_topology(
    request: Request,
    service: str | None = Query(default=None),
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    edges = list_topology_edges_internal(tenant_id, service=service)
    return {"items": [edge.model_dump(mode="json") for edge in edges], "total": len(edges)}


@app.put("/topology")
@app.put("/v1/topology")
async def replace_topology(
    request: Request,
    payload: TopologyUpdateRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    edges = replace_topology_edges(tenant_id, payload)
    write_audit_event(
        tenant_id=tenant_id,
        action="topology.replaced",
        entity_type="topology",
        entity_id=tenant_id,
        details={"edge_count": len(edges)},
    )
    return {"items": [edge.model_dump(mode="json") for edge in edges], "total": len(edges)}


@app.get("/chatops/config")
@app.get("/v1/chatops/config")
async def get_chatops_config(request: Request) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    config = get_chatops_config_internal(tenant_id)
    if config is None:
        return {"item": None}
    return {"item": chatops_to_view(config)}


@app.post("/chatops/config")
@app.post("/v1/chatops/config")
async def upsert_chatops(
    request: Request,
    payload: ChatOpsConfigCreateRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request)
    existing = get_chatops_config_internal(tenant_id)
    if existing is None:
        config = ChatOpsConfig(tenant_id=tenant_id, **payload.model_dump())
    else:
        config = ChatOpsConfig(
            id=existing.id,
            tenant_id=existing.tenant_id,
            created_at=existing.created_at,
            updated_at=existing.updated_at,
            last_status=existing.last_status,
            last_error=existing.last_error,
            last_sent_at=existing.last_sent_at,
            signing_secret=(
                payload.signing_secret
                if payload.signing_secret is not None
                else existing.signing_secret
            ),
            **payload.model_dump(exclude={"signing_secret"}),
        )
    upsert_chatops_config(config)
    write_audit_event(
        tenant_id=tenant_id,
        action="chatops.config_upserted",
        entity_type="chatops",
        entity_id=config.id,
        details={"provider": config.provider, "enabled": config.enabled},
    )
    return chatops_to_view(config)


@app.post("/chatops/commands")
@app.post("/v1/chatops/commands")
async def run_chatops_command(
    request: Request,
    payload: ChatOpsCommandRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request, payload.tenant_id)
    command = payload.command.strip()
    tokens = command.split()

    if not tokens:
        CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action="invalid", status="error").inc()
        raise HTTPException(status_code=400, detail="Empty command")

    if tokens[0].lower() not in {"/airs", "airs"}:
        CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action="invalid", status="error").inc()
        raise HTTPException(status_code=400, detail="Command must start with /airs")

    action = tokens[1].lower() if len(tokens) > 1 else "help"
    argument = tokens[2] if len(tokens) > 2 else None

    if action == "status":
        filters: list[dict[str, Any]] = [tenant_scope_filter(tenant_id)]
        filters.append({"terms": {"status": ["open", "acknowledged"]}})
        if argument:
            filters.append(
                {
                    "bool": {
                        "should": [
                            {"term": {"service.keyword": argument}},
                            {"term": {"service": argument}},
                        ],
                        "minimum_should_match": 1,
                    }
                }
            )
        query = {
            "query": {"bool": {"filter": filters}},
            "size": 10,
            "sort": [{"updated_at": {"order": "desc"}}],
        }
        result = os_client.search(index=settings.opensearch.incidents_index, body=query)
        hits = result.get("hits", {}).get("hits", [])
        summary = ", ".join([f"{hit['_id']}:{hit['_source'].get('status')}" for hit in hits]) or "none"
        text = f"Open incidents ({len(hits)}): {summary}"
        await deliver_chatops_message(
            tenant_id,
            text,
            context={"action": "status", "service": argument, "count": len(hits)},
        )
        CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action=action, status="ok").inc()
        return {"action": "status", "count": len(hits), "items": [{"id": h["_id"], **h["_source"]} for h in hits]}

    if action == "ack":
        if not argument:
            CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action=action, status="error").inc()
            raise HTTPException(status_code=400, detail="Usage: /airs ack <incident_id>")
        updated = await apply_incident_status_transition(
            tenant_id,
            argument,
            IncidentStatus.acknowledged,
        )
        await deliver_chatops_message(
            tenant_id,
            f"Acknowledged incident {argument}",
            context={"action": action, "incident_id": argument},
        )
        CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action=action, status="ok").inc()
        return {"action": action, "incident": updated}

    if action == "resolve":
        if not argument:
            CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action=action, status="error").inc()
            raise HTTPException(status_code=400, detail="Usage: /airs resolve <incident_id>")
        updated = await apply_incident_status_transition(
            tenant_id,
            argument,
            IncidentStatus.resolved,
        )
        await deliver_chatops_message(
            tenant_id,
            f"Resolved incident {argument}",
            context={"action": action, "incident_id": argument},
        )
        CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action=action, status="ok").inc()
        return {"action": action, "incident": updated}

    if action == "rca":
        if not argument:
            CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action=action, status="error").inc()
            raise HTTPException(status_code=400, detail="Usage: /airs rca <incident_id>")
        try:
            result = os_client.get(index=settings.opensearch.incidents_index, id=argument)
        except Exception as exc:  # noqa: BLE001
            CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action=action, status="error").inc()
            raise HTTPException(status_code=404, detail="Incident not found") from exc
        assert_tenant_access(result["_source"], tenant_id)
        incident = {"id": result["_id"], **result["_source"]}
        rca = incident.get("rca")
        text = f"RCA for {argument}: {rca.get('root_cause') if isinstance(rca, dict) else 'not ready'}"
        await deliver_chatops_message(
            tenant_id,
            text,
            context={"action": action, "incident_id": argument},
        )
        CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action=action, status="ok").inc()
        return {"action": action, "incident_id": argument, "rca": rca}

    help_text = "Supported: /airs status [service], /airs ack <id>, /airs resolve <id>, /airs rca <id>"
    await deliver_chatops_message(tenant_id, help_text, context={"action": "help"})
    CHATOPS_COMMANDS_TOTAL.labels(tenant_id=tenant_id, action="help", status="ok").inc()
    return {"action": "help", "message": help_text}


def replay_level_filters(severities: list[Any]) -> list[str]:
    levels: set[str] = set()
    for item in severities:
        value = str(item.value) if hasattr(item, "value") else str(item)
        name = value.lower()
        if name == "critical":
            levels.update({"critical", "fatal", "error"})
        elif name == "warning":
            levels.update({"warning", "warn", "error"})
        elif name == "info":
            levels.update({"info", "debug"})
    return sorted(levels)


@app.post("/replay")
@app.post("/v1/replay")
async def replay_filtered_logs(
    request: Request,
    payload: ReplayFilterRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request, payload.tenant_id)
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer unavailable")

    filters: list[dict[str, Any]] = [tenant_scope_filter(tenant_id)]
    if payload.service:
        filters.append(
            {
                "bool": {
                    "should": [
                        {"term": {"service.keyword": payload.service}},
                        {"term": {"service": payload.service}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        )
    if payload.from_time or payload.to_time:
        range_filter: dict[str, Any] = {}
        if payload.from_time is not None:
            range_filter["gte"] = payload.from_time.isoformat()
        if payload.to_time is not None:
            range_filter["lte"] = payload.to_time.isoformat()
        filters.append({"range": {"timestamp": range_filter}})

    level_filters = replay_level_filters(payload.severity_filter)
    if level_filters:
        filters.append({"terms": {"level": level_filters}})

    query = {
        "query": {"bool": {"filter": filters}},
        "size": payload.max_messages,
        "sort": [{"timestamp": {"order": "asc"}}],
    }
    result = os_client.search(index=settings.opensearch.logs_index, body=query)
    hits = result.get("hits", {}).get("hits", [])
    docs = [hit["_source"] for hit in hits]

    selected_rules: list[DetectionRule] = []
    if payload.rule_ids:
        for rule_id in payload.rule_ids:
            selected_rules.append(get_rule_or_404(rule_id, tenant_id))

    replay_docs: list[dict[str, Any]] = []
    for doc in docs:
        candidate = {**doc, "tenant_id": doc.get("tenant_id", tenant_id)}
        if selected_rules and not any(detect_rule_match(rule, candidate) for rule in selected_rules):
            continue
        replay_docs.append(candidate)

    replayed = 0
    for doc in replay_docs:
        await produce_json(producer, settings.kafka.topics.logs, doc)
        REPLAYED_MESSAGES_TOTAL.inc()
        replayed += 1

    write_audit_event(
        tenant_id=tenant_id,
        action="replay.filtered",
        entity_type="kafka",
        entity_id=settings.kafka.topics.logs,
        details={
            "requested": payload.max_messages,
            "matched_logs": len(docs),
            "replayed": replayed,
            "service": payload.service,
            "rule_ids": payload.rule_ids,
        },
    )

    return {
        "requested": payload.max_messages,
        "matched_logs": len(docs),
        "replayed": replayed,
        "service": payload.service,
        "rule_ids": payload.rule_ids,
    }


@app.post("/ingest/simulate")
@app.post("/v1/ingest/simulate")
async def simulate_ingest(
    request: Request,
    payload: SimulateIngestRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request, payload.tenant_id)
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer unavailable")

    interval = 1.0 / payload.rate_per_second
    sent = 0
    for idx in range(payload.count):
        log_event = {
            "timestamp": datetime.now(UTC).isoformat(),
            "service": payload.service,
            "level": payload.level.lower(),
            "message": f"{payload.pattern} [{idx + 1}/{payload.count}]",
            "tenant_id": tenant_id,
            **payload.metadata,
        }
        await produce_json(producer, settings.kafka.topics.logs, log_event)
        sent += 1
        SIMULATION_MESSAGES_TOTAL.labels(tenant_id=tenant_id, service=payload.service).inc()
        if idx < payload.count - 1:
            await asyncio.sleep(interval)

    write_audit_event(
        tenant_id=tenant_id,
        action="ingest.simulated",
        entity_type="simulation",
        entity_id=payload.service,
        details={
            "service": payload.service,
            "pattern": payload.pattern,
            "count": payload.count,
            "rate_per_second": payload.rate_per_second,
        },
    )
    return {"sent": sent, "service": payload.service, "tenant_id": tenant_id}


@app.post("/rules/{rule_id}/test-against-history")
@app.post("/v1/rules/{rule_id}/test-against-history")
async def test_rule_against_history(
    request: Request,
    rule_id: str,
    payload: RuleHistoryTestRequest,
) -> dict[str, Any]:
    tenant_id = resolve_tenant_id(request, payload.tenant_id)
    rule = get_rule_or_404(rule_id, tenant_id)

    filters: list[dict[str, Any]] = [tenant_scope_filter(tenant_id)]
    if payload.service:
        filters.append(
            {
                "bool": {
                    "should": [
                        {"term": {"service.keyword": payload.service}},
                        {"term": {"service": payload.service}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        )
    if payload.from_time or payload.to_time:
        window: dict[str, Any] = {}
        if payload.from_time is not None:
            window["gte"] = payload.from_time.isoformat()
        if payload.to_time is not None:
            window["lte"] = payload.to_time.isoformat()
        filters.append({"range": {"timestamp": window}})
    if payload.from_time is None and payload.to_time is None:
        filters.append({"range": {"timestamp": {"gte": "now-24h"}}})

    query = {
        "query": {"bool": {"filter": filters}},
        "size": payload.limit,
        "sort": [{"timestamp": {"order": "desc"}}],
    }
    result = os_client.search(index=settings.opensearch.logs_index, body=query)
    hits = result.get("hits", {}).get("hits", [])

    total = len(hits)
    matched = 0
    true_positive = 0
    false_positive = 0
    false_negative = 0

    for hit in hits:
        doc = {**hit.get("_source", {}), "tenant_id": tenant_id}
        predicted = detect_rule_match(rule, doc)
        level = str(doc.get("level", "info")).lower()
        message = str(doc.get("message", "")).lower()
        actual_anomaly = level in {"critical", "fatal", "error", "warning", "warn"} or any(
            token in message for token in ["error", "exception", "timeout", "oom", "5xx"]
        )

        if predicted:
            matched += 1
            if actual_anomaly:
                true_positive += 1
            else:
                false_positive += 1
        elif actual_anomaly:
            false_negative += 1

    precision = (
        true_positive / (true_positive + false_positive)
        if (true_positive + false_positive)
        else 0.0
    )
    recall = (
        true_positive / (true_positive + false_negative)
        if (true_positive + false_negative)
        else 0.0
    )

    write_audit_event(
        tenant_id=tenant_id,
        action="rule.tested_against_history",
        entity_type="rule",
        entity_id=rule_id,
        details={
            "service": payload.service,
            "sampled": total,
            "matched": matched,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
        },
    )

    return {
        "rule_id": rule_id,
        "tenant_id": tenant_id,
        "service": payload.service,
        "sampled": total,
        "matched": matched,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
    }

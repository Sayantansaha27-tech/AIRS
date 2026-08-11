from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from airs_shared.consumer import build_consumer, commit_safely
from airs_shared.dlq import build_dlq_payload
from airs_shared.kafka import produce_json
from airs_shared.models import AnomalyEvent, DataSource, Incident, Severity
from airs_shared.monitoring import metrics_response
from airs_shared.opensearch import (
    async_ensure_index,
    build_async_client,
    build_client,
    ensure_retention_policy,
)
from airs_shared.settings import get_settings
from airs_shared.state import StateStore, build_state_store
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram

settings = get_settings()
app = FastAPI(title="AIRS Correlation Service")
logger = logging.getLogger("correlation-service")


@dataclass
class IncidentCluster:
    tenant_id: str
    service: str
    first_seen: datetime
    last_seen: datetime
    last_received_at: datetime
    window_duration_minutes: int
    min_signal_count: int
    anomalies: list[AnomalyEvent] = field(default_factory=list)
    fingerprints: set[str] = field(default_factory=set)
    # Set once the cluster has produced an incident. The cluster stays resident
    # for the rest of its window so that later anomalies for the same service
    # deduplicate against it and amend that incident instead of opening a new
    # one for every signal.
    incident_id: str | None = None
    created_at: datetime | None = None
    parent_incident_id: str | None = None
    emitted_severity: Severity | None = None


@dataclass
class ServiceCorrelationConfig:
    window_duration_minutes: int
    min_signal_count: int
    fetched_at: datetime


consumer: AIOKafkaConsumer | None = None
producer: AIOKafkaProducer | None = None
worker_task: asyncio.Task[None] | None = None
clusters: dict[tuple[str, str], IncidentCluster] = {}
state: StateStore | None = None
service_config_cache: dict[tuple[str, str], ServiceCorrelationConfig] = {}

os_client = build_client(settings.opensearch.url)
os_async = build_async_client(settings.opensearch.url)
SOURCE_CONFIG_CACHE_TTL_SECONDS = 60
TOPOLOGY_INDEX = "airs-topology"
INCIDENTS_CREATED_TOTAL = Counter(
    "airs_incidents_created_total",
    "Count of created incidents",
    labelnames=("service", "severity"),
)
INCIDENTS_AMENDED_TOTAL = Counter(
    "airs_incidents_amended_total",
    "Count of amendments applied to already-open incidents",
    labelnames=("service",),
)
ANOMALIES_REPLAYED_TOTAL = Counter(
    "airs_anomalies_replayed_total",
    "Anomalies skipped because they had already been correlated",
    labelnames=("service",),
)
CORRELATION_PROCESSING_DURATION_SECONDS = Histogram(
    "airs_correlation_processing_duration_seconds",
    "Correlation processing latency in seconds",
    labelnames=("service",),
)
DLQ_PUBLISHED_TOTAL = Counter(
    "airs_dlq_published_total",
    "Count of DLQ events published by source topic",
    labelnames=("source_topic",),
)
CONSUMER_BATCH_SIZE = Gauge(
    "airs_correlation_batch_size",
    "Number of messages fetched in the latest batch",
)
ACTIVE_CLUSTERS = Gauge(
    "airs_correlation_active_clusters",
    "Number of currently open in-memory correlation clusters",
)


def pick_severity(anomalies: list[AnomalyEvent]) -> Severity:
    ranking = {Severity.info: 1, Severity.warning: 2, Severity.critical: 3}
    return max(anomalies, key=lambda item: ranking[item.severity]).severity


def cluster_summary(service: str, anomalies: list[AnomalyEvent]) -> str:
    top_reason = (
        anomalies[-1].reasons[0] if anomalies and anomalies[-1].reasons else "correlated anomalies"
    )
    return f"{service} incident from {len(anomalies)} correlated anomalies ({top_reason})"


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


async def get_service_config(tenant_id: str, service: str) -> ServiceCorrelationConfig:
    now = datetime.now(UTC)
    cache_key = (tenant_id, service)
    cached = service_config_cache.get(cache_key)
    if (
        cached is not None
        and (now - cached.fetched_at).total_seconds() < SOURCE_CONFIG_CACHE_TTL_SECONDS
    ):
        return cached

    config = ServiceCorrelationConfig(
        window_duration_minutes=settings.pipeline.correlation_window_minutes,
        min_signal_count=2,
        fetched_at=now,
    )

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
        result = await os_async.search(index=settings.opensearch.sources_index, body=query)
        hits = result.get("hits", {}).get("hits", [])
        if hits:
            source = DataSource.model_validate({"id": hits[0]["_id"], **hits[0]["_source"]})
            config.window_duration_minutes = source.window_duration_minutes
            config.min_signal_count = source.min_signal_count
    except Exception:  # noqa: BLE001
        pass

    service_config_cache[cache_key] = config
    return config


async def fetch_topology_neighbors(tenant_id: str, service: str) -> set[str]:
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
        "size": 200,
    }

    try:
        result = await os_async.search(index=TOPOLOGY_INDEX, body=query)
    except Exception:  # noqa: BLE001
        return set()

    neighbors: set[str] = set()
    for hit in result.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        upstream = str(source.get("upstream") or "")
        downstream = str(source.get("downstream") or "")
        if upstream and upstream != service:
            neighbors.add(upstream)
        if downstream and downstream != service:
            neighbors.add(downstream)

    return neighbors


async def find_parent_incident(
    *,
    tenant_id: str,
    service: str,
    related_services: set[str],
) -> dict[str, Any] | None:
    if not related_services:
        return None

    should_services = [{"term": {"service.keyword": name}} for name in sorted(related_services)]
    should_services.extend([{"term": {"service": name}} for name in sorted(related_services)])

    query = {
        "query": {
            "bool": {
                "filter": [
                    tenant_scope_filter(tenant_id),
                    {"terms": {"status": ["open", "acknowledged"]}},
                    {"range": {"updated_at": {"gte": "now-30m"}}},
                ],
                "must_not": [
                    {"term": {"service.keyword": service}},
                    {"term": {"service": service}},
                ],
                "should": should_services,
                "minimum_should_match": 1,
            }
        },
        "size": 1,
        "sort": [{"updated_at": {"order": "desc"}}],
    }

    try:
        result = await os_async.search(index=settings.opensearch.incidents_index, body=query)
    except Exception:  # noqa: BLE001
        return None

    hits = result.get("hits", {}).get("hits", [])
    if not hits:
        return None
    return hits[0]


async def emit_incident(cluster: IncidentCluster, *, publish: bool = True) -> None:
    """Create or amend the incident for a cluster.

    The first call mints an incident and records its id on the cluster. Later
    calls amend that same incident in place. `publish` controls whether the
    result is republished to incidents-topic: amendments that do not change
    severity are persisted but not republished, so a long-running incident does
    not trigger one RCA generation per correlated anomaly.
    """
    if not cluster.anomalies or producer is None:
        return

    timeline = [
        {
            "timestamp": item.timestamp.isoformat(),
            "severity": item.severity,
            "message": item.log_message,
            "reasons": item.reasons,
        }
        for item in sorted(cluster.anomalies, key=lambda x: x.timestamp)
    ]

    related_services = await fetch_topology_neighbors(cluster.tenant_id, cluster.service)

    first_emit = cluster.incident_id is None
    parent_hit = None
    if first_emit:
        # An incident's parent is decided once, when it opens. Re-resolving it on
        # every amendment would let the causal link flap as neighbours churn.
        parent_hit = await find_parent_incident(
            tenant_id=cluster.tenant_id,
            service=cluster.service,
            related_services=related_services,
        )
        cluster.parent_incident_id = parent_hit["_id"] if parent_hit is not None else None
        cluster.created_at = datetime.now(UTC)

    severity = pick_severity(cluster.anomalies)
    incident = Incident(
        id=cluster.incident_id or str(uuid4()),
        severity=severity,
        service=cluster.service,
        tenant_id=cluster.tenant_id,
        created_at=cluster.created_at or datetime.now(UTC),
        updated_at=datetime.now(UTC),
        anomaly_ids=[item.id for item in cluster.anomalies],
        timeline=timeline,
        summary=cluster_summary(cluster.service, cluster.anomalies),
        parent_incident_id=cluster.parent_incident_id,
        related_services=sorted(related_services),
    )
    cluster.incident_id = incident.id

    payload = incident.model_dump(mode="json")
    await os_async.index(index=settings.opensearch.incidents_index, id=incident.id, body=payload)

    if parent_hit is not None:
        parent_source = parent_hit.get("_source", {})
        existing_children = list(parent_source.get("child_incident_ids") or [])
        if incident.id not in existing_children:
            existing_children.append(incident.id)
        existing_related = set(parent_source.get("related_services") or [])
        existing_related.update(related_services)
        existing_related.add(cluster.service)
        parent_source["child_incident_ids"] = sorted(existing_children)
        parent_source["related_services"] = sorted(existing_related)
        parent_source["updated_at"] = datetime.now(UTC).isoformat()
        await os_async.index(
            index=settings.opensearch.incidents_index,
            id=parent_hit["_id"],
            body=parent_source,
        )

    if publish:
        await produce_json(producer, settings.kafka.topics.incidents, payload)

    if first_emit:
        INCIDENTS_CREATED_TOTAL.labels(
            service=incident.service,
            severity=incident.severity.value,
        ).inc()
    else:
        INCIDENTS_AMENDED_TOTAL.labels(service=incident.service).inc()

    cluster.emitted_severity = severity


def within_window(start: datetime, incoming: datetime, window_minutes: int) -> bool:
    window = timedelta(minutes=window_minutes)
    return incoming - start <= window


def open_cluster(
    anomaly: AnomalyEvent,
    config: ServiceCorrelationConfig,
    received_at: datetime,
) -> IncidentCluster:
    return IncidentCluster(
        tenant_id=anomaly.tenant_id,
        service=anomaly.service,
        first_seen=anomaly.timestamp,
        last_seen=anomaly.timestamp,
        last_received_at=received_at,
        window_duration_minutes=config.window_duration_minutes,
        min_signal_count=config.min_signal_count,
        anomalies=[anomaly],
        fingerprints={anomaly.fingerprint},
    )


async def already_processed(anomaly: AnomalyEvent) -> bool:
    """Whether this exact anomaly has been correlated before.

    Manual offset commits give at-least-once delivery, so a crash between
    processing and committing replays the batch. Without this, a replay opens a
    second incident for anomalies already grouped into the first.

    The key is the anomaly id, which is minted once by anomaly-service and
    travels with the event, so a replayed message carries the same key. Held in
    the shared state store, which makes it durable across a restart and shared
    between replicas: the property in-memory deduplication could never have.
    """
    if state is None:
        return False
    ttl = max(settings.pipeline.correlation_window_minutes * 60 * 4, 3600)
    return await state.seen_before(f"anomaly-seen:{anomaly.tenant_id}:{anomaly.id}", ttl)


async def process_anomaly(anomaly: AnomalyEvent) -> None:
    if await already_processed(anomaly):
        ANOMALIES_REPLAYED_TOTAL.labels(service=anomaly.service).inc()
        return

    received_at = datetime.now(UTC)
    config = await get_service_config(anomaly.tenant_id, anomaly.service)
    cluster_key = (anomaly.tenant_id, anomaly.service)
    existing = clusters.get(cluster_key)

    if existing is None or not within_window(
        existing.first_seen,
        anomaly.timestamp,
        existing.window_duration_minutes,
    ):
        # The previous window is over. Close it out, then open a fresh cluster.
        if existing is not None and existing.incident_id is None:
            await emit_incident(existing)
        cluster = clusters[cluster_key] = open_cluster(anomaly, config, received_at)
        if anomaly.severity == Severity.critical:
            await emit_incident(cluster)
        return

    if anomaly.fingerprint in existing.fingerprints:
        # Repeat of a signal this incident already carries. Extend the window so a
        # sustained storm keeps one incident open rather than opening thousands.
        existing.last_seen = anomaly.timestamp
        existing.last_received_at = received_at
        return

    existing.anomalies.append(anomaly)
    existing.fingerprints.add(anomaly.fingerprint)
    existing.last_seen = anomaly.timestamp
    existing.last_received_at = received_at

    if existing.incident_id is not None:
        # Already open. Amend it, and only republish when severity escalates, so
        # a growing incident does not cost one RCA generation per anomaly.
        escalated = pick_severity(existing.anomalies) != existing.emitted_severity
        await emit_incident(existing, publish=escalated)
        return

    # Emit immediately for high-signal clusters instead of waiting for window expiry.
    if (
        anomaly.severity == Severity.critical
        or len(existing.anomalies) >= existing.min_signal_count
    ):
        await emit_incident(existing)


async def flush_stale_clusters() -> None:
    now = datetime.now(UTC)
    stale_cluster_keys = [
        cluster_key
        for cluster_key, cluster in clusters.items()
        if now - cluster.last_received_at > timedelta(minutes=cluster.window_duration_minutes)
        and cluster.anomalies
    ]

    for cluster_key in stale_cluster_keys:
        cluster = clusters.pop(cluster_key)
        if cluster.incident_id is None:
            # Never reached min_signal_count. Emit it now that the window closed.
            await emit_incident(cluster)
        elif pick_severity(cluster.anomalies) != cluster.emitted_severity:
            await emit_incident(cluster)
    ACTIVE_CLUSTERS.set(len(clusters))


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
            source_topic=settings.kafka.topics.anomalies,
            payload=payload,
            error=error,
            partition=partition,
            offset=offset,
        ),
    )
    DLQ_PUBLISHED_TOTAL.labels(source_topic=settings.kafka.topics.anomalies).inc()


async def consume_loop() -> None:
    assert consumer is not None
    while True:
        try:
            records = await consumer.getmany(timeout_ms=1000, max_records=200)
            CONSUMER_BATCH_SIZE.set(sum(len(tp_records) for tp_records in records.values()))
            for tp, tp_records in records.items():
                for message in tp_records:
                    payload = json.loads(message.value.decode("utf-8"))
                    started_at = perf_counter()
                    try:
                        anomaly = AnomalyEvent.model_validate(payload)
                        await process_anomaly(anomaly)
                        CORRELATION_PROCESSING_DURATION_SECONDS.labels(
                            service=anomaly.service
                        ).observe(perf_counter() - started_at)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("Failed to correlate anomaly: %s", exc)
                        await publish_to_dlq(
                            payload=payload,
                            error=exc,
                            partition=tp.partition,
                            offset=message.offset,
                        )

            if records:
                await commit_safely(consumer, where="correlation-service")

            await flush_stale_clusters()
            ACTIVE_CLUSTERS.set(len(clusters))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Correlation loop failed: %s", exc)
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup() -> None:
    global consumer, producer, worker_task, state

    state = await build_state_store(settings.redis.url, namespace="airs-correlation")

    await async_ensure_index(os_async, settings.opensearch.incidents_index)
    await async_ensure_index(os_async, TOPOLOGY_INDEX)
    ensure_retention_policy(
        os_client,
        index_name=settings.opensearch.incidents_index,
        retention_days=settings.pipeline.incident_retention_days,
    )

    consumer = build_consumer(
        settings.kafka.topics.anomalies,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id="airs-correlation-service",
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

    for cluster in list(clusters.values()):
        if cluster.incident_id is None:
            await emit_incident(cluster)
    clusters.clear()

    if consumer is not None:
        await consumer.stop()
    if producer is not None:
        await producer.stop()
    if state is not None:
        await state.aclose()
    await os_async.close()


@app.get("/health")
async def health() -> dict[str, str | bool]:
    return {
        "status": "ok",
        "service": "correlation-service",
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
    return {"status": "ok", "service": "correlation-service"}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    if await ready_check():
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "service": "correlation-service"},
        )
    return JSONResponse(
        status_code=503,
        content={"status": "degraded", "service": "correlation-service"},
    )


@app.get("/metrics")
async def metrics() -> object:
    return metrics_response()

from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import json
import logging
import math
import re
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import perf_counter

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Gauge, Histogram

from airs_shared.dlq import build_dlq_payload
from airs_shared.kafka import produce_json
from airs_shared.models import (
    AnomalyEvent,
    DetectionRule,
    RuleMatchType,
    Severity,
    SuppressionWindow,
)
from airs_shared.monitoring import metrics_response
from airs_shared.normalize import normalize_log
from airs_shared.opensearch import build_client, ensure_index
from airs_shared.settings import get_settings

settings = get_settings()
app = FastAPI(title="AIRS Anomaly Service")
logger = logging.getLogger("anomaly-service")

KEYWORDS = ["error", "exception", "timeout", "connection refused", "oom", "5xx"]
CRITICAL_MARKERS = ["oom", "connection refused", "5xx"]
RULES_INDEX = "airs-rules"
SUPPRESSIONS_INDEX = "airs-suppressions"
ASSET_REFRESH_SECONDS = 30
EWMA_ALPHA = 0.3


@dataclass
class SeasonalSlot:
    ewma: float = 0.0
    ewvar: float = 1.0
    initialized: bool = False


@dataclass
class ServiceSeasonalBaseline:
    alpha: float
    slots: dict[int, SeasonalSlot] = field(
        default_factory=lambda: {hour: SeasonalSlot() for hour in range(24)}
    )
    current_bucket: datetime | None = None
    current_count: int = 0

    def ingest(self, ts: datetime) -> None:
        bucket = ts.replace(second=0, microsecond=0)
        if self.current_bucket is None:
            self.current_bucket = bucket

        if bucket == self.current_bucket:
            self.current_count += 1
            return

        self._finalize_bucket(self.current_bucket, self.current_count)

        skipped = int((bucket - self.current_bucket).total_seconds() // 60) - 1
        cursor = self.current_bucket
        for _ in range(max(skipped, 0)):
            cursor += timedelta(minutes=1)
            self._finalize_bucket(cursor, 0)

        self.current_bucket = bucket
        self.current_count = 1

    def _finalize_bucket(self, bucket: datetime, count: int) -> None:
        slot = self.slots[bucket.hour]
        if not slot.initialized:
            slot.ewma = float(count)
            slot.ewvar = max(float(count), 1.0)
            slot.initialized = True
            return

        previous_mean = slot.ewma
        residual = float(count) - previous_mean
        slot.ewma = self.alpha * float(count) + (1 - self.alpha) * slot.ewma
        slot.ewvar = max(self.alpha * (residual**2) + (1 - self.alpha) * slot.ewvar, 1.0)

    def zscore(self, ts: datetime) -> float | None:
        slot = self.slots[ts.hour]
        if not slot.initialized:
            return None

        std = math.sqrt(max(slot.ewvar, 1.0))
        return (self.current_count - slot.ewma) / std


consumer: AIOKafkaConsumer | None = None
producer: AIOKafkaProducer | None = None
worker_task: asyncio.Task[None] | None = None

os_client = build_client(settings.opensearch.url)
baselines: dict[tuple[str, str], ServiceSeasonalBaseline] = {}
rules_cache: list[DetectionRule] = []
suppressions_cache: list[SuppressionWindow] = []
last_assets_refresh: datetime | None = None

ANOMALIES_EMITTED_TOTAL = Counter(
    "airs_anomalies_emitted_total",
    "Count of anomalies emitted",
    labelnames=("service", "severity"),
)
ANOMALY_PROCESSING_DURATION_SECONDS = Histogram(
    "airs_anomaly_processing_duration_seconds",
    "Anomaly processing latency in seconds",
    labelnames=("service",),
)
ANOMALY_CONFIDENCE_SCORE = Histogram(
    "airs_anomaly_confidence_score",
    "Distribution of anomaly confidence scores",
    buckets=(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
)
ANOMALIES_SUPPRESSED_TOTAL = Counter(
    "airs_anomalies_suppressed_total",
    "Count of suppressed anomaly candidates",
    labelnames=("reason",),
)
RULES_ACTIVE_TOTAL = Gauge(
    "airs_rules_active_total",
    "Count of enabled detection rules loaded into anomaly-service",
)
SUPPRESSIONS_ACTIVE_TOTAL = Gauge(
    "airs_suppressions_active_total",
    "Count of currently active suppression windows",
)
DLQ_PUBLISHED_TOTAL = Counter(
    "airs_dlq_published_total",
    "Count of DLQ events published by source topic",
    labelnames=("source_topic",),
)
CONSUMER_BATCH_SIZE = Gauge(
    "airs_anomaly_batch_size",
    "Number of messages fetched in the latest batch",
)


def build_fingerprint(tenant_id: str, service: str, message: str) -> str:
    key = f"{tenant_id}:{service}:{message.lower().strip()}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def detect_keywords(message: str) -> list[str]:
    lower = message.lower()
    return [kw for kw in KEYWORDS if kw in lower]


def get_baseline(tenant_id: str, service: str) -> ServiceSeasonalBaseline:
    key = (tenant_id, service)
    baseline = baselines.get(key)
    if baseline is not None:
        return baseline

    baseline = ServiceSeasonalBaseline(alpha=EWMA_ALPHA)
    baselines[key] = baseline
    return baseline


def derive_severity(level: str, score: float, matched_keywords: list[str]) -> Severity:
    level_lower = level.lower()
    if score >= 4.0 or level_lower in {"critical", "fatal"}:
        return Severity.critical
    if any(marker in matched_keywords for marker in CRITICAL_MARKERS):
        return Severity.critical
    if score >= settings.pipeline.anomaly_threshold or level_lower in {"error", "warn", "warning"}:
        return Severity.warning
    return Severity.info


def combine_severity(base: Severity, matched_rules: list[DetectionRule]) -> Severity:
    ranking = {Severity.info: 1, Severity.warning: 2, Severity.critical: 3}
    result = base
    for rule in matched_rules:
        if ranking[rule.severity] > ranking[result]:
            result = rule.severity
    return result


def should_emit(
    score: float | None,
    level: str,
    keywords: list[str],
    matched_rules: list[DetectionRule],
) -> bool:
    if matched_rules:
        return True
    if keywords:
        return True
    if level.lower() in {"error", "critical", "fatal"}:
        return True
    if score is not None and score >= settings.pipeline.anomaly_threshold:
        return True
    return False


def compute_confidence_score(
    *,
    level: str,
    score: float,
    matched_keywords: list[str],
    matched_rules: list[DetectionRule],
) -> float:
    base = 0.1
    z_component = min(max(score, 0.0) / 6.0, 0.35)
    keyword_component = min(len(matched_keywords) * 0.1, 0.25)

    level_lower = level.lower()
    if level_lower in {"critical", "fatal"}:
        level_component = 0.25
    elif level_lower == "error":
        level_component = 0.15
    elif level_lower in {"warn", "warning"}:
        level_component = 0.1
    else:
        level_component = 0.0

    rule_component = min(sum(rule.confidence_boost for rule in matched_rules), 0.4)
    score_value = max(
        0.0,
        min(1.0, base + z_component + keyword_component + level_component + rule_component),
    )
    return round(score_value, 3)


def service_matches(pattern: str, service: str) -> bool:
    if pattern in {"", "*"}:
        return True
    return fnmatch.fnmatch(service, pattern)


def matches_rule(
    *,
    rule: DetectionRule,
    service: str,
    message: str,
    score: float | None,
) -> bool:
    if not service_matches(rule.service_pattern, service):
        return False

    lower_message = message.lower()

    if rule.match_type == RuleMatchType.keyword:
        return rule.pattern.lower() in lower_message

    if rule.match_type == RuleMatchType.regex:
        try:
            return re.search(rule.pattern, message, flags=re.IGNORECASE) is not None
        except re.error:
            return False

    if rule.match_type == RuleMatchType.threshold:
        try:
            threshold = float(rule.pattern)
        except ValueError:
            return False
        return score is not None and score >= threshold

    if rule.match_type == RuleMatchType.composite:
        clauses = [part.strip().lower() for part in rule.pattern.split("&&") if part.strip()]
        return bool(clauses) and all(clause in lower_message for clause in clauses)

    return False


def active_suppressions(now: datetime) -> list[SuppressionWindow]:
    active: list[SuppressionWindow] = []
    for suppression in suppressions_cache:
        if not suppression.enabled:
            continue
        if suppression.starts_at <= now <= suppression.ends_at:
            active.append(suppression)
    return active


def get_matching_suppression(
    tenant_id: str,
    service: str,
    ts: datetime,
) -> SuppressionWindow | None:
    for suppression in suppressions_cache:
        if not suppression.enabled:
            continue
        if suppression.tenant_id != tenant_id:
            continue
        if not (suppression.starts_at <= ts <= suppression.ends_at):
            continue
        if service_matches(suppression.service_pattern, service):
            return suppression
    return None


def fetch_rules() -> list[DetectionRule]:
    query = {
        "query": {"term": {"enabled": True}},
        "size": 500,
        "sort": [{"updated_at": {"order": "desc"}}],
    }
    try:
        result = os_client.search(index=RULES_INDEX, body=query)
    except Exception:  # noqa: BLE001
        return []

    items: list[DetectionRule] = []
    for hit in result.get("hits", {}).get("hits", []):
        try:
            items.append(DetectionRule.model_validate({"id": hit["_id"], **hit["_source"]}))
        except Exception:  # noqa: BLE001
            continue
    return items


def fetch_suppressions() -> list[SuppressionWindow]:
    query = {
        "query": {"term": {"enabled": True}},
        "size": 500,
        "sort": [{"updated_at": {"order": "desc"}}],
    }
    try:
        result = os_client.search(index=SUPPRESSIONS_INDEX, body=query)
    except Exception:  # noqa: BLE001
        return []

    items: list[SuppressionWindow] = []
    for hit in result.get("hits", {}).get("hits", []):
        try:
            items.append(SuppressionWindow.model_validate({"id": hit["_id"], **hit["_source"]}))
        except Exception:  # noqa: BLE001
            continue
    return items


async def refresh_assets_if_needed(force: bool = False) -> None:
    global last_assets_refresh, rules_cache, suppressions_cache

    now = datetime.now(UTC)
    if not force and last_assets_refresh is not None:
        elapsed = (now - last_assets_refresh).total_seconds()
        if elapsed < ASSET_REFRESH_SECONDS:
            SUPPRESSIONS_ACTIVE_TOTAL.set(len(active_suppressions(now)))
            return

    rules_cache = fetch_rules()
    suppressions_cache = fetch_suppressions()
    last_assets_refresh = now

    RULES_ACTIVE_TOTAL.set(len([rule for rule in rules_cache if rule.enabled]))
    SUPPRESSIONS_ACTIVE_TOTAL.set(len(active_suppressions(now)))


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
            source_topic=settings.kafka.topics.processed_logs,
            payload=payload,
            error=error,
            partition=partition,
            offset=offset,
        ),
    )
    DLQ_PUBLISHED_TOTAL.labels(source_topic=settings.kafka.topics.processed_logs).inc()


async def consume_loop() -> None:
    assert consumer is not None

    while True:
        try:
            await refresh_assets_if_needed()

            records = await consumer.getmany(timeout_ms=1000, max_records=200)
            CONSUMER_BATCH_SIZE.set(sum(len(tp_records) for tp_records in records.values()))
            for tp, tp_records in records.items():
                for message in tp_records:
                    payload = json.loads(message.value.decode("utf-8"))
                    started_at = perf_counter()
                    try:
                        log_event = normalize_log(payload)

                        suppression = get_matching_suppression(
                            log_event.tenant_id,
                            log_event.service,
                            log_event.timestamp,
                        )
                        if suppression is not None:
                            ANOMALIES_SUPPRESSED_TOTAL.labels(reason="suppression_window").inc()
                            continue

                        baseline = get_baseline(log_event.tenant_id, log_event.service)
                        baseline.ingest(log_event.timestamp)
                        score = baseline.zscore(log_event.timestamp)
                        score_value = round(score if score is not None else 0.0, 3)

                        matched_keywords = detect_keywords(log_event.message)

                        matched_rules: list[DetectionRule] = []
                        for rule in rules_cache:
                            if rule.tenant_id != log_event.tenant_id:
                                continue
                            if matches_rule(
                                rule=rule,
                                service=log_event.service,
                                message=log_event.message,
                                score=score,
                            ):
                                matched_rules.append(rule)

                        if not should_emit(score, log_event.level, matched_keywords, matched_rules):
                            continue

                        confidence_score = compute_confidence_score(
                            level=log_event.level,
                            score=score_value,
                            matched_keywords=matched_keywords,
                            matched_rules=matched_rules,
                        )
                        ANOMALY_CONFIDENCE_SCORE.observe(confidence_score)

                        if confidence_score < 0.3 and not matched_rules:
                            ANOMALIES_SUPPRESSED_TOTAL.labels(reason="low_confidence").inc()
                            continue

                        reasons: list[str] = []
                        if score is not None and score >= settings.pipeline.anomaly_threshold:
                            reasons.append(f"zscore={score_value}")
                        if matched_keywords:
                            reasons.append(f"keywords={','.join(matched_keywords)}")
                        if log_event.level.lower() in {"error", "critical", "fatal"}:
                            reasons.append(f"level={log_event.level.lower()}")
                        if matched_rules:
                            reasons.extend([f"rule={rule.id}" for rule in matched_rules])

                        base_severity = derive_severity(
                            log_event.level,
                            score_value,
                            matched_keywords,
                        )
                        final_severity = combine_severity(base_severity, matched_rules)

                        anomaly = AnomalyEvent(
                            timestamp=log_event.timestamp,
                            service=log_event.service,
                            severity=final_severity,
                            anomaly_score=max(score_value, 0.0),
                            reasons=reasons,
                            log_message=log_event.message,
                            fingerprint=build_fingerprint(
                                log_event.tenant_id,
                                log_event.service,
                                log_event.message,
                            ),
                            confidence_score=confidence_score,
                            tenant_id=log_event.tenant_id,
                            metadata=log_event.metadata,
                        )

                        if producer is None:
                            continue

                        await produce_json(
                            producer,
                            settings.kafka.topics.anomalies,
                            anomaly.model_dump(mode="json"),
                        )
                        ANOMALIES_EMITTED_TOTAL.labels(
                            service=anomaly.service,
                            severity=anomaly.severity.value,
                        ).inc()
                        ANOMALY_PROCESSING_DURATION_SECONDS.labels(service=anomaly.service).observe(
                            perf_counter() - started_at
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("Failed to process anomaly candidate: %s", exc)
                        await publish_to_dlq(
                            payload=payload,
                            error=exc,
                            partition=tp.partition,
                            offset=message.offset,
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Anomaly loop failed: %s", exc)
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup() -> None:
    global consumer, producer, worker_task

    ensure_index(os_client, RULES_INDEX)
    ensure_index(os_client, SUPPRESSIONS_INDEX)
    await refresh_assets_if_needed(force=True)

    consumer = AIOKafkaConsumer(
        settings.kafka.topics.processed_logs,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id="airs-anomaly-service",
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
        "service": "anomaly-service",
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
    return {"status": "ok", "service": "anomaly-service"}


@app.get("/health/ready")
async def health_ready() -> JSONResponse:
    if await ready_check():
        return JSONResponse(
            status_code=200,
            content={"status": "ok", "service": "anomaly-service"},
        )
    return JSONResponse(
        status_code=503,
        content={"status": "degraded", "service": "anomaly-service"},
    )


@app.get("/metrics")
async def metrics() -> object:
    return metrics_response()

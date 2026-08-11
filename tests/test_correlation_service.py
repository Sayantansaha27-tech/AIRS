"""correlation-service: grouping contract, dedup, storm behaviour, DLQ path."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from airs_shared.models import AnomalyEvent, Incident, Severity

pytestmark = pytest.mark.anyio

BASE = datetime(2026, 8, 10, 10, 0, tzinfo=UTC)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def fixed_config(correlation, monkeypatch):
    """Pin the per-service window so tests do not depend on OpenSearch."""

    def _config(window_minutes: int = 10, min_signals: int = 2):
        async def _get(tenant_id, service):
            return correlation.ServiceCorrelationConfig(
                window_duration_minutes=window_minutes,
                min_signal_count=min_signals,
                fetched_at=datetime.now(UTC),
            )

        monkeypatch.setattr(correlation, "get_service_config", _get)

    _config()
    return _config


def anomaly_at(
    offset_seconds: int,
    *,
    severity: Severity = Severity.warning,
    fingerprint: str = "fp",
    service: str = "orders-service",
) -> AnomalyEvent:
    return AnomalyEvent(
        timestamp=BASE + timedelta(seconds=offset_seconds),
        service=service,
        severity=severity,
        anomaly_score=3.0,
        reasons=["keywords=timeout"],
        log_message=f"message {fingerprint}",
        fingerprint=fingerprint,
    )


# ----------------------------------------------------------------- contract


async def test_emitted_incident_satisfies_the_topic_contract(correlation, producer, fixed_config):
    await correlation.process_anomaly(anomaly_at(0, fingerprint="a"))
    await correlation.process_anomaly(anomaly_at(1, fingerprint="b"))

    payload = producer.payloads_for(correlation.settings.kafka.topics.incidents)[0]
    incident = Incident.model_validate(payload)
    assert incident.service == "orders-service"
    assert incident.status.value == "open"
    assert len(incident.timeline) == 2
    assert incident.anomaly_ids
    assert incident.rca is None, "RCA is added later by ai-service"


async def test_timeline_is_ordered_by_event_time(correlation, producer, fixed_config):
    await correlation.process_anomaly(anomaly_at(60, fingerprint="late"))
    await correlation.process_anomaly(anomaly_at(0, fingerprint="early"))

    payload = producer.payloads_for(correlation.settings.kafka.topics.incidents)[0]
    stamps = [entry["timestamp"] for entry in payload["timeline"]]
    assert stamps == sorted(stamps)


# --------------------------------------------------------------- happy path


async def test_single_warning_does_not_emit_before_min_signal_count(
    correlation, producer, fixed_config
):
    await correlation.process_anomaly(anomaly_at(0, fingerprint="only"))
    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 0
    assert len(correlation.clusters) == 1, "held open, awaiting a second signal"


async def test_second_distinct_signal_emits(correlation, producer, fixed_config):
    await correlation.process_anomaly(anomaly_at(0, fingerprint="a"))
    await correlation.process_anomaly(anomaly_at(1, fingerprint="b"))
    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 1


async def test_critical_emits_immediately(correlation, producer, fixed_config):
    await correlation.process_anomaly(anomaly_at(0, severity=Severity.critical))
    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 1


async def test_separate_services_do_not_group_together(correlation, producer, fixed_config):
    await correlation.process_anomaly(anomaly_at(0, fingerprint="a", service="orders"))
    await correlation.process_anomaly(anomaly_at(1, fingerprint="b", service="payments"))
    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 0
    assert len(correlation.clusters) == 2


async def test_anomaly_outside_the_window_starts_a_new_incident(
    correlation, producer, fixed_config
):
    fixed_config(window_minutes=10)
    await correlation.process_anomaly(anomaly_at(0, fingerprint="a"))
    await correlation.process_anomaly(anomaly_at(1, fingerprint="b"))
    first = producer.payloads_for(correlation.settings.kafka.topics.incidents)[0]["id"]

    # 20 minutes later, well outside the window
    await correlation.process_anomaly(anomaly_at(1200, fingerprint="c"))
    await correlation.process_anomaly(anomaly_at(1201, fingerprint="d"))

    ids = [p["id"] for p in producer.payloads_for(correlation.settings.kafka.topics.incidents)]
    assert len(set(ids)) == 2
    assert ids[0] == first


# ------------------------------------------------------ dedup and storm load


async def test_repeat_fingerprint_deduplicates(correlation, producer, fixed_config):
    for i in range(20):
        await correlation.process_anomaly(anomaly_at(i, fingerprint="same"))

    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 0, (
        "identical signals never reach min_signal_count"
    )
    cluster = next(iter(correlation.clusters.values()))
    assert len(cluster.anomalies) == 1


async def test_critical_storm_produces_one_incident(correlation, producer, fixed_config):
    """Regression: clusters were discarded on emit, taking their fingerprint
    set with them, so 50 identical critical anomalies produced 50 incidents."""
    for i in range(50):
        await correlation.process_anomaly(
            anomaly_at(i, severity=Severity.critical, fingerprint="same")
        )

    payloads = producer.payloads_for(correlation.settings.kafka.topics.incidents)
    assert len(payloads) == 1
    assert len({p["id"] for p in payloads}) == 1


async def test_distinct_signal_storm_amends_one_incident(correlation, producer, fixed_config):
    for i in range(50):
        await correlation.process_anomaly(anomaly_at(i, fingerprint=f"fp-{i}"))

    payloads = producer.payloads_for(correlation.settings.kafka.topics.incidents)
    assert len({p["id"] for p in payloads}) == 1, "one incident, not 25"
    assert len(payloads) == 1, "no republish without severity escalation"

    cluster = next(iter(correlation.clusters.values()))
    assert len(cluster.anomalies) == 50, "all signals recorded on the incident"


async def test_escalation_republishes_so_rca_is_regenerated(correlation, producer, fixed_config):
    await correlation.process_anomaly(anomaly_at(0, fingerprint="a"))
    await correlation.process_anomaly(anomaly_at(1, fingerprint="b"))
    await correlation.process_anomaly(anomaly_at(2, fingerprint="c", severity=Severity.critical))

    payloads = producer.payloads_for(correlation.settings.kafka.topics.incidents)
    assert len(payloads) == 2, "creation plus escalation"
    assert len({p["id"] for p in payloads}) == 1, "same incident throughout"
    assert payloads[0]["severity"] == "warning"
    assert payloads[1]["severity"] == "critical"


async def test_amendment_preserves_incident_identity(correlation, producer, fixed_config):
    await correlation.process_anomaly(anomaly_at(0, fingerprint="a"))
    await correlation.process_anomaly(anomaly_at(1, fingerprint="b"))
    created = producer.payloads_for(correlation.settings.kafka.topics.incidents)[0]

    await correlation.process_anomaly(anomaly_at(2, fingerprint="c", severity=Severity.critical))
    amended = producer.payloads_for(correlation.settings.kafka.topics.incidents)[1]

    assert amended["id"] == created["id"]
    assert amended["created_at"] == created["created_at"], "created_at is stable"
    assert amended["updated_at"] >= created["updated_at"]
    assert len(amended["timeline"]) == 3


# ----------------------------------------------------------- stale flushing


async def test_stale_cluster_is_flushed(correlation, producer, fixed_config):
    fixed_config(window_minutes=10)
    await correlation.process_anomaly(anomaly_at(0, fingerprint="lonely"))
    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 0

    cluster = next(iter(correlation.clusters.values()))
    cluster.last_received_at = datetime.now(UTC) - timedelta(minutes=11)
    await correlation.flush_stale_clusters()

    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 1
    assert not correlation.clusters


async def test_already_emitted_stale_cluster_is_not_republished(
    correlation, producer, fixed_config
):
    await correlation.process_anomaly(anomaly_at(0, fingerprint="a"))
    await correlation.process_anomaly(anomaly_at(1, fingerprint="b"))
    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 1

    cluster = next(iter(correlation.clusters.values()))
    cluster.last_received_at = datetime.now(UTC) - timedelta(minutes=11)
    await correlation.flush_stale_clusters()

    assert producer.count_for(correlation.settings.kafka.topics.incidents) == 1
    assert not correlation.clusters


# ------------------------------------------------------------ service graph


async def test_topology_neighbour_becomes_the_parent(
    correlation, producer, fixed_config, monkeypatch
):
    async def _neighbours(tenant_id, service):
        return {"postgres-primary"}

    async def _parent(**kwargs):
        return {"_id": "parent-123", "_source": {}}

    monkeypatch.setattr(correlation, "fetch_topology_neighbors", _neighbours)
    monkeypatch.setattr(correlation, "find_parent_incident", _parent)

    await correlation.process_anomaly(anomaly_at(0, severity=Severity.critical))

    payload = producer.payloads_for(correlation.settings.kafka.topics.incidents)[0]
    assert payload["parent_incident_id"] == "parent-123"
    assert payload["related_services"] == ["postgres-primary"]


async def test_parent_is_resolved_once_and_not_re_resolved(
    correlation, producer, fixed_config, monkeypatch
):
    calls: list[int] = []

    async def _find(**kwargs):
        calls.append(1)
        return {"_id": "parent-123", "_source": {}}

    async def _neighbours(tenant_id, service):
        return {"db"}

    monkeypatch.setattr(correlation, "fetch_topology_neighbors", _neighbours)
    monkeypatch.setattr(correlation, "find_parent_incident", _find)

    await correlation.process_anomaly(anomaly_at(0, fingerprint="a"))
    await correlation.process_anomaly(anomaly_at(1, fingerprint="b"))
    await correlation.process_anomaly(anomaly_at(2, fingerprint="c", severity=Severity.critical))

    assert len(calls) == 1, "the causal link must not flap as neighbours churn"


# ----------------------------------------------------------------- DLQ path


async def test_malformed_anomaly_routes_to_dlq(correlation, producer):
    await correlation.publish_to_dlq(
        payload={"service": "s", "severity": "not-a-severity"},
        error=ValueError("Input should be 'critical', 'warning' or 'info'"),
        partition=1,
        offset=77,
    )

    dlq = producer.payloads_for(correlation.settings.kafka.topics.dlq)
    assert len(dlq) == 1
    assert dlq[0]["source_topic"] == correlation.settings.kafka.topics.anomalies
    assert dlq[0]["original_partition"] == 1
    assert dlq[0]["original_offset"] == 77

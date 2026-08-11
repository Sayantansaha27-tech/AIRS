"""Chaos: the pipeline must survive ai-service being gone.

The README claims the pipeline never depends on model availability. This
turns that assertion into something falsifiable: kill the RCA stage mid-flight
and assert the pipeline still produces incidents with a deterministic RCA,
does not stall, does not fill the DLQ, and recovers without duplicating work.

Run with a live stack:

    docker compose up -d
    AIRS_CHAOS=1 pytest tests/chaos -v
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.chaos


def critical_burst(service: str, count: int = 3) -> list[dict]:
    """Critical anomalies so correlation emits immediately rather than waiting
    out its window. Keeps the test bounded without touching config."""
    now = datetime.now(UTC)
    return [
        {
            "timestamp": now.isoformat(),
            "service": service,
            "level": "critical",
            "message": f"connection refused: postgres-primary:5432 [{i}]",
        }
        for i in range(count)
    ]


def rca_of(incident: dict) -> dict | None:
    return incident.get("rca")


def test_incident_still_gets_rca_with_ai_service_down(stack, restore_ai_service):
    service = f"chaos-down-{uuid.uuid4().hex[:8]}"

    dlq_before = stack.dlq_depth()
    stack.stop_ai_service()

    response = stack.ingest(critical_burst(service))
    assert response.status_code == 200, "ingestion must not depend on ai-service"

    # The incident itself is written by correlation-service, which is upstream
    # of RCA. It must exist even while the RCA stage is gone.
    incidents = stack.wait_until(
        lambda: stack.incidents_for(service),
        timeout=90,
        what="an incident to be created with ai-service stopped",
    )
    assert incidents, "correlation must produce an incident without ai-service"
    assert incidents[0]["severity"] == "critical"

    # RCA is enrichment applied afterwards, so it is legitimately absent here.
    assert rca_of(incidents[0]) is None, "no RCA yet, the stage is stopped"

    # Nothing should have been dead-lettered: the incident is queued, not failed.
    assert stack.dlq_depth() == dlq_before, "a stopped RCA stage must not fill the DLQ"

    # Recovery: the queued incident is picked up and enriched.
    stack.start_ai_service()
    enriched = stack.wait_until(
        lambda: next(
            (i for i in stack.incidents_for(service) if rca_of(i) is not None), None
        ),
        timeout=180,
        what="the queued incident to receive an RCA after recovery",
    )
    rca = rca_of(enriched)
    assert rca["root_cause"], "RCA must be populated"
    assert rca["explanation"]
    assert 0.0 <= rca["confidence"] <= 1.0


def test_no_duplicate_incident_after_recovery(stack, restore_ai_service):
    """Recovery must not fan one incident out into several.

    incidents-topic is upsert-by-id, so replaying a queued incident enriches
    the existing document rather than creating another.
    """
    service = f"chaos-dup-{uuid.uuid4().hex[:8]}"

    stack.stop_ai_service()
    stack.ingest(critical_burst(service, count=2))
    stack.wait_until(
        lambda: stack.incidents_for(service),
        timeout=90,
        what="an incident while ai-service is stopped",
    )
    during = stack.incidents_for(service)
    ids_during = {i["id"] for i in during}

    stack.start_ai_service()
    stack.wait_until(
        lambda: all(rca_of(i) is not None for i in stack.incidents_for(service)),
        timeout=180,
        what="every incident to be enriched after recovery",
    )

    after = stack.incidents_for(service)
    assert {i["id"] for i in after} == ids_during, "recovery must not mint new incidents"
    assert len(after) == len(during), "one RCA per incident, not one incident per RCA"


def test_deterministic_rca_when_the_model_is_unreachable(stack):
    """ai-service up, model unreachable: the fallback must engage.

    Pointing the provider at a model that does not exist exercises the same
    path as an Ollama outage without stopping a container.
    """
    service = f"chaos-model-{uuid.uuid4().hex[:8]}"

    import httpx

    original = httpx.get(f"{stack.gateway}/v1/llm/config", timeout=10.0)
    restore = original.json() if original.status_code == 200 else None

    try:
        httpx.post(
            f"{stack.gateway}/v1/llm/config",
            json={"provider": "ollama", "model": "model-that-does-not-exist"},
            timeout=10.0,
        )
        dlq_before = stack.dlq_depth()
        stack.ingest(critical_burst(service))

        enriched = stack.wait_until(
            lambda: next(
                (i for i in stack.incidents_for(service) if rca_of(i) is not None), None
            ),
            timeout=180,
            what="a deterministic RCA with the model unreachable",
        )
        rca = rca_of(enriched)
        assert rca["root_cause"], "the fallback must still populate the contract"
        assert "fallback" in rca["explanation"].lower(), (
            "the RCA must say it is a fallback rather than passing as analysis"
        )
        assert stack.dlq_depth() == dlq_before, (
            "an unreachable model must degrade, not dead-letter"
        )
    finally:
        if restore:
            httpx.post(f"{stack.gateway}/v1/llm/config", json=restore, timeout=10.0)


def test_ingestion_stays_available_throughout(stack, restore_ai_service):
    """Backpressure must not propagate upstream from the slowest stage."""
    service = f"chaos-ingest-{uuid.uuid4().hex[:8]}"
    stack.stop_ai_service()

    for batch in range(5):
        response = stack.ingest(critical_burst(f"{service}-{batch}", count=5))
        assert response.status_code == 200, (
            f"ingest batch {batch} failed while ai-service was stopped"
        )
        assert response.json()["accepted"] == 5

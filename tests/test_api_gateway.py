"""api-gateway: request contracts, tenant scoping, pagination, status rules.

api-gateway has no Kafka consumer and therefore no DLQ path: it is a
synchronous REST surface that returns errors to its caller. Its equivalent of
a DLQ test is that malformed input is rejected at the contract boundary rather
than reaching a handler, which is what the contract tests below assert.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from airs_shared.models import (
    ChatOpsConfigCreateRequest,
    DataSourceCreateRequest,
    DetectionRuleCreateRequest,
    IncidentStatus,
    ReplayFilterRequest,
    SimulateIngestRequest,
    SuppressionCreateRequest,
)
from pydantic import ValidationError

# ----------------------------------------------------------------- contract


def test_simulate_requires_service_and_pattern():
    with pytest.raises(ValidationError) as exc:
        SimulateIngestRequest.model_validate({"service": "checkout", "count": 500})
    assert any(err["loc"] == ("pattern",) for err in exc.value.errors())

    ok = SimulateIngestRequest.model_validate(
        {"service": "checkout", "pattern": "connection refused", "count": 500}
    )
    assert ok.rate_per_second == 10, "documented default"


def test_simulate_count_is_bounded():
    with pytest.raises(ValidationError):
        SimulateIngestRequest.model_validate({"service": "s", "pattern": "p", "count": 10_001})


def test_source_endpoint_must_be_http():
    with pytest.raises(ValidationError):
        DataSourceCreateRequest.model_validate(
            {"name": "n", "endpoint": "ftp://host/logs", "default_service": "s"}
        )
    ok = DataSourceCreateRequest.model_validate(
        {"name": "n", "endpoint": "  https://host/logs  ", "default_service": "s"}
    )
    assert ok.endpoint == "https://host/logs", "endpoint is trimmed"


def test_chatops_webhook_must_be_http():
    with pytest.raises(ValidationError):
        ChatOpsConfigCreateRequest.model_validate({"webhook_url": "not-a-url"})


def test_suppression_window_must_end_after_it_starts():
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        SuppressionCreateRequest.model_validate(
            {
                "service_pattern": "*",
                "reason": "maintenance",
                "starts_at": now,
                "ends_at": now - timedelta(minutes=1),
            }
        )


def test_replay_window_must_end_after_it_starts():
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        ReplayFilterRequest.model_validate(
            {"from_time": now, "to_time": now - timedelta(minutes=1)}
        )


def test_detection_rule_pattern_cannot_be_blank():
    with pytest.raises(ValidationError):
        DetectionRuleCreateRequest.model_validate({"name": "r", "pattern": "   "})


# ----------------------------------------------------------- tenant scoping


def test_tenant_resolution_prefers_explicit_over_header(gateway):
    class Req:
        headers = {"x-tenant-id": "from-header"}

    assert gateway.resolve_tenant_id(Req(), "explicit") == "explicit"
    assert gateway.resolve_tenant_id(Req()) == "from-header"

    class NoHeader:
        headers: dict[str, str] = {}

    assert gateway.resolve_tenant_id(NoHeader()) == "default"


def test_tenant_header_is_normalized(gateway):
    class Req:
        headers = {"x-tenant-id": "  Team-A  "}

    assert gateway.resolve_tenant_id(Req()) == "team-a"


def test_default_tenant_also_matches_documents_written_before_tenancy(gateway):
    """Legacy documents have no tenant_id field and must remain visible."""
    scope = gateway.tenant_scope_filter("default")
    clauses = scope["bool"]["should"]
    assert {"term": {"tenant_id": "default"}} in clauses
    assert any("must_not" in str(clause) for clause in clauses)

    assert gateway.tenant_scope_filter("team-a") == {"term": {"tenant_id": "team-a"}}


def test_cross_tenant_document_access_is_a_404(gateway):
    gateway.assert_tenant_access({"tenant_id": "team-a"}, "team-a")

    with pytest.raises(Exception) as exc:
        gateway.assert_tenant_access({"tenant_id": "team-b"}, "team-a")
    assert getattr(exc.value, "status_code", None) == 404, (
        "404 not 403, so a wrong tenant cannot probe for existence"
    )


# ---------------------------------------------------------------- responses


def test_source_view_never_leaks_the_auth_token(gateway):
    from airs_shared.models import DataSource

    source = DataSource(
        name="n",
        endpoint="https://host/logs",
        default_service="s",
        auth_token="super-secret",
    )
    view = gateway.source_to_view(source)
    payload = view.model_dump()

    assert "auth_token" not in payload
    assert payload["has_auth_token"] is True
    assert "super-secret" not in str(payload)


def test_webhook_view_never_leaks_the_signing_secret(gateway):
    sub = gateway.WebhookSubscription(name="pager", url="https://hooks.example/x", secret="shhh")
    payload = gateway.webhook_to_view(sub)

    assert "secret" not in payload
    assert payload["has_secret"] is True
    assert "shhh" not in str(payload)


# --------------------------------------------------------------- pagination


def test_cursor_round_trips(gateway):
    values = ["2026-08-10T10:00:00Z", "incident-123"]
    assert gateway.decode_cursor(gateway.encode_cursor(values)) == values


def test_encode_cursor_is_none_when_there_is_no_next_page(gateway):
    assert gateway.encode_cursor([]) is None


@pytest.mark.parametrize("bad", ["not-base64!!", "", "eyJhIjogMX0="])
def test_malformed_cursor_is_a_400(gateway, bad):
    with pytest.raises(Exception) as exc:
        gateway.decode_cursor(bad)
    assert getattr(exc.value, "status_code", None) == 400


# ---------------------------------------------------------- status machine


@pytest.mark.parametrize(
    ("current", "target", "allowed"),
    [
        (IncidentStatus.open, IncidentStatus.acknowledged, True),
        (IncidentStatus.open, IncidentStatus.resolved, True),
        (IncidentStatus.acknowledged, IncidentStatus.resolved, True),
        (IncidentStatus.resolved, IncidentStatus.acknowledged, True),
        (IncidentStatus.acknowledged, IncidentStatus.open, False),
        (IncidentStatus.resolved, IncidentStatus.open, False),
    ],
)
def test_status_transitions(gateway, current, target, allowed):
    assert (target in gateway.ALLOWED_STATUS_TRANSITIONS[current]) is allowed


# --------------------------------------------------------- incident queries


def test_incident_query_always_scopes_by_tenant(gateway):
    query = gateway.build_incident_query("team-a", None, None, None, None)
    assert query["bool"]["filter"] == [{"term": {"tenant_id": "team-a"}}]


def test_incident_query_composes_filters(gateway):
    query = gateway.build_incident_query(
        "team-a", "critical", "orders-service", "2026-08-10T00:00:00Z", None
    )
    filters = query["bool"]["filter"]
    assert {"term": {"severity": "critical"}} in filters
    assert {"term": {"service": "orders-service"}} in filters
    assert any("range" in f for f in filters)


# ----------------------------------------------------------- schema registry


def test_schema_registry_covers_every_validated_topic(gateway):
    from airs_shared.schema_registry import contract_model_map, topic_contracts

    contracts = {c.topic for c in topic_contracts(gateway.settings)}
    topics = gateway.settings.kafka.topics

    assert contracts == {
        topics.logs,
        topics.processed_logs,
        topics.anomalies,
        topics.incidents,
    }
    assert topics.dlq not in contracts, (
        "the DLQ must accept payloads that failed validation elsewhere"
    )
    assert set(contract_model_map(gateway.settings)) == contracts

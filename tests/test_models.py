from airs_shared.models import (
    ChatOpsConfigCreateRequest,
    DataSourceCreateRequest,
    DetectionRuleCreateRequest,
    NormalizedLogEvent,
    RCAFeedbackRequest,
    RCARegenerateRequest,
    RCAResult,
    ReplayFilterRequest,
    SimulateIngestRequest,
    SourceMethod,
    SuppressionCreateRequest,
    TopologyUpdateRequest,
)
from pydantic import ValidationError


def test_rca_result_schema() -> None:
    payload = {
        "root_cause": "Dependency timeout spike",
        "confidence": 0.82,
        "explanation": "Upstream dependency exceeded timeout budget.",
        "suggested_fix": "Scale dependency workers and increase connection pool.",
        "affected_services": ["orders-service"],
        "evidence": [
            {
                "timestamp": "2026-02-16T10:01:00Z",
                "message": "connection refused",
                "service": "orders-service",
            }
        ],
    }

    result = RCAResult.model_validate(payload)
    assert result.root_cause
    assert 0.0 <= result.confidence <= 1.0


def test_data_source_defaults() -> None:
    source = DataSourceCreateRequest(
        name="Checkout API",
        endpoint="https://example.internal/logs",
        default_service="checkout-service",
    )
    assert source.method == SourceMethod.get
    assert source.poll_interval_seconds == 30
    assert source.window_duration_minutes == 10
    assert source.min_signal_count == 2
    assert source.enabled is True


def test_data_source_endpoint_validation() -> None:
    try:
        DataSourceCreateRequest(
            name="Bad endpoint",
            endpoint="ftp://example.internal/logs",
            default_service="checkout-service",
        )
    except ValidationError:
        assert True
        return
    raise AssertionError("Expected endpoint validator to fail for non-http URLs")


def test_detection_rule_defaults() -> None:
    rule = DetectionRuleCreateRequest(
        name="Timeout Rule",
        pattern="timeout",
    )
    assert rule.match_type == "keyword"
    assert rule.service_pattern == "*"
    assert rule.enabled is True


def test_suppression_requires_valid_window() -> None:
    try:
        SuppressionCreateRequest(
            service_pattern="orders-*",
            reason="maintenance",
            starts_at="2026-02-16T10:00:00Z",
            ends_at="2026-02-16T09:59:59Z",
        )
    except ValidationError:
        assert True
        return
    raise AssertionError("Expected suppression validation to fail when ends_at <= starts_at")


def test_rca_feedback_request_validation() -> None:
    payload = RCAFeedbackRequest(rating="helpful", correction="Looks accurate")
    assert payload.rating == "helpful"
    assert payload.correction == "Looks accurate"


def test_rca_regenerate_request_normalization() -> None:
    payload = RCARegenerateRequest(notes="  rerun with new logs  ")
    assert payload.notes == "rerun with new logs"


def test_tenant_normalization_on_logs() -> None:
    event = NormalizedLogEvent(
        timestamp="2026-02-16T10:00:00Z",
        service="orders-service",
        level="error",
        message="timeout",
        tenant_id="  Team-A  ",
    )
    assert event.tenant_id == "team-a"


def test_replay_filter_window_validation() -> None:
    try:
        ReplayFilterRequest(
            from_time="2026-02-16T10:00:00Z",
            to_time="2026-02-16T09:59:59Z",
        )
    except ValidationError:
        assert True
        return
    raise AssertionError("Expected replay filter validation to fail when to_time <= from_time")


def test_simulation_request_tenant_normalization() -> None:
    payload = SimulateIngestRequest(
        service="payments",
        pattern="Synthetic timeout",
        tenant_id=" Tenant-X ",
    )
    assert payload.tenant_id == "tenant-x"


def test_topology_update_request() -> None:
    payload = TopologyUpdateRequest(
        edges=[{"upstream": "api", "downstream": "db", "dependency_type": "db"}]
    )
    assert len(payload.edges) == 1
    assert payload.edges[0].dependency_type.value == "db"


def test_chatops_config_url_validation() -> None:
    payload = ChatOpsConfigCreateRequest(provider="slack", webhook_url="https://hooks.example.com")
    assert payload.provider == "slack"

from datetime import UTC

from airs_shared.normalize import normalize_log


def test_normalize_string_log_defaults() -> None:
    event = normalize_log("service started")
    assert event.service == "unknown-service"
    assert event.level == "info"
    assert event.message == "service started"
    assert event.tenant_id == "default"


def test_normalize_dict_log() -> None:
    event = normalize_log(
        {
            "timestamp": "2026-02-16T10:00:00",
            "service": "api-gateway",
            "level": "ERROR",
            "message": "timeout",
            "trace_id": "abc123",
        }
    )
    assert event.service == "api-gateway"
    assert event.level == "error"
    assert event.tenant_id == "default"
    assert event.metadata["trace_id"] == "abc123"
    assert event.timestamp.tzinfo == UTC


def test_normalize_dict_log_with_tenant() -> None:
    event = normalize_log(
        {
            "timestamp": "2026-02-16T10:00:00Z",
            "service": "api-gateway",
            "level": "ERROR",
            "message": "timeout",
            "tenant_id": "Team-A",
        }
    )
    assert event.tenant_id == "team-a"


def test_normalize_is_idempotent_across_pipeline_hops() -> None:
    """Every stage re-normalizes, so normalization must not nest metadata.

    Regression: metadata was not excluded when collecting unknown keys, so
    each hop wrapped the previous metadata one level deeper. By the time an
    anomaly was emitted, trace_id sat three levels down.
    """
    raw = {
        "timestamp": "2026-08-10T10:00:00Z",
        "service": "orders-service",
        "level": "error",
        "message": "timeout",
        "trace_id": "abc123",
    }

    event = raw
    for _ in range(4):
        event = normalize_log(event).model_dump(mode="json")
        assert event["metadata"] == {"trace_id": "abc123"}


def test_normalize_merges_rather_than_overwrites_metadata() -> None:
    event = normalize_log({"message": "m", "metadata": {"pod": "orders-1"}, "trace_id": "abc"})
    assert event.metadata == {"pod": "orders-1", "trace_id": "abc"}


def test_normalize_preserves_non_dict_metadata() -> None:
    assert normalize_log({"message": "m", "metadata": "opaque"}).metadata == {"metadata": "opaque"}

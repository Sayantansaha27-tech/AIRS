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

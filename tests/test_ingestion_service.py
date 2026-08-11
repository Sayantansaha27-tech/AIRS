"""ingestion-service: input contract, happy path, DLQ path."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# ----------------------------------------------------------------- contract


async def test_accepts_object_and_publishes_normalized_envelope(ingestion, producer):
    accepted = await ingestion.ingest_one(
        {
            "timestamp": "2026-08-10T10:00:00Z",
            "service": "orders-service",
            "level": "ERROR",
            "message": "timeout while creating order",
            "trace_id": "abc123",
        }
    )

    assert accepted is True
    (topic, payload) = producer.messages[0]
    assert topic == ingestion.settings.kafka.topics.logs
    assert payload["service"] == "orders-service"
    assert payload["level"] == "error", "level must be lowercased"
    assert payload["tenant_id"] == "default"
    assert payload["metadata"]["trace_id"] == "abc123", "unknown keys go to metadata"


async def test_accepts_bare_string_as_unknown_service(ingestion, producer):
    assert await ingestion.ingest_one("plain text line") is True
    payload = producer.payloads_for(ingestion.settings.kafka.topics.logs)[0]
    assert payload["service"] == "unknown-service"
    assert payload["level"] == "info"
    assert payload["message"] == "plain text line"


async def test_default_service_applied_only_when_absent(ingestion, producer):
    await ingestion.ingest_one({"message": "no service"}, default_service="fallback-svc")
    await ingestion.ingest_one(
        {"service": "explicit-svc", "message": "has service"},
        default_service="fallback-svc",
    )
    services = [p["service"] for p in producer.payloads_for(ingestion.settings.kafka.topics.logs)]
    assert services == ["fallback-svc", "explicit-svc"]


async def test_tenant_id_is_normalized(ingestion, producer):
    await ingestion.ingest_one({"message": "m"}, tenant_id="  Team-A  ")
    payload = producer.payloads_for(ingestion.settings.kafka.topics.logs)[0]
    assert payload["tenant_id"] == "team-a"


# --------------------------------------------------------------- happy path


async def test_batch_publishes_one_message_per_event(ingestion, producer):
    batch = [{"service": "s", "message": f"line {i}"} for i in range(25)]
    for raw in batch:
        assert await ingestion.ingest_one(raw) is True

    assert producer.count_for(ingestion.settings.kafka.topics.logs) == 25
    assert producer.count_for(ingestion.settings.kafka.topics.dlq) == 0


async def test_no_producer_rejects_without_raising(ingestion):
    ingestion.producer = None
    assert await ingestion.ingest_one({"service": "s", "message": "m"}) is False


# ----------------------------------------------------------------- DLQ path


async def test_unparseable_timestamp_goes_to_dlq(ingestion, producer):
    accepted = await ingestion.ingest_one(
        {"timestamp": "NOT-A-TIMESTAMP", "service": "s", "message": "poison"}
    )

    assert accepted is False
    assert producer.count_for(ingestion.settings.kafka.topics.logs) == 0

    dlq = producer.payloads_for(ingestion.settings.kafka.topics.dlq)
    assert len(dlq) == 1
    assert dlq[0]["source_topic"] == ingestion.settings.kafka.topics.logs
    assert "NOT-A-TIMESTAMP" in dlq[0]["failure_reason"]
    assert dlq[0]["payload"]["message"] == "poison", "original payload is preserved"


async def test_poison_event_does_not_abandon_the_rest_of_the_batch(ingestion, producer):
    """The regression this service's DLQ path was added for.

    Previously normalize_log() raised through ingest_one(), and the broad
    except in poll_source() abandoned every remaining log in the batch.
    """
    batch = [
        {"timestamp": "2026-08-10T10:00:00Z", "service": "s", "message": "good 1"},
        {"timestamp": "NOT-A-TIMESTAMP", "service": "s", "message": "poison"},
        {"timestamp": "2026-08-10T10:00:02Z", "service": "s", "message": "good 2"},
    ]
    results = [await ingestion.ingest_one(raw, source="poller") for raw in batch]

    assert results == [True, False, True]
    assert producer.count_for(ingestion.settings.kafka.topics.logs) == 2
    assert producer.count_for(ingestion.settings.kafka.topics.dlq) == 1


async def test_oversized_payload_goes_to_dlq(ingestion, producer):
    limit = ingestion.settings.pipeline.max_log_payload_kb * 1024
    accepted = await ingestion.ingest_one({"service": "s", "message": "x" * (limit + 1)})

    assert accepted is False
    assert producer.count_for(ingestion.settings.kafka.topics.logs) == 0
    dlq = producer.payloads_for(ingestion.settings.kafka.topics.dlq)
    assert len(dlq) == 1
    assert "max_log_payload_kb" in dlq[0]["failure_reason"]


async def test_dlq_entry_records_the_ingest_channel(ingestion, producer):
    await ingestion.ingest_one({"timestamp": "bad", "message": "m"}, source="poller")
    dlq = producer.payloads_for(ingestion.settings.kafka.topics.dlq)[0]
    # Ingestion is the head of the pipeline, so there is no upstream offset.
    assert dlq["ingest_source"] == "poller"
    assert dlq["original_partition"] is None
    assert dlq["original_offset"] is None

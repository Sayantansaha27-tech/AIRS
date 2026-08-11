"""log-processor: input contract, happy path, DLQ path."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# ----------------------------------------------------------------- contract


async def test_forwards_the_same_envelope_it_consumed(log_processor, producer):
    """processed-logs-topic carries the identical schema as logs-topic.

    The stage exists to decouple indexing throughput from detection
    throughput, not to change shape.
    """
    event = {
        "timestamp": "2026-08-10T10:00:00Z",
        "service": "orders-service",
        "level": "error",
        "message": "timeout while creating order",
        "tenant_id": "default",
        "metadata": {"pod": "orders-7d9f"},
    }
    await log_processor.handle_message(event)

    topic, payload = producer.messages[0]
    assert topic == log_processor.settings.kafka.topics.processed_logs
    assert payload["service"] == event["service"]
    assert payload["message"] == event["message"]
    assert payload["metadata"]["pod"] == "orders-7d9f"


async def test_output_validates_against_the_topic_contract(log_processor, producer):
    from airs_shared.models import NormalizedLogEvent

    await log_processor.handle_message({"service": "s", "message": "m"})
    payload = producer.payloads_for(log_processor.settings.kafka.topics.processed_logs)[0]
    assert NormalizedLogEvent.model_validate(payload)


# --------------------------------------------------------------- happy path


async def test_indexes_before_forwarding(log_processor, producer, monkeypatch):
    indexed: list[tuple[str, str]] = []
    monkeypatch.setattr(
        log_processor,
        "upsert_doc",
        lambda client, index, doc_id, body: indexed.append((index, doc_id)),
    )

    await log_processor.handle_message({"service": "orders-service", "message": "m"})

    assert len(indexed) == 1
    assert indexed[0][0] == log_processor.settings.opensearch.logs_index
    assert producer.count_for(log_processor.settings.kafka.topics.processed_logs) == 1


async def test_document_id_is_stable_for_the_same_event(log_processor, monkeypatch):
    """Re-ingesting the same event must overwrite, not duplicate."""
    ids: list[str] = []
    monkeypatch.setattr(
        log_processor,
        "upsert_doc",
        lambda client, index, doc_id, body: ids.append(doc_id),
    )
    event = {
        "timestamp": "2026-08-10T10:00:00Z",
        "service": "orders-service",
        "message": "identical",
    }
    await log_processor.handle_message(dict(event))
    await log_processor.handle_message(dict(event))

    assert ids[0] == ids[1]


# ----------------------------------------------------------------- DLQ path


async def test_indexing_failure_routes_to_dlq(log_processor, producer, monkeypatch):
    def explode(*args, **kwargs):
        raise ConnectionError("opensearch cluster_block_exception")

    monkeypatch.setattr(log_processor, "upsert_doc", explode)

    payload = {"service": "orders-service", "message": "m"}
    with pytest.raises(ConnectionError):
        await log_processor.handle_message(payload)

    # consume_loop is what catches and dead-letters; exercise that contract.
    await log_processor.publish_to_dlq(
        payload=payload,
        error=ConnectionError("opensearch cluster_block_exception"),
        partition=0,
        offset=12345,
    )

    dlq = producer.payloads_for(log_processor.settings.kafka.topics.dlq)
    assert len(dlq) == 1
    assert dlq[0]["source_topic"] == log_processor.settings.kafka.topics.logs
    assert dlq[0]["original_partition"] == 0
    assert dlq[0]["original_offset"] == 12345
    assert "cluster_block_exception" in dlq[0]["failure_reason"]
    assert dlq[0]["payload"] == payload


async def test_dlq_publish_is_a_noop_without_a_producer(log_processor):
    log_processor.producer = None
    # Must not raise: the consumer loop calls this from an except block.
    await log_processor.publish_to_dlq(
        payload={"a": 1}, error=ValueError("x"), partition=0, offset=1
    )

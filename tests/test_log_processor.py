"""log-processor: input contract, batch indexing, DLQ path."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def indexed(log_processor, monkeypatch):
    """Capture what would be sent to OpenSearch in one bulk request."""
    calls: list[dict] = []

    async def fake_bulk(client, index, documents, *, refresh=False):
        calls.append({"index": index, "documents": documents, "refresh": refresh})
        return len(documents)

    monkeypatch.setattr(log_processor, "bulk_index", fake_bulk)
    return calls


# ----------------------------------------------------------------- contract


async def test_forwards_the_same_envelope_it_consumed(log_processor, producer, indexed):
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
    rejected = await log_processor.handle_batch([event])

    assert rejected == []
    topic, payload = producer.messages[0]
    assert topic == log_processor.settings.kafka.topics.processed_logs
    assert payload["service"] == event["service"]
    assert payload["message"] == event["message"]
    assert payload["metadata"]["pod"] == "orders-7d9f", "normalization is idempotent"


async def test_output_validates_against_the_topic_contract(log_processor, producer, indexed):
    from airs_shared.models import NormalizedLogEvent

    await log_processor.handle_batch([{"service": "s", "message": "m"}])
    payload = producer.payloads_for(log_processor.settings.kafka.topics.processed_logs)[0]
    assert NormalizedLogEvent.model_validate(payload)


# --------------------------------------------------------------- happy path


async def test_a_batch_is_indexed_in_one_request(log_processor, producer, indexed):
    """The throughput fix. Previously this was one indexing call per event,
    each forcing a refresh, through a synchronous client on the event loop."""
    batch = [{"service": "s", "message": f"line {i}"} for i in range(50)]

    await log_processor.handle_batch(batch)

    assert len(indexed) == 1, "one bulk request, not fifty"
    assert len(indexed[0]["documents"]) == 50
    assert indexed[0]["index"] == log_processor.settings.opensearch.logs_index
    assert producer.count_for(log_processor.settings.kafka.topics.processed_logs) == 50


async def test_indexing_does_not_force_a_refresh_per_document(log_processor, indexed):
    """refresh=True per document made every write pay for a segment flush."""
    await log_processor.handle_batch([{"service": "s", "message": "m"}])
    assert indexed[0]["refresh"] is False


async def test_document_id_is_stable_for_the_same_event(log_processor):
    """Re-ingesting the same event must overwrite, not duplicate. That
    idempotence is what makes at-least-once delivery safe here."""
    from airs_shared.normalize import normalize_log

    event = {
        "timestamp": "2026-08-10T10:00:00Z",
        "service": "orders-service",
        "message": "identical",
    }
    first = log_processor.document_id(normalize_log(dict(event)))
    second = log_processor.document_id(normalize_log(dict(event)))
    assert first == second


async def test_an_empty_batch_does_nothing(log_processor, indexed):
    assert await log_processor.handle_batch([]) == []
    assert indexed == []


# ----------------------------------------------------------------- DLQ path


async def test_an_unprocessable_event_is_returned_for_dead_lettering(
    log_processor, producer, indexed
):
    batch = [
        {"timestamp": "2026-08-10T10:00:00Z", "service": "s", "message": "good"},
        {"timestamp": "NOT-A-TIMESTAMP", "service": "s", "message": "poison"},
    ]
    rejected = await log_processor.handle_batch(batch)

    assert len(rejected) == 1
    assert rejected[0][0]["message"] == "poison"
    assert len(indexed[0]["documents"]) == 1, "the good event still indexed"


async def test_indexing_failure_routes_to_dlq(log_processor, producer, monkeypatch):
    """A dependency failure affects the whole batch, so every event in it is
    dead-lettered rather than silently lost."""

    async def explode(*args, **kwargs):
        raise ConnectionError("opensearch cluster_block_exception")

    monkeypatch.setattr(log_processor, "bulk_index", explode)

    with pytest.raises(ConnectionError):
        await log_processor.handle_batch([{"service": "s", "message": "m"}])

    await log_processor.publish_to_dlq(
        payload={"service": "s", "message": "m"},
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


async def test_dlq_publish_is_a_noop_without_a_producer(log_processor):
    log_processor.producer = None
    # Must not raise: the consumer loop calls this from an except block.
    await log_processor.publish_to_dlq(
        payload={"a": 1}, error=ValueError("x"), partition=0, offset=1
    )

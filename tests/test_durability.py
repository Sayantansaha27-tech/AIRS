"""At-least-once delivery, shared state, and replay safety.

These cover the one failure mode in AIRS that silently lost data: auto-commit
advanced offsets on a timer regardless of whether the handler had finished, so
a crash mid-batch resumed past records that were never processed. They did not
reach the DLQ either, because they never failed.

Manual commits fix that and move the problem: at-least-once means a crash
after processing but before committing replays the batch. That is only safe if
handlers are idempotent, which is what the second half of this file pins.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from airs_shared.consumer import build_consumer, commit_safely
from airs_shared.models import AnomalyEvent, Severity
from airs_shared.state import InMemoryStateStore, RedisStateStore

pytestmark = pytest.mark.anyio

BASE = datetime(2026, 8, 11, 10, 0, tzinfo=UTC)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# ------------------------------------------------------- consumer semantics


async def test_consumers_never_auto_commit():
    """The defect itself. Offsets must not advance on a timer."""
    consumer = build_consumer("some-topic", bootstrap_servers="localhost:9092", group_id="g")
    assert consumer._enable_auto_commit is False


async def test_new_groups_start_at_the_beginning_not_the_tail():
    """auto_offset_reset=latest meant a first deploy silently skipped every
    record already on the topic, which looked like an empty pipeline."""
    consumer = build_consumer("t", bootstrap_servers="localhost:9092", group_id="g")
    assert consumer._auto_offset_reset == "earliest"


async def test_a_slow_batch_is_not_mistaken_for_a_dead_consumer():
    """RCA can take a minute per incident when a model hangs. Being evicted
    mid-batch is what turns slowness into a rebalance storm."""
    consumer = build_consumer("t", bootstrap_servers="localhost:9092", group_id="g")
    assert consumer._max_poll_interval_ms >= 300_000


async def test_commit_failure_after_a_rebalance_does_not_crash_the_loop():
    """If the group rebalanced mid-batch this consumer no longer owns those
    partitions. The new owner reprocesses, which is what at-least-once
    promises, so the rejected commit is not worth crashing over."""

    class RebalancedConsumer:
        async def commit(self):
            raise RuntimeError("UnknownMemberIdError: consumer was evicted")

    await commit_safely(RebalancedConsumer(), where="test")


# --------------------------------------------------------------- state store


async def test_in_memory_store_round_trips():
    store = InMemoryStateStore()
    await store.set("k", {"a": 1})
    assert await store.get("k") == {"a": 1}
    await store.delete("k")
    assert await store.get("k") is None


async def test_in_memory_store_reports_a_missing_key_as_none():
    assert await InMemoryStateStore().get("nope") is None


async def test_seen_before_is_false_once_then_true():
    store = InMemoryStateStore()
    assert await store.seen_before("key", 60) is False
    assert await store.seen_before("key", 60) is True
    assert await store.seen_before("key", 60) is True


async def test_seen_before_distinguishes_keys():
    store = InMemoryStateStore()
    assert await store.seen_before("a", 60) is False
    assert await store.seen_before("b", 60) is False


async def test_in_memory_seen_set_is_bounded():
    """A long-running process must not grow without limit."""
    store = InMemoryStateStore()
    for i in range(100_100):
        await store.seen_before(f"k{i}", 60)
    assert len(store._seen) <= 100_000


async def test_redis_store_uses_set_nx_for_atomicity():
    """Two replicas asking the same question concurrently must not both be
    told the key is new, or both will open an incident for it."""
    calls: list[dict] = []

    class FakeRedis:
        async def set(self, key, value, ex=None, nx=None):
            calls.append({"key": key, "ex": ex, "nx": nx})
            # Redis returns True on first insert, None when NX finds it present.
            return True if len(calls) == 1 else None

    store = RedisStateStore(FakeRedis(), namespace="ns")
    assert await store.seen_before("k", 120) is False
    assert await store.seen_before("k", 120) is True

    assert calls[0]["nx"] is True, "must be SET NX, not GET then SET"
    assert calls[0]["ex"] == 120
    assert calls[0]["key"] == "ns:k", "keys are namespaced"


async def test_redis_store_survives_unparseable_values():
    class FakeRedis:
        async def get(self, key):
            return b"not json"

    assert await RedisStateStore(FakeRedis()).get("k") is None


async def test_build_state_store_falls_back_when_redis_is_absent():
    """A service must start without Redis. Shared state is better, local state
    is far better than refusing to run."""
    from airs_shared.state import build_state_store

    store = await build_state_store("redis://127.0.0.1:1/0")
    assert isinstance(store, InMemoryStateStore)


# ------------------------------------------------------------ replay safety


def anomaly(anomaly_id: str, fingerprint: str = "fp", offset: int = 0) -> AnomalyEvent:
    return AnomalyEvent(
        id=anomaly_id,
        timestamp=BASE + timedelta(seconds=offset),
        service="orders-service",
        severity=Severity.critical,
        anomaly_score=5.0,
        reasons=["keywords=oom"],
        log_message="OOM killer invoked",
        fingerprint=fingerprint,
    )


@pytest.fixture
def correlation_with_state(correlation):
    correlation.state = InMemoryStateStore()
    yield correlation
    correlation.state = None


async def test_a_replayed_anomaly_does_not_open_a_second_incident(
    correlation_with_state, producer, monkeypatch
):
    """The property that makes at-least-once safe here.

    A crash between processing and committing replays the batch. Without
    idempotency, the replay opens another incident for anomalies already
    grouped into the first.
    """
    corr = correlation_with_state
    monkeypatch.setattr(
        corr,
        "get_service_config",
        lambda t, s: corr.ServiceCorrelationConfig(
            window_duration_minutes=10, min_signal_count=2, fetched_at=datetime.now(UTC)
        ),
    )

    event = anomaly("anomaly-1")
    await corr.process_anomaly(event)
    incidents_after_first = producer.count_for("incidents-topic")

    # Same event id, exactly as a Kafka replay would deliver it.
    await corr.process_anomaly(event)

    assert producer.count_for("incidents-topic") == incidents_after_first
    assert len(corr.clusters) == 1, "the replay must not open a second cluster"


async def test_replayed_anomalies_are_counted_not_silently_dropped(
    correlation_with_state, monkeypatch
):
    corr = correlation_with_state
    monkeypatch.setattr(
        corr,
        "get_service_config",
        lambda t, s: corr.ServiceCorrelationConfig(
            window_duration_minutes=10, min_signal_count=2, fetched_at=datetime.now(UTC)
        ),
    )

    event = anomaly("anomaly-2")
    await corr.process_anomaly(event)
    assert await corr.already_processed(event) is True


async def test_distinct_anomalies_are_still_processed(
    correlation_with_state, producer, monkeypatch
):
    """Idempotency must not swallow genuinely new signal."""
    corr = correlation_with_state
    monkeypatch.setattr(
        corr,
        "get_service_config",
        lambda t, s: corr.ServiceCorrelationConfig(
            window_duration_minutes=10, min_signal_count=2, fetched_at=datetime.now(UTC)
        ),
    )

    await corr.process_anomaly(anomaly("a1", fingerprint="f1", offset=0))
    await corr.process_anomaly(anomaly("a2", fingerprint="f2", offset=1))

    cluster = next(iter(corr.clusters.values()))
    assert len(cluster.anomalies) == 2


async def test_idempotency_is_scoped_per_tenant(correlation_with_state):
    corr = correlation_with_state
    first = anomaly("shared-id")
    second = anomaly("shared-id")
    object.__setattr__(second, "tenant_id", "other-tenant")

    assert await corr.already_processed(first) is False
    assert await corr.already_processed(second) is False, "different tenant, different key"


async def test_without_a_state_store_nothing_is_deduplicated(correlation):
    """Degrades to previous behaviour rather than crashing."""
    correlation.state = None
    assert await correlation.already_processed(anomaly("x")) is False

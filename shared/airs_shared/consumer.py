"""Consumer construction with at-least-once delivery.

Every consumer previously ran `enable_auto_commit=True`. Offsets advanced on a
timer regardless of whether the handler for those records had finished, so a
process killed mid-batch resumed past records it never processed. Those records
did not reach the DLQ either, because they never failed: they were simply never
seen. It was the only silent data-loss path in AIRS.

Committing after a batch is handled converts that to at-least-once, which moves
the problem rather than removing it: a crash after processing but before
committing replays the batch. That is only safe if handlers are idempotent,
which is why this lands together with the durable idempotency keys in
`airs_shared.state`.

`auto_offset_reset` also changes from latest to earliest. A new consumer group
starting at the tail silently skipped every record already on the topic, which
made a first deploy against existing data look like an empty pipeline.
"""

from __future__ import annotations

import logging

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger("airs.consumer")


def build_consumer(
    *topics: str,
    bootstrap_servers: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
    max_poll_interval_ms: int = 300_000,
) -> AIOKafkaConsumer:
    """A consumer that commits only what it has actually handled."""
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        # A slow batch must not be mistaken for a dead consumer. RCA generation
        # can take a minute per incident when a model is unreachable, and being
        # evicted mid-batch is what turns slowness into a rebalance storm.
        max_poll_interval_ms=max_poll_interval_ms,
    )


async def commit_safely(consumer: AIOKafkaConsumer, *, where: str) -> None:
    """Commit, tolerating the rebalance that makes committing impossible.

    If the group rebalanced while the batch was in flight, this consumer no
    longer owns those partitions and the commit is rejected. That is not an
    error worth crashing over: the new owner will reprocess from the last
    committed offset, which is exactly what at-least-once promises.
    """
    try:
        await consumer.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Offset commit failed in %s (%s). Records will be reprocessed, "
            "which is safe because handlers are idempotent.",
            where,
            exc,
        )

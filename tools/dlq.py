#!/usr/bin/env python3
"""Inspect and drain the dead-letter queue.

The DLQ was write-only. Events landed there with their source topic, partition,
offset and failure reason, and nothing consumed them: no drain, no replay, no
way to answer "what is in there" without a raw console consumer. That gap is
recorded in 01-scope-and-non-goals.md.

Run it inside the Docker network. Kafka advertises itself as `kafka:9092`, so a
host shell can reach the bootstrap port but cannot then fetch from the broker:

    docker compose exec ai-service python /app/tools/dlq.py peek
    docker compose exec ai-service python /app/tools/dlq.py peek --limit 200 --verbose
    docker compose exec ai-service python /app/tools/dlq.py drain --dry-run
    docker compose exec ai-service python /app/tools/dlq.py drain

Outside compose, point it somewhere reachable with `--bootstrap`.

Draining republishes the original payload to the topic it failed on, so the
pipeline reprocesses it. Whether that succeeds depends on whether you fixed
the cause: a malformed event will fail again and return to the DLQ. Fix first,
then drain.

Peeking never commits an offset and joins no consumer group, so it is
repeatable and cannot advance any service's position.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "shared"))

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: E402
from aiokafka.errors import (  # noqa: E402
    KafkaConnectionError,
    UnknownTopicOrPartitionError,
)
from airs_shared.settings import get_settings  # noqa: E402

settings = get_settings()

# config/airs.yaml names Kafka by its Docker network hostname, which does not
# resolve from the host where an operator actually runs this. Overridable.
DEFAULT_BOOTSTRAP = settings.kafka.bootstrap_servers


async def read_dlq(limit: int, bootstrap: str, timeout_ms: int = 5000) -> list[dict[str, Any]]:
    """Read from the beginning without committing, so peeking is repeatable."""
    consumer = AIOKafkaConsumer(
        settings.kafka.topics.dlq,
        bootstrap_servers=bootstrap,
        group_id=None,  # no group, so this never advances anyone's offsets
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    try:
        await consumer.start()
    except UnknownTopicOrPartitionError:
        # The topic is created on first publish, so its absence means nothing
        # has ever been dead-lettered. That is good news, not an error.
        await consumer.stop()
        return []
    except KafkaConnectionError as exc:
        await consumer.stop()
        raise SystemExit(
            f"Cannot reach Kafka at {bootstrap}: {exc}\n"
            "Running from the host? Pass --bootstrap localhost:9092"
        ) from exc

    try:
        events: list[dict[str, Any]] = []
        while len(events) < limit:
            batch = await consumer.getmany(timeout_ms=timeout_ms, max_records=limit)
            if not batch:
                break
            for records in batch.values():
                for message in records:
                    try:
                        events.append(json.loads(message.value.decode("utf-8")))
                    except json.JSONDecodeError:
                        continue
                    if len(events) >= limit:
                        break
        return events
    finally:
        await consumer.stop()


def summarize(events: list[dict[str, Any]]) -> None:
    if not events:
        print("DLQ is empty.")
        return

    by_topic = Counter(e.get("source_topic", "unknown") for e in events)
    # Group by the first line of the reason: the rest is usually a stack or a
    # value that differs per event and would fragment the count.
    by_reason = Counter(str(e.get("failure_reason", "unknown")).split("\n")[0][:90] for e in events)

    print(f"{len(events)} event(s) in the DLQ\n")
    print("By source topic")
    for topic, count in by_topic.most_common():
        print(f"  {count:>6}  {topic}")

    print("\nBy failure reason")
    for reason, count in by_reason.most_common(10):
        print(f"  {count:>6}  {reason}")


async def drain(limit: int, dry_run: bool, only_topic: str | None, bootstrap: str) -> int:
    events = await read_dlq(limit, bootstrap)
    candidates = [
        e
        for e in events
        if (only_topic is None or e.get("source_topic") == only_topic)
        and e.get("payload") is not None
    ]

    skipped = len(events) - len(candidates)
    if skipped:
        print(f"Skipping {skipped} event(s): no payload, or filtered out by --topic")

    if not candidates:
        print("Nothing to replay.")
        return 0

    if dry_run:
        print(f"DRY RUN. Would replay {len(candidates)} event(s):")
        for topic, count in Counter(e["source_topic"] for e in candidates).most_common():
            print(f"  {count:>6}  -> {topic}")
        print("\nRe-run without --dry-run to replay.")
        return 0

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    replayed = 0
    try:
        for event in candidates:
            topic = event["source_topic"]
            # Deliberately not validated against the topic contract. These
            # events failed once, and the operator has decided to try again.
            await producer.send_and_wait(
                topic, json.dumps(event["payload"], default=str).encode("utf-8")
            )
            replayed += 1
    finally:
        await producer.stop()

    print(f"Replayed {replayed} event(s).")
    print(
        "If the cause is not fixed they will fail again and return to the DLQ. "
        "Check airs_dlq_published_total before and after."
    )
    return replayed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    peek = sub.add_parser("peek", help="summarise what is in the DLQ")
    peek.add_argument("--limit", type=int, default=1000)
    peek.add_argument("--verbose", action="store_true", help="print each event")

    drain_cmd = sub.add_parser("drain", help="replay events to their source topic")
    drain_cmd.add_argument("--limit", type=int, default=1000)
    drain_cmd.add_argument("--topic", help="only replay events from this source topic")
    drain_cmd.add_argument("--dry-run", action="store_true")

    for sub_parser in (peek, drain_cmd):
        sub_parser.add_argument(
            "--bootstrap",
            default=DEFAULT_BOOTSTRAP,
            help=f"Kafka bootstrap servers (default from config: {DEFAULT_BOOTSTRAP}). "
            "Use localhost:9092 when running from the host.",
        )

    args = parser.parse_args()

    if args.command == "peek":
        events = asyncio.run(read_dlq(args.limit, args.bootstrap))
        summarize(events)
        if args.verbose:
            print()
            for event in events:
                print(json.dumps(event, indent=2, default=str))
        return 0

    asyncio.run(drain(args.limit, args.dry_run, args.topic, args.bootstrap))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

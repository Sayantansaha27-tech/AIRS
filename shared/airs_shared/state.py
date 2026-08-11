"""Shared state, so a service can lose its process without losing its memory.

anomaly-service and correlation-service both held per-service state in a
process dict. That cost three things at once, documented in ADR-005 and
09-postmortem.md: neither service could run two replicas, an unclean exit lost
open correlation clusters, and the deduplication fingerprint could not become
a durable idempotency key, which is what blocked moving Kafka off auto-commit.

This is the backing store that unblocks all three. It degrades to an in-memory
implementation when Redis is unavailable, so a laptop with no Redis still
works and behaves exactly as it did before.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

logger = logging.getLogger("airs.state")


class StateStore(Protocol):
    """Small deliberately: only what the pipeline actually needs."""

    async def get(self, key: str) -> dict[str, Any] | None: ...

    async def set(
        self, key: str, value: dict[str, Any], ttl_seconds: int | None = None
    ) -> None: ...

    async def delete(self, key: str) -> None: ...

    async def keys(self, prefix: str) -> list[str]: ...

    async def seen_before(self, key: str, ttl_seconds: int) -> bool:
        """True if this key has been seen. Records it either way.

        The idempotency primitive. Must be atomic, because two replicas asking
        the same question at the same time must not both get "no".
        """
        ...

    async def aclose(self) -> None: ...


class InMemoryStateStore:
    """Fallback when Redis is not configured or not reachable.

    Behaviourally identical for a single replica. It just does not survive a
    restart and is not shared, which is exactly the limitation this module
    exists to remove, so anything relying on it should say so out loud.
    """

    def __init__(self) -> None:
        self._data: dict[str, dict[str, Any]] = {}
        self._seen: set[str] = set()

    async def get(self, key: str) -> dict[str, Any] | None:
        return self._data.get(key)

    async def set(self, key: str, value: dict[str, Any], ttl_seconds: int | None = None) -> None:
        self._data[key] = value

    async def delete(self, key: str) -> None:
        self._data.pop(key, None)

    async def keys(self, prefix: str) -> list[str]:
        return [k for k in self._data if k.startswith(prefix)]

    async def seen_before(self, key: str, ttl_seconds: int) -> bool:
        if key in self._seen:
            return True
        self._seen.add(key)
        # Bound the set so a long-running process cannot grow without limit.
        if len(self._seen) > 100_000:
            self._seen = set(list(self._seen)[-50_000:])
        return False

    async def aclose(self) -> None:
        return None


class RedisStateStore:
    """Redis-backed, shared across replicas and surviving restarts."""

    def __init__(self, client: Any, namespace: str = "airs") -> None:
        self._redis = client
        self._ns = namespace

    def _key(self, key: str) -> str:
        return f"{self._ns}:{key}"

    async def get(self, key: str) -> dict[str, Any] | None:
        raw = await self._redis.get(self._key(key))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

    async def set(self, key: str, value: dict[str, Any], ttl_seconds: int | None = None) -> None:
        payload = json.dumps(value, default=str)
        if ttl_seconds:
            await self._redis.set(self._key(key), payload, ex=ttl_seconds)
        else:
            await self._redis.set(self._key(key), payload)

    async def delete(self, key: str) -> None:
        await self._redis.delete(self._key(key))

    async def keys(self, prefix: str) -> list[str]:
        pattern = f"{self._key(prefix)}*"
        found: list[str] = []
        cursor = 0
        while True:
            cursor, batch = await self._redis.scan(cursor=cursor, match=pattern, count=500)
            found.extend(k.decode("utf-8") if isinstance(k, bytes) else k for k in batch)
            if cursor == 0:
                break
        trim = len(self._ns) + 1
        return [k[trim:] for k in found]

    async def seen_before(self, key: str, ttl_seconds: int) -> bool:
        """SET NX is atomic, which is the whole point.

        Two replicas processing the same anomaly concurrently must not both be
        told it is new, or both will open an incident for it.
        """
        created = await self._redis.set(self._key(key), "1", ex=ttl_seconds, nx=True)
        return not bool(created)

    async def aclose(self) -> None:
        return None


async def build_state_store(redis_url: str, namespace: str = "airs") -> StateStore:
    """Redis if it answers, memory if it does not.

    Falling back rather than failing is deliberate: state that is shared is
    better, but state that is local is far better than a service that will not
    start. The log line is loud because the difference matters operationally.
    """
    try:
        from redis import asyncio as redis_async

        client = redis_async.from_url(redis_url)
        await client.ping()
        logger.info("State store: Redis at %s (shared, survives restart)", redis_url)
        return RedisStateStore(client, namespace=namespace)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "State store: Redis unavailable (%s). Falling back to in-process memory. "
            "State will NOT survive restart and MUST NOT be relied on with more "
            "than one replica.",
            exc,
        )
        return InMemoryStateStore()

"""Sink interface: where RCA output goes.

RCA results are written to OpenSearch today. The planned ServiceNow
integration adds a second destination with entirely different properties: a
work note on a ticket in someone else's system, over a network, with auth,
rate limits, and no undo.

This module defines the contract so storage, a webhook and a ticket work note
are three implementations of one interface, and ai-service does not learn that
ServiceNow exists. See ADR-007 in docs/03-decisions.md.

Nothing here is wired into the running services yet.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger("airs.sinks")


@dataclass(frozen=True)
class SinkPayload:
    """An RCA result addressed to a destination.

    `idempotency_key` is the part that matters. Writing a work note twice is a
    visible defect in someone else's system, and outbound retries make double
    writes likely rather than theoretical. Derived from stable incident
    identity, so a retry of the same result reuses the same key.
    """

    incident_id: str
    tenant_id: str
    service: str
    severity: str
    rca: dict[str, Any]
    external_id: str | None = None
    """Identity in the destination system, carried through from the source."""
    source_system: str | None = None
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_incident(cls, incident: Any) -> SinkPayload:
        """Build a payload from an enriched Incident.

        This is the join between the two seams: whatever a source recorded as
        the originating identity travels on the incident and arrives here, so a
        sink can address the right record without knowing where it came from.
        """
        if incident.rca is None:
            raise ValueError("cannot deliver an incident that has no RCA")
        return cls(
            incident_id=incident.id,
            tenant_id=incident.tenant_id,
            service=incident.service,
            severity=str(getattr(incident.severity, "value", incident.severity)),
            rca=incident.rca.model_dump(mode="json"),
            external_id=incident.external_id,
            source_system=incident.source_system,
            occurred_at=incident.updated_at,
        )

    @property
    def idempotency_key(self) -> str:
        """Stable across retries of the same RCA for the same incident.

        Includes the root cause, so a regenerated RCA that reached a different
        conclusion is a genuinely new write rather than a suppressed duplicate.
        """
        root_cause = str(self.rca.get("root_cause", ""))
        material = f"{self.tenant_id}:{self.incident_id}:{root_cause}"
        return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


@dataclass
class SinkResult:
    delivered: bool
    sink: str
    detail: str | None = None
    attempts: int = 1
    skipped_duplicate: bool = False


class Sink(ABC):
    """A destination for RCA output.

    Implementations must not raise on delivery failure. A sink that cannot
    deliver returns an unsuccessful `SinkResult`, because one unreachable
    destination must never stop the others or fail the incident. Raising is
    reserved for programmer error.
    """

    name: str = "sink"

    @abstractmethod
    async def deliver(self, payload: SinkPayload) -> SinkResult:
        """Write the RCA to this destination."""

    async def health(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None


class OutboundAdapter:
    """Retry, backoff and idempotency for calls leaving the process.

    Wraps any outbound operation so that every third-party integration gets the
    same behaviour rather than each reimplementing it slightly differently.

    Idempotency is enforced here rather than in each sink, because the failure
    it prevents (a retry succeeding after a response was lost, producing two
    work notes) is a property of retrying, not of any particular destination.

    The seen-key set is in-process, which bounds this to a single replica. That
    is the same constraint correlation state has, and it moves to Redis at the
    same time. Recorded rather than hidden: see docs/05-failure-modes.md.
    """

    def __init__(
        self,
        *,
        max_attempts: int = 3,
        base_delay_seconds: float = 1.0,
        max_delay_seconds: float = 30.0,
        remember_keys: int = 10_000,
    ) -> None:
        self.max_attempts = max_attempts
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.remember_keys = remember_keys
        self._delivered: dict[str, datetime] = {}

    def already_delivered(self, key: str) -> bool:
        return key in self._delivered

    def record(self, key: str) -> None:
        if len(self._delivered) >= self.remember_keys:
            # Drop the oldest half rather than growing without bound.
            oldest = sorted(self._delivered.items(), key=lambda kv: kv[1])
            for stale, _ in oldest[: self.remember_keys // 2]:
                self._delivered.pop(stale, None)
        self._delivered[key] = datetime.now(UTC)

    async def send(self, operation, *, idempotency_key: str, description: str) -> SinkResult:
        """Run `operation` with retry and backoff, at most once per key.

        `operation` is an awaitable callable returning a truthy value on
        success. It is expected to raise on failure, which is what triggers a
        retry.
        """
        if self.already_delivered(idempotency_key):
            logger.debug("Skipping duplicate outbound call: %s", description)
            return SinkResult(
                delivered=True,
                sink=description,
                detail="already delivered, suppressed by idempotency key",
                attempts=0,
                skipped_duplicate=True,
            )

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                await operation()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt < self.max_attempts:
                    delay = min(
                        self.base_delay_seconds * (2 ** (attempt - 1)),
                        self.max_delay_seconds,
                    )
                    await asyncio.sleep(delay)
                continue
            self.record(idempotency_key)
            return SinkResult(delivered=True, sink=description, attempts=attempt)

        logger.warning("Outbound call failed after %d attempts: %s", self.max_attempts, description)
        return SinkResult(
            delivered=False,
            sink=description,
            detail=str(last_error)[:500] if last_error else "unknown error",
            attempts=self.max_attempts,
        )


async def fan_out(sinks: list[Sink], payload: SinkPayload) -> list[SinkResult]:
    """Deliver to every sink, letting each fail independently.

    Gathered with return_exceptions so one unreachable destination cannot deny
    the others, which is the same isolation property the pipeline stages have.
    """
    outcomes = await asyncio.gather(
        *(sink.deliver(payload) for sink in sinks),
        return_exceptions=True,
    )

    results: list[SinkResult] = []
    for sink, outcome in zip(sinks, outcomes, strict=True):
        if isinstance(outcome, BaseException):
            # A sink is contracted not to raise, so this is a bug in the sink.
            logger.exception("Sink %s raised instead of returning a result", sink.name)
            results.append(SinkResult(delivered=False, sink=sink.name, detail=str(outcome)[:500]))
        else:
            results.append(outcome)
    return results

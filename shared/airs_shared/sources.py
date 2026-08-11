"""Source interface: where work enters the pipeline.

AIRS currently has one shape of input, a log event arriving on `logs-topic`.
The planned ServiceNow integration breaks that assumption: a ServiceNow
incident is not a log line, it is already an incident.

This module defines the contract both satisfy, so a new input is a new
`Source` implementation rather than a new pipeline. See ADR-007 in
docs/03-decisions.md.

Nothing here is wired into the running services yet. It exists so the
ServiceNow work lands without surgery on core services, and it should be
deleted rather than left as evidence of a plan if that work does not happen.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class SourceKind(StrEnum):
    """What a source yields, which decides where it enters the pipeline."""

    logs = "logs"
    """Raw log events. Enter at logs-topic and traverse every stage."""

    incidents = "incidents"
    """Already-formed incidents from an external system. Enter at
    incidents-topic, skipping detection and correlation, because grouping was
    someone else's decision and re-deriving it would be wrong."""


@dataclass(frozen=True)
class SourceRecord:
    """One item pulled from a source, before normalization.

    `payload` is deliberately untyped. Normalization is the pipeline's job and
    a source should not have opinions about the envelope; its responsibility is
    to fetch, to say what kind of thing it fetched, and to carry enough
    identity for the sink to write back later.
    """

    payload: dict[str, Any]
    kind: SourceKind = SourceKind.logs
    tenant_id: str = "default"
    external_id: str | None = None
    """Identity in the originating system, for example a ServiceNow sys_id.
    Carried through so a sink can write a result back to the right record."""
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceCursor:
    """Where a source got to, so the next poll does not re-fetch everything.

    Persisted by the caller rather than the source, so a source stays
    stateless and testable. `token` is opaque: a timestamp, an offset, a
    ServiceNow sys_updated_on watermark, whatever that source needs.
    """

    token: str | None = None
    last_polled_at: datetime | None = None


class Source(ABC):
    """A place work comes from.

    Implementations must be safe to call repeatedly. `fetch` is expected to be
    driven by a scheduler that owns the cursor and the polling interval, so a
    source does not sleep, retry on a schedule, or manage its own lifecycle
    beyond `aclose`.
    """

    #: Stable identifier, used in metrics and log lines.
    name: str = "source"

    #: What this source yields. Decides the entry point into the pipeline.
    kind: SourceKind = SourceKind.logs

    @abstractmethod
    async def fetch(self, cursor: SourceCursor) -> tuple[list[SourceRecord], SourceCursor]:
        """Pull the next batch and return it with an advanced cursor.

        Must return an empty list rather than raising when there is simply
        nothing new. Raising is reserved for a source that is actually broken,
        because the caller treats an exception as a failed poll.
        """

    async def health(self) -> bool:
        """Whether the source is reachable. Default assumes it is."""
        return True

    async def aclose(self) -> None:
        """Release connections. Called on shutdown."""
        return None

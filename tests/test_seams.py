"""Source and sink seams: the contract the ServiceNow work will land on.

These interfaces are not wired into the running services. The tests pin the
behaviour the seams promise, so the ServiceNow implementation has something to
satisfy rather than something to interpret. See ADR-007 in docs/03-decisions.md.
"""

from __future__ import annotations

import pytest
from airs_shared.sinks import OutboundAdapter, Sink, SinkPayload, SinkResult, fan_out
from airs_shared.sources import Source, SourceCursor, SourceKind, SourceRecord

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def payload(incident_id: str = "inc-1", root_cause: str = "pool exhausted") -> SinkPayload:
    return SinkPayload(
        incident_id=incident_id,
        tenant_id="default",
        service="orders-service",
        severity="critical",
        rca={"root_cause": root_cause, "confidence": 0.8},
        external_id="INC0012345",
    )


# ----------------------------------------------------------------- sources


class StubSource(Source):
    name = "stub"

    def __init__(self, kind: SourceKind = SourceKind.logs) -> None:
        self.kind = kind
        self.calls: list[str | None] = []

    async def fetch(self, cursor: SourceCursor):
        self.calls.append(cursor.token)
        records = [
            SourceRecord(
                payload={"message": "m"},
                kind=self.kind,
                external_id="INC0012345",
            )
        ]
        return records, SourceCursor(token="next-token")


async def test_source_returns_records_and_advances_its_cursor():
    source = StubSource()
    records, cursor = await source.fetch(SourceCursor())

    assert len(records) == 1
    assert cursor.token == "next-token", "the caller persists this, not the source"
    assert await source.health() is True


async def test_source_kind_decides_the_pipeline_entry_point():
    """A ServiceNow incident is already an incident, so re-deriving grouping
    for it would be wrong. The kind is what carries that distinction."""
    assert StubSource(SourceKind.logs).kind == SourceKind.logs
    assert StubSource(SourceKind.incidents).kind == SourceKind.incidents


async def test_source_record_carries_external_identity():
    """Without this a sink cannot write a result back to the right record."""
    records, _ = await StubSource().fetch(SourceCursor())
    assert records[0].external_id == "INC0012345"
    assert records[0].tenant_id == "default"
    assert records[0].fetched_at is not None


# ------------------------------------------------------------- idempotency


def test_idempotency_key_is_stable_for_the_same_rca():
    assert payload().idempotency_key == payload().idempotency_key


def test_idempotency_key_changes_when_the_conclusion_changes():
    """A regenerated RCA reaching a different conclusion is a real new write,
    not a duplicate to suppress."""
    assert payload(root_cause="pool exhausted").idempotency_key != (
        payload(root_cause="disk full").idempotency_key
    )


def test_idempotency_key_is_scoped_per_incident():
    assert payload("inc-1").idempotency_key != payload("inc-2").idempotency_key


# ---------------------------------------------------------- outbound adapter


async def test_retries_then_succeeds():
    attempts = {"n": 0}

    async def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError("connection reset")

    adapter = OutboundAdapter(base_delay_seconds=0)
    result = await adapter.send(flaky, idempotency_key="k1", description="servicenow")

    assert result.delivered is True
    assert result.attempts == 3


async def test_gives_up_after_max_attempts_without_raising():
    async def always_fails():
        raise TimeoutError("upstream timed out")

    adapter = OutboundAdapter(max_attempts=3, base_delay_seconds=0)
    result = await adapter.send(always_fails, idempotency_key="k2", description="servicenow")

    assert result.delivered is False
    assert result.attempts == 3
    assert "timed out" in result.detail


async def test_a_duplicate_key_is_not_sent_twice():
    """The failure this prevents: a retry succeeding after the response was
    lost, producing two work notes on someone else's ticket."""
    calls = {"n": 0}

    async def operation():
        calls["n"] += 1

    adapter = OutboundAdapter(base_delay_seconds=0)
    first = await adapter.send(operation, idempotency_key="same", description="servicenow")
    second = await adapter.send(operation, idempotency_key="same", description="servicenow")

    assert calls["n"] == 1, "the second call must not reach the destination"
    assert first.delivered and second.delivered
    assert second.skipped_duplicate is True
    assert first.skipped_duplicate is False


async def test_a_failed_call_is_not_marked_delivered():
    """A failure must remain retryable rather than being suppressed later."""
    calls = {"n": 0}

    async def fails_once_then_works():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("nope")

    adapter = OutboundAdapter(max_attempts=1, base_delay_seconds=0)
    first = await adapter.send(fails_once_then_works, idempotency_key="k3", description="sn")
    assert first.delivered is False

    second = await adapter.send(fails_once_then_works, idempotency_key="k3", description="sn")
    assert second.delivered is True
    assert second.skipped_duplicate is False


def test_remembered_keys_are_bounded():
    adapter = OutboundAdapter(remember_keys=10)
    for i in range(25):
        adapter.record(f"key-{i}")
    assert len(adapter._delivered) <= 10, "must not grow without bound"


# -------------------------------------------------------------------- sinks


class RecordingSink(Sink):
    def __init__(self, name: str, *, fail: bool = False, raises: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.raises = raises
        self.received: list[SinkPayload] = []

    async def deliver(self, p: SinkPayload) -> SinkResult:
        if self.raises:
            raise RuntimeError("sink is buggy")
        self.received.append(p)
        return SinkResult(delivered=not self.fail, sink=self.name)


async def test_fan_out_delivers_to_every_sink():
    sinks = [RecordingSink("storage"), RecordingSink("webhook")]
    results = await fan_out(sinks, payload())

    assert all(r.delivered for r in results)
    assert all(len(s.received) == 1 for s in sinks)


async def test_one_failing_sink_does_not_deny_the_others():
    sinks = [RecordingSink("storage"), RecordingSink("ticket", fail=True)]
    results = await fan_out(sinks, payload())

    by_name = {r.sink: r for r in results}
    assert by_name["storage"].delivered is True
    assert by_name["ticket"].delivered is False


async def test_a_sink_that_raises_is_contained():
    """Sinks are contracted not to raise. If one does, it is a bug in that
    sink and must not take the others down with it."""
    sinks = [RecordingSink("storage"), RecordingSink("buggy", raises=True)]
    results = await fan_out(sinks, payload())

    by_name = {r.sink: r for r in results}
    assert by_name["storage"].delivered is True
    assert by_name["buggy"].delivered is False
    assert "buggy" in by_name["buggy"].detail


async def test_core_services_do_not_import_the_seams():
    """The constraint ADR-007 exists to protect: no ServiceNow-specific
    assumption, and no sink coupling, inside the pipeline services."""
    from pathlib import Path

    repo = Path(__file__).resolve().parents[1]
    for service in ("ai-service", "correlation-service", "anomaly-service"):
        source = (repo / "services" / service / "app" / "main.py").read_text()
        assert "servicenow" not in source.lower(), f"{service} knows about ServiceNow"


# ------------------------------------------------- external incident identity


def test_incident_carries_external_identity():
    """The join between the two seams.

    SourceRecord and SinkPayload both have external_id, but the only thing
    travelling between them on Kafka is the Incident. Without this the ticket
    id is lost mid-pipeline and the sink has nothing to address.
    """
    from airs_shared.models import Incident, Severity

    incident = Incident(
        severity=Severity.critical,
        service="orders-service",
        summary="pulled from ServiceNow",
        source_system="servicenow",
        external_id="a1b2c3d4e5f6",
    )
    assert incident.is_external is True

    round_tripped = Incident.model_validate(incident.model_dump(mode="json"))
    assert round_tripped.external_id == "a1b2c3d4e5f6"
    assert round_tripped.source_system == "servicenow"


def test_pipeline_incidents_are_not_external():
    from airs_shared.models import Incident, Severity

    incident = Incident(severity=Severity.warning, service="s", summary="x")
    assert incident.is_external is False
    assert incident.external_id is None


def test_provenance_fields_are_optional_and_backward_compatible():
    """Existing incident documents have neither field and must still load."""
    from airs_shared.models import Incident

    legacy = {
        "id": "inc-1",
        "severity": "warning",
        "service": "orders-service",
        "summary": "written before provenance existed",
    }
    assert Incident.model_validate(legacy).external_id is None


def test_sink_payload_is_built_from_an_enriched_incident():
    from airs_shared.models import Incident, RCAResult, Severity

    incident = Incident(
        severity=Severity.critical,
        service="orders-service",
        summary="s",
        source_system="servicenow",
        external_id="a1b2c3d4e5f6",
        rca=RCAResult(
            root_cause="pool exhausted",
            confidence=0.8,
            explanation="e",
            suggested_fix="f",
        ),
    )

    payload = SinkPayload.from_incident(incident)
    assert payload.external_id == "a1b2c3d4e5f6"
    assert payload.source_system == "servicenow"
    assert payload.severity == "critical"
    assert payload.rca["root_cause"] == "pool exhausted"


def test_an_incident_without_rca_cannot_be_delivered():
    from airs_shared.models import Incident, Severity

    incident = Incident(severity=Severity.warning, service="s", summary="x")
    with pytest.raises(ValueError, match="no RCA"):
        SinkPayload.from_incident(incident)

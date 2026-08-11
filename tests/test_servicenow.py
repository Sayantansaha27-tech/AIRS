"""ServiceNow source and sink, against a fake instance.

No real ServiceNow is contacted. httpx is driven by a MockTransport that
returns canned Table API responses, so these run in CI and on a laptop with no
credentials.
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest
from airs_shared.models import Incident, RCAResult, Severity
from airs_shared.sinks import OutboundAdapter, SinkPayload
from airs_shared.sources import SourceCursor, SourceKind

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services/connector-service/app"))

from servicenow import (  # noqa: E402
    ServiceNowSource,
    ServiceNowWorkNoteSink,
    derive_service,
    derive_severity,
    format_work_note,
    to_incident,
)

pytestmark = pytest.mark.anyio

INSTANCE = "https://dev386810.service-now.com"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def sn_row(**overrides) -> dict:
    """A ServiceNow incident as the Table API returns it with display values."""
    row = {
        "sys_id": {"value": "a1b2c3d4e5f6", "display_value": "a1b2c3d4e5f6"},
        "number": {"value": "INC0010042", "display_value": "INC0010042"},
        "short_description": {
            "value": "Orders API returning 500s",
            "display_value": "Orders API returning 500s",
        },
        "description": {
            "value": "Customers cannot complete checkout.",
            "display_value": "Customers cannot complete checkout.",
        },
        "priority": {"value": "1", "display_value": "1 - Critical"},
        "opened_at": {"value": "2026-08-11 02:14:03", "display_value": "2026-08-11 02:14:03"},
        "sys_updated_on": {
            "value": "2026-08-11 02:20:00",
            "display_value": "2026-08-11 02:20:00",
        },
        "sys_updated_by": {"value": "jsmith", "display_value": "jsmith"},
        "cmdb_ci": {"value": "abc123", "display_value": "orders-service"},
        "assignment_group": {"value": "def456", "display_value": "Platform"},
        "category": {"value": "software", "display_value": "Software"},
    }
    row.update(overrides)
    return row


def transport(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ------------------------------------------------------------------- mapping


def test_maps_a_servicenow_incident_onto_the_pipeline_contract():
    incident = to_incident(sn_row())

    assert isinstance(incident, Incident)
    assert incident.external_id == "a1b2c3d4e5f6"
    assert incident.source_system == "servicenow"
    assert incident.is_external is True
    assert incident.severity == Severity.critical
    assert incident.service == "orders-service"
    assert "INC0010042" in incident.summary
    assert len(incident.timeline) == 2, "short_description plus description"


@pytest.mark.parametrize(
    ("priority", "expected"),
    [
        ("1 - Critical", Severity.critical),
        ("2 - High", Severity.critical),
        ("3 - Moderate", Severity.warning),
        ("4 - Low", Severity.info),
        ("5 - Planning", Severity.info),
        ("", Severity.warning),
        ("nonsense", Severity.warning),
    ],
)
def test_priority_maps_to_severity(priority, expected):
    row = sn_row(priority={"value": priority[:1], "display_value": priority})
    assert derive_severity(row) is expected


def test_service_prefers_the_configuration_item():
    assert derive_service(sn_row()) == "orders-service"


def test_service_falls_back_through_group_then_category():
    no_ci = sn_row(cmdb_ci={"value": "", "display_value": ""})
    assert derive_service(no_ci) == "Platform"

    no_group = sn_row(
        cmdb_ci={"value": "", "display_value": ""},
        assignment_group={"value": "", "display_value": ""},
    )
    assert derive_service(no_group) == "Software"


def test_service_never_returns_empty():
    bare = {"sys_id": {"value": "x"}, "number": {"value": "INC1"}}
    assert derive_service(bare) == "unknown-service"


def test_mapping_survives_a_sparse_record():
    """PDIs and real instances both return partial rows. Must not raise."""
    incident = to_incident({"sys_id": {"value": "x"}, "number": {"value": "INC1"}})
    assert incident.external_id == "x"
    assert incident.summary.startswith("INC1")


# ---------------------------------------------------------------- loop guard


def test_query_excludes_our_own_updates():
    """The loop guard. Without it AIRS re-reads its own work notes forever."""
    source = ServiceNowSource(instance_url=INSTANCE, username="airs_integration", password="x")
    query = source.build_query(SourceCursor())
    assert "sys_updated_by!=airs_integration" in query


def test_query_advances_with_the_cursor():
    source = ServiceNowSource(instance_url=INSTANCE, username="airs_integration", password="x")
    query = source.build_query(SourceCursor(token="2026-08-11 02:20:00"))
    assert "sys_updated_on>2026-08-11 02:20:00" in query
    assert "sys_updated_by!=airs_integration" in query


async def test_a_write_by_airs_is_never_re_ingested():
    """End to end on the guard: the fake instance honours sys_updated_by."""
    seen_queries: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = request.url.params.get("sysparm_query", "")
        seen_queries.append(query)
        rows = [
            sn_row(),
            sn_row(
                sys_id={"value": "written-by-airs"},
                sys_updated_by={"value": "airs_integration"},
            ),
        ]
        # Mimic the instance applying the exclusion clause.
        if "sys_updated_by!=airs_integration" in query:
            rows = [r for r in rows if r["sys_updated_by"]["value"] != "airs_integration"]
        return httpx.Response(200, json={"result": rows})

    source = ServiceNowSource(
        instance_url=INSTANCE,
        username="airs_integration",
        password="x",
        client=transport(handler),
    )
    records, _ = await source.fetch(SourceCursor())

    ids = [r.external_id for r in records]
    assert "written-by-airs" not in ids
    assert ids == ["a1b2c3d4e5f6"]


# -------------------------------------------------------------------- fetch


async def test_fetch_returns_records_and_advances_the_watermark():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": [sn_row()]})

    source = ServiceNowSource(
        instance_url=INSTANCE, username="u", password="p", client=transport(handler)
    )
    records, cursor = await source.fetch(SourceCursor())

    assert len(records) == 1
    assert records[0].kind is SourceKind.incidents, "enters at incidents-topic"
    assert records[0].external_id == "a1b2c3d4e5f6"
    assert cursor.token == "2026-08-11 02:20:00"


async def test_watermark_takes_the_latest_row():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "result": [
                    sn_row(sys_updated_on={"value": "2026-08-11 02:20:00"}),
                    sn_row(
                        sys_id={"value": "later"},
                        sys_updated_on={"value": "2026-08-11 09:00:00"},
                    ),
                ]
            },
        )

    source = ServiceNowSource(
        instance_url=INSTANCE, username="u", password="p", client=transport(handler)
    )
    _, cursor = await source.fetch(SourceCursor())
    assert cursor.token == "2026-08-11 09:00:00"


async def test_empty_result_is_not_an_error():
    """Nothing new must return empty, not raise. The caller treats an
    exception as a failed poll."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": []})

    source = ServiceNowSource(
        instance_url=INSTANCE, username="u", password="p", client=transport(handler)
    )
    records, cursor = await source.fetch(SourceCursor(token="2026-08-11 02:20:00"))
    assert records == []
    assert cursor.token == "2026-08-11 02:20:00", "watermark held"


async def test_rows_without_a_sys_id_are_skipped():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": [{"number": {"value": "INC1"}}]})

    source = ServiceNowSource(
        instance_url=INSTANCE, username="u", password="p", client=transport(handler)
    )
    records, _ = await source.fetch(SourceCursor())
    assert records == []


async def test_auth_failure_raises_so_the_caller_records_a_failed_poll():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": {"message": "User is not authenticated"}})

    source = ServiceNowSource(
        instance_url=INSTANCE, username="u", password="wrong", client=transport(handler)
    )
    with pytest.raises(httpx.HTTPStatusError):
        await source.fetch(SourceCursor())

    assert await source.health() is False


# --------------------------------------------------------------------- sink


def enriched_payload(**overrides) -> SinkPayload:
    incident = Incident(
        severity=Severity.critical,
        service="orders-service",
        summary="INC0010042: Orders API returning 500s",
        source_system=overrides.pop("source_system", "servicenow"),
        external_id=overrides.pop("external_id", "a1b2c3d4e5f6"),
        rca=RCAResult(
            root_cause="Database connection pool exhaustion",
            confidence=0.78,
            explanation="Pool saturation preceded the downstream timeouts.",
            suggested_fix="Raise max_pool_size",
            affected_services=["orders-service", "postgres-primary"],
            evidence=[
                {
                    "timestamp": "2026-08-11T02:14:03Z",
                    "service": "orders-service",
                    "message": "connection not available",
                }
            ],
        ),
    )
    return SinkPayload.from_incident(incident)


def test_work_note_is_readable_by_a_human():
    note = format_work_note(enriched_payload())
    assert "AIRS automated root cause analysis" in note
    assert "Database connection pool exhaustion" in note
    assert "Confidence: 0.78" in note
    assert "Raise max_pool_size" in note
    assert "orders-service, postgres-primary" in note
    assert "connection not available" in note


async def test_dry_run_writes_nothing():
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    sink = ServiceNowWorkNoteSink(
        instance_url=INSTANCE,
        username="u",
        password="p",
        dry_run=True,
        client=transport(handler),
    )
    result = await sink.deliver(enriched_payload())

    assert result.delivered is True
    assert calls == [], "dry run must not touch the instance"
    assert "dry run" in result.detail


async def test_live_write_patches_the_originating_ticket():
    seen: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json as _json

        seen.append((str(request.url), _json.loads(request.content)))
        return httpx.Response(200, json={"result": {}})

    sink = ServiceNowWorkNoteSink(
        instance_url=INSTANCE,
        username="u",
        password="p",
        dry_run=False,
        client=transport(handler),
    )
    result = await sink.deliver(enriched_payload())

    assert result.delivered is True
    url, body = seen[0]
    assert url.endswith("/api/now/table/incident/a1b2c3d4e5f6")
    assert "work_notes" in body
    assert "Database connection pool exhaustion" in body["work_notes"]


async def test_pipeline_originated_incidents_are_skipped():
    """An incident AIRS detected itself has no ticket to update."""
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    sink = ServiceNowWorkNoteSink(
        instance_url=INSTANCE,
        username="u",
        password="p",
        dry_run=False,
        client=transport(handler),
    )
    result = await sink.deliver(enriched_payload(source_system=None, external_id=None))

    assert result.delivered is True
    assert calls == []
    assert "did not originate in ServiceNow" in result.detail


async def test_a_retry_never_double_posts_a_work_note():
    """The failure OutboundAdapter exists to prevent: a lost response causing
    two work notes on a real person's ticket."""
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={})

    sink = ServiceNowWorkNoteSink(
        instance_url=INSTANCE,
        username="u",
        password="p",
        dry_run=False,
        adapter=OutboundAdapter(base_delay_seconds=0),
        client=transport(handler),
    )
    payload = enriched_payload()
    first = await sink.deliver(payload)
    second = await sink.deliver(payload)

    assert len(calls) == 1, "the same RCA must not be posted twice"
    assert first.delivered and second.delivered
    assert second.skipped_duplicate is True


async def test_servicenow_being_down_does_not_raise():
    """Sinks are contracted not to raise: an unreachable ticketing system must
    never fail the incident."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("instance hibernating")

    sink = ServiceNowWorkNoteSink(
        instance_url=INSTANCE,
        username="u",
        password="p",
        dry_run=False,
        adapter=OutboundAdapter(max_attempts=2, base_delay_seconds=0),
        client=transport(handler),
    )
    result = await sink.deliver(enriched_payload())

    assert result.delivered is False
    assert result.attempts == 2
    assert await sink.health() is False

"""connector-service: routing, delivery, and the ADR-007 boundary."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from airs_shared.models import Incident, RCAResult, Severity
from airs_shared.sinks import Sink, SinkPayload, SinkResult
from airs_shared.sources import Source, SourceCursor, SourceKind, SourceRecord

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "services/connector-service/app"))

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def connector(producer, monkeypatch):
    """Load connector-service with Kafka stubbed."""
    from tests.conftest import _reset_prometheus_registry

    _reset_prometheus_registry()
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "airs_service_connector", REPO / "services/connector-service/app/main.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["airs_service_connector"] = module
    spec.loader.exec_module(module)

    module.producer = producer
    module.sources.clear()
    module.sinks.clear()
    module.cursors.clear()
    yield module
    module.producer = None
    module.sources.clear()
    module.sinks.clear()
    sys.modules.pop("airs_service_connector", None)


def enriched_incident(**overrides) -> Incident:
    return Incident(
        severity=Severity.critical,
        service="orders-service",
        summary="INC0010042: Orders API returning 500s",
        source_system=overrides.pop("source_system", "servicenow"),
        external_id=overrides.pop("external_id", "a1b2c3d4e5f6"),
        rca=overrides.pop(
            "rca",
            RCAResult(
                root_cause="pool exhausted",
                confidence=0.8,
                explanation="e",
                suggested_fix="f",
            ),
        ),
    )


# ------------------------------------------------------------------- routing


def test_incident_sources_bypass_detection_and_correlation(connector):
    """The reason connector-service exists rather than extending ingestion.

    A ServiceNow incident is already grouped. Sending it to logs-topic would
    re-run detection and correlation and produce a second, competing opinion
    about the same event.
    """
    assert connector.topic_for(SourceKind.incidents) == "incidents-topic"
    assert connector.topic_for(SourceKind.logs) == "logs-topic"


class StubSource(Source):
    name = "stub"
    kind = SourceKind.incidents

    def __init__(self, payloads: list[dict], token: str = "t1") -> None:
        self.payloads = payloads
        self.token = token
        self.seen_cursors: list[str | None] = []

    async def fetch(self, cursor: SourceCursor):
        self.seen_cursors.append(cursor.token)
        records = [
            SourceRecord(payload=p, kind=SourceKind.incidents, external_id=p.get("external_id"))
            for p in self.payloads
        ]
        return records, SourceCursor(token=self.token)


async def test_poll_publishes_incidents_to_the_incidents_topic(connector, producer):
    incident = enriched_incident(rca=None)
    source = StubSource([incident.model_dump(mode="json")])

    published = await connector.poll_once(source)

    assert published == 1
    assert producer.count_for("incidents-topic") == 1
    payload = producer.payloads_for("incidents-topic")[0]
    assert payload["external_id"] == "a1b2c3d4e5f6"
    assert payload["source_system"] == "servicenow"


async def test_cursor_advances_between_polls(connector):
    source = StubSource([], token="2026-08-11 09:00:00")
    await connector.poll_once(source)
    await connector.poll_once(source)

    assert source.seen_cursors == [None, "2026-08-11 09:00:00"]


async def test_a_malformed_record_is_dead_lettered_not_dropped(connector, producer):
    """A mapping bug in a connector must be visible, not silent."""
    source = StubSource([{"not": "an incident"}])

    published = await connector.poll_once(source)

    assert published == 0
    assert producer.count_for("incidents-topic") == 0
    dlq = producer.payloads_for("airs-dlq-topic")
    assert len(dlq) == 1
    assert dlq[0]["connector"] == "stub"


async def test_one_bad_record_does_not_abandon_the_batch(connector, producer):
    good = enriched_incident(rca=None).model_dump(mode="json")
    source = StubSource([good, {"broken": True}, good])

    published = await connector.poll_once(source)

    assert published == 2
    assert producer.count_for("airs-dlq-topic") == 1


# ------------------------------------------------------------------ delivery


class RecordingSink(Sink):
    def __init__(self, name="stub-sink", delivered=True):
        self.name = name
        self.delivered = delivered
        self.received: list[SinkPayload] = []

    async def deliver(self, payload: SinkPayload) -> SinkResult:
        self.received.append(payload)
        return SinkResult(delivered=self.delivered, sink=self.name)


async def test_enriched_incident_reaches_the_sinks(connector):
    sink = RecordingSink()
    connector.sinks.append(sink)

    await connector.deliver_enriched(enriched_incident().model_dump(mode="json"))

    assert len(sink.received) == 1
    assert sink.received[0].external_id == "a1b2c3d4e5f6"
    assert sink.received[0].rca["root_cause"] == "pool exhausted"


async def test_an_incident_without_rca_is_not_delivered(connector):
    sink = RecordingSink()
    connector.sinks.append(sink)

    await connector.deliver_enriched(enriched_incident(rca=None).model_dump(mode="json"))

    assert sink.received == []


async def test_a_failing_sink_does_not_stop_the_others(connector):
    ok = RecordingSink("good")
    bad = RecordingSink("bad", delivered=False)
    connector.sinks.extend([ok, bad])

    await connector.deliver_enriched(enriched_incident().model_dump(mode="json"))

    assert len(ok.received) == 1, "a broken destination must not deny a working one"


async def test_delivery_with_no_sinks_configured_is_a_noop(connector):
    await connector.deliver_enriched(enriched_incident().model_dump(mode="json"))


# ---------------------------------------------------------------- build/config


def test_servicenow_is_not_built_when_disabled(connector, monkeypatch):
    monkeypatch.setattr(connector.settings.connectors.servicenow, "enabled", False)
    assert connector.build_sources() == {}
    assert connector.build_sinks() == []


def test_servicenow_is_not_built_without_a_password(connector, monkeypatch):
    """Enabled but unconfigured must fail closed and say so, not crash."""
    cfg = connector.settings.connectors.servicenow
    monkeypatch.setattr(cfg, "enabled", True)
    monkeypatch.setattr(cfg, "instance_url", "https://dev386810.service-now.com")
    monkeypatch.delenv(cfg.password_env, raising=False)

    assert connector.build_sources() == {}
    assert connector.build_sinks() == []


def test_servicenow_is_built_when_fully_configured(connector, monkeypatch):
    cfg = connector.settings.connectors.servicenow
    monkeypatch.setattr(cfg, "enabled", True)
    monkeypatch.setattr(cfg, "instance_url", "https://dev386810.service-now.com")
    monkeypatch.setenv(cfg.password_env, "secret")

    built = connector.build_sources()
    assert "servicenow" in built
    source, interval = built["servicenow"]
    assert source.kind is SourceKind.incidents
    assert interval == cfg.poll_interval_seconds

    sinks = connector.build_sinks()
    assert len(sinks) == 1
    assert sinks[0].dry_run is True, "must default to not writing to real tickets"


def test_dry_run_is_the_default_in_config():
    """A work note is visible to real people. Writing must be chosen."""
    from airs_shared.settings import ServiceNowSettings

    assert ServiceNowSettings().dry_run is True
    assert ServiceNowSettings().enabled is False


def test_password_is_never_stored_in_config():
    """The config file is committed. It names an env var, it does not hold a
    credential."""
    from airs_shared.settings import ServiceNowSettings

    fields = set(ServiceNowSettings.model_fields)
    assert "password" not in fields
    assert "password_env" in fields

    committed = (REPO / "config/airs.yaml").read_text()
    assert "password_env" in committed
    assert "SERVICENOW_PASSWORD" in committed


# ----------------------------------------------------------- ADR-007 boundary


def test_no_core_service_knows_servicenow_exists():
    """The constraint the whole seam exists to protect."""
    for service in (
        "ai-service",
        "correlation-service",
        "anomaly-service",
        "api-gateway",
        "ingestion-service",
        "log-processor",
    ):
        source = (REPO / "services" / service / "app" / "main.py").read_text().lower()
        assert "servicenow" not in source, f"{service} mentions ServiceNow"


def test_ai_service_publishes_rather_than_delivering(ai):
    """ai-service must not call a third party to hand off an RCA.

    It announces on Kafka and connector-service delivers, so a slow or
    unreachable ticketing system cannot slow RCA generation. ai-service does
    make HTTP calls, to the model provider, which is why this inspects the
    handoff function specifically rather than the whole module.
    """
    import inspect

    body = inspect.getsource(ai.announce_enriched)
    assert "produce_json" in body, "handoff is a publish"
    assert "enriched_incidents" in body
    assert "httpx" not in body, "handoff must not call a third party directly"

    # The real coupling test: ai-service must not import the sink seam at all.
    module_source = (REPO / "services/ai-service/app/main.py").read_text()
    assert "airs_shared.sinks" not in module_source
    assert not hasattr(ai, "fan_out")


def test_enriched_topic_is_separate_from_incidents_topic():
    """Publishing a delivery must not re-enter the RCA stage and loop."""
    from airs_shared.settings import TopicSettings

    topics = TopicSettings()
    assert topics.enriched_incidents != topics.incidents

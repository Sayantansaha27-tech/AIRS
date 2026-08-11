"""ai-service: RCA contract, severity routing, fallback cascade, DLQ path."""

from __future__ import annotations

import pytest
from airs_shared.models import Incident, RCAResult, Severity

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def incident(severity: Severity = Severity.critical) -> Incident:
    return Incident(
        severity=severity,
        service="orders-service",
        summary="orders-service incident from 2 correlated anomalies",
        anomaly_ids=["a1", "a2"],
        timeline=[
            {
                "timestamp": "2026-08-10T10:00:03Z",
                "severity": "warning",
                "message": "timeout while creating order",
                "reasons": ["zscore=3.1"],
            },
            {
                "timestamp": "2026-08-10T10:00:05Z",
                "severity": "critical",
                "message": "connection refused: postgres-primary:5432",
                "reasons": ["keywords=connection refused"],
            },
        ],
    )


class StubProvider:
    def __init__(self, payload=None, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.calls: list[str] = []

    async def generate(self, prompt: str, context: dict) -> dict:
        self.calls.append(context.get("model", ""))
        if self.error is not None:
            raise self.error
        return dict(self.payload)


VALID_LLM_PAYLOAD = {
    "root_cause": "Database connection pool exhaustion",
    "confidence": 0.8,
    "explanation": "Pool saturation preceded the downstream timeouts.",
    "suggested_fix": "Raise max_pool_size",
    "affected_services": ["orders-service", "postgres-primary"],
    "evidence": [],
}


# ------------------------------------------------------------------ routing


@pytest.mark.parametrize(
    ("severity", "expected_model", "expected_use_llm"),
    [
        ("critical", "qwen2.5:7b-instruct", True),
        ("warning", "qwen2.5:1.5b-instruct", True),
        ("info", "deterministic", False),
    ],
)
def test_severity_routing_tiers_differ(ai, severity, expected_model, expected_use_llm):
    active = ai.RuntimeLLMConfig(provider="ollama", model="qwen2.5:7b-instruct")
    model, use_llm = ai.select_model_for_context({"severity": severity}, active)
    assert model == expected_model
    assert use_llm is expected_use_llm


def test_warning_tier_follows_the_active_provider_after_a_switch(ai):
    """Regression: the warning tier read the static fallback_model, so after a
    runtime switch to OpenAI it asked OpenAI for an Ollama model name."""
    active = ai.RuntimeLLMConfig(provider="openai", model="gpt-4.1-mini")
    model, use_llm = ai.select_model_for_context({"severity": "warning"}, active)
    assert model == "gpt-4.1-mini"
    assert use_llm is True


def test_info_stays_deterministic_on_any_provider(ai):
    for provider, model in (("ollama", "qwen2.5:7b-instruct"), ("openai", "gpt-4.1-mini")):
        active = ai.RuntimeLLMConfig(provider=provider, model=model)
        assert ai.select_model_for_context({"severity": "info"}, active) == (
            "deterministic",
            False,
        )


def test_force_model_overrides_routing(ai):
    active = ai.RuntimeLLMConfig(provider="ollama", model="qwen2.5:7b-instruct")
    assert ai.select_model_for_context(
        {"severity": "info", "force_model": "llama3:70b"}, active
    ) == ("llama3:70b", True)


# ----------------------------------------------------------------- contract


async def test_llm_result_validates_against_rca_contract(ai, monkeypatch):
    provider = StubProvider(payload=VALID_LLM_PAYLOAD)
    monkeypatch.setattr(ai, "build_provider", lambda a, c: provider)

    rca, model = await ai.generate_rca(ai.build_incident_context(incident()))

    assert isinstance(rca, RCAResult)
    assert model == "qwen2.5:7b-instruct"
    assert rca.root_cause == "Database connection pool exhaustion"


async def test_confidence_is_blended_not_passed_through(ai, monkeypatch):
    monkeypatch.setattr(ai, "build_provider", lambda a, c: StubProvider(payload=VALID_LLM_PAYLOAD))
    context = ai.build_incident_context(incident())
    rca, _ = await ai.generate_rca(context)

    heuristic = ai.heuristic_confidence(context)
    assert rca.confidence == pytest.approx(round((0.8 + heuristic) / 2, 2))
    assert rca.confidence != 0.8, "model self-assessment is not trusted alone"


async def test_missing_evidence_is_backfilled_from_context(ai, monkeypatch):
    payload = dict(VALID_LLM_PAYLOAD, evidence=[])
    monkeypatch.setattr(ai, "build_provider", lambda a, c: StubProvider(payload=payload))

    rca, _ = await ai.generate_rca(ai.build_incident_context(incident()))
    assert rca.evidence, "an RCA without evidence cannot be checked by a human"


def test_context_carries_what_the_model_needs(ai):
    context = ai.build_incident_context(incident())
    for key in ("incident_id", "service", "severity", "timeline", "logs", "top_logs"):
        assert key in context
    assert len(context["top_logs"]) <= 15


# -------------------------------------------------------- fallback cascade


async def test_falls_back_to_the_low_cost_tier_then_deterministic(ai, monkeypatch):
    provider = StubProvider(error=ConnectionError("connection refused"))
    monkeypatch.setattr(ai, "build_provider", lambda a, c: provider)
    monkeypatch.setattr(ai.asyncio, "sleep", _no_sleep)

    rca, model = await ai.generate_rca(ai.build_incident_context(incident()))

    assert model == "deterministic"
    assert isinstance(rca, RCAResult)
    retries = ai.settings.llm.retries + 1
    assert len(provider.calls) == retries * 2, "primary tier then low-cost tier"
    assert provider.calls[0] == "qwen2.5:7b-instruct"
    assert provider.calls[-1] == "qwen2.5:1.5b-instruct"


async def test_malformed_model_json_falls_through_to_deterministic(ai, monkeypatch):
    # Providers wrap undecodable output as {"raw": ...}, which fails RCAResult.
    monkeypatch.setattr(
        ai, "build_provider", lambda a, c: StubProvider(payload={"raw": "I think..."})
    )
    monkeypatch.setattr(ai.asyncio, "sleep", _no_sleep)

    rca, model = await ai.generate_rca(ai.build_incident_context(incident()))
    assert model == "deterministic"
    assert isinstance(rca, RCAResult)


async def test_unbuildable_provider_falls_back_rather_than_raising(ai, monkeypatch):
    """Regression: build_provider() sat outside the guard, so a misconfigured
    provider raised and the incident was dead-lettered with no RCA."""

    def _explode(active, cfg):
        raise RuntimeError("Missing environment variable: OPENAI_API_KEY")

    monkeypatch.setattr(ai, "build_provider", _explode)
    ai.runtime_llm_config = ai.RuntimeLLMConfig(provider="openai", model="gpt-4.1-mini")

    for severity in (Severity.critical, Severity.warning, Severity.info):
        rca, model = await ai.generate_rca(ai.build_incident_context(incident(severity)))
        assert model == "deterministic"
        assert isinstance(rca, RCAResult)


async def test_info_severity_never_calls_a_model(ai, monkeypatch):
    provider = StubProvider(payload=VALID_LLM_PAYLOAD)
    monkeypatch.setattr(ai, "build_provider", lambda a, c: provider)

    rca, model = await ai.generate_rca(ai.build_incident_context(incident(Severity.info)))
    assert model == "deterministic"
    assert provider.calls == []


def test_deterministic_fallback_is_schema_valid_with_no_model(ai):
    rca = ai.deterministic_fallback(
        {
            "logs": [
                {
                    "timestamp": "2026-08-10T10:00:00Z",
                    "service": "orders-service",
                    "message": "connection refused",
                }
            ],
            "anomalies": [{"id": "a1"}],
        }
    )
    assert isinstance(rca, RCAResult)
    assert rca.affected_services == ["orders-service"]
    assert "fallback" in rca.explanation.lower(), "must say it is a fallback"


def test_deterministic_fallback_handles_an_empty_context(ai):
    rca = ai.deterministic_fallback({"logs": [], "anomalies": []})
    assert isinstance(rca, RCAResult)
    assert rca.affected_services == ["unknown-service"]


# --------------------------------------------------------------- happy path


async def test_process_incident_attaches_rca_and_records_the_path(ai, monkeypatch):
    stored: list[dict] = []
    monkeypatch.setattr(ai, "upsert_doc", lambda c, i, d, body: stored.append(body))
    monkeypatch.setattr(ai, "build_provider", lambda a, c: StubProvider(payload=VALID_LLM_PAYLOAD))

    await ai.process_incident(incident().model_dump(mode="json"))

    assert len(stored) == 1
    assert stored[0]["rca"]["root_cause"] == "Database connection pool exhaustion"
    assert stored[0]["updated_at"] is not None


# ----------------------------------------------------------------- DLQ path


async def test_undecodable_incident_routes_to_dlq(ai, producer):
    await ai.publish_to_dlq(
        payload={"id": "x", "service": "s"},
        error=ValueError("Field required: severity"),
        partition=0,
        offset=42,
    )

    dlq = producer.payloads_for(ai.settings.kafka.topics.dlq)
    assert len(dlq) == 1
    assert dlq[0]["source_topic"] == ai.settings.kafka.topics.incidents
    assert dlq[0]["original_offset"] == 42


async def test_a_model_outage_never_dead_letters_an_incident(ai, monkeypatch):
    """The load-bearing claim: RCA generation degrades, it does not fail."""
    monkeypatch.setattr(
        ai,
        "build_provider",
        lambda a, c: StubProvider(error=TimeoutError("model timed out")),
    )
    monkeypatch.setattr(ai.asyncio, "sleep", _no_sleep)
    stored: list[dict] = []
    monkeypatch.setattr(ai, "upsert_doc", lambda c, i, d, body: stored.append(body))

    await ai.process_incident(incident().model_dump(mode="json"))

    assert stored[0]["rca"] is not None


async def _no_sleep(seconds: float) -> None:
    """Collapse retry backoff so the cascade tests stay fast."""
    return None


# ------------------------------------------------------------ feedback loop


def test_operator_corrections_reach_the_context(ai, monkeypatch):
    """Closes the loop. Ratings were previously written and read by nothing,
    so "feedback loop" described collection rather than a loop."""
    monkeypatch.setattr(
        ai,
        "fetch_operator_corrections",
        lambda t, s: [
            {
                "previously_concluded": "network partition",
                "operator_said": "It was the connection pool, not the network.",
                "rating": "incorrect",
            }
        ],
    )

    context = ai.build_incident_context(incident())
    assert context["operator_corrections"][0]["operator_said"].startswith("It was the")


def test_corrections_are_visible_to_the_model(ai, monkeypatch):
    from rca import build_prompt

    monkeypatch.setattr(
        ai,
        "fetch_operator_corrections",
        lambda t, s: [{"operator_said": "connection pool exhaustion", "rating": "incorrect"}],
    )
    prompt = build_prompt(ai.build_incident_context(incident()))
    assert "connection pool exhaustion" in prompt
    assert "operator_corrections" in prompt


def test_the_prompt_tells_the_model_to_weight_corrections():
    from rca import PROMPT_TEMPLATE

    assert "operator_corrections" in PROMPT_TEMPLATE
    assert "weigh that above" in PROMPT_TEMPLATE.lower()


def test_only_corrected_feedback_is_queried(ai, monkeypatch):
    """A bare 'unhelpful' with no correction tells a model nothing
    actionable, so it is not worth prompt budget."""
    captured: list[dict] = []

    class FakeClient:
        def search(self, index, body):
            captured.append(body)
            return {"hits": {"hits": []}}

    monkeypatch.setattr(ai, "os_client", FakeClient())
    ai.fetch_operator_corrections("default", "orders-service")

    filters = str(captured[0]["query"]["bool"]["filter"])
    assert "correction" in filters
    assert "incorrect" in filters


def test_missing_feedback_index_is_not_an_error(ai, monkeypatch):
    class BrokenClient:
        def search(self, index, body):
            raise RuntimeError("index_not_found_exception")

    monkeypatch.setattr(ai, "os_client", BrokenClient())
    assert ai.fetch_operator_corrections("default", "s") == []

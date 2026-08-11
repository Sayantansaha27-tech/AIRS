"""Provider registry, adapters, and JSON coercion.

The point of this design is that adding a model endpoint is configuration, not
code. These tests pin that: an OpenAI-compatible server nobody anticipated is
reachable through config alone, and switching provider moves every routing
tier together rather than leaving one pointing at a model the new provider has
never heard of.
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest
from airs_shared.settings import DETERMINISTIC, LLMSettings, ProviderSettings

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services/ai-service/app"))

from ai_providers import (  # noqa: E402
    AnthropicProvider,
    OllamaProvider,
    OpenAICompatibleProvider,
    ProviderConfigError,
    build,
    coerce_json,
    register,
    supported_kinds,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def transport(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ------------------------------------------------------------ JSON coercion


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ('{"root_cause": "x"}', {"root_cause": "x"}),
        ('```json\n{"root_cause": "x"}\n```', {"root_cause": "x"}),
        ('```\n{"root_cause": "x"}\n```', {"root_cause": "x"}),
        ('Here is the analysis:\n{"root_cause": "x"}', {"root_cause": "x"}),
        ({"root_cause": "x"}, {"root_cause": "x"}),
    ],
)
def test_coerce_json_recovers_badly_packaged_json(raw, expected):
    """Small models wrap JSON in prose and fences even when told not to.
    Discarding a correct answer over packaging would be wasteful."""
    assert coerce_json(raw) == expected


@pytest.mark.parametrize("raw", ["I think the database is down.", "", None, 42])
def test_coerce_json_never_invents_a_result(raw):
    """Unparseable output must fail validation upstream, not be guessed at."""
    result = coerce_json(raw)
    assert "raw" in result
    assert "root_cause" not in result


# ---------------------------------------------------------------- registry


def test_built_in_kinds_are_registered():
    assert set(supported_kinds()) >= {"ollama", "openai_compatible", "anthropic"}


def test_unknown_kind_is_a_config_error_naming_what_is_known():
    cfg = ProviderSettings(kind="telepathy", base_url="http://x")
    with pytest.raises(ProviderConfigError, match="Known kinds"):
        build(provider_name="p", cfg=cfg, model="m", timeout_seconds=5)


def test_missing_base_url_is_a_config_error():
    with pytest.raises(ProviderConfigError, match="base_url"):
        build(
            provider_name="p",
            cfg=ProviderSettings(kind="ollama"),
            model="m",
            timeout_seconds=5,
        )


def test_missing_api_key_is_a_config_error_naming_the_variable(monkeypatch):
    monkeypatch.delenv("SOME_KEY", raising=False)
    cfg = ProviderSettings(kind="anthropic", base_url="http://x", api_key_env="SOME_KEY")
    with pytest.raises(ProviderConfigError, match="SOME_KEY"):
        build(provider_name="anthropic", cfg=cfg, model="m", timeout_seconds=5)


def test_a_local_server_needs_no_api_key():
    """vLLM and LM Studio usually run unauthenticated."""
    cfg = ProviderSettings(kind="openai_compatible", base_url="http://localhost:8000/v1")
    provider = build(provider_name="local", cfg=cfg, model="m", timeout_seconds=5)
    assert isinstance(provider, OpenAICompatibleProvider)
    assert provider.api_key is None


def test_a_new_api_shape_can_be_registered_without_touching_core():
    class Custom(OpenAICompatibleProvider):
        kind = "custom"

    register(
        "custom",
        lambda cfg, name, model, timeout: Custom(
            base_url=cfg.base_url, model=model, timeout_seconds=timeout
        ),
    )
    built = build(
        provider_name="x",
        cfg=ProviderSettings(kind="custom", base_url="http://x"),
        model="m",
        timeout_seconds=5,
    )
    assert isinstance(built, Custom)


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("ollama", OllamaProvider),
        ("openai_compatible", OpenAICompatibleProvider),
        ("anthropic", AnthropicProvider),
    ],
)
def test_kind_selects_the_adapter(kind, expected, monkeypatch):
    monkeypatch.setenv("K", "secret")
    cfg = ProviderSettings(kind=kind, base_url="http://x", api_key_env="K")
    assert isinstance(build(provider_name="p", cfg=cfg, model="m", timeout_seconds=5), expected)


# ------------------------------------------------------------------ routing


def test_default_routing_maps_severity_to_tiers():
    llm = LLMSettings()
    assert llm.tier_for("critical") == "primary"
    assert llm.tier_for("warning") == "economy"
    assert llm.tier_for("info") == DETERMINISTIC


def test_unknown_severity_falls_to_deterministic():
    assert LLMSettings().tier_for("catastrophic") == DETERMINISTIC


def test_routing_is_configurable_without_code():
    """Someone who wants the good model on everything can say so."""
    llm = LLMSettings(routing={"critical": "primary", "warning": "primary", "info": "economy"})
    assert llm.tier_for("warning") == "primary"
    assert llm.tier_for("info") == "economy"


def test_every_provider_declares_both_tiers():
    """This is what makes a provider switch coherent: the tiers move together
    instead of one being left on a model the new provider does not have."""
    for name, provider in LLMSettings().providers.items():
        assert provider.model_for("primary"), f"{name} has no primary model"
        assert provider.model_for("economy"), f"{name} has no economy model"


def test_a_provider_can_be_added_by_config_alone():
    llm = LLMSettings(
        provider="groq",
        providers={
            "groq": ProviderSettings(
                kind="openai_compatible",
                base_url="https://api.groq.com/openai/v1",
                api_key_env="GROQ_API_KEY",
                models={"primary": "llama-3.3-70b-versatile", "economy": "llama-3.1-8b-instant"},
            )
        },
    )
    active = llm.active()
    assert active is not None
    assert active.model_for("primary") == "llama-3.3-70b-versatile"


# ----------------------------------------------------------------- adapters


async def test_openai_compatible_posts_chat_completions():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, json={"choices": [{"message": {"content": '{"root_cause":"pool"}'}}]}
        )

    provider = OpenAICompatibleProvider(
        base_url="http://server/v1",
        model="m",
        timeout_seconds=5,
        api_key="k",
        client=transport(handler),
    )
    result = await provider.generate("prompt", {})

    assert result == {"root_cause": "pool"}
    assert str(seen[0].url).endswith("/chat/completions")
    assert seen[0].headers["authorization"] == "Bearer k"


async def test_openai_compatible_retries_without_json_mode_when_unsupported():
    """Not every compatible server implements response_format."""
    attempts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json as _json

        body = _json.loads(request.content)
        attempts.append(body)
        if "response_format" in body:
            return httpx.Response(400, text="response_format is not supported")
        return httpx.Response(
            200, json={"choices": [{"message": {"content": '{"root_cause":"ok"}'}}]}
        )

    provider = OpenAICompatibleProvider(
        base_url="http://server/v1", model="m", timeout_seconds=5, client=transport(handler)
    )
    result = await provider.generate("prompt", {})

    assert result == {"root_cause": "ok"}
    assert len(attempts) == 2
    assert "response_format" not in attempts[1]


async def test_anthropic_uses_its_own_request_shape():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, json={"content": [{"type": "text", "text": '{"root_cause":"cert"}'}]}
        )

    provider = AnthropicProvider(
        base_url="https://api.anthropic.com/v1",
        model="claude-sonnet-5",
        api_key="k",
        timeout_seconds=5,
        client=transport(handler),
    )
    result = await provider.generate("prompt", {})

    assert result == {"root_cause": "cert"}
    assert str(seen[0].url).endswith("/messages")
    assert seen[0].headers["x-api-key"] == "k"
    assert "anthropic-version" in seen[0].headers


async def test_ollama_uses_its_native_generate_api():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"response": '{"root_cause":"disk"}'})

    provider = OllamaProvider(
        base_url="http://ollama:11434", model="m", timeout_seconds=5, client=transport(handler)
    )
    result = await provider.generate("prompt", {})

    assert result == {"root_cause": "disk"}
    assert str(seen[0].url).endswith("/api/generate")


async def test_context_model_overrides_the_constructor_model():
    """How the tier cascade asks one provider for a different model."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json as _json

        seen.append(_json.loads(request.content)["model"])
        return httpx.Response(200, json={"response": "{}"})

    provider = OllamaProvider(
        base_url="http://x", model="primary-model", timeout_seconds=5, client=transport(handler)
    )
    await provider.generate("p", {"model": "economy-model"})

    assert seen == ["economy-model"]


async def test_a_provider_error_propagates_rather_than_being_swallowed():
    """Retry and fallback are the caller's job, so failure must surface."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="upstream exploded")

    provider = OpenAICompatibleProvider(
        base_url="http://x/v1", model="m", timeout_seconds=5, client=transport(handler)
    )
    with pytest.raises(httpx.HTTPStatusError):
        await provider.generate("p", {})

"""Maps a provider `kind` to the adapter that implements it.

Providers are configuration. Adding OpenRouter, Together, Groq, vLLM or a
local llama.cpp server needs a config entry and no code, because they all
speak the OpenAI chat-completions API. A genuinely different API shape needs a
new adapter registered here, which is the only case that touches Python.
"""

from __future__ import annotations

import os
from collections.abc import Callable

from airs_shared.settings import ProviderSettings

from .anthropic_provider import AnthropicProvider
from .base import BaseLLMProvider
from .ollama_provider import OllamaProvider
from .openai_compatible import OpenAICompatibleProvider


class ProviderConfigError(RuntimeError):
    """Configuration is wrong, as opposed to the model being unavailable.

    Raised so the caller can tell "you have not set an API key" apart from
    "the model timed out". Both end at the deterministic fallback, but only one
    of them is worth waking someone for.
    """


def _api_key(cfg: ProviderSettings, provider_name: str) -> str:
    if not cfg.api_key_env:
        return ""
    key = os.getenv(cfg.api_key_env, "")
    if not key:
        raise ProviderConfigError(
            f"Provider '{provider_name}' needs {cfg.api_key_env} in the environment"
        )
    return key


def _build_openai_compatible(
    cfg: ProviderSettings, name: str, model: str, timeout: int
) -> BaseLLMProvider:
    return OpenAICompatibleProvider(
        base_url=cfg.base_url,
        model=model,
        timeout_seconds=timeout,
        # Local servers usually need no key, so an unset api_key_env is fine.
        api_key=_api_key(cfg, name) or None,
        extra_headers=cfg.extra_headers,
    )


def _build_anthropic(cfg: ProviderSettings, name: str, model: str, timeout: int) -> BaseLLMProvider:
    return AnthropicProvider(
        base_url=cfg.base_url,
        model=model,
        api_key=_api_key(cfg, name),
        timeout_seconds=timeout,
        extra_headers=cfg.extra_headers,
    )


def _build_ollama(cfg: ProviderSettings, name: str, model: str, timeout: int) -> BaseLLMProvider:
    return OllamaProvider(
        base_url=cfg.base_url,
        model=model,
        timeout_seconds=timeout,
        extra_headers=cfg.extra_headers,
    )


Builder = Callable[[ProviderSettings, str, str, int], BaseLLMProvider]

REGISTRY: dict[str, Builder] = {
    "openai_compatible": _build_openai_compatible,
    "anthropic": _build_anthropic,
    "ollama": _build_ollama,
}


def register(kind: str, builder: Builder) -> None:
    """Add an adapter for an API shape none of the built-ins match."""
    REGISTRY[kind] = builder


def supported_kinds() -> list[str]:
    return sorted(REGISTRY)


def build(
    *, provider_name: str, cfg: ProviderSettings, model: str, timeout_seconds: int
) -> BaseLLMProvider:
    builder = REGISTRY.get(cfg.kind)
    if builder is None:
        raise ProviderConfigError(
            f"Provider '{provider_name}' has unknown kind '{cfg.kind}'. "
            f"Known kinds: {', '.join(supported_kinds())}"
        )
    if not cfg.base_url:
        raise ProviderConfigError(f"Provider '{provider_name}' has no base_url")
    if not model:
        raise ProviderConfigError(f"Provider '{provider_name}' has no model for this tier")
    return builder(cfg, provider_name, model, timeout_seconds)

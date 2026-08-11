from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class TopicSettings(BaseModel):
    logs: str = "logs-topic"
    processed_logs: str = "processed-logs-topic"
    anomalies: str = "anomalies-topic"
    incidents: str = "incidents-topic"
    # Incidents that have received an RCA, for outbound delivery. Separate from
    # incidents-topic so that publishing a delivery does not re-enter the RCA
    # stage and loop.
    enriched_incidents: str = "enriched-incidents-topic"
    dlq: str = "airs-dlq-topic"


class KafkaSettings(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    topics: TopicSettings = Field(default_factory=TopicSettings)


class OpenSearchSettings(BaseModel):
    url: str = "http://localhost:9200"
    logs_index: str = "airs-logs"
    incidents_index: str = "airs-incidents"
    sources_index: str = "airs-sources"


DETERMINISTIC = "deterministic"


class ProviderSettings(BaseModel):
    """One place a model can be reached.

    `kind` selects the adapter that speaks to it. Most inference servers speak
    the OpenAI chat-completions API, so `openai_compatible` covers OpenAI,
    vLLM, LM Studio, llama.cpp, Together, Groq, OpenRouter, DeepSeek, Mistral
    and anything else that implements it: those are configuration, not code.

    `models` maps a **tier name** to a model id. Tiers are what routing selects,
    so a provider owns its own answer to "what is the capable model here" and
    "what is the cheap one". That is what makes switching provider coherent:
    both tiers move together instead of one being left pointing at a model the
    new provider has never heard of.
    """

    kind: str = "openai_compatible"
    base_url: str = ""
    api_key_env: str = ""
    models: dict[str, str] = Field(default_factory=dict)
    extra_headers: dict[str, str] = Field(default_factory=dict)

    def model_for(self, tier: str) -> str | None:
        return self.models.get(tier)


def _default_providers() -> dict[str, ProviderSettings]:
    return {
        "ollama": ProviderSettings(
            kind="ollama",
            base_url="http://localhost:11434",
            models={"primary": "qwen2.5:7b-instruct", "economy": "qwen2.5:1.5b-instruct"},
        ),
        "openai": ProviderSettings(
            kind="openai_compatible",
            base_url="https://api.openai.com/v1",
            api_key_env="OPENAI_API_KEY",
            models={"primary": "gpt-4.1", "economy": "gpt-4.1-mini"},
        ),
        "anthropic": ProviderSettings(
            kind="anthropic",
            base_url="https://api.anthropic.com/v1",
            api_key_env="ANTHROPIC_API_KEY",
            models={"primary": "claude-sonnet-5", "economy": "claude-haiku-4-5-20251001"},
        ),
    }


class LLMSettings(BaseModel):
    # Free-form on purpose. A closed enum here would mean adding a provider
    # requires a code change, which is the thing this design exists to avoid.
    provider: str = "ollama"
    timeout_seconds: int = 8
    retries: int = 2

    # Severity to tier. A tier of "deterministic" skips the model entirely.
    routing: dict[str, str] = Field(
        default_factory=lambda: {
            "critical": "primary",
            "warning": "economy",
            "info": DETERMINISTIC,
        }
    )
    providers: dict[str, ProviderSettings] = Field(default_factory=_default_providers)

    def active(self, provider_name: str | None = None) -> ProviderSettings | None:
        return self.providers.get(provider_name or self.provider)

    def tier_for(self, severity: str) -> str:
        return self.routing.get(severity.lower(), DETERMINISTIC)


class PipelineSettings(BaseModel):
    baseline_window_minutes: int = 15
    correlation_window_minutes: int = 10
    anomaly_threshold: float = 2.5
    dashboard_poll_seconds: int = 5
    source_poll_tick_seconds: int = 5
    log_retention_days: int = 5
    incident_retention_days: int = 30
    max_log_payload_kb: int = 256
    max_manual_analyze_kb: int = 512


class ServiceNowSettings(BaseModel):
    """Connector configuration.

    The password is never held here. It is read from the environment variable
    named by `password_env`, so a config file committed to the repository can
    describe the integration completely without carrying a credential.
    """

    enabled: bool = False
    instance_url: str = ""
    username: str = "admin"
    password_env: str = "SERVICENOW_PASSWORD"
    tenant_id: str = "default"
    poll_interval_seconds: int = Field(default=60, ge=15, le=3600)
    page_size: int = Field(default=50, ge=1, le=200)
    # Defaults to on. A work note is visible to real people on a real ticket,
    # so writing must be a decision someone made, not a default they inherited.
    dry_run: bool = True


class ConnectorSettings(BaseModel):
    servicenow: ServiceNowSettings = Field(default_factory=ServiceNowSettings)


class RedisSettings(BaseModel):
    url: str = "redis://localhost:6379/0"


class AppSettings(BaseModel):
    name: str = "AIRS"
    environment: str = "local"


class AIRSSettings(BaseModel):
    app: AppSettings = Field(default_factory=AppSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    opensearch: OpenSearchSettings = Field(default_factory=OpenSearchSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    connectors: ConnectorSettings = Field(default_factory=ConnectorSettings)


@lru_cache(maxsize=1)
def get_settings() -> AIRSSettings:
    import os

    configured = os.getenv("AIRS_CONFIG_FILE")
    repo_root = Path(__file__).resolve().parents[2]
    candidates = [
        Path(configured) if configured else None,
        Path.cwd() / "config" / "airs.yaml",
        repo_root / "config" / "airs.yaml",
        Path.home() / ".config" / "airs.yaml",
    ]

    for candidate in candidates:
        if candidate is None or not candidate.exists():
            continue
        with candidate.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}
        return AIRSSettings.model_validate(payload)

    return AIRSSettings()

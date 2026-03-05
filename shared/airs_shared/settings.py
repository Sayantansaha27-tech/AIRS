from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


class TopicSettings(BaseModel):
    logs: str = "logs-topic"
    processed_logs: str = "processed-logs-topic"
    anomalies: str = "anomalies-topic"
    incidents: str = "incidents-topic"
    dlq: str = "airs-dlq-topic"


class KafkaSettings(BaseModel):
    bootstrap_servers: str = "localhost:9092"
    topics: TopicSettings = Field(default_factory=TopicSettings)


class OpenSearchSettings(BaseModel):
    url: str = "http://localhost:9200"
    logs_index: str = "airs-logs"
    incidents_index: str = "airs-incidents"
    sources_index: str = "airs-sources"


class LLMSettings(BaseModel):
    provider: Literal["ollama", "openai"] = "ollama"
    model: str = "qwen2.5:7b-instruct"
    fallback_model: str = "rjmalagon/qwen2:1.5b-instruct"
    timeout_seconds: int = 8
    retries: int = 2
    ollama_base_url: str = "http://localhost:11434"
    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key_env: str = "OPENAI_API_KEY"


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

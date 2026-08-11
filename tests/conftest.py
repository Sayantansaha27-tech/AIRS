"""Shared fixtures for service-level tests.

The six services are FastAPI apps loaded from ``services/<name>/app/main.py``.
They are not an installed package and several share the module name ``main``,
so they are loaded by path under distinct module names.

Every service builds a synchronous OpenSearch client at import time, so
``opensearchpy.OpenSearch`` is patched before the module body runs. Kafka
producers are replaced with a recorder that captures every published message,
which is what lets the DLQ tests assert on topic routing without a broker.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import REGISTRY

REPO_ROOT = Path(__file__).resolve().parents[1]
SERVICE_APP_DIRS = {
    "ingestion": REPO_ROOT / "services/ingestion-service/app",
    "log_processor": REPO_ROOT / "services/log-processor/app",
    "anomaly": REPO_ROOT / "services/anomaly-service/app",
    "correlation": REPO_ROOT / "services/correlation-service/app",
    "ai": REPO_ROOT / "services/ai-service/app",
    "gateway": REPO_ROOT / "services/api-gateway/app",
}


class ProducerSpy:
    """Stands in for AIOKafkaProducer, recording what each service publishes."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, dict[str, Any]]] = []

    async def send_and_wait(self, topic: str, value: bytes) -> None:
        self.messages.append((topic, json.loads(value.decode("utf-8"))))

    def topics(self) -> list[str]:
        return [topic for topic, _ in self.messages]

    def payloads_for(self, topic: str) -> list[dict[str, Any]]:
        return [payload for name, payload in self.messages if name == topic]

    def count_for(self, topic: str) -> int:
        return len(self.payloads_for(topic))


def _reset_prometheus_registry() -> None:
    """Clear the default registry between service loads.

    Services deliberately share metric names (every stage exports
    ``airs_dlq_published_total``), which is correct in production where each
    runs in its own process but collides when several are imported into one
    test process. Metric objects already held by a loaded module keep working;
    they are simply no longer collected, and no test scrapes /metrics.
    """
    for collector in list(REGISTRY._collector_to_names):
        REGISTRY.unregister(collector)


def load_service(name: str):
    """Import a service module by path with its dependencies neutralised."""
    app_dir = SERVICE_APP_DIRS[name]
    module_name = f"airs_service_{name}"
    if module_name in sys.modules:
        return sys.modules[module_name]

    _reset_prometheus_registry()
    # ai-service imports sibling modules (rca, ai_providers) by bare name.
    sys.path.insert(0, str(app_dir))
    try:
        with (
            patch("opensearchpy.OpenSearch", MagicMock()),
            patch("opensearchpy.AsyncOpenSearch", MagicMock()),
        ):
            spec = importlib.util.spec_from_file_location(module_name, app_dir / "main.py")
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(app_dir))
    return module


@pytest.fixture
def producer() -> ProducerSpy:
    return ProducerSpy()


@pytest.fixture
def ingestion(producer: ProducerSpy):
    module = load_service("ingestion")
    module.producer = producer
    yield module
    module.producer = None


@pytest.fixture
def log_processor(producer: ProducerSpy, monkeypatch: pytest.MonkeyPatch):
    module = load_service("log_processor")
    module.producer = producer

    async def _noop_bulk(client, index, documents, *, refresh=False):
        return len(documents)

    monkeypatch.setattr(module, "bulk_index", _noop_bulk)
    yield module
    module.producer = None


@pytest.fixture
def anomaly(producer: ProducerSpy):
    module = load_service("anomaly")
    module.producer = producer
    module.baselines.clear()
    module.rules_cache = []
    module.suppressions_cache = []
    yield module
    module.producer = None
    module.baselines.clear()


@pytest.fixture
def correlation(producer: ProducerSpy, monkeypatch: pytest.MonkeyPatch):
    module = load_service("correlation")
    module.producer = producer
    module.clusters.clear()
    module.service_config_cache.clear()

    # Isolate the correlation logic from OpenSearch-backed enrichment.
    # These are async now, so the stubs must be too.
    async def _no_neighbours(tenant_id, service):
        return set()

    async def _no_parent(**kwargs):
        return None

    class _NoopIndex:
        async def index(self, **kwargs):
            return None

        async def search(self, **kwargs):
            return {"hits": {"hits": []}}

        async def close(self):
            return None

    monkeypatch.setattr(module, "os_async", _NoopIndex())
    monkeypatch.setattr(module, "fetch_topology_neighbors", _no_neighbours)
    monkeypatch.setattr(module, "find_parent_incident", _no_parent)
    yield module
    module.producer = None
    module.clusters.clear()


@pytest.fixture
def gateway(producer: ProducerSpy):
    """api-gateway is a producer only. It has no Kafka consumer and no DLQ."""
    module = load_service("gateway")
    module.producer = producer
    yield module
    module.producer = None


@pytest.fixture
def ai(producer: ProducerSpy, monkeypatch: pytest.MonkeyPatch):
    module = load_service("ai")
    module.producer = producer
    module.runtime_llm_config = None
    for name in (
        "fetch_source_metadata",
        "fetch_recent_incidents",
        "fetch_active_suppressions",
        "fetch_topology",
    ):
        monkeypatch.setattr(module, name, _empty_for(name))
    monkeypatch.setattr(module, "upsert_doc", lambda *a, **k: None)
    yield module
    module.producer = None
    module.runtime_llm_config = None


def _empty_for(name: str):
    empty: Any = {} if name == "fetch_source_metadata" else []
    return lambda *a, **k: empty

"""Chaos tests run against a live stack, not against mocks.

They are skipped unless AIRS_CHAOS=1 and the stack answers on its health
endpoints, so a normal `pytest` run is unaffected.

    docker compose up -d
    AIRS_CHAOS=1 pytest tests/chaos -v

These tests stop and start containers. Do not point them at anything you care
about.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

GATEWAY = os.getenv("AIRS_GATEWAY_URL", "http://localhost:8000")
INGESTION = os.getenv("AIRS_INGESTION_URL", "http://localhost:8001")
AI_SERVICE = os.getenv("AIRS_AI_URL", "http://localhost:8005")
OPENSEARCH = os.getenv("AIRS_OPENSEARCH_URL", "http://localhost:9200")
AI_CONTAINER = os.getenv("AIRS_AI_CONTAINER", "airs-ai-service")
CHAOS_DIR = str(Path(__file__).resolve().parent)


def _stack_is_up() -> bool:
    for url in (f"{GATEWAY}/health/live", f"{INGESTION}/health/live"):
        try:
            if httpx.get(url, timeout=3.0).status_code != 200:
                return False
        except Exception:
            return False
    return True


def pytest_collection_modifyitems(config, items):
    """Gate only the chaos tests.

    This hook is handed every collected item in the session, not just the ones
    under this directory, so it must select its own before adding a skip.
    """
    chaos_items = [item for item in items if CHAOS_DIR in str(item.path)]
    if not chaos_items:
        return

    if os.getenv("AIRS_CHAOS") != "1":
        skip = pytest.mark.skip(reason="chaos tests need AIRS_CHAOS=1 and a live stack")
    elif not _stack_is_up():
        skip = pytest.mark.skip(
            reason=f"stack not reachable at {GATEWAY} and {INGESTION}; run docker compose up -d"
        )
    else:
        return

    for item in chaos_items:
        item.add_marker(skip)


class Stack:
    """Thin driver over the running compose stack."""

    gateway = GATEWAY
    ingestion = INGESTION
    ai_service = AI_SERVICE
    opensearch = OPENSEARCH

    @staticmethod
    def compose(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["docker", "compose", *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=120,
        )

    @classmethod
    def stop_ai_service(cls) -> None:
        cls.compose("stop", "ai-service")

    @classmethod
    def start_ai_service(cls) -> None:
        cls.compose("start", "ai-service")
        cls.wait_until(
            lambda: _get(f"{AI_SERVICE}/health/live") is not None,
            timeout=90,
            what="ai-service to come back",
        )

    @staticmethod
    def wait_until(predicate, *, timeout: float, what: str, interval: float = 2.0) -> Any:
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = predicate()
            if last:
                return last
            time.sleep(interval)
        raise AssertionError(f"timed out after {timeout}s waiting for {what}")

    @classmethod
    def ingest(cls, logs: list[dict[str, Any]]) -> httpx.Response:
        return httpx.post(f"{INGESTION}/ingest", json={"logs": logs}, timeout=30.0)

    @classmethod
    def incidents_for(cls, service: str) -> list[dict[str, Any]]:
        response = httpx.get(
            f"{GATEWAY}/v1/incidents",
            params={"service": service, "size": 100},
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json().get("items", [])

    @classmethod
    def dlq_depth(cls) -> int:
        """Sum of airs_dlq_published_total across every service that exports it."""
        total = 0
        for port in (8001, 8002, 8003, 8004, 8005):
            body = _get(f"http://localhost:{port}/metrics")
            if body is None:
                continue
            for line in body.splitlines():
                if line.startswith("airs_dlq_published_total{"):
                    total += int(float(line.rsplit(" ", 1)[1]))
        return total


def _get(url: str) -> str | None:
    try:
        response = httpx.get(url, timeout=5.0)
        return response.text if response.status_code == 200 else None
    except Exception:
        return None


@pytest.fixture
def stack() -> Stack:
    return Stack()


@pytest.fixture
def restore_ai_service():
    """Guarantee ai-service is running again even if an assertion fails."""
    yield
    try:
        Stack.start_ai_service()
    except Exception:
        subprocess.run(["docker", "compose", "start", "ai-service"], check=False)

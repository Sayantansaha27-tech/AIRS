#!/usr/bin/env python3
"""Per-stage throughput for the CPU-bound work in each pipeline stage.

    python evals/bench_pipeline.py
    python evals/bench_pipeline.py --events 100000 --json bench.json

SCOPE, READ THIS BEFORE QUOTING ANY NUMBER
-------------------------------------------
This measures the in-process computation each stage performs: normalization,
baseline update and scoring, rule evaluation, clustering, and deterministic
RCA construction. Kafka and OpenSearch are stubbed out.

That makes these numbers an **upper bound**, not a throughput figure for the
deployed system. AIRS calls OpenSearch through the synchronous client from
inside async loops, and log-processor indexes with refresh=True on every
event, so the deployed pipeline is I/O bound well below these ceilings. What
this benchmark is good for is showing where the CPU cost sits and catching a
regression in the detection or correlation logic.

End-to-end numbers require the full stack. See docs/06-evals.md.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "shared"))
sys.path.insert(0, str(REPO_ROOT / "services/ai-service/app"))


def reset_prometheus_registry() -> None:
    """Services share metric names, which collides when two are imported here."""
    from prometheus_client import REGISTRY

    for collector in list(REGISTRY._collector_to_names):
        REGISTRY.unregister(collector)


MESSAGES = [
    "timeout while creating order",
    "HikariPool-1 - Connection is not available, request timed out after 30000ms",
    "GET /v2/inventory/reserve returned 503 Service Unavailable",
    "request completed in 12ms",
    "cache hit ratio 0.94",
    "java.lang.OutOfMemoryError: Java heap space",
]
LEVELS = ["info", "info", "info", "warning", "error", "critical"]


def hardware() -> dict[str, Any]:
    def _sysctl(key: str) -> str:
        try:
            return subprocess.run(
                ["sysctl", "-n", key], capture_output=True, text=True, check=True
            ).stdout.strip()
        except Exception:
            return "unknown"

    memory = _sysctl("hw.memsize")
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu": _sysctl("machdep.cpu.brand_string"),
        "cores": _sysctl("hw.ncpu"),
        "memory_gb": round(int(memory) / 1024**3) if memory.isdigit() else "unknown",
        "measured_at": datetime.now(UTC).isoformat(),
    }


def synthetic_events(count: int) -> list[dict[str, Any]]:
    base = datetime(2026, 8, 10, 3, 0, tzinfo=UTC)
    return [
        {
            "timestamp": (base + timedelta(seconds=i // 20)).isoformat(),
            "service": f"service-{i % 12}",
            "level": LEVELS[i % len(LEVELS)],
            "message": f"{MESSAGES[i % len(MESSAGES)]} seq={i}",
            "trace_id": f"trace-{i}",
        }
        for i in range(count)
    ]


def timed(label: str, count: int, fn) -> dict[str, Any]:
    """Run fn, report throughput and per-event latency percentiles."""
    samples: list[float] = []
    started = time.perf_counter()
    fn(samples)
    elapsed = time.perf_counter() - started

    result = {
        "stage": label,
        "events": count,
        "seconds": round(elapsed, 4),
        "events_per_second": round(count / elapsed) if elapsed else 0,
    }
    if samples:
        ordered = sorted(samples)
        result |= {
            "p50_us": round(statistics.median(ordered) * 1e6, 2),
            "p95_us": round(ordered[int(len(ordered) * 0.95)] * 1e6, 2),
            "p99_us": round(ordered[int(len(ordered) * 0.99)] * 1e6, 2),
        }
    return result


def bench_normalize(events: list[dict[str, Any]]) -> dict[str, Any]:
    from airs_shared.normalize import normalize_log

    def run(samples: list[float]) -> None:
        for event in events:
            t0 = time.perf_counter()
            normalize_log(event)
            samples.append(time.perf_counter() - t0)

    return timed("normalize", len(events), run)


def bench_contract_validation(events: list[dict[str, Any]]) -> dict[str, Any]:
    """The produce-boundary check every stage pays on every publish."""
    from airs_shared.normalize import normalize_log
    from airs_shared.schema_registry import validate_topic_payload
    from airs_shared.settings import get_settings

    settings = get_settings()
    topic = settings.kafka.topics.logs
    payloads = [normalize_log(e).model_dump(mode="json") for e in events]

    def run(samples: list[float]) -> None:
        for payload in payloads:
            t0 = time.perf_counter()
            validate_topic_payload(settings=settings, topic=topic, payload=payload)
            samples.append(time.perf_counter() - t0)

    return timed("contract validation", len(payloads), run)


def bench_anomaly_detection(events: list[dict[str, Any]]) -> dict[str, Any]:
    import importlib.util
    from unittest.mock import MagicMock, patch

    from airs_shared.normalize import normalize_log

    app_dir = REPO_ROOT / "services/anomaly-service/app"
    sys.path.insert(0, str(app_dir))
    reset_prometheus_registry()
    with patch("opensearchpy.OpenSearch", MagicMock()):
        spec = importlib.util.spec_from_file_location("bench_anomaly", app_dir / "main.py")
        anomaly = importlib.util.module_from_spec(spec)
        sys.modules["bench_anomaly"] = anomaly
        spec.loader.exec_module(anomaly)

    normalized = [normalize_log(e) for e in events]

    def run(samples: list[float]) -> None:
        for event in normalized:
            t0 = time.perf_counter()
            baseline = anomaly.get_baseline(event.tenant_id, event.service)
            baseline.ingest(event.timestamp)
            score = baseline.zscore(event.timestamp)
            keywords = anomaly.detect_keywords(event.message)
            if anomaly.should_emit(score, event.level, keywords, []):
                anomaly.compute_confidence_score(
                    level=event.level,
                    score=round(score or 0.0, 3),
                    matched_keywords=keywords,
                    matched_rules=[],
                )
                anomaly.build_fingerprint(event.tenant_id, event.service, event.message)
            samples.append(time.perf_counter() - t0)

    return timed("anomaly detection", len(normalized), run)


def bench_correlation(count: int) -> dict[str, Any]:
    import asyncio
    import importlib.util
    from unittest.mock import MagicMock, patch

    from airs_shared.models import AnomalyEvent, Severity

    app_dir = REPO_ROOT / "services/correlation-service/app"
    sys.path.insert(0, str(app_dir))
    reset_prometheus_registry()
    with patch("opensearchpy.OpenSearch", MagicMock()):
        spec = importlib.util.spec_from_file_location("bench_corr", app_dir / "main.py")
        corr = importlib.util.module_from_spec(spec)
        sys.modules["bench_corr"] = corr
        spec.loader.exec_module(corr)

    corr.upsert_doc = lambda *a, **k: None
    corr.fetch_topology_neighbors = lambda t, s: set()
    corr.find_parent_incident = lambda **k: None
    corr.producer = object()

    async def _noop(*args, **kwargs):
        return None

    corr.produce_json = _noop
    corr.get_service_config = lambda t, s: corr.ServiceCorrelationConfig(
        window_duration_minutes=10, min_signal_count=2, fetched_at=datetime.now(UTC)
    )

    base = datetime.now(UTC)
    anomalies = [
        AnomalyEvent(
            timestamp=base + timedelta(milliseconds=i * 10),
            service=f"service-{i % 12}",
            severity=Severity.warning if i % 5 else Severity.critical,
            anomaly_score=3.0,
            reasons=["keywords=timeout"],
            log_message=f"message {i}",
            fingerprint=f"fp-{i % 400}",
        )
        for i in range(count)
    ]

    def run(samples: list[float]) -> None:
        async def go() -> None:
            for anomaly in anomalies:
                t0 = time.perf_counter()
                await corr.process_anomaly(anomaly)
                samples.append(time.perf_counter() - t0)

        asyncio.run(go())

    return timed("correlation", len(anomalies), run)


def bench_deterministic_rca(count: int) -> dict[str, Any]:
    from rca import deterministic_fallback

    context = {
        "service": "orders-service",
        "logs": [
            {
                "timestamp": "2026-08-10T02:14:03Z",
                "service": "orders-service",
                "message": f"connection timed out seq={i}",
            }
            for i in range(15)
        ],
        "anomalies": [{"id": f"a{i}"} for i in range(6)],
    }

    def run(samples: list[float]) -> None:
        for _ in range(count):
            t0 = time.perf_counter()
            deterministic_fallback(context)
            samples.append(time.perf_counter() - t0)

    return timed("deterministic RCA", count, run)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=50_000)
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    hw = hardware()
    print("Hardware")
    for key, value in hw.items():
        print(f"  {key:14s} {value}")
    print(f"\nGenerating {args.events:,} synthetic events...")
    events = synthetic_events(args.events)

    results = [
        bench_normalize(events),
        bench_contract_validation(events),
        bench_anomaly_detection(events),
        bench_correlation(min(args.events, 20_000)),
        bench_deterministic_rca(min(args.events, 20_000)),
    ]

    print(f"\n{'stage':22s} {'events':>9s} {'ev/sec':>12s} {'p50 us':>9s} {'p99 us':>9s}")
    print("-" * 66)
    for r in results:
        print(
            f"{r['stage']:22s} {r['events']:>9,} {r['events_per_second']:>12,} "
            f"{r.get('p50_us', 0):>9.2f} {r.get('p99_us', 0):>9.2f}"
        )
    print("-" * 66)
    print("CPU-bound work only. Kafka and OpenSearch are stubbed out.")
    print("These are upper bounds, not deployed throughput. See docs/06-evals.md.")

    if args.json:
        args.json.write_text(json.dumps({"hardware": hw, "stages": results}, indent=2))
        print(f"\nWrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

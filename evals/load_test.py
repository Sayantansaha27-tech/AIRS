#!/usr/bin/env python3
"""End-to-end load test against a running stack.

    docker compose up -d
    python evals/load_test.py --rate 200 --seconds 30

Measures what the per-stage benchmark cannot: sustained ingestion before lag
grows, per-stage latency through real Kafka and OpenSearch, end-to-end
log-to-incident time, and DLQ rate under load.

This drives real traffic into a real pipeline. Do not point it at anything you
care about.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

INGESTION = "http://localhost:8001"
GATEWAY = "http://localhost:8000"
OPENSEARCH = "http://localhost:9200"
SERVICE_PORTS = {
    "ingestion-service": 8001,
    "log-processor": 8002,
    "anomaly-service": 8003,
    "correlation-service": 8004,
    "ai-service": 8005,
}

MESSAGES = [
    "request completed in 12ms",
    "cache hit ratio 0.94",
    "user session refreshed",
    "timeout while creating order",
    "connection refused: postgres-primary:5432",
]


def _int_or_nan(value: float) -> Any:
    return float("nan") if value != value else int(value)


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
        "cpu": _sysctl("machdep.cpu.brand_string"),
        "cores": _sysctl("hw.ncpu"),
        "memory_gb": round(int(memory) / 1024**3) if memory.isdigit() else "unknown",
        "docker": subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
        ).stdout.strip(),
        "measured_at": datetime.now(UTC).isoformat(),
    }


class ScrapeFailed(Exception):
    """A service did not answer /metrics.

    This must not be reported as zero. A service whose event loop is blocked
    stops serving /metrics, and treating that as zero turns a real symptom into
    a negative counter delta, which reads as nonsense rather than as the
    unresponsive service it actually is.
    """


def scrape(port: int, metric: str) -> float:
    """Sum a Prometheus counter across all its label combinations."""
    try:
        body = httpx.get(f"http://localhost:{port}/metrics", timeout=5.0).text
    except Exception as exc:
        raise ScrapeFailed(f"port {port} did not answer /metrics: {exc}") from exc
    total = 0.0
    for line in body.splitlines():
        if line.startswith(f"{metric}{{") or line.startswith(f"{metric} "):
            try:
                total += float(line.rsplit(" ", 1)[1])
            except (ValueError, IndexError):
                continue
    return total


def counters() -> tuple[dict[str, float], list[str]]:
    """Snapshot the funnel counters, reporting which services did not answer."""
    unreachable: list[str] = []
    values: dict[str, float] = {}

    for key, port, metric in (
        ("ingested", 8001, "airs_logs_ingested_total"),
        ("processed", 8002, "airs_logs_processed_total"),
        ("anomalies", 8003, "airs_anomalies_emitted_total"),
        ("incidents", 8004, "airs_incidents_created_total"),
        ("amended", 8004, "airs_incidents_amended_total"),
    ):
        try:
            values[key] = scrape(port, metric)
        except ScrapeFailed:
            values[key] = float("nan")
            name = next(n for n, p in SERVICE_PORTS.items() if p == port)
            if name not in unreachable:
                unreachable.append(name)

    dlq = 0.0
    for name, port in SERVICE_PORTS.items():
        try:
            dlq += scrape(port, "airs_dlq_published_total")
        except ScrapeFailed:
            if name not in unreachable:
                unreachable.append(name)
    values["dlq"] = dlq

    return values, unreachable


async def drive_load(
    client: httpx.AsyncClient,
    *,
    rate: int,
    seconds: int,
    batch: int,
    service_prefix: str,
    error_ratio: float,
) -> dict[str, Any]:
    """Post batches at a target rate, recording per-request latency."""
    latencies: list[float] = []
    accepted = 0
    rejected = 0
    batches_per_second = max(rate // batch, 1)
    interval = 1.0 / batches_per_second

    started = time.perf_counter()
    deadline = started + seconds
    sent_batches = 0

    while time.perf_counter() < deadline:
        cycle_start = time.perf_counter()
        logs = []
        for i in range(batch):
            index = sent_batches * batch + i
            is_error = (index % 100) < int(error_ratio * 100)
            logs.append(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "service": f"{service_prefix}-{index % 8}",
                    "level": "error" if is_error else "info",
                    "message": (MESSAGES[3 + (index % 2)] if is_error else MESSAGES[index % 3])
                    + f" seq={index}",
                }
            )

        t0 = time.perf_counter()
        try:
            response = await client.post(f"{INGESTION}/ingest", json={"logs": logs}, timeout=30.0)
            latencies.append(time.perf_counter() - t0)
            if response.status_code == 200:
                accepted += response.json().get("accepted", 0)
            else:
                rejected += batch
        except Exception:
            rejected += batch
            latencies.append(time.perf_counter() - t0)

        sent_batches += 1
        elapsed_cycle = time.perf_counter() - cycle_start
        if elapsed_cycle < interval:
            await asyncio.sleep(interval - elapsed_cycle)

    wall = time.perf_counter() - started
    ordered = sorted(latencies)
    return {
        "target_rate_per_second": rate,
        "duration_seconds": round(wall, 2),
        "batches_sent": sent_batches,
        "events_accepted": accepted,
        "events_rejected": rejected,
        "achieved_rate_per_second": round(accepted / wall, 1) if wall else 0,
        "batch_latency_p50_ms": round(statistics.median(ordered) * 1000, 2) if ordered else 0,
        "batch_latency_p95_ms": round(ordered[int(len(ordered) * 0.95)] * 1000, 2)
        if ordered
        else 0,
        "batch_latency_p99_ms": round(ordered[int(len(ordered) * 0.99)] * 1000, 2)
        if ordered
        else 0,
    }


async def measure_end_to_end(client: httpx.AsyncClient, samples: int) -> dict[str, Any]:
    """Time a critical event from POST /ingest to a queryable incident.

    Critical severity emits an incident on arrival, so this measures pipeline
    transit rather than the correlation window.
    """
    timings: list[float] = []
    for _ in range(samples):
        service = f"e2e-{uuid.uuid4().hex[:8]}"
        t0 = time.perf_counter()
        await client.post(
            f"{INGESTION}/ingest",
            json={
                "logs": [
                    {
                        "timestamp": datetime.now(UTC).isoformat(),
                        "service": service,
                        "level": "critical",
                        "message": "connection refused: postgres-primary:5432",
                    }
                ]
            },
            timeout=30.0,
        )

        deadline = time.perf_counter() + 120
        while time.perf_counter() < deadline:
            try:
                r = await client.get(
                    f"{OPENSEARCH}/airs-incidents/_search",
                    params={"q": f"service:{service}", "size": 1},
                    timeout=10.0,
                )
                if r.status_code == 200 and r.json()["hits"]["total"]["value"] > 0:
                    timings.append(time.perf_counter() - t0)
                    break
            except Exception:
                pass
            await asyncio.sleep(0.25)

    if not timings:
        return {"samples": 0, "note": "no incident observed within timeout"}
    ordered = sorted(timings)
    return {
        "samples": len(ordered),
        "p50_seconds": round(statistics.median(ordered), 2),
        "p95_seconds": round(ordered[int(len(ordered) * 0.95)], 2),
        "max_seconds": round(max(ordered), 2),
    }


async def measure_rca_latency(client: httpx.AsyncClient, samples: int) -> dict[str, Any]:
    """Time RCA generation directly, bypassing the pipeline."""
    timings: list[float] = []
    for i in range(samples):
        t0 = time.perf_counter()
        try:
            r = await client.post(
                f"{GATEWAY}/v1/analyze",
                json={
                    "logs": [
                        {
                            "timestamp": datetime.now(UTC).isoformat(),
                            "service": "orders-service",
                            "level": "critical",
                            "message": f"HikariPool-1 connection not available, waiting=47 seq={i}",
                        }
                    ]
                },
                timeout=300.0,
            )
            if r.status_code == 200:
                timings.append(time.perf_counter() - t0)
        except Exception:
            continue

    if not timings:
        return {"samples": 0}
    ordered = sorted(timings)
    return {
        "samples": len(ordered),
        "p50_seconds": round(statistics.median(ordered), 2),
        "max_seconds": round(max(ordered), 2),
    }


async def main_async(args: argparse.Namespace) -> int:
    hw = hardware()
    print("Hardware")
    for key, value in hw.items():
        print(f"  {key:14s} {value}")

    async with httpx.AsyncClient() as client:
        try:
            health = (await client.get(f"{GATEWAY}/health", timeout=10.0)).json()
        except Exception:
            print("\nStack is not reachable. Run: docker compose up -d")
            return 1
        print(f"\nGateway dependencies: {health.get('dependencies')}")

        before, _ = counters()
        print(f"\nDriving {args.rate} events/sec for {args.seconds}s...")
        load = await drive_load(
            client,
            rate=args.rate,
            seconds=args.seconds,
            batch=args.batch,
            service_prefix=args.service_prefix,
            error_ratio=args.error_ratio,
        )

        print("Draining...")
        await asyncio.sleep(args.drain_seconds)
        after, unreachable = counters()

        delta = {k: after[k] - before[k] for k in before}
        if unreachable:
            print(
                "\n  WARNING: these services did not answer /metrics after the run, "
                "which means their event loop is blocked, not that they did no work:"
            )
            for name in unreachable:
                print(f"    - {name}")
        funnel = {
            "events_accepted": load["events_accepted"],
            "logs_ingested": int(delta["ingested"]),
            "logs_processed": int(delta["processed"]),
            "anomalies_emitted": int(delta["anomalies"]),
            "incidents_created": _int_or_nan(delta["incidents"]),
            "incidents_amended": _int_or_nan(delta["amended"]),
            "dlq_events": int(delta["dlq"]),
            "unreachable_after_run": unreachable,
        }

        print("\nEnd-to-end latency (critical event to queryable incident)...")
        e2e = await measure_end_to_end(client, args.e2e_samples)

        rca: dict[str, Any] = {"skipped": True}
        if args.rca_samples:
            print(f"RCA generation latency ({args.rca_samples} samples)...")
            rca = await measure_rca_latency(client, args.rca_samples)

    print(f"\n{'=' * 62}\nIngestion\n{'=' * 62}")
    for key in (
        "target_rate_per_second",
        "achieved_rate_per_second",
        "events_accepted",
        "events_rejected",
        "batch_latency_p50_ms",
        "batch_latency_p95_ms",
        "batch_latency_p99_ms",
    ):
        print(f"  {key:28s} {load[key]}")

    print(f"\n{'=' * 62}\nPipeline funnel\n{'=' * 62}")
    for key, value in funnel.items():
        if isinstance(value, list):
            print(f"  {key:28s} {', '.join(value) if value else 'none'}")
        elif value != value:  # NaN
            print(f"  {key:28s} unknown (service unreachable)")
        else:
            print(f"  {key:28s} {value:,}")

    print(f"\n{'=' * 62}\nLatency\n{'=' * 62}")
    print(f"  end-to-end  {e2e}")
    print(f"  rca         {rca}")

    report = {
        "hardware": hw,
        "ingestion": load,
        "funnel": funnel,
        "end_to_end": e2e,
        "rca": rca,
    }
    if args.json:
        args.json.write_text(json.dumps(report, indent=2))
        print(f"\nWrote {args.json}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=200, help="target events/sec")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--batch", type=int, default=20)
    parser.add_argument("--drain-seconds", type=int, default=20)
    parser.add_argument("--error-ratio", type=float, default=0.1)
    parser.add_argument("--service-prefix", default="loadtest")
    parser.add_argument("--e2e-samples", type=int, default=5)
    parser.add_argument("--rca-samples", type=int, default=0)
    parser.add_argument("--json", type=Path)
    return asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())

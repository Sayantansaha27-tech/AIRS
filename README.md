# AIRS: AI Incident Response System

> **An event-driven, Kafka-native incident intelligence platform that ingests raw logs, detects anomalies, correlates them into incidents, and generates AI-powered Root Cause Analysis. Streaming end to end, and fully runnable on one machine.**

[![CI](https://github.com/Sayantansaha27-tech/AIRS/actions/workflows/ci.yml/badge.svg)](https://github.com/Sayantansaha27-tech/AIRS/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-compose-blue)](docker-compose.yml)

---

## Why AIRS?

### The problem every systems builder has hit

You're running a fleet of services. Something breaks at 2 AM. You open Grafana and see a spike. You open Kibana and see 10,000 error lines. You open Slack and see five engineers all looking at different logs, each with a different theory. Forty minutes pass before anyone agrees on a root cause, a cascading timeout from a single DB pool exhaustion.

**The raw material for diagnosing that incident was always there.** It was sitting in your logs. What was missing was a system that could:

1. Watch all of it continuously
2. Know what "anomalous" means for each service
3. Group related signals before the noise drowns them
4. Reason about root causes the way a senior engineer would, but at machine speed

AIRS is that system.

---

### A systems builder's perspective

Most "AI observability" tools today are SaaS wrappers around a log search UI with a chat box bolted on. They're useful, but they make a fundamental architectural mistake: they treat AI as a query layer, not a pipeline stage.

AIRS is designed differently:

**AI lives inside the event pipeline, not outside it.**

```text
Raw logs
   │
   ▼  ingestion-service (normalize + fan-out)
Kafka: logs-topic
   │
   ▼  log-processor (index + forward)
Kafka: processed-logs-topic
   │
   ▼  anomaly-service (z-score + keyword/level rules)
Kafka: anomalies-topic
   │
   ▼  correlation-service (time-window + dedup)
Kafka: incidents-topic
   │
   ▼  ai-service (RCA generation: Ollama or OpenAI)
OpenSearch: airs-incidents
   │
   ▼  api-gateway (REST + SSE + ChatOps)
   │
   ▼  Next.js dashboard
```

This means:

- **Every stage fails and lags independently.** The anomaly detector can lag without blocking ingestion. The AI service can be slow without stalling correlation. Each stage is its own consumer group, so backpressure at one does not propagate upstream. Note this is isolation, not horizontal scale: anomaly-service and correlation-service both hold per-service state in memory, so running two replicas of either splits that state rather than sharing it. Scaling those two needs the state moved out first.
- **The AI is opt-out, not opt-in.** Incidents always exist. RCA is async enrichment on top of them. A model timeout never breaks your incident pipeline.
- **You own the data.** Default setup is fully local: Kafka, OpenSearch, and Ollama running in Docker. No data leaves your machine unless you choose OpenAI mode.
- **The provider is a runtime detail.** Swap Ollama for OpenAI (or back) with a single API call. No restarts required.

The result is a system that is simultaneously **operationally boring** (Kafka + OpenSearch are battle-tested infrastructure) and **analytically powerful** (LLM reasoning applied at exactly the right place in the pipeline).

---

## Architecture

### Full pipeline

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AIRS: AI Incident Response System                        │
│                                                                             │
│  External APIs  ─┐                                                          │
│  Log Agents     ─┤──▶  Ingestion Service ──▶  Kafka: logs-topic            │
│  Manual Ingest  ─┘          (port 8001)              │                      │
│                                                       │                      │
│                                            Log Processor (8002)             │
│                                           ╱           │                      │
│                                  OpenSearch       processed-logs-topic       │
│                                  (airs-logs)           │                      │
│                                                       │                      │
│                                            Anomaly Service (8003)           │
│                                                       │                      │
│                                            anomalies-topic                  │
│                                                       │                      │
│                                            Correlation Service (8004)       │
│                                           ╱           │                      │
│                                  OpenSearch       incidents-topic            │
│                               (airs-incidents)         │                      │
│                                                       │                      │
│                     Ollama ───▶  AI Service (8005) ◀──┘                     │
│                  (or OpenAI)      │                                          │
│                                   │                                          │
│                                   ▼                                          │
│                          API Gateway (8000)                                  │
│                         REST + SSE + ChatOps                                 │
│                                   │                                          │
│                                   ▼                                          │
│                          Next.js UI (3000)                                   │
│                                                                             │
│  Prometheus (9090) ◀── /metrics (all services)                              │
│  Grafana (3001) ◀───── Prometheus                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Kafka topic flow

```text
logs-topic ──────────────────────────────────────────────────────────┐
                                                                     │
processed-logs-topic ────────────────────────────────────────────┐  │
                                                                  │  │
anomalies-topic ─────────────────────────────────────────────┐  │  │
                                                              │  │  │
incidents-topic ──────────────────────────────────────────┐  │  │  │
                                                           │  │  │  │
airs-dlq-topic (dead letter queue for all failures) ◀─────┘──┘──┘──┘
```

Every pipeline stage publishes to the DLQ when it cannot process an event:
ingestion-service, log-processor, anomaly-service, correlation-service and
ai-service. api-gateway has no DLQ path by design, because it is a synchronous
REST surface that returns errors to its caller rather than parking them.

**The DLQ is currently write-only.** Events land there with their source topic,
partition, offset and failure reason, and `airs_dlq_published_total` makes the
rate visible, but nothing consumes the topic and there is no drain or
replay-from-DLQ tooling. `POST /v1/admin/replay` replays a topic and offset
range, which is not the same thing. Draining is a manual operation today; see
[`docs/07-runbook.md`](docs/07-runbook.md).

### Service dependency graph

```text
kafka ◀── ingestion-service
kafka ◀── log-processor ◀── opensearch
kafka ◀── anomaly-service
kafka ◀── correlation-service ◀── opensearch
kafka ◀── ai-service ◀── redis, ollama, opensearch
kafka ◀── api-gateway ◀── redis, ai-service, opensearch
```

Redis is a startup and health dependency of ai-service and api-gateway, and
both will report degraded without it, but **nothing reads or writes it yet**.
It is provisioned ahead of the RCA cache and rate-limiting work, not currently
serving either.

### Anomaly detection algorithm

```text
For each processed log event:

  1.  Drop the event if an active suppression window matches its service
  2.  Update the per-(tenant, service) seasonal baseline:
      an EWMA of per-minute event counts held in 24 hour-of-day slots,
      alpha 0.3, tracking mean and variance together
  3.  Compute z-score against the slot for this event's hour:
      (current_minute_count - slot_mean) / sqrt(slot_variance)
      Undefined until that hour-of-day slot has been observed before
  4.  Apply keyword scan: ["error","exception","timeout","connection refused","oom","5xx"]
  5.  Apply level check: level in {error, critical, fatal}
  6.  Apply custom rules: service_pattern + keyword/regex/threshold/composite
  7.  Emit if ANY of: a rule matched, a keyword matched,
      level is error/critical/fatal, or z-score >= anomaly_threshold (2.5)
  8.  Compute confidence_score = f(zscore, keyword count, level, rule boosts)
      Drop if confidence < 0.3 and no rule matched
  9.  anomaly_score = max(zscore, 0.0)
 10.  Severity from level, z-score and critical markers,
      raised to the highest severity among any matching rules
```

Three things worth being precise about, because they are easy to overstate:

- **The baseline is a volume baseline, not an error-rate baseline.** It counts
  every log event for a service, so it detects a change in traffic shape. A
  service that doubles its info-level chatter registers as anomalous.
- **The z-score is not the primary trigger.** Step 7 is a disjunction, so in
  normal operation the keyword and level gates fire far more often than the
  threshold does. `anomaly_threshold` widens the net; it does not gate it.
- **Baselines are in-memory, per process.** They are lost on restart and are
  not shared between replicas, so running two anomaly-service instances gives
  each a partial view and neither the full one. See
  [`docs/05-failure-modes.md`](docs/05-failure-modes.md).

### Correlation algorithm

Correlation works on two axes: a time window per service, and a service graph.

```text
For each anomaly event:

  1.  Look up the in-memory cluster for (tenant_id, service)
  2.  If none, or the anomaly falls outside the open cluster's window
      (window_duration_minutes, default 10, per-service via its data source):
        close the old cluster, open a new one
  3.  If the anomaly's fingerprint is already in the cluster:
        extend the window and stop (dedup)
  4.  Otherwise add it to the cluster
  5.  Emit when the cluster first reaches min_signal_count (default 2),
      or immediately if the anomaly is critical
  6.  Once open, later anomalies amend that same incident:
      timeline appended, severity re-escalated, incident id stable
  7.  Persist to OpenSearch. Republish to incidents-topic on creation and on
      severity escalation, so a growing incident does not cost one RCA
      generation per correlated anomaly
  8.  Clusters idle for longer than their window are closed and flushed
```

Alongside the time window, each new incident is linked into the **service
graph**: correlation-service queries the `airs-topology` index for the
service's neighbours, then looks for an open incident on one of those
neighbours in the last 30 minutes. If it finds one, the new incident is
recorded as its child. That is what turns "five services are all on fire" into
one parent incident with four children.

Three clarifications, since each is easy to assume otherwise:

- **The service graph is operator-configured, not inferred.** You declare
  edges via `PUT /v1/topology`. AIRS does not learn topology from traffic.
- **Suppression windows are applied in anomaly-service, not here.** A
  suppressed signal never becomes an anomaly, so it never reaches correlation.
- **The fingerprint is computed upstream too**, in anomaly-service, as
  `sha1(tenant_id:service:message)`. Correlation consumes it, it does not
  build it.

### RCA generation flow

```text
Incident arrives on incidents-topic
        │
        ▼
Build context:
  - incident metadata (service, severity, timeline)
  - last 5 similar incidents
  - active suppressions
  - service topology edges
  - source metadata
  - top 15 log lines (critical/fatal prioritized)
        │
        ▼
Route by severity:
  critical  ──▶  primary model (qwen2.5:7b-instruct)
  warning   ──▶  low-cost model (rjmalagon/qwen2:1.5b-instruct)
  info      ──▶  deterministic fallback (no LLM call)
        │
        ▼
Generate with retries (8s timeout, 2 retries, exponential backoff)
  │
  ├── success ──▶ enrich with hybrid confidence
  │               (blend LLM confidence + heuristic score)
  │               upsert to OpenSearch
  │
  ├── retries exhausted ──▶ retry once on the low-cost model
  │
  └── still failing, or provider unavailable
                    ──▶ deterministic fallback
                        (template RCA over the incident context)
```

The routing tiers really do differ: `critical` gets a 7B model, `warning` a
1.5B one, and `info` never calls a model at all. The low-cost tier only
applies while the active provider is the one it was configured for; after a
runtime switch to a different provider, `warning` follows the active model
rather than asking that provider for a model name it does not have.

**What the deterministic fallback does and does not buy you.** Every incident
gets an RCA even with no model reachable, including when the provider is
misconfigured. But the fallback is a template over the incident context, not
an analysis: it names the service, counts the anomalies, quotes the top log
lines and suggests generic remediation. It keeps the pipeline whole and the
schema populated. It does not tell you what broke.

It is also not free. With a model unreachable, an incident burns its full
retry budget on both tiers before falling through, which at the default 8s
timeout and 2 retries is roughly 54 seconds. The RCA consumer is sequential,
so `incidents-topic` lag grows for as long as the outage lasts. The pipeline
degrades rather than stalling, and `airs_rca_success_total{path="deterministic"}`
is how you see it happening.

### End-to-end latency

"Real time" is worth being concrete about, because the honest answer is
"depends entirely on severity".

| Segment | Cost |
| --- | --- |
| Each of the four consumer hops | up to ~1s, `getmany(timeout_ms=1000)` |
| Correlation, critical anomaly | emits on arrival |
| Correlation, first two distinct warning signals | emits on the second |
| Correlation, a single isolated warning | waits for the window to close, **default 10 minutes** |
| RCA, model reachable | one inference, 8s timeout |
| RCA, model unreachable | ~54s, then the deterministic fallback |
| UI | 5s poll. The dashboard polls REST and does not consume the SSE endpoint |
| `GET /v1/stream` | 2s poll against OpenSearch, pushed over SSE |

So a critical incident goes from log line to RCA in seconds. A lone warning
that never gets a second distinct signal is not an incident at all until its
correlation window expires. Both behaviours are deliberate, and neither is
what "real time" implies on its own.

Measured throughput and per-stage percentiles, with the hardware and method
stated, are in [`docs/06-evals.md`](docs/06-evals.md).

---

## Features

| Category | Feature |
| --- | --- |
| **Ingestion** | HTTP POST endpoint, external API polling, synthetic load simulation |
| **Processing** | Log normalization, OpenSearch indexing, dead-letter queue |
| **Detection** | per-service seasonal (hour-of-day) EWMA baselines, z-score scoring, keyword scan, configurable detection rules per service |
| **Correlation** | Time-window grouping per service, fingerprint deduplication, parent/child linking across a configured service graph |
| **AI / RCA** | Severity-based model routing, hybrid confidence scoring, deterministic fallback, RCA feedback capture |
| **API** | REST CRUD, cursor pagination, SSE stream (2s poll), schema registry, audit log |
| **Ops** | Prometheus metrics on all services, Grafana dashboards, log/incident retention policies |
| **Multi-tenancy** | `tenant_id` on all pipeline entities, `x-tenant-id` header scoping (scaffolding, not a security boundary: [see below](#multi-tenancy-and-authentication)) |
| **ChatOps** | Slack webhook integration, slash command handler |
| **UI** | Dark-mode Next.js dashboard, incident list + detail, RCA panel, topology view, sources management |

---

## Quickstart

### Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- 8 GB RAM minimum (OpenSearch + Ollama are memory-heavy)
- ~5 GB disk for LLM model weights

### 1. Clone

```bash
git clone https://github.com/Sayantansaha27-tech/AIRS.git
cd AIRS
```

### 2. Configure

```bash
cp .env.example .env
# Optional: add OPENAI_API_KEY=sk-... if you want the OpenAI provider
```

### 3. Start the stack

```bash
docker compose up --build
```

All services start with health-gated dependency ordering: the pipeline will not start consuming until Kafka, OpenSearch, Redis, and Ollama are healthy.

### 4. Pull LLM models (first run only)

```bash
docker exec -it airs-ollama ollama pull qwen2.5:7b-instruct
docker exec -it airs-ollama ollama pull rjmalagon/qwen2:1.5b-instruct
```

### 5. Open the dashboard

| Service | URL | Credentials |
| --- | --- | --- |
| Dashboard UI | <http://localhost:3000> | none |
| API Gateway | <http://localhost:8000> | none |
| Grafana | <http://localhost:3001> | admin / admin |
| Prometheus | <http://localhost:9090> | none |
| OpenSearch | <http://localhost:9200> | none |
| Ollama | <http://localhost:11434> | none |

---

## Usage examples

### Ingest a log event

```bash
curl -X POST http://localhost:8001/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "logs": [
      {
        "timestamp": "2026-03-05T14:00:00Z",
        "service": "payments-service",
        "level": "error",
        "message": "connection refused: postgres-primary:5432"
      }
    ]
  }'
```

`/ingest` always takes a batch. A bare log object is rejected with a 422.
Events that fail normalization are published to the DLQ and the rest of the
batch still proceeds, so a partial success returns the accepted count.

### Manually analyze a log batch

```bash
curl -X POST http://localhost:8000/v1/analyze \
  -H 'Content-Type: application/json' \
  -d '{
    "logs": [
      {
        "timestamp": "2026-03-05T14:00:00Z",
        "service": "orders-service",
        "level": "error",
        "message": "timeout while creating order"
      },
      {
        "timestamp": "2026-03-05T14:00:01Z",
        "service": "orders-service",
        "level": "critical",
        "message": "OOM killer invoked, heap exhausted"
      }
    ]
  }'
```

### Switch the LLM provider at runtime

```bash
# Switch to OpenAI (requires OPENAI_API_KEY in env)
curl -X POST http://localhost:8000/v1/llm/config \
  -H 'Content-Type: application/json' \
  -d '{"provider": "openai", "model": "gpt-4.1-mini"}'

# Switch back to Ollama
curl -X POST http://localhost:8000/v1/llm/config \
  -H 'Content-Type: application/json' \
  -d '{"provider": "ollama", "model": "qwen2.5:7b-instruct"}'
```

### Simulate synthetic load

```bash
curl -X POST http://localhost:8000/v1/ingest/simulate \
  -H 'Content-Type: application/json' \
  -d '{
    "service": "checkout-service",
    "pattern": "connection refused: postgres-primary:5432",
    "level": "error",
    "count": 500,
    "rate_per_second": 50
  }'
```

`service` and `pattern` are both required. The generator emits `count` copies
of `pattern` at `rate_per_second`, so this call holds the request open for
about 10 seconds before responding.

### Stream incidents in real time (SSE)

```bash
curl -N http://localhost:8000/v1/stream
```

---

## Configuration

All pipeline tuning lives in [`config/airs.yaml`](config/airs.yaml). No environment variables needed for local development beyond `OPENAI_API_KEY`.

```yaml
pipeline:
  baseline_window_minutes: 15     # rolling window for z-score baseline
  correlation_window_minutes: 10  # how long anomalies are grouped into one incident
  anomaly_threshold: 2.5          # minimum z-score to emit an anomaly
  dashboard_poll_seconds: 5       # UI polling interval
  source_poll_tick_seconds: 5     # external source polling interval
  log_retention_days: 5           # OpenSearch log index TTL
  incident_retention_days: 30     # OpenSearch incident index TTL
  max_log_payload_kb: 256
  max_manual_analyze_kb: 512

llm:
  provider: ollama                        # or "openai"
  model: qwen2.5:7b-instruct
  fallback_model: rjmalagon/qwen2:1.5b-instruct
  timeout_seconds: 8
  retries: 2
  ollama_base_url: http://ollama:11434
  openai_base_url: https://api.openai.com/v1
```

---

## API Reference

### Health & Observability

| Method | Path | Description |
| --- | --- | --- |
| GET | `/health` | Aggregate health (all dependencies) |
| GET | `/health/live` | Liveness probe |
| GET | `/health/ready` | Readiness probe (503 if any dep unhealthy) |
| GET | `/metrics` | Prometheus metrics |

### Incidents

| Method | Path | Description |
| --- | --- | --- |
| GET | `/v1/incidents` | List incidents (page/size or cursor pagination) |
| GET | `/v1/incidents/{id}` | Get incident by ID |
| PATCH | `/v1/incidents/{id}/status` | Update status |
| POST | `/v1/incidents/{id}/acknowledge` | Acknowledge |
| POST | `/v1/incidents/{id}/resolve` | Resolve |
| POST | `/v1/incidents/{id}/rca/feedback` | Submit RCA feedback |
| POST | `/v1/incidents/{id}/rca/regenerate` | Re-run RCA (optionally with extra logs) |

Query params for list: `page`, `size`, `severity`, `service`, `start_time`, `end_time`, `cursor`, `limit`

### Sources (external log endpoints)

| Method | Path | Description |
| --- | --- | --- |
| GET | `/v1/sources` | List sources |
| POST | `/v1/sources` | Create source |
| PUT | `/v1/sources/{id}` | Update source |
| POST | `/v1/sources/{id}/enable` | Enable |
| POST | `/v1/sources/{id}/disable` | Disable |
| POST | `/v1/sources/{id}/sync` | Force immediate poll |
| DELETE | `/v1/sources/{id}` | Delete source |

### Detection Rules

| Method | Path | Description |
| --- | --- | --- |
| GET | `/v1/rules` | List custom detection rules |
| POST | `/v1/rules` | Create rule |
| PUT | `/v1/rules/{id}` | Update rule |
| DELETE | `/v1/rules/{id}` | Delete rule |
| POST | `/v1/rules/{id}/test-against-history` | Back-test rule against stored logs |

### Suppression Windows

| Method | Path | Description |
| --- | --- | --- |
| GET | `/v1/suppressions` | List windows |
| POST | `/v1/suppressions` | Create window |
| PUT | `/v1/suppressions/{id}` | Update window |
| DELETE | `/v1/suppressions/{id}` | Delete window |

### Topology, ChatOps & Admin

| Method | Path | Description |
| --- | --- | --- |
| GET | `/v1/topology` | Get service dependency graph |
| PUT | `/v1/topology` | Upsert topology edges |
| GET/POST | `/v1/chatops/config` | Get/set ChatOps config |
| POST | `/v1/chatops/commands` | Handle slash commands |
| GET | `/v1/audit` | Audit log |
| GET | `/v1/stream` | SSE real-time incident stream |
| GET | `/v1/schema-registry` | List topic schemas |
| GET | `/v1/schema-registry/{topic}` | Get schema for topic |
| POST | `/v1/llm/config` | Switch LLM provider at runtime |
| POST | `/v1/analyze` | Ad-hoc log batch analysis |
| POST | `/v1/admin/replay` | Replay Kafka offset range |
| POST | `/v1/replay` | Filtered replay |
| POST | `/v1/ingest/simulate` | Synthetic load generator |
| GET | `/v1/webhooks` | List webhooks |
| POST | `/v1/webhooks` | Register webhook |
| PUT | `/v1/webhooks/{id}` | Update webhook |
| DELETE | `/v1/webhooks/{id}` | Remove webhook |

---

## LLM Provider Abstraction

Adding a new LLM provider is a single-file change:

```python
# services/ai-service/app/ai_providers/my_provider.py

from ai_providers.base import BaseLLMProvider


class MyProvider(BaseLLMProvider):
    async def generate(self, prompt: str, context: dict) -> dict:
        # Call your model API here.
        # Must return a dict matching the RCAResult schema:
        # {
        #   "root_cause": str,
        #   "confidence": float (0–1),
        #   "explanation": str,
        #   "suggested_fix": str,
        #   "affected_services": list[str],
        #   "evidence": list[dict]
        # }
        ...
```

Register it in `build_provider()` in `ai-service/app/main.py` and select it via:

```bash
curl -X POST http://localhost:8000/v1/llm/config \
  -d '{"provider": "my_provider", "model": "my-model-name"}'
```

---

## Kafka Topic Contracts

See [`docs/04-integration-contracts.md`](docs/04-integration-contracts.md) for full
schema definitions, the DLQ envelope, the error taxonomy and retry semantics.
Summary:

| Topic | Schema | Producer | Consumers |
| --- | --- | --- | --- |
| `logs-topic` | `NormalizedLogEvent` v1 | ingestion-service | log-processor |
| `processed-logs-topic` | `NormalizedLogEvent` v1 | log-processor | anomaly-service |
| `anomalies-topic` | `AnomalyEvent` v1 | anomaly-service | correlation-service |
| `incidents-topic` | `Incident` v1 | correlation-service | ai-service |
| `airs-dlq-topic` | `DLQEvent` | all services | monitoring |

---

## Observability

Every service exposes `/metrics` in Prometheus format. The Grafana dashboard (auto-provisioned at startup) surfaces:

- **Ingestion rate**, events/sec by service
- **Anomaly rate**, anomalies/sec by severity
- **Incident creation rate**, incidents/min
- **RCA generation latency**, p50/p95/p99 histogram by model
- **RCA path split** (model-generated vs deterministic fallback) per service and severity
- **DLQ depth**, failures by source topic
- **Consumer batch sizes**, Kafka consumer health

Key Prometheus metrics:

```text
airs_rca_generation_duration_seconds{model, severity}
airs_rca_success_total{service, severity, path}   # path = llm | deterministic
airs_rca_failure_total{service, severity}
airs_rca_routed_model_total{model, severity}
airs_dlq_published_total{source_topic}
airs_incidents_created_total{service, severity}
airs_incidents_amended_total{service}
airs_correlation_active_clusters
airs_ai_batch_size
```

The one to watch is `airs_rca_success_total{path="deterministic"}`. Every
incident gets an RCA whether or not a model answered, so a rising fallback
rate is the only signal that the AI path is degraded. A flat failure count
alone will not tell you.

---

## Multi-Tenancy and authentication

**There is no authentication in AIRS.** No API keys, no tokens, no middleware,
no per-route guards. Every endpoint on the API gateway is open to anyone who
can reach the port. This is a local-first demonstration system, and it should
not be exposed to a network you do not control.

Multi-tenancy is **schema-level scaffolding**, and it is worth being exact
about what that does and does not give you:

What exists:

- All pipeline entities (`NormalizedLogEvent`, `AnomalyEvent`, `Incident`)
  carry `tenant_id`, defaulting to `"default"`
- The API gateway reads an `x-tenant-id` header and scopes queries by it
- OpenSearch queries filter on `tenant_id` across every service path
- Anomaly detection baselines, detection rules and suppression windows are
  all keyed per tenant, so tenants do not pollute each other's signal

What that is not:

- **`tenant_id` is not a security boundary.** It is an unverified request
  header. With no authentication behind it, any caller reads or mutates any
  tenant's incidents by changing one header value. It separates tenants from
  each other's *noise*, not from each other's *data*.
- **Tenants share pipeline capacity.** There is one Kafka consumer group per
  service for all tenants, so one tenant's log storm consumes another
  tenant's throughput. There is no per-tenant quota or rate limit.
- **Tenants share indices.** Retention, mapping and reindexing are global
  operations, so per-tenant retention policy is not expressible today.

Getting from here to a multi-tenant SaaS deployment means adding
authentication that establishes tenant identity, deriving `tenant_id` from
that identity rather than from a header, and then partitioning capacity. The
schema work is genuinely done. The boundary work has not been started.

---

## Repository Structure

```text
.
├── config/
│   └── airs.yaml                     # Central pipeline + LLM configuration
├── docs/
│   ├── 00-problem.md                 # The incident this system exists for
│   ├── 01-scope-and-non-goals.md     # Limits, including no auth. Read first
│   ├── 02-architecture.md            # Per-service reference
│   ├── 03-decisions.md               # ADRs with rejected options
│   ├── 04-integration-contracts.md   # Topics, schemas, errors, retries
│   ├── 05-failure-modes.md           # What breaks, blast radius, mitigation
│   ├── 06-evals.md                   # Measured throughput, latency, RCA quality
│   ├── 07-runbook.md                 # Install, upgrade, rollback, triage
│   ├── 08-handoff.md                 # Operating this without its author
│   ├── 09-postmortem.md              # What would be done differently
│   └── architecture.mmd              # Mermaid source for the pipeline diagram
├── infra/
│   ├── Dockerfile.python             # Shared Python service base image
│   └── monitoring/
│       ├── prometheus.yml            # Prometheus scrape config
│       └── grafana/                  # Provisioned dashboards + datasources
├── services/
│   ├── ingestion-service/app/        # Log intake + external source polling
│   ├── log-processor/app/            # Normalization + OpenSearch indexing
│   ├── anomaly-service/app/          # z-score + rule-based detection
│   ├── correlation-service/app/      # Incident grouping + deduplication
│   ├── ai-service/app/               # RCA generation + LLM providers
│   │   ├── ai_providers/             # base.py, ollama_provider.py, openai_provider.py
│   │   └── rca.py                    # Prompt builder + deterministic fallback
│   └── api-gateway/app/              # REST API + SSE + ChatOps + admin
├── shared/
│   └── airs_shared/                  # Pydantic models, Kafka helpers, settings
│       ├── models.py                 # All shared data models
│       ├── settings.py               # Config loader (pydantic-settings + YAML)
│       ├── kafka.py                  # AIOKafka producer/consumer helpers
│       ├── opensearch.py             # Client + index management
│       ├── normalize.py              # Log normalization
│       ├── dlq.py                    # Dead-letter queue payload builder
│       ├── monitoring.py             # Prometheus response helper
│       └── schema_registry.py       # Built-in topic schema registry
├── ui/                               # Next.js + Tailwind frontend
├── tests/                            # Pytest suite
├── docker-compose.yml                # Full stack (with healthchecks)
├── pyproject.toml                    # Python project + ruff/mypy config
├── requirements.txt                  # Pinned Python dependencies
├── .env.example                      # Environment variable template
└── Makefile                          # Convenience targets
```

---

## Development

### Backend

```bash
# Install deps
pip install -r requirements.txt

# Lint
ruff check .

# Format
ruff format .

# Type check (advisory)
mypy shared

# Tests
pytest
```

### Frontend

```bash
cd ui
npm install
npm run build   # or: npm run dev
```

### Run a single service against Docker infra

```bash
# Start only infra
docker compose up kafka opensearch redis ollama -d

# Run ai-service locally with hot reload
AIRS_CONFIG_FILE=config/airs.yaml PYTHONPATH=shared \
  uvicorn services/ai-service/app/main:app --reload --port 8005
```

---

## Design Decisions

### Why Kafka instead of a message queue or direct HTTP?

HTTP between services creates tight coupling and backpressure problems at scale. A message queue (RabbitMQ/SQS) would work but loses the replayability and topic compaction properties that make debugging incident pipelines tractable. Kafka gives us durable, ordered, replayable streams, and the DLQ pattern for free.

### Why OpenSearch instead of Elasticsearch or Postgres?

Incident data is fundamentally document-oriented and time-series-heavy. Full-text search over log messages and time-range queries on incidents are first-class operations. OpenSearch is freely licensed, self-hostable, and the API is identical to Elasticsearch if you need to swap.

### Why a deterministic RCA fallback?

LLM calls fail. Models time out. API keys expire. A system that falls over silently when the AI is unavailable is worse than no AI at all. The deterministic fallback ensures every incident always has a machine-generated RCA. It may be less insightful, but it is always present and always consistent.

### Why severity-based model routing?

Inference cost and latency are real. Running the largest model on every `info`-level incident is wasteful. Routing `critical` incidents to the capable model and `info` incidents to the deterministic path is a pragmatic cost/quality tradeoff that keeps the system fast under load.

### Why a shared `airs_shared` library?

Microservices that don't share a contract layer drift. Each service having its own Pydantic models for `Incident` is a bug waiting to happen. The shared library is the single source of truth for all data contracts, settings loading, and Kafka helpers.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions, code style, and the PR process.

---

## License

[Apache-2.0](LICENSE), free to use, modify, and deploy, with an explicit patent grant. See [NOTICE](NOTICE) for attribution requirements.

# AIRS — AI Incident Response System

> **An event-driven, Kafka-native incident intelligence platform that ingests raw logs, detects anomalies, correlates them into incidents, and generates AI-powered Root Cause Analysis — all in real time, all locally runnable.**

[![CI](https://github.com/Sayantansaha27-tech/AIRS/actions/workflows/ci.yml/badge.svg)](https://github.com/Sayantansaha27-tech/AIRS/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-yellow.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-compose-blue)](docker-compose.yml)

---

## Why AIRS?

### The problem every systems builder has hit

You're running a fleet of services. Something breaks at 2 AM. You open Grafana and see a spike. You open Kibana and see 10,000 error lines. You open Slack and see five engineers all looking at different logs, each with a different theory. Forty minutes pass before anyone agrees on a root cause — a cascading timeout from a single DB pool exhaustion.

**The raw material for diagnosing that incident was always there.** It was sitting in your logs. What was missing was a system that could:

1. Watch all of it continuously
2. Know what "anomalous" means for each service
3. Group related signals before the noise drowns them
4. Reason about root causes the way a senior engineer would — but at machine speed

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
   ▼  ai-service (RCA generation — Ollama or OpenAI)
OpenSearch: airs-incidents
   │
   ▼  api-gateway (REST + SSE + ChatOps)
   │
   ▼  Next.js dashboard
```

This means:

- **Every stage is independently scalable.** The anomaly detector can lag without blocking ingestion. The AI service can be slow without stalling correlation.
- **The AI is opt-out, not opt-in.** Incidents always exist. RCA is async enrichment on top of them. A model timeout never breaks your incident pipeline.
- **You own the data.** Default setup is fully local: Kafka, OpenSearch, and Ollama running in Docker. No data leaves your machine unless you choose OpenAI mode.
- **The provider is a runtime detail.** Swap Ollama for OpenAI — or back — with a single API call. No restarts required.

The result is a system that is simultaneously **operationally boring** (Kafka + OpenSearch are battle-tested infrastructure) and **analytically powerful** (LLM reasoning applied at exactly the right place in the pipeline).

---

## Architecture

### Full pipeline

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AIRS — AI Incident Response System                       │
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
│                  (or OpenAI)      │  Redis cache                             │
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

### Service dependency graph

```text
kafka ◀── ingestion-service
kafka ◀── log-processor ◀── opensearch
kafka ◀── anomaly-service
kafka ◀── correlation-service ◀── opensearch
kafka ◀── ai-service ◀── redis, ollama, opensearch
kafka ◀── api-gateway ◀── redis, ai-service, opensearch
```

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

```text
For each anomaly event:

  1.  Check active suppression windows (fnmatch against service_pattern)
  2.  If suppressed → drop
  3.  Look up open incidents for this service within correlation_window (10 min)
  4.  Compute fingerprint = hash(service + sorted_reasons)
  5.  If duplicate fingerprint in window → skip (dedup)
  6.  If existing open incident → append anomaly to timeline + re-escalate severity
  7.  If no existing incident → create new incident
  8.  Persist to OpenSearch + emit to incidents-topic
```

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
  warning   ──▶  fallback model (rjmalagon/qwen2:1.5b-instruct)
  info      ──▶  deterministic fallback (no LLM call)
        │
        ▼
Generate with retries (8s timeout, 2 retries, exponential backoff)
  │
  ├── success ──▶ enrich with hybrid confidence
  │               (blend LLM confidence + heuristic score)
  │               upsert to OpenSearch
  │
  └── all retries exhausted ──▶ deterministic fallback
                                (keyword-based RCA, never fails)
```

---

## Features

| Category | Feature |
| --- | --- |
| **Ingestion** | HTTP POST endpoint, external API polling, synthetic load simulation |
| **Processing** | Log normalization, OpenSearch indexing, dead-letter queue |
| **Detection** | per-service seasonal (hour-of-day) EWMA baselines, z-score scoring, keyword scan, configurable detection rules per service |
| **Correlation** | Time-window grouping, fingerprint deduplication, suppression windows |
| **AI / RCA** | Severity-based model routing, hybrid confidence scoring, deterministic fallback, feedback loop |
| **API** | REST CRUD, cursor pagination, SSE stream, schema registry, audit log |
| **Ops** | Prometheus metrics on all services, Grafana dashboards, log/incident retention policies |
| **Multi-tenancy** | `tenant_id` on all pipeline entities, `x-tenant-id` header scoping |
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

All services start with health-gated dependency ordering — the pipeline will not start consuming until Kafka, OpenSearch, Redis, and Ollama are healthy.

### 4. Pull LLM models (first run only)

```bash
docker exec -it airs-ollama ollama pull qwen2.5:7b-instruct
docker exec -it airs-ollama ollama pull rjmalagon/qwen2:1.5b-instruct
```

### 5. Open the dashboard

| Service | URL | Credentials |
| --- | --- | --- |
| Dashboard UI | <http://localhost:3000> | — |
| API Gateway | <http://localhost:8000> | — |
| Grafana | <http://localhost:3001> | admin / admin |
| Prometheus | <http://localhost:9090> | — |
| OpenSearch | <http://localhost:9200> | — |
| Ollama | <http://localhost:11434> | — |

---

## Usage examples

### Ingest a log event

```bash
curl -X POST http://localhost:8001/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "timestamp": "2026-03-05T14:00:00Z",
    "service": "payments-service",
    "level": "error",
    "message": "connection refused: postgres-primary:5432"
  }'
```

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
        "message": "OOM killer invoked — heap exhausted"
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
  -d '{"service": "checkout-service", "count": 500, "error_rate": 0.4}'
```

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

See [`docs/contracts.md`](docs/contracts.md) for full schema definitions. Summary:

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

- **Ingestion rate** — events/sec by service
- **Anomaly rate** — anomalies/sec by severity
- **Incident creation rate** — incidents/min
- **RCA generation latency** — p50/p95/p99 histogram by model
- **RCA success/failure ratio** — per service + severity
- **DLQ depth** — failures by source topic
- **Consumer batch sizes** — Kafka consumer health

Key Prometheus metrics:

```text
airs_rca_generation_duration_seconds{model, severity}
airs_rca_success_total{service, severity}
airs_rca_failure_total{service, severity}
airs_rca_routed_model_total{model, severity}
airs_dlq_published_total{source_topic}
airs_ai_batch_size
```

---

## Multi-Tenancy

AIRS includes multi-tenant groundwork:

- All pipeline entities (`NormalizedLogEvent`, `AnomalyEvent`, `Incident`) carry `tenant_id` (default: `"default"`)
- API Gateway accepts `x-tenant-id` header to scope all queries
- OpenSearch queries filter by `tenant_id` in all service paths

This means the schema and infrastructure are ready for a multi-tenant SaaS deployment — you only need to add authentication middleware and per-tenant Kafka consumer groups.

---

## Repository Structure

```text
.
├── config/
│   └── airs.yaml                     # Central pipeline + LLM configuration
├── docs/
│   ├── architecture.mmd              # Mermaid architecture diagram
│   └── contracts.md                  # Kafka topic schema contracts
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
├── samples/
│   └── logs.jsonl                    # Demo log dataset
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

HTTP between services creates tight coupling and backpressure problems at scale. A message queue (RabbitMQ/SQS) would work but loses the replayability and topic compaction properties that make debugging incident pipelines tractable. Kafka gives us durable, ordered, replayable streams — and the DLQ pattern for free.

### Why OpenSearch instead of Elasticsearch or Postgres?

Incident data is fundamentally document-oriented and time-series-heavy. Full-text search over log messages and time-range queries on incidents are first-class operations. OpenSearch is freely licensed, self-hostable, and the API is identical to Elasticsearch if you need to swap.

### Why a deterministic RCA fallback?

LLM calls fail. Models time out. API keys expire. A system that falls over silently when the AI is unavailable is worse than no AI at all. The deterministic fallback ensures every incident always has a machine-generated RCA — it may be less insightful, but it is always present and always consistent.

### Why severity-based model routing?

Inference cost and latency are real. Running the largest model on every `info`-level incident is wasteful. Routing `critical` incidents to the capable model and `info` incidents to the deterministic path is a pragmatic cost/quality tradeoff that keeps the system fast under load.

### Why a shared `airs_shared` library?

Microservices that don't share a contract layer drift. Each service having its own Pydantic models for `Incident` is a bug waiting to happen. The shared library is the single source of truth for all data contracts, settings loading, and Kafka helpers.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions, code style, and the PR process.

---

## License

[Apache-2.0](LICENSE) — free to use, modify, and deploy, with an explicit patent grant. See [NOTICE](NOTICE) for attribution requirements.

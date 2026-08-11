# 07: Runbook

Operating procedures. Every command here is meant to be run as written.

**Before anything else:** AIRS has no authentication. Every procedure assumes
you are on a network you control. See
[01-scope-and-non-goals.md](01-scope-and-non-goals.md).

---

## Install

### Requirements

- Docker Desktop or Docker Engine with the Compose plugin
- 8 GB RAM minimum. OpenSearch alone takes most of it
- ~6 GB disk for model weights

### First run

```bash
git clone https://github.com/Sayantansaha27-tech/AIRS.git
cd AIRS
cp .env.example .env
docker compose up --build -d
```

Every service is gated on the health of what it touches at startup, so the
pipeline will not begin consuming until Kafka, OpenSearch, Redis and Ollama
report healthy. Expect two to three minutes on a cold start, dominated by
OpenSearch.

Watch it come up:

```bash
docker compose ps
```

### Pull the models

Nothing works end to end until the models exist. Ollama starts empty.

```bash
docker exec airs-ollama ollama pull qwen2.5:7b-instruct
docker exec airs-ollama ollama pull rjmalagon/qwen2:1.5b-instruct
```

Until then, RCA falls through to the deterministic path on every incident,
which is correct behaviour and is exactly what
`airs_rca_success_total{path="deterministic"}` is for.

### Verify

```bash
curl -s localhost:8000/health | jq
```

Then push an event through the whole pipeline:

```bash
curl -X POST localhost:8001/ingest \
  -H 'Content-Type: application/json' \
  -d '{"logs":[{"service":"smoke-test","level":"critical","message":"connection refused: postgres-primary:5432"}]}'

sleep 20
curl -s "localhost:8000/v1/incidents?service=smoke-test" | jq '.items[0] | {id, severity, rca: .rca.root_cause}'
```

A critical anomaly emits an incident on arrival, so this should return within
seconds. If `rca` is null, ai-service has not caught up yet or the models are
not pulled.

---

## Release gate: the chaos test

Run before publishing any change that touches ai-service, correlation-service
or the RCA path. It is not in CI because it needs the full stack and stops
containers as part of the test.

```bash
docker compose up -d
AIRS_CHAOS=1 pytest tests/chaos -v
```

It asserts that a stopped ai-service does not stall ingestion, does not fill
the DLQ, still produces incidents, and that recovery enriches the queued
incidents without duplicating them. If it fails, do not ship.

---

## Upgrade

Stages are independent consumer groups, so they can be replaced one at a time.
Order matters only in that you want the consumer of a topic upgraded before
its producer if the schema changed.

```bash
git pull
docker compose build
docker compose up -d --no-deps log-processor
docker compose ps log-processor
docker compose up -d --no-deps anomaly-service
# ... and so on
```

**Drain gracefully.** `docker compose up -d` sends SIGTERM and waits, which
lets correlation-service flush its open clusters. Never `kill -9` a service
during an upgrade: see the data-loss note under Rollback.

**Watch for after an upgrade:**

- `airs_dlq_published_total` rising means the new version is rejecting events
  the old one accepted. Usually a contract change.
- Consumer group lag climbing on the upgraded stage's topic.

---

## Rollback

```bash
git checkout <previous-tag>
docker compose build
docker compose up -d
```

**What rollback does not undo.** Incidents already written to OpenSearch stay
written, in the shape the newer version wrote them. If the upgrade changed the
incident schema, roll the index forward or reindex rather than assuming the
old code can read new documents.

**Data loss during an ungraceful restart.** Consumers use Kafka auto-commit,
so offsets advance on a timer regardless of whether processing finished. A
`kill -9` mid-batch loses the in-flight records permanently, and they will not
appear in the DLQ because they never failed. Always stop gracefully. Full
explanation in [05-failure-modes.md](05-failure-modes.md).

---

## Backup

Everything durable is in OpenSearch. Kafka holds only in-flight data.

### Register a snapshot repository (once)

```bash
docker exec airs-opensearch mkdir -p /usr/share/opensearch/snapshots

curl -X PUT "localhost:9200/_snapshot/airs_backup" \
  -H 'Content-Type: application/json' \
  -d '{"type":"fs","settings":{"location":"/usr/share/opensearch/snapshots"}}'
```

`path.repo` must include that directory in the OpenSearch config, otherwise
registration fails with a repository verification error.

### Take a snapshot

```bash
curl -X PUT "localhost:9200/_snapshot/airs_backup/snap-$(date +%Y%m%d-%H%M%S)?wait_for_completion=true" \
  -H 'Content-Type: application/json' \
  -d '{"indices":"airs-*","include_global_state":false}'
```

### Restore

```bash
curl -X POST "localhost:9200/airs-incidents/_close"

curl -X POST "localhost:9200/_snapshot/airs_backup/<snapshot-name>/_restore" \
  -H 'Content-Type: application/json' \
  -d '{"indices":"airs-incidents"}'
```

### What to back up

| Index | Back up? | Why |
| --- | --- | --- |
| `airs-incidents` | **Yes** | The product of the whole pipeline |
| `airs-rules` | **Yes** | Operator-authored, not reproducible |
| `airs-suppressions` | **Yes** | Operator-authored |
| `airs-topology` | **Yes** | Operator-authored |
| `airs-sources` | **Yes** | Contains auth tokens. Treat the snapshot as a secret |
| `airs-rca-feedback` | Yes | Small, and the only human judgement in the system |
| `airs-audit` | Depends | Compliance question, not a technical one |
| `airs-logs` | No | High volume, 5 day retention, reproducible from source |

---

## On-call triage

### Start here

```bash
curl -s localhost:8000/health | jq          # which dependency is unhappy
docker compose ps                            # what is not running
```

Then the two metrics that localise most problems:

```bash
# Is anything dead-lettering, and from where?
for p in 8001 8002 8003 8004 8005; do
  echo "--- :$p"; curl -s localhost:$p/metrics | grep '^airs_dlq_published_total'
done

# Is RCA running on the model or on the fallback?
curl -s localhost:8005/metrics | grep '^airs_rca_success_total'
```

### Symptom: no incidents are being created

Work the pipeline forwards, one stage at a time.

```bash
curl -s localhost:8001/metrics | grep '^airs_logs_ingested_total'    # arriving?
curl -s localhost:8002/metrics | grep '^airs_logs_processed_total'   # forwarded?
curl -s localhost:8003/metrics | grep '^airs_anomalies_emitted_total' # detected?
curl -s localhost:8003/metrics | grep '^airs_anomalies_suppressed_total'
curl -s localhost:8004/metrics | grep '^airs_incidents_created_total'
```

The first counter that is flat is your stage. Common causes:

- **Ingested but not processed:** log-processor cannot reach OpenSearch. Check
  disk, below.
- **Processed but nothing detected:** everything is genuinely below threshold,
  or a broad suppression window is active. Check
  `airs_anomalies_suppressed_total{reason="suppression_window"}` and
  `GET /v1/suppressions`.
- **Detected but no incident:** normal for isolated warnings. A single warning
  waits for its correlation window (default 10 minutes) before becoming an
  incident. Check `airs_correlation_active_clusters` is non-zero.

### Symptom: incidents exist but RCA is null

```bash
curl -s localhost:8005/health | jq '.dependencies'
docker exec airs-ollama ollama list
curl -s localhost:8005/metrics | grep -E '^airs_rca_(success|failure)_total'
```

`llm: false` with the models absent is the usual answer. Pull them.

If RCA is being produced but always deterministic, the model is reachable but
failing: check ai-service logs for validation failures, and consider routing to
the larger model.

### Symptom: everything is dead-lettering at once

Almost always OpenSearch out of disk.

```bash
curl -s "localhost:9200/_cat/allocation?v"
curl -s "localhost:9200/_cat/indices?v&s=store.size:desc"
```

Recovery:

```bash
# 1. Reclaim space
curl -X DELETE "localhost:9200/airs-logs"

# 2. Clear the read-only block OpenSearch applied at the flood watermark
curl -X PUT "localhost:9200/_all/_settings" \
  -H 'Content-Type: application/json' \
  -d '{"index.blocks.read_only_allow_delete": null}'

# 3. Confirm writes work again
curl -s localhost:8002/metrics | grep '^airs_logs_processed_total'
```

Note the dashboard keeps looking healthy throughout this failure, because
reads continue working while writes fail. Do not use the UI to decide whether
this is happening.

### Symptom: RCA is minutes behind

Expected while a model is unreachable: an incident burns its full retry budget
on both tiers, roughly 54 seconds, before falling through. To shed the backlog
immediately, force the deterministic path:

```bash
curl -X POST localhost:8000/v1/llm/config \
  -H 'Content-Type: application/json' \
  -d '{"provider":"ollama","model":"deterministic-only-not-a-real-model"}'
```

That fails fast into the fallback rather than waiting on timeouts. Set it back
when the model is healthy.

### Symptom: one service is producing hundreds of incidents

Check whether grouping is working:

```bash
curl -s localhost:8003/metrics | grep '^airs_anomalies_emitted_total'
curl -s localhost:8004/metrics | grep -E '^airs_incidents_(created|amended)_total'
```

Incidents tracking anomalies roughly one-to-one means deduplication is not
happening. The usual cause is fingerprint over-specificity: the service embeds
a unique id in each message, so no two messages share a fingerprint. Confirm by
reading an incident timeline. Mitigate with a suppression window, then fix the
log format upstream.

---

## Routine operations

### Create a maintenance window

**Create it at least a minute before the work starts.** anomaly-service polls
for suppression changes every 30 seconds, so a window created at the moment
maintenance begins will miss its first anomalies.

```bash
curl -X POST localhost:8000/v1/suppressions \
  -H 'Content-Type: application/json' \
  -d '{
    "service_pattern": "payments-*",
    "reason": "planned database failover",
    "starts_at": "2026-08-11T01:00:00Z",
    "ends_at": "2026-08-11T03:00:00Z"
  }'
```

### Add a detection rule, and back-test it first

```bash
curl -X POST localhost:8000/v1/rules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "OOM detector",
    "service_pattern": "orders-*",
    "match_type": "regex",
    "pattern": "OOM|OutOfMemory",
    "severity": "critical",
    "confidence_boost": 0.4
  }'

curl -X POST localhost:8000/v1/rules/<rule-id>/test-against-history \
  -H 'Content-Type: application/json' -d '{"limit": 5000}'
```

Back-test before enabling anything broad. A rule matching a common substring
turns every log line into a critical anomaly.

### Declare service topology

Parent/child incident linking does nothing until you declare the graph.

```bash
curl -X PUT localhost:8000/v1/topology \
  -H 'Content-Type: application/json' \
  -d '{"edges":[
    {"upstream":"orders-service","downstream":"postgres-primary","dependency_type":"db"},
    {"upstream":"checkout-service","downstream":"inventory-service","dependency_type":"sync"}
  ]}'
```

### Drain the DLQ

There is no tooling for this. The DLQ is write-only, and draining is manual:

```bash
docker exec airs-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic airs-dlq-topic --from-beginning --max-messages 100
```

Inspect `failure_reason`, fix the cause, and re-ingest the `payload` field
through `POST /ingest` if the events still matter. Recorded as a gap in
[01-scope-and-non-goals.md](01-scope-and-non-goals.md).

### Switch LLM provider without a restart

```bash
curl -X POST localhost:8000/v1/llm/config \
  -H 'Content-Type: application/json' \
  -d '{"provider":"openai","model":"gpt-4.1-mini"}'
```

Requires `OPENAI_API_KEY` in the environment. If it is missing, RCA falls back
to the deterministic path rather than failing, so check the fallback rate to
confirm the switch actually took.

---

## Shutdown

```bash
docker compose stop     # graceful, flushes correlation clusters
docker compose down     # also removes containers
docker compose down -v  # also deletes all data. There is no undo
```

---

Previous: [06: Evals](06-evals.md) | Next: [08: Handoff](08-handoff.md)

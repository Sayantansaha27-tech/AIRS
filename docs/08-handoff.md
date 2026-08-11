# 08: Handoff

What you need to run AIRS without the person who built it.

Read [07-runbook.md](07-runbook.md) for procedures. This document is for
context: the things that are obvious once someone tells you and puzzling until
they do.

---

## The one-paragraph version

Logs enter through ingestion-service and travel through four Kafka topics.
Each stage is a separate consumer group, so any stage can lag or fail without
stopping the one in front of it. anomaly-service decides what is unusual for
each service, correlation-service groups related anomalies into incidents and
writes them to OpenSearch, and ai-service adds a root-cause analysis
afterwards. **The incident is written before the RCA exists**, which is why a
model outage costs you an explanation and never an incident.

---

## Five things that will confuse you

### 1. RCA arriving late is normal, not broken

RCA is enrichment applied after the incident is already durable. An incident
with `rca: null` is a correct intermediate state, not a failure. Under a model
outage it can stay null for minutes while the queue drains.

The metric that tells you whether the AI path is healthy is **not** the failure
count:

```
airs_rca_success_total{path="deterministic"}
```

Every incident gets an RCA either way, so a total model outage produces a
flawless-looking success rate. The `path` label is the only thing that
distinguishes reasoning from a template.

### 2. A single warning takes ten minutes to become an incident

This surprises everyone. A critical anomaly emits an incident on arrival. A
warning waits until a second **distinct** signal arrives for the same service,
or until its correlation window expires, which defaults to 10 minutes.

So "I sent one error log and nothing happened" is expected behaviour. Send a
critical, or send two different messages.

### 3. Config changes are not immediate

Everything operator-authored propagates by polling:

| Change | Takes effect within |
| --- | --- |
| Detection rules | 30s |
| Suppression windows | 30s |
| Data sources (ingestion) | 5s |
| Data sources (correlation) | 60s |

The one that bites is suppression. A maintenance window created at the moment
maintenance starts will miss its first anomalies. Create it a minute early.

### 4. Two services keep state in memory and do not scale horizontally

anomaly-service holds per-service baselines. correlation-service holds open
incident clusters. Neither is shared or persisted.

Consequences you will eventually meet:

- **Do not run two replicas of either.** They will each see half the stream and
  neither will have a complete picture.
- A restart loses correlation clusters unless the shutdown was graceful.
- A restart loses anomaly baselines entirely, and they take an hour of wall
  clock per hour-of-day slot to relearn. Detection still works during that
  window, because the keyword and level gates do not depend on the baseline.

### 5. The dashboard lies when OpenSearch is full

Reads keep working while writes fail, so the UI shows the last known state and
gives no indication that nothing new is being stored. Never use the dashboard
to decide whether ingestion is healthy. Use `airs_dlq_published_total`.

---

## What will page you, ranked by likelihood

**1. OpenSearch out of disk.** The most likely real failure. Log volume against
a 5 day retention that silently no-ops if the ISM plugin is absent. Symptom is
DLQ growth across every topic at once. Procedure in the runbook.

**2. Models not pulled.** After any fresh deploy or a wiped Ollama volume, RCA
silently degrades to deterministic. Not an outage, but the system stops
providing its main value. `docker exec airs-ollama ollama list`.

**3. A log format that defeats templatizing.** Messages are masked before
fingerprinting, but a format that varies in a way the patterns do not cover
still splits one signal into many. Symptom is one service producing far more
incidents than usual with near-identical timelines.

**4. A detection rule matching too broadly.** Someone adds a keyword rule for
"error". Everything becomes a critical anomaly. Always back-test with
`POST /v1/rules/{id}/test-against-history` before enabling.

---

## Things that are deliberately missing

Do not spend a day looking for these. They are not hidden, they do not exist,
and [01-scope-and-non-goals.md](01-scope-and-non-goals.md) explains why.

- **Authentication.** None, anywhere. `tenant_id` is an unverified header.
- **Automatic DLQ recovery.** `tools/dlq.py` drains it when you ask; nothing
  drains it on its own, because retrying a permanently bad event forever is
  worse than leaving it.
- **Exactly-once delivery.** You get at-least-once: a crash replays, and
  correlation deduplicates the replay.
- **An RCA cache in Redis.** Redis now holds correlation deduplication keys,
  but nothing caches RCA results.
- **Learning from feedback.** Corrections are retrieved into the RCA context
  for later incidents on the same service. Nothing trains or tunes.
- **Topology inference.** The service graph is declared, not learned.

---

## Where to change things

| You want to | Go to |
| --- | --- |
| Change what counts as anomalous | `services/anomaly-service/app/main.py`, `should_emit` and `derive_severity` |
| Change incident grouping | `services/correlation-service/app/main.py`, `process_anomaly` |
| Change the RCA prompt | `services/ai-service/app/rca.py`, `PROMPT_TEMPLATE` |
| Change model routing | `services/ai-service/app/main.py`, `select_model_for_context` |
| Add an LLM provider | `services/ai-service/app/ai_providers/`, then `build_provider` |
| Change a data contract | `shared/airs_shared/models.py`. **This is shared by all six services** |
| Tune windows and thresholds | `config/airs.yaml` |
| Add an API route | `services/api-gateway/app/main.py` |

**The one rule:** anything in `shared/airs_shared/models.py` is a wire contract.
Changing a field there changes what every service produces and consumes, and
`produce_json` validates against it at the produce boundary. Add optional
fields freely; removing or renaming one needs every stage redeployed together.

---

## Verifying a change

```bash
pytest                                  # 146 tests, no infrastructure needed
ruff check . && ruff format --check .
```

Before shipping anything touching the RCA path, correlation, or ai-service:

```bash
docker compose up -d
AIRS_CHAOS=1 pytest tests/chaos -v
python evals/run_eval.py --llm --model qwen2.5:7b-instruct
```

The chaos suite is the gate that protects the claim the whole system rests on:
that the pipeline does not depend on model availability. The eval harness
tells you whether a prompt or model change made RCA quality better or worse,
which is otherwise unknowable by reading diffs.

---

## First week

**Day one.** Bring the stack up, pull the models, push a log through with the
smoke test in the runbook, and watch it arrive as an incident with an RCA.

**Day two.** Break it deliberately. Stop ai-service and watch incidents keep
being created. Fill a disk and watch everything dead-letter while the
dashboard keeps looking fine. Both failures are documented in
[05-failure-modes.md](05-failure-modes.md); seeing them is worth more than
reading about them.

**Day three.** Declare your real service topology and write one detection rule
for a failure your team actually has. Back-test it. This is where AIRS stops
being a demo.

**By the end of the week**, read [09-postmortem.md](09-postmortem.md). It is
the shortest document and the most honest one about what is weak.

---

Previous: [07: Runbook](07-runbook.md) | Next: [09: Postmortem](09-postmortem.md)

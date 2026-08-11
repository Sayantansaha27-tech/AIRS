# 01: Scope and non-goals

This document exists so that nobody has to reverse-engineer the boundaries
from the code. Read the non-goals first. They are more informative than the
goals, and they are where an evaluation of this system should start.

---

## Read this first: what AIRS is not safe for

### There is no authentication. None.

Not weak authentication. Not authentication that needs hardening. There is no
authentication anywhere in AIRS.

The API gateway exposes 41 routes. None of them check a key, a token, a
session or a header. Anyone who can open a TCP connection to port 8000 can
read every incident, mutate every incident, register a webhook pointing
anywhere, create a data source with an embedded auth token, and trigger a
synthetic load generator.

**Do not expose this to a network you do not control.** It is designed to run
on a laptop or inside a private network segment, and that is the only
deployment shape it is correct for.

### Multi-tenancy is scaffolding, not a boundary

`tenant_id` is threaded through every entity in the pipeline, and OpenSearch
queries genuinely filter on it. Detection rules, suppression windows and
anomaly baselines are all keyed per tenant. That work is real and it is done.

But tenant identity arrives as an unverified `x-tenant-id` request header.
With no authentication behind it, changing one header value moves you between
tenants. So:

> `tenant_id` separates tenants from each other's **noise**. It does not
> separate them from each other's **data**.

That distinction is the whole thing. The scaffolding means a future
authenticated deployment would not need a data migration, which is a genuine
saving. It does not mean the system is one middleware away from
multi-tenant-safe. Deriving `tenant_id` from an authenticated identity instead
of a header is the actual work, and it has not been started.

Three further gaps, so this is not read as a single missing feature:

- **No capacity isolation.** One Kafka consumer group per service serves all
  tenants. A single tenant's log storm consumes every other tenant's pipeline
  throughput. There is no per-tenant quota or rate limit.
- **No index isolation.** All tenants share indices, so retention, mapping
  changes and reindexing are global operations. Per-tenant retention policy is
  not expressible.
- **No tenant lifecycle.** There is no create, suspend or delete for a tenant,
  and no way to purge one tenant's data without a hand-written query.

### The DLQ is write-only

Every pipeline stage parks unprocessable events on `airs-dlq-topic` with the
source topic, partition, offset and failure reason. Nothing consumes that
topic. There is no drain tool, no replay-from-DLQ path, and no alert on depth
beyond the raw counter.

`POST /v1/admin/replay` replays a topic and offset range. That is a different
operation and it is not a substitute.

So the DLQ prevents silent data loss and gives you a place to look. It does
not give you recovery. Draining is a manual operation, documented in
[07-runbook.md](07-runbook.md).

### RCA quality is not guaranteed, and cannot be

The system always produces an RCA. It does not always produce a *correct* one,
and it has no way to know the difference at generation time.

Two failure shapes matter:

- A small model confidently proposing a plausible, wrong cause. Every RCA
  carries its evidence for exactly this reason: the reader is meant to be able
  to disagree in seconds.
- The deterministic fallback, which is a template over the incident context,
  not an analysis. It names the service, counts anomalies and quotes log
  lines. It keeps the pipeline whole. It does not tell you what broke, and it
  should never be mistaken for something that does.

Measured accuracy per incident class, and an explicit list of what is not
measured, are in [06-evals.md](06-evals.md).

---

## What AIRS is for

A single deployment, operated by the team that owns it, that turns a log
stream into grouped incidents with a first-draft root cause attached, and does
so without depending on a model being available.

Concretely, in scope and working:

| Area | What it does |
| --- | --- |
| Ingestion | HTTP batch intake, polling of external log endpoints, synthetic load generation |
| Normalization | Arbitrary JSON or plain strings into one log envelope, with unprocessable events dead-lettered |
| Detection | Per-service, per-hour-of-day EWMA baselines, keyword and level rules, operator-defined detection rules with back-testing |
| Correlation | Time-window grouping per service, fingerprint dedup, parent/child linking across an operator-declared service graph |
| RCA | Severity-routed model selection, hybrid confidence, deterministic fallback that never depends on model availability |
| Suppression | Time-bounded windows matched by service glob, applied before anomalies are emitted |
| Operations | Prometheus metrics on every service, provisioned Grafana dashboards, index retention policies, audit log |
| Delivery | REST, SSE, outbound webhooks, Slack ChatOps with slash commands |

---

## Explicit non-goals

These are decisions, not gaps. Each one was cheaper to decline than to do
badly.

**1. AIRS does not replace your observability stack.** It does not want to be
Grafana, Datadog or your log search. It consumes logs and emits incidents. If
you already have dashboards you like, keep them.

**2. AIRS does not act on incidents.** No auto-remediation, no auto-rollback,
no auto-scaling. It diagnoses and it notifies. A system that is sometimes
wrong about causes must not be allowed to take actions, and this one is
sometimes wrong about causes.

**3. AIRS does not do metrics or traces.** Logs only. Traces would genuinely
improve correlation, and the service graph exists partly because traces do
not. This is a real limitation, not a philosophical position.

**4. AIRS does not learn your service topology.** The graph is declared
through `PUT /v1/topology`. Inferring it from traffic is a reasonable thing to
want and it is not implemented.

**5. AIRS does not fine-tune or train anything.** It uses off-the-shelf
instruction models through a provider interface. RCA feedback is captured and
stored; nothing consumes it. Calling that a feedback loop would be a lie, so
the README calls it feedback capture.

**6. AIRS does not guarantee exactly-once processing.** Consumers use Kafka
auto-commit, so a crash mid-batch can lose in-flight events. See
[05-failure-modes.md](05-failure-modes.md) for the blast radius. Moving to
manual commits is understood and deliberately deferred.

**7. AIRS is not horizontally scalable today.** Stages fail and lag
independently, which is the property that matters most, but anomaly-service
and correlation-service both hold per-service state in process memory. Two
replicas of either split that state rather than sharing it. Scaling them means
externalising state first.

**8. AIRS does not do alert routing or on-call scheduling.** It notifies a
webhook or a Slack channel. It is not PagerDuty and should not grow into it.

---

## Deliberate extension points

Two seams exist because specific work is coming and it should land without
surgery. Both are described in [03-decisions.md](03-decisions.md).

- **Sources.** Log ingestion and a future ServiceNow incident poller satisfy
  the same interface, so pulling incidents from a ticketing system is a new
  source, not a new pipeline.
- **Sinks.** RCA output goes to storage today. The same interface covers a
  webhook and a ticket work note, so writing an RCA back to a ServiceNow
  incident as a work note is a new sink, not a change to ai-service.

---

## The honest summary

AIRS is a working demonstration of an event-driven incident pipeline with
AI applied as a pipeline stage rather than a chat box, built to be operated
and to fail predictably. It is not a product. The distance between the two is
mostly authentication, capacity isolation, and DLQ recovery, and this document
exists so that distance is stated rather than discovered.

---

Previous: [00: The problem](00-problem.md) | Next: [02: Architecture](02-architecture.md)

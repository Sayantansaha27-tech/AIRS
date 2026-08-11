# 05: Failure modes

What breaks, how far it spreads, how you find out, and what to do.

The first section is a defect found by auditing this repository's own README
against its code. It is first because it is the most instructive thing in this
document, and because the first fix proposed for it was wrong.

---

## The one that was actually broken: deduplication died during storms

### What the README claimed

> Correlation: Time-window grouping, fingerprint deduplication, suppression windows

Fingerprint deduplication was a headline feature. It was real, and it stopped
working under exactly the load it exists for.

### What the code did

`correlation-service` grouped anomalies into an in-memory cluster keyed by
`(tenant_id, service)`. Repeat fingerprints within a live cluster deduplicated
correctly. But the moment a cluster emitted its incident, it was **removed**
from the map:

```python
if anomaly.severity == Severity.critical:
    await emit_incident(cluster)
    clusters.pop(cluster_key, None)  # fingerprints die here
```

The fingerprint set died with the cluster. The next anomaly for that service
found no cluster, opened a fresh one, and deduplicated against nothing.

Critical anomalies emit on arrival, so they never deduplicated at all.

### Blast radius, measured

Feeding synthetic anomalies through `process_anomaly` with OpenSearch and
Kafka stubbed:

| Input | Incidents produced | Expected |
| --- | --- | --- |
| 50 critical anomalies, **identical** fingerprint | **50** | 1 |
| 50 warning anomalies, distinct fingerprints | 25 | 1 |
| 50 warning anomalies, identical fingerprint | 0 (held until window close) | correct |

A pod in a crash loop emitting the same OOM line produces one incident per
line. Each incident is independently published to `incidents-topic`, so
ai-service generates a full RCA for every one of them. At roughly 8 seconds
per inference, 50 duplicate incidents is around 7 minutes of queue.

The failure compounds: the storm that most needs grouping is the one that
disables grouping, and then floods the most expensive stage in the pipeline.

### The first fix, which was wrong

The obvious repair is to not pop the cluster. That alone is incorrect, and
worth recording because the reasoning is the useful part.

Leaving the cluster resident makes every subsequent distinct anomaly amend an
already-open incident, and the amendment path republishes to
`incidents-topic`. A long-running incident accumulating 200 distinct anomalies
would then trigger 200 RCA generations for a single incident. That converts a
duplicate-incident storm into a duplicate-RCA storm against the same
bottleneck. Net throughput would have been no better, and the symptom would
have moved somewhere harder to see.

### The fix as shipped

Three parts, and all three are needed:

1. **The cluster stays resident** for the rest of its window after emitting.
   Repeat fingerprints deduplicate and extend the window.
2. **Amendments target the same incident.** `incident_id`, `created_at` and
   the resolved parent link are held on the cluster and reused, so an
   amendment is an upsert rather than a new document.
3. **Amendments republish only on severity escalation.** Persistence to
   OpenSearch always happens; the Kafka publish that costs an RCA does not.

After the fix, 50 identical critical anomalies produce **1 incident and 1
Kafka publish**. A warning cluster that escalates to critical produces 1
incident and 2 publishes, so the RCA is regenerated against the worse
severity, which is the behaviour you want.

### Detection

`airs_incidents_created_total` rising in step with `airs_anomalies_emitted_total`
means grouping is not happening. Healthy operation shows incidents far below
anomalies. `airs_incidents_amended_total` should carry the difference.

---

## Kafka lag

**Trigger.** Any stage consuming slower than the stage before it produces.
Most often ai-service during a model outage, where each incident costs roughly
54 seconds instead of 8.

**Blast radius.** Contained to the affected stage and everything after it.
Consumer groups are per stage, so anomaly detection does not slow because RCA
is slow, and ingestion does not fail because anomaly detection is behind.
Incidents remain complete and queryable throughout: only the RCA enrichment is
late.

**Detection.** Consumer group lag on the affected topic.
`airs_ai_batch_size` sitting at its `max_records` ceiling means the consumer
is saturated rather than idle.

**Mitigation.**

- Model outage: the deterministic fallback already engages. Confirm via
  `airs_rca_success_total{path="deterministic"}`. Lag drains once the model
  returns.
- To shed load immediately, set the LLM provider to a model that fast-fails,
  or create a broad suppression window to stem anomalies at the source.
- ai-service processes messages sequentially in one task. There is no
  concurrency control to tune. Genuine throughput needs concurrent inference,
  which is not built.

**Not mitigated.** Kafka retention is finite. A stage lagging longer than
topic retention loses the un-consumed tail permanently, and nothing detects
that beyond the lag metric.

---

## OpenSearch disk pressure

**Trigger.** Log volume against `log_retention_days` (5). OpenSearch applies a
read-only index block at its flood-stage watermark, typically 95%.

**Blast radius.** Wide, and this is the worst one in the system. Once indices
go read-only:

- log-processor cannot index, so **every log event dead-letters**. The DLQ is
  on Kafka, so the events survive, but at full ingest rate the DLQ becomes the
  primary destination for the entire stream.
- correlation-service cannot persist incidents, so incidents dead-letter too.
- ai-service cannot upsert RCA results.
- api-gateway reads keep working, so **the dashboard looks healthy** while
  nothing new is being stored.

That last point is the trap: the UI shows the last known state and gives no
indication that writes are failing.

**Detection.** `airs_dlq_published_total` climbing across multiple
`source_topic` labels at once is close to diagnostic: single-topic DLQ growth
is bad input, all-topic DLQ growth is a shared dependency. Confirm with
`GET _cat/allocation?v`.

**Mitigation.** Delete old indices, raise the watermark to buy time, then
clear the read-only block:

```bash
curl -X PUT "localhost:9200/_all/_settings" \
  -H 'Content-Type: application/json' \
  -d '{"index.blocks.read_only_allow_delete": null}'
```

Full procedure in [07-runbook.md](07-runbook.md).

**Standing gap.** Nothing alerts on disk before the watermark, and ISM
retention silently no-ops if the plugin is absent. Both should be fixed.

---

## Consumer group rebalance mid-batch

**Trigger.** A consumer joins or leaves: deploy, restart, crash, or a liveness
probe failure.

**Blast radius.** This is the durability gap in the system and it is worth
being exact.

Every consumer runs `enable_auto_commit=True`. Offsets are committed on a
timer, independent of whether the handler for those messages succeeded. So:

1. `getmany()` returns up to 200 records.
2. The loop begins processing them.
3. Auto-commit fires, marking all 200 consumed.
4. The process is killed at record 50.
5. On restart, consumption resumes past all 200. **Records 51 to 200 are gone.**

They are not in the DLQ, because they never failed. They were never processed.

`auto_offset_reset="latest"` compounds it: a consumer group with no committed
offset starts at the tail, so a first deploy against a topic with existing
data silently skips all of it.

**Blast radius by stage.** Losing logs loses detection input. Losing anomalies
loses incident signal. Losing an incident means it never receives an RCA,
though the incident document itself already exists in OpenSearch because
correlation-service writes before it publishes.

correlation-service additionally loses its in-memory clusters on an unclean
exit. Graceful shutdown flushes open clusters; `SIGKILL` does not. anomaly
baselines are lost too, and take an hour of wall clock per hour-of-day slot to
relearn.

**Detection.** Effectively none. This is silent loss. It would show as a gap
between `airs_logs_ingested_total` and `airs_logs_processed_total`, but no
alert compares them.

**Mitigation today.** Drain gracefully. `docker compose stop` sends SIGTERM
and the shutdown hooks flush correlation clusters.

**Fixed.** Consumers now commit only after a batch is fully handled, which
converts at-most-once into at-least-once.

That change alone would not have been safe. At-least-once means a crash after
processing but before committing replays the batch, and replaying an anomaly
would have opened a second incident. So it landed together with a durable
idempotency key: correlation records each anomaly id in Redis before
processing it, and a replayed anomaly is skipped and counted in
`airs_anomalies_replayed_total`. Indexing was already idempotent by document
id.

`auto_offset_reset` also moved from `latest` to `earliest`, so a new consumer
group no longer silently skips everything already on the topic.

---

## Model returns malformed JSON

**Trigger.** Small instruction-tuned models under `format: json` still emit
prose, truncate mid-object, or wrap objects in explanation. The 1.5B model
used for the `warning` tier does this noticeably more than the 7B.

**Blast radius.** Contained by design, and this path is well covered:

1. Provider tries `json.loads`. On failure it returns `{"raw": "<text>"}`.
2. `RCAResult.model_validate` rejects that, missing required fields.
3. The exception is caught by the generation cascade.
4. Retry, then the low-cost tier, then the deterministic template.

The incident always ends up with a schema-valid RCA. No dead-letter, no null
field, no partial write.

**Detection.** `airs_rca_success_total{path="deterministic"}` rising while the
model is demonstrably reachable means the model is answering but not usefully.
That distinction, reachable versus useful, is the one this metric was split to
expose.

**Mitigation.** Route the affected severity to the larger model, or switch
provider at runtime with `POST /v1/llm/config`. Neither needs a restart.

**Known weakness.** A model can return *valid* JSON with a *wrong* answer, and
nothing here detects that. Confidence is a blend of model self-assessment and a
structural heuristic, and neither measures correctness. This is what
[06-evals.md](06-evals.md) exists to quantify, and it is the reason every RCA
carries its evidence.

---

## Fingerprint over-specificity

**Trigger.** Log messages embedding unique values: request ids, row ids,
durations.

```
timeout while creating order req_id=8a3f  ->  fingerprint A
timeout while creating order req_id=9b2c  ->  fingerprint B
```

**Blast radius.** Deduplication silently stops working for that service. The
same failure repeated 500 times yields 500 distinct fingerprints, so the
cluster fills with signals that are really one signal. Grouping still happens
by service and window, so this degrades incident quality rather than producing
an incident storm, but the timeline becomes noise and RCA context fills with
500 near-identical lines.

**Detection.** Manual: an incident whose timeline is many near-identical
messages.

**Mitigation.** Normalize the message before it reaches AIRS, or write a
detection rule matching the stable prefix.

**Fixed.** Messages are templatized before fingerprinting: UUIDs, timestamps,
IP addresses, hex, long hashes, quantities with units and bare numbers are
masked to placeholders, so the same failure with a different request id is one
signal rather than many.

    timeout while creating order req_id=8a3f2b1c9d0e duration=1204ms
    timeout while creating order req_id=<hash> duration=<qty>

The remaining risk inverts: templatizing too aggressively would collapse
genuinely different failures into one fingerprint. The patterns are
deliberately conservative and there is a test asserting that unrelated
messages still differ.

---

## Blocking I/O on the event loop

**Trigger.** Present at all times under load, rather than an event.

Every service uses the **synchronous** `opensearch-py` client, called from
inside async consumer loops and async request handlers. Each call blocks the
entire event loop for its duration.

**Blast radius.**

- log-processor indexes with `refresh=True` on every event, forcing an
  OpenSearch refresh per document. This is the single largest throughput
  limiter in the ingest path.
- `GET /v1/stream` runs a blocking search every 2 seconds **per connected
  client**, on the gateway's only event loop. SSE connections scale badly for
  a reason unrelated to SSE.
- Health endpoints call `os_client.ping()` synchronously, so a slow
  OpenSearch makes liveness checks slow, which can trigger restarts of
  services that are themselves fine.

**Detection.** Throughput plateaus well below CPU saturation. Numbers in
[06-evals.md](06-evals.md).

**Fixed for the two stages that mattered.** log-processor and
correlation-service now use the async OpenSearch client, and log-processor
indexes a whole Kafka batch in one bulk request with `refresh=False`.
Sustained throughput went from roughly 200 events/sec to roughly 1,000, and
correlation-service stays responsive at rates where it previously stopped
answering its own health endpoint.

**Still present in api-gateway.** `GET /v1/stream` runs a blocking search
every two seconds per connected client, and the health endpoints call
`ping()` synchronously. Neither is on the pipeline's hot path, which is why
they were not converted first.

---

## Suppression window misses its start

**Trigger.** A maintenance window created less than 30 seconds before it
begins.

**Blast radius.** Small and self-correcting. anomaly-service refreshes rules
and suppressions from OpenSearch every 30 seconds, so anomalies emitted in
that gap become real incidents, and the maintenance generates alerts it was
meant to silence.

**Detection.** Incidents created for a service inside a declared window.

**Mitigation.** Create windows at least a minute early. Documented in the
runbook rather than fixed, because a config-change notification path is
disproportionate to the harm.

---

## Summary

| Failure | Blast radius | Detected by | Fixed? |
| --- | --- | --- | --- |
| Dedup dies during storms | Incident and RCA storm | Incidents tracking anomalies 1:1 | **Yes** |
| Provider unbuildable | Incident gets no RCA | DLQ growth on incidents-topic | **Yes** |
| Warning tier ignores runtime provider | Wasted retries, always falls back | Fallback rate | **Yes** |
| Missing OpenSearch startup gate | Crash loop on cold start | Container restarts | **Yes** |
| Fallback invisible in metrics | Outage looks like health | Nothing, that was the point | **Yes** |
| Kafka lag | Late RCA, incidents unaffected | Consumer group lag | Bounded, by design |
| OpenSearch full | Everything dead-letters, UI looks fine | DLQ across all topics | Manual |
| Rebalance mid-batch | Replay, deduplicated | `airs_anomalies_replayed_total` | **Yes** |
| Malformed model JSON | None, cascade absorbs it | Fallback rate | By design |
| Fingerprint over-specificity | Degraded incident quality | Manual | **Yes** |
| Blocking I/O | Throughput ceiling | Load testing | **Pipeline yes, gateway no** |
| Late suppression window | Spurious incidents | Manual | Documented |

Every row that lost data or capped throughput is now fixed. What remains is
the gateway's blocking I/O, which affects SSE fan-out rather than the
pipeline, and OpenSearch disk pressure, which is an operational procedure
rather than a defect.

---

Previous: [04: Integration contracts](04-integration-contracts.md) | Next: [06: Evals](06-evals.md)

# 04: Integration contracts

Every message on every topic, the DLQ envelope, the error taxonomy, and what
retry actually means at each stage.

Contracts are enforced at the **produce** boundary. `airs_shared.kafka.produce_json`
validates the payload against the model registered for that topic before
publishing, so a malformed event cannot reach a topic even by accident. The
live mapping is served at `GET /v1/schema-registry`.

---

## Topics

| Topic | Model | Producer | Consumer group |
| --- | --- | --- | --- |
| `logs-topic` | `NormalizedLogEvent` v1 | ingestion-service, api-gateway | `airs-log-processor` |
| `processed-logs-topic` | `NormalizedLogEvent` v1 | log-processor | `airs-anomaly-service` |
| `anomalies-topic` | `AnomalyEvent` v1 | anomaly-service | `airs-correlation-service` |
| `incidents-topic` | `Incident` v1 | correlation-service | `airs-ai-service` |
| `airs-dlq-topic` | `DLQEvent` (unvalidated) | all pipeline stages | none |

`airs-dlq-topic` is deliberately outside the registry. A DLQ that rejected
malformed payloads would have nowhere to put the rejection.

---

## `NormalizedLogEvent` v1

`logs-topic` and `processed-logs-topic`.

```json
{
  "timestamp": "2026-08-10T10:00:03Z",
  "service": "orders-service",
  "level": "error",
  "message": "timeout while creating order",
  "tenant_id": "default",
  "metadata": {"trace_id": "abc123", "pod": "orders-7d9f"}
}
```

| Field | Type | Rules |
| --- | --- | --- |
| `timestamp` | datetime | ISO 8601. `Z` accepted. Naive values assumed UTC, all values normalized to UTC. **Unparseable values are rejected, not defaulted.** |
| `service` | string | Defaults to `unknown-service` when absent |
| `level` | string | Lowercased. Free text, not an enum: `warn` and `warning` both pass |
| `message` | string | Coerced to string, may be empty |
| `tenant_id` | string | Trimmed, lowercased, 1 to 64 chars. Defaults to `default` |
| `metadata` | object | Every input key that is not one of the above |

Normalization is deliberately forgiving in one direction only. A bare string
becomes a log line attributed to `unknown-service` at `info`. Unknown keys are
preserved rather than dropped. But a timestamp that cannot be parsed is an
error, because a silently defaulted timestamp corrupts every downstream
time-window calculation and is undetectable afterwards.

`processed-logs-topic` carries the identical schema. The stage exists to index
into `airs-logs` and to decouple indexing throughput from detection throughput,
not to change shape.

## `AnomalyEvent` v1

`anomalies-topic`.

```json
{
  "id": "3f2b...",
  "timestamp": "2026-08-10T10:00:05Z",
  "service": "orders-service",
  "tenant_id": "default",
  "severity": "warning",
  "anomaly_score": 3.1,
  "confidence_score": 0.72,
  "reasons": ["zscore=3.1", "keywords=timeout", "level=error"],
  "log_message": "timeout while creating order",
  "fingerprint": "a1b2c3d4e5f60718",
  "metadata": {}
}
```

| Field | Rules |
| --- | --- |
| `severity` | Enum: `critical`, `warning`, `info` |
| `anomaly_score` | `max(zscore, 0.0)`. Keyword hits do **not** contribute |
| `confidence_score` | 0.0 to 1.0, from z-score, keyword count, level and rule boosts |
| `reasons` | Human-readable triggers. `rule=<id>` entries reference `airs-rules` |
| `fingerprint` | `sha1(tenant_id:service:message.lower().strip())[:16]` |

The fingerprint is the dedup key and it is built **here**, not in correlation.
Because it hashes the whole message, two occurrences of the same error with
different embedded ids (a request id, a row id) produce different
fingerprints and will not deduplicate. That is a known limitation; see
[05-failure-modes.md](05-failure-modes.md).

## `Incident` v1

`incidents-topic`.

```json
{
  "id": "9c8b...",
  "status": "open",
  "severity": "critical",
  "created_at": "2026-08-10T10:01:00Z",
  "updated_at": "2026-08-10T10:03:00Z",
  "service": "orders-service",
  "tenant_id": "default",
  "anomaly_ids": ["3f2b...", "7d1a..."],
  "timeline": [
    {
      "timestamp": "2026-08-10T10:00:03Z",
      "severity": "warning",
      "message": "timeout while creating order",
      "reasons": ["zscore=3.1"]
    }
  ],
  "summary": "orders-service incident from 3 correlated anomalies (zscore=3.1)",
  "parent_incident_id": null,
  "child_incident_ids": [],
  "related_services": ["postgres-primary"],
  "rca": null
}
```

| Field | Rules |
| --- | --- |
| `status` | `open`, `acknowledged`, `resolved`. Transitions enforced by the gateway |
| `created_at` | Stable across amendments |
| `updated_at` | Bumped on every amendment |
| `parent_incident_id` | Resolved once, when the incident opens. Never re-resolved |
| `related_services` | Topology neighbours at emission time |
| `rca` | `null` until ai-service enriches it |

**An incident is published more than once.** It is published on creation and
again whenever its severity escalates. The `id` is stable, so consumers must
treat `incidents-topic` as **upsert by id, not append**. ai-service does; any
new consumer must too.

Amendments that do not change severity are persisted to OpenSearch without
being republished, so a busy incident does not cost one RCA generation per
correlated anomaly.

## `RCAResult`

Embedded in `Incident.rca`. Never on a topic of its own.

```json
{
  "root_cause": "Database connection pool exhaustion in orders-service",
  "confidence": 0.78,
  "explanation": "Pool saturation at 10:00:03 preceded the downstream timeouts...",
  "suggested_fix": "Raise max_pool_size, or reduce per-request hold time",
  "affected_services": ["orders-service", "postgres-primary"],
  "evidence": [
    {
      "timestamp": "2026-08-10T10:00:03Z",
      "message": "timeout while creating order",
      "service": "orders-service"
    }
  ]
}
```

`confidence` is hybrid: the mean of the model's self-reported confidence and a
heuristic derived from signal volume. Model self-assessment is not trustworthy
on its own, and neither is a purely structural heuristic, so the blend is a
deliberate hedge rather than a measurement. Do not read it as a probability.

For deterministic-fallback results, `explanation` states that a fallback was
used. Programmatically, the signal is
`airs_rca_success_total{path="deterministic"}`.

## DLQ envelope

`airs-dlq-topic`. Not schema-validated.

```json
{
  "source_topic": "processed-logs-topic",
  "original_topic": "processed-logs-topic",
  "original_partition": 0,
  "original_offset": 12345,
  "failure_reason": "1 validation error for NormalizedLogEvent...",
  "failure_timestamp": "2026-08-10T10:02:00Z",
  "retry_count": 0,
  "payload": { "...": "the original message, verbatim" },
  "original_payload": { "...": "same" },
  "error": "same as failure_reason",
  "failed_at": "same as failure_timestamp"
}
```

Two things to know:

- **Field names are duplicated.** `source_topic`/`original_topic`,
  `failure_reason`/`error`, `failure_timestamp`/`failed_at` and
  `payload`/`original_payload` are pairs carrying identical values. This is
  compatibility cruft from two naming conventions and should be collapsed;
  until then, consumers may read either.
- **`retry_count` is always 0.** Nothing consumes the DLQ, so nothing
  increments it. The field is reserved for a future drain worker.

ingestion-service entries carry an extra `ingest_source` field (`api` or
`poller`) and no partition or offset, because ingestion is the head of the
pipeline and the event never came from a topic.

---

## Error taxonomy

What each class of failure does, and where it surfaces.

| Class | Example | Behaviour | Where you see it |
| --- | --- | --- | --- |
| **Malformed input** | Unparseable timestamp | Event to DLQ, batch continues | `airs_dlq_published_total`, `airs_logs_rejected_total{reason="unprocessable"}` |
| **Oversized payload** | > 256 KB log event | Event to DLQ, batch continues | `airs_logs_rejected_total{reason="payload_too_large"}` |
| **Contract violation** | Producer builds an invalid model | Raises before publish, caught by the stage's handler, event to DLQ | `airs_dlq_published_total` |
| **Transient dependency** | OpenSearch refuses a connection | Exception in the message handler, event to DLQ, consumer loop continues | DLQ counter plus service logs |
| **Model unavailable** | Ollama down, or provider unbuildable | Retry both tiers, then deterministic RCA. Incident is never dead-lettered for this | `airs_rca_success_total{path="deterministic"}` |
| **Model returns bad JSON** | Prose instead of an object | Provider wraps it as `{"raw": ...}`, `RCAResult` validation fails, falls through the cascade | Same as above |
| **Loop-level failure** | Consumer loop itself raises | Logged, 1s sleep, loop restarts. Offsets already auto-committed | Service logs |
| **Suppressed signal** | Matching maintenance window | Dropped before becoming an anomaly. **Not** a DLQ event | `airs_anomalies_suppressed_total{reason="suppression_window"}` |
| **Low confidence** | Confidence < 0.3, no rule matched | Dropped silently | `airs_anomalies_suppressed_total{reason="low_confidence"}` |

The two drop paths at the bottom are intentional data loss and the only ones
not recoverable from the DLQ. Both are counted, so the loss is visible even
though it is not reversible.

## Retry semantics

Retry means different things at different stages, and conflating them causes
incorrect assumptions about durability.

| Stage | Retries? | Detail |
| --- | --- | --- |
| Ingestion HTTP | No | Caller's responsibility. A rejected event is in the DLQ |
| Source polling | Next tick | A failed poll is recorded on the source and retried at its next interval. No backoff |
| Kafka produce | Yes | `send_and_wait`, aiokafka's internal retry |
| Consumer handlers | **No** | One attempt. Failure goes to the DLQ immediately. There is no in-stage redelivery |
| LLM generation | Yes | `retries` (2) per tier, exponential backoff (1s, 2s), 8s timeout each. Two tiers, so up to 6 attempts |
| Webhook delivery | Yes | `max_attempts` (default 3) with exponential backoff (1s, 2s), `timeout_seconds` (default 5). Body is HMAC-SHA256 signed as `x-airs-signature` when the subscription has a secret |
| DLQ | Never | Nothing consumes it |

The important line is **consumer handlers**. There is no per-message retry
between stages. A message that fails once is dead-lettered, with no attempt to
distinguish a transient dependency failure from permanently malformed input.
An OpenSearch blip therefore dead-letters good events that would have
succeeded a second later. Recorded in [05-failure-modes.md](05-failure-modes.md);
fixing it means classifying errors and adding a retry topic.

---

## Configuration stores

Config lives in OpenSearch, not Kafka, because it is read-mostly state rather
than a stream. Full document shapes: `DataSource`, `DetectionRule`,
`SuppressionWindow`, `TopologyEdge`, `ChatOpsConfig` and `WebhookSubscription`
in [`shared/airs_shared/models.py`](../shared/airs_shared/models.py).

Propagation is by polling, so config changes are **not** immediate:

| Store | Consumer | Lag |
| --- | --- | --- |
| `airs-rules` | anomaly-service | up to 30s |
| `airs-suppressions` | anomaly-service | up to 30s |
| `airs-sources` | ingestion-service | up to 5s |
| `airs-sources` | correlation-service | up to 60s |
| `airs-topology` | correlation-service, ai-service | read per incident, no cache |

A suppression window created for a maintenance start must therefore be created
at least 30 seconds early, or the first anomalies of the maintenance still get
through.

---

Previous: [03: Decisions](03-decisions.md) | Next: [05: Failure modes](05-failure-modes.md)

# Kafka Contracts (MVP)

## logs-topic

Normalized log envelope:

```json
{
  "timestamp": "2026-02-16T10:00:03Z",
  "service": "orders-service",
  "level": "error",
  "tenant_id": "default",
  "message": "timeout while creating order",
  "metadata": {}
}
```

## processed-logs-topic

Same schema as `logs-topic` after normalization and enrichment.

## anomalies-topic

```json
{
  "id": "uuid",
  "timestamp": "2026-02-16T10:00:05Z",
  "service": "orders-service",
  "tenant_id": "default",
  "severity": "warning",
  "anomaly_score": 3.1,
  "confidence_score": 0.72,
  "reasons": ["zscore=3.1", "keywords=timeout"],
  "log_message": "timeout while creating order",
  "fingerprint": "abc123def456",
  "metadata": {}
}
```

## incidents-topic

```json
{
  "id": "uuid",
  "status": "open",
  "severity": "critical",
  "created_at": "2026-02-16T10:01:00Z",
  "updated_at": "2026-02-16T10:01:00Z",
  "service": "orders-service",
  "tenant_id": "default",
  "anomaly_ids": ["..."],
  "timeline": [
    {
      "timestamp": "2026-02-16T10:00:03Z",
      "severity": "warning",
      "message": "timeout while creating order",
      "reasons": ["zscore=3.1"]
    }
  ],
  "summary": "orders-service incident from 3 correlated anomalies"
}
```

## airs-dlq-topic

```json
{
  "source_topic": "logs-topic",
  "original_topic": "logs-topic",
  "original_partition": 0,
  "original_offset": 12345,
  "failure_reason": "...",
  "failure_timestamp": "2026-02-16T10:02:00Z",
  "retry_count": 0,
  "payload": {},
  "original_payload": {},
  "error": "...",
  "failed_at": "2026-02-16T10:02:00Z"
}
```

## External Source Config (OpenSearch `airs-sources`)

```json
{
  "id": "uuid",
  "name": "Checkout API",
  "tenant_id": "default",
  "endpoint": "https://service.internal/logs",
  "default_service": "checkout-service",
  "method": "GET",
  "headers": {},
  "body": null,
  "response_logs_field": "data.logs",
  "poll_interval_seconds": 30,
  "window_duration_minutes": 10,
  "min_signal_count": 2,
  "enabled": true,
  "created_at": "2026-02-16T10:00:00Z",
  "updated_at": "2026-02-16T10:00:00Z",
  "last_polled_at": null,
  "last_success_at": null,
  "last_status": "never",
  "last_error": null,
  "total_ingested": 0
}
```

## Detection Rule Config (OpenSearch `airs-rules`)

```json
{
  "id": "uuid",
  "name": "OOM Detector",
  "tenant_id": "default",
  "service_pattern": "orders-*",
  "match_type": "regex",
  "pattern": "OOM|OutOfMemory",
  "severity": "critical",
  "confidence_boost": 0.4,
  "enabled": true,
  "created_at": "2026-02-16T10:00:00Z",
  "updated_at": "2026-02-16T10:00:00Z",
  "match_count": 0
}
```

## Suppression Window Config (OpenSearch `airs-suppressions`)

```json
{
  "id": "uuid",
  "service_pattern": "payments-*",
  "tenant_id": "default",
  "reason": "planned maintenance",
  "starts_at": "2026-02-16T10:00:00Z",
  "ends_at": "2026-02-16T11:00:00Z",
  "enabled": true,
  "created_at": "2026-02-16T09:55:00Z",
  "updated_at": "2026-02-16T09:55:00Z"
}
```

## RCA Feedback (OpenSearch `airs-rca-feedback`)

```json
{
  "id": "uuid",
  "incident_id": "uuid",
  "tenant_id": "default",
  "service": "orders-service",
  "severity": "critical",
  "rating": "helpful",
  "correction": "Timeout was caused by database connection pool exhaustion.",
  "submitted_at": "2026-02-16T10:20:00Z",
  "rca": {
    "root_cause": "...",
    "confidence": 0.78,
    "explanation": "...",
    "suggested_fix": "...",
    "affected_services": ["orders-service"],
    "evidence": []
  }
}
```

## Service Topology Edge (OpenSearch `airs-topology`)

```json
{
  "id": "uuid",
  "tenant_id": "default",
  "upstream": "orders-service",
  "downstream": "postgres-primary",
  "dependency_type": "db",
  "created_at": "2026-02-16T10:00:00Z",
  "updated_at": "2026-02-16T10:00:00Z"
}
```

## ChatOps Config (OpenSearch `airs-chatops`)

```json
{
  "id": "uuid",
  "tenant_id": "default",
  "provider": "slack",
  "webhook_url": "https://hooks.slack.com/services/...",
  "signing_secret": "optional",
  "enabled": true,
  "last_status": "never",
  "last_error": null,
  "last_sent_at": null
}
```

## Built-In Topic Schema Registry

- `logs-topic` -> `NormalizedLogEvent` v1
- `processed-logs-topic` -> `NormalizedLogEvent` v1
- `anomalies-topic` -> `AnomalyEvent` v1
- `incidents-topic` -> `Incident` v1

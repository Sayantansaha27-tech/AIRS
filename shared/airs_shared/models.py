from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator

DEFAULT_TENANT_ID = "default"


def normalize_tenant(value: str) -> str:
    tenant = value.strip().lower()
    if not tenant:
        raise ValueError("tenant_id cannot be empty")
    return tenant


class Severity(StrEnum):
    critical = "critical"
    warning = "warning"
    info = "info"


class IncidentStatus(StrEnum):
    open = "open"
    acknowledged = "acknowledged"
    resolved = "resolved"


class SourceMethod(StrEnum):
    get = "GET"
    post = "POST"


class RuleMatchType(StrEnum):
    keyword = "keyword"
    regex = "regex"
    threshold = "threshold"
    composite = "composite"


class DependencyType(StrEnum):
    sync = "sync"
    asynchronous = "async"
    db = "db"


class NormalizedLogEvent(BaseModel):
    timestamp: datetime
    service: str
    level: str = "info"
    message: str
    tenant_id: str = Field(default=DEFAULT_TENANT_ID, min_length=1, max_length=64)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, value: Any) -> datetime:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            raise ValueError("Invalid timestamp")

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, value: str) -> str:
        return normalize_tenant(value)


class AnomalyEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime
    service: str
    severity: Severity
    anomaly_score: float
    reasons: list[str]
    log_message: str
    fingerprint: str
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    tenant_id: str = Field(default=DEFAULT_TENANT_ID, min_length=1, max_length=64)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, value: str) -> str:
        return normalize_tenant(value)


class RCAResult(BaseModel):
    # Non-empty on purpose. A model that returns a blank conclusion has not
    # produced an analysis, and the contract must say so rather than letting a
    # work note with an empty "Root cause:" reach a real ticket. Rejecting it
    # here is what makes the generation cascade fall through to the next tier,
    # and ultimately to the deterministic template, which always fills it.
    root_cause: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    explanation: str
    suggested_fix: str
    affected_services: list[str] = Field(default_factory=list)
    evidence: list[dict[str, Any]] = Field(default_factory=list)

    @field_validator("root_cause", "explanation", "suggested_fix", mode="before")
    @classmethod
    def strip_text(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value


class Incident(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    status: IncidentStatus = IncidentStatus.open
    severity: Severity
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    service: str
    tenant_id: str = Field(default=DEFAULT_TENANT_ID, min_length=1, max_length=64)
    anomaly_ids: list[str] = Field(default_factory=list)
    timeline: list[dict[str, Any]] = Field(default_factory=list)
    summary: str
    parent_incident_id: str | None = None
    child_incident_ids: list[str] = Field(default_factory=list)
    related_services: list[str] = Field(default_factory=list)
    rca: RCAResult | None = None

    # Provenance for incidents that did not originate in this pipeline.
    # An incident pulled from an external system must be able to carry its
    # identity there all the way to the sink that writes the RCA back, and the
    # only thing travelling between a source and a sink is this model.
    source_system: str | None = Field(default=None, max_length=64)
    external_id: str | None = Field(default=None, max_length=128)

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, value: str) -> str:
        return normalize_tenant(value)

    @field_validator("source_system", "external_id")
    @classmethod
    def normalize_provenance(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @property
    def is_external(self) -> bool:
        """True when this incident was pulled from another system.

        Externally sourced incidents skip detection and correlation, because
        the grouping decision was already made elsewhere and re-deriving it
        would produce a second, competing opinion about the same event.
        """
        return self.external_id is not None


class IngestRequest(BaseModel):
    logs: list[dict[str, Any] | str]


class AnalyzeRequest(BaseModel):
    logs: list[dict[str, Any] | str]


class LLMConfigRequest(BaseModel):
    provider: str
    model: str


class RCAFeedbackRequest(BaseModel):
    rating: Literal["helpful", "not_helpful", "incorrect"]
    correction: str | None = Field(default=None, max_length=2000)

    @field_validator("correction")
    @classmethod
    def normalize_correction(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class RCARegenerateRequest(BaseModel):
    notes: str | None = Field(default=None, max_length=2000)
    logs: list[dict[str, Any] | str] | None = None

    @field_validator("notes")
    @classmethod
    def normalize_notes(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class ReplayRequest(BaseModel):
    topic: str
    partition: int = 0
    offset: int = 0
    max_messages: int = Field(default=500, ge=1, le=5000)


class ReplayFilterRequest(BaseModel):
    service: str | None = None
    from_time: datetime | None = None
    to_time: datetime | None = None
    severity_filter: list[Severity] = Field(default_factory=list)
    rule_ids: list[str] = Field(default_factory=list)
    max_messages: int = Field(default=500, ge=1, le=5000)
    tenant_id: str | None = None

    @field_validator("tenant_id")
    @classmethod
    def normalize_optional_tenant(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_tenant(value)

    @field_validator("to_time")
    @classmethod
    def validate_replay_window(cls, value: datetime | None, info: Any) -> datetime | None:
        from_time = info.data.get("from_time")
        if value is not None and from_time is not None and value <= from_time:
            raise ValueError("to_time must be later than from_time")
        return value


class SimulateIngestRequest(BaseModel):
    service: str = Field(min_length=1, max_length=120)
    pattern: str = Field(min_length=1, max_length=500)
    count: int = Field(default=50, ge=1, le=10000)
    rate_per_second: int = Field(default=10, ge=1, le=1000)
    level: str = Field(default="error", min_length=1, max_length=20)
    metadata: dict[str, Any] = Field(default_factory=dict)
    tenant_id: str | None = None

    @field_validator("tenant_id")
    @classmethod
    def normalize_optional_tenant(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_tenant(value)


class RuleHistoryTestRequest(BaseModel):
    service: str | None = None
    from_time: datetime | None = None
    to_time: datetime | None = None
    limit: int = Field(default=5000, ge=1, le=20000)
    tenant_id: str | None = None

    @field_validator("tenant_id")
    @classmethod
    def normalize_optional_tenant(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_tenant(value)

    @field_validator("to_time")
    @classmethod
    def validate_window(cls, value: datetime | None, info: Any) -> datetime | None:
        from_time = info.data.get("from_time")
        if value is not None and from_time is not None and value <= from_time:
            raise ValueError("to_time must be later than from_time")
        return value


class DataSourceCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    endpoint: str
    default_service: str = Field(min_length=1, max_length=80)
    method: SourceMethod = SourceMethod.get
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] | None = None
    response_logs_field: str | None = None
    poll_interval_seconds: int = Field(default=30, ge=5, le=3600)
    window_duration_minutes: int = Field(default=10, ge=1, le=120)
    min_signal_count: int = Field(default=2, ge=1, le=50)
    enabled: bool = True
    auth_token: str | None = None

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        endpoint = value.strip()
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("endpoint must start with http:// or https://")
        return endpoint

    @field_validator("response_logs_field")
    @classmethod
    def validate_response_logs_field(cls, value: str | None) -> str | None:
        if value is None:
            return None
        field = value.strip()
        return field or None

    @field_validator("auth_token")
    @classmethod
    def normalize_auth_token(cls, value: str | None) -> str | None:
        if value is None:
            return None
        token = value.strip()
        return token or None


class DataSource(DataSourceCreateRequest):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str = Field(default=DEFAULT_TENANT_ID, min_length=1, max_length=64)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_polled_at: datetime | None = None
    last_success_at: datetime | None = None
    last_status: Literal["never", "ok", "error"] = "never"
    last_error: str | None = None
    total_ingested: int = Field(default=0, ge=0)

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, value: str) -> str:
        return normalize_tenant(value)


class DataSourceView(BaseModel):
    id: str
    tenant_id: str
    name: str
    endpoint: str
    default_service: str
    method: SourceMethod
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] | None = None
    response_logs_field: str | None = None
    poll_interval_seconds: int
    window_duration_minutes: int
    min_signal_count: int
    enabled: bool
    created_at: datetime
    updated_at: datetime
    last_polled_at: datetime | None = None
    last_success_at: datetime | None = None
    last_status: Literal["never", "ok", "error"] = "never"
    last_error: str | None = None
    total_ingested: int = Field(default=0, ge=0)
    has_auth_token: bool = False


class DetectionRuleCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    service_pattern: str = "*"
    match_type: RuleMatchType = RuleMatchType.keyword
    pattern: str = Field(min_length=1, max_length=500)
    severity: Severity = Severity.warning
    confidence_boost: float = Field(default=0.2, ge=0.0, le=1.0)
    enabled: bool = True

    @field_validator("service_pattern", "pattern")
    @classmethod
    def normalize_pattern(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("pattern fields cannot be empty")
        return normalized


class DetectionRule(DetectionRuleCreateRequest):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str = Field(default=DEFAULT_TENANT_ID, min_length=1, max_length=64)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    match_count: int = Field(default=0, ge=0)

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, value: str) -> str:
        return normalize_tenant(value)


class SuppressionCreateRequest(BaseModel):
    service_pattern: str
    reason: str = Field(min_length=1, max_length=300)
    starts_at: datetime
    ends_at: datetime
    enabled: bool = True

    @field_validator("service_pattern")
    @classmethod
    def normalize_service_pattern(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("service_pattern is required")
        return normalized

    @field_validator("ends_at")
    @classmethod
    def validate_ends_after_start(cls, value: datetime, info: Any) -> datetime:
        starts_at = info.data.get("starts_at")
        if starts_at is not None and value <= starts_at:
            raise ValueError("ends_at must be after starts_at")
        return value


class SuppressionWindow(SuppressionCreateRequest):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str = Field(default=DEFAULT_TENANT_ID, min_length=1, max_length=64)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, value: str) -> str:
        return normalize_tenant(value)


class TopologyEdgeInput(BaseModel):
    upstream: str = Field(min_length=1, max_length=120)
    downstream: str = Field(min_length=1, max_length=120)
    dependency_type: DependencyType = DependencyType.sync


class TopologyEdge(TopologyEdgeInput):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str = Field(default=DEFAULT_TENANT_ID, min_length=1, max_length=64)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, value: str) -> str:
        return normalize_tenant(value)


class TopologyUpdateRequest(BaseModel):
    edges: list[TopologyEdgeInput] = Field(default_factory=list)


class ChatOpsConfigCreateRequest(BaseModel):
    provider: Literal["slack", "teams"] = "slack"
    webhook_url: str
    signing_secret: str | None = None
    enabled: bool = True

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: str) -> str:
        url = value.strip()
        if not url.startswith(("http://", "https://")):
            raise ValueError("webhook_url must start with http:// or https://")
        return url

    @field_validator("signing_secret")
    @classmethod
    def normalize_signing_secret(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class ChatOpsConfig(ChatOpsConfigCreateRequest):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str = Field(default=DEFAULT_TENANT_ID, min_length=1, max_length=64)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_status: Literal["never", "ok", "error"] = "never"
    last_error: str | None = None
    last_sent_at: datetime | None = None

    @field_validator("tenant_id")
    @classmethod
    def validate_tenant_id(cls, value: str) -> str:
        return normalize_tenant(value)


class ChatOpsCommandRequest(BaseModel):
    command: str = Field(min_length=1, max_length=500)
    channel_id: str | None = None
    user_id: str | None = None
    tenant_id: str | None = None

    @field_validator("tenant_id")
    @classmethod
    def normalize_optional_tenant(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_tenant(value)


class SchemaContract(BaseModel):
    topic: str
    version: int
    payload_model: str

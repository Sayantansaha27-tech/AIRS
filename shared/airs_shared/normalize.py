from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from airs_shared.models import NormalizedLogEvent


def normalize_log(raw: dict[str, Any] | str) -> NormalizedLogEvent:
    if isinstance(raw, str):
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "service": "unknown-service",
            "level": "info",
            "message": raw,
            "metadata": {},
        }
        return NormalizedLogEvent.model_validate(payload)

    timestamp = raw.get("timestamp") or datetime.now(UTC).isoformat()
    service = raw.get("service") or "unknown-service"
    level = str(raw.get("level") or "info").lower()
    message = str(raw.get("message") or "")
    tenant_id = str(raw.get("tenant_id") or "default")

    metadata = {
        k: v
        for k, v in raw.items()
        if k not in {"timestamp", "service", "level", "message", "tenant_id"}
    }
    return NormalizedLogEvent.model_validate(
        {
            "timestamp": timestamp,
            "service": service,
            "level": level,
            "message": message,
            "tenant_id": tenant_id,
            "metadata": metadata,
        }
    )

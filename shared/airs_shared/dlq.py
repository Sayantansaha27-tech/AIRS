from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def build_dlq_payload(
    *,
    source_topic: str,
    payload: dict[str, Any],
    error: Exception | str,
    partition: int | None = None,
    offset: int | None = None,
    retry_count: int = 0,
) -> dict[str, Any]:
    reason = str(error)[:500]
    failed_at = datetime.now(UTC).isoformat()
    return {
        "source_topic": source_topic,
        "original_topic": source_topic,
        "original_partition": partition,
        "original_offset": offset,
        "failure_reason": reason,
        "failure_timestamp": failed_at,
        "retry_count": retry_count,
        "payload": payload,
        "original_payload": payload,
        "error": reason,
        "failed_at": failed_at,
    }

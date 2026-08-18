from __future__ import annotations

import json
from datetime import UTC, datetime

from airs_shared.models import RCAResult

PROMPT_TEMPLATE = """
You are an incident response assistant.
{corrections_note}Analyze the incident context and return strict JSON with this schema:
{{
  "root_cause": "string",
  "confidence": 0.0,
  "explanation": "string",
  "suggested_fix": "string",
  "affected_services": ["string"],
  "evidence": [{{"timestamp": "ISO-8601", "message": "string", "service": "string"}}]
}}

Context:
{context_json}
"""


CORRECTIONS_NOTE = """
An engineer previously judged an analysis of this same service wrong and said
what the cause actually was, under `operator_corrections`. Weigh that above
your own prior: they saw the system, you are only reading logs.

"""


def build_prompt(context: dict) -> str:
    """Render the prompt, mentioning corrections only when there are some.

    The instruction used to be unconditional. Small models then treated it as
    content rather than as an instruction and answered about it, producing a
    root cause of "the engineer previously judged the cause as incorrect" on
    incidents that had no feedback at all. An instruction about data that is
    not present is worse than no instruction.
    """
    payload = dict(context)
    corrections = payload.get("operator_corrections") or []
    if not corrections:
        payload.pop("operator_corrections", None)

    return PROMPT_TEMPLATE.format(
        corrections_note=CORRECTIONS_NOTE if corrections else "",
        context_json=json.dumps(payload, default=str),
    )


def heuristic_confidence(context: dict) -> float:
    logs = context.get("logs", [])
    anomalies = context.get("anomalies", [])
    base = 0.4
    if anomalies:
        base += min(0.3, len(anomalies) * 0.05)
    if logs:
        base += min(0.2, len(logs) * 0.01)
    return min(base, 0.95)


def deterministic_fallback(context: dict) -> RCAResult:
    logs = context.get("logs", [])
    anomalies = context.get("anomalies", [])
    services = sorted({item.get("service", "unknown-service") for item in logs})
    service = services[0] if services else "unknown-service"

    top_messages = [item.get("message", "") for item in logs[:3]]
    evidence = [
        {
            "timestamp": item.get("timestamp", datetime.now(UTC).isoformat()),
            "message": item.get("message", ""),
            "service": item.get("service", service),
        }
        for item in logs[:5]
    ]

    anomaly_count = len(anomalies)
    summary = (
        f"{service} shows repeated anomalous behavior"
        if anomaly_count
        else f"{service} shows elevated error signals"
    )

    explanation = (
        "Rule-based fallback was used because the configured LLM response failed validation. "
        f"Observed {anomaly_count} anomalies and key messages: {top_messages}."
    )

    return RCAResult(
        root_cause=summary,
        confidence=heuristic_confidence(context),
        explanation=explanation,
        suggested_fix=(
            "Check recent deployment/config changes, inspect dependency timeouts, "
            "and restart affected pods if needed."
        ),
        affected_services=services or [service],
        evidence=evidence,
    )

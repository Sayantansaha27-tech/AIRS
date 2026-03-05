from __future__ import annotations

from typing import Any, Type

from pydantic import BaseModel

from airs_shared.models import AnomalyEvent, Incident, NormalizedLogEvent, SchemaContract
from airs_shared.settings import AIRSSettings


def topic_contracts(settings: AIRSSettings) -> list[SchemaContract]:
    return [
        SchemaContract(
            topic=settings.kafka.topics.logs,
            version=1,
            payload_model=NormalizedLogEvent.__name__,
        ),
        SchemaContract(
            topic=settings.kafka.topics.processed_logs,
            version=1,
            payload_model=NormalizedLogEvent.__name__,
        ),
        SchemaContract(
            topic=settings.kafka.topics.anomalies,
            version=1,
            payload_model=AnomalyEvent.__name__,
        ),
        SchemaContract(
            topic=settings.kafka.topics.incidents,
            version=1,
            payload_model=Incident.__name__,
        ),
    ]


def contract_model_map(settings: AIRSSettings) -> dict[str, Type[BaseModel]]:
    return {
        settings.kafka.topics.logs: NormalizedLogEvent,
        settings.kafka.topics.processed_logs: NormalizedLogEvent,
        settings.kafka.topics.anomalies: AnomalyEvent,
        settings.kafka.topics.incidents: Incident,
    }


def validate_topic_payload(
    *,
    settings: AIRSSettings,
    topic: str,
    payload: dict[str, Any],
) -> None:
    model = contract_model_map(settings).get(topic)
    if model is None:
        return
    model.model_validate(payload)

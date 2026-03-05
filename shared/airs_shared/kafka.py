from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer

from airs_shared.schema_registry import validate_topic_payload
from airs_shared.settings import get_settings

settings = get_settings()


async def produce_json(producer: AIOKafkaProducer, topic: str, payload: dict[str, Any]) -> None:
    validate_topic_payload(settings=settings, topic=topic, payload=payload)
    await producer.send_and_wait(topic, json.dumps(payload, default=str).encode("utf-8"))

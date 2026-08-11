from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any


class BaseLLMProvider(ABC):
    """Talks to one model endpoint and returns a dict shaped like RCAResult.

    Implementations are responsible for transport and for getting the response
    into a dict. They are not responsible for retries, routing, validation or
    fallback: those belong to the caller, so that every provider behaves the
    same way when it fails.
    """

    #: Identifier used in metrics and logs.
    kind: str = "base"

    @abstractmethod
    async def generate(self, prompt: str, context: dict) -> dict:
        raise NotImplementedError

    async def aclose(self) -> None:
        return None


def coerce_json(raw: Any) -> dict:
    """Best effort at getting a dict out of whatever a model returned.

    Small models routinely wrap JSON in prose or fences even when asked not to.
    Recovering from that is worth doing, because the alternative is discarding
    an answer that was correct and badly packaged.

    Anything genuinely unparseable is returned as {"raw": ...}, which fails
    RCAResult validation upstream and falls through to the next tier. That is
    deliberate: a provider must never invent a result.
    """
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {"raw": raw}

    text = raw.strip()
    if not text:
        return {"raw": raw}

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {"raw": raw}
    except json.JSONDecodeError:
        pass

    # Strip a ```json fence if there is one.
    if "```" in text:
        segments = text.split("```")
        for segment in segments:
            candidate = segment.strip()
            if candidate.lower().startswith("json"):
                candidate = candidate[4:].strip()
            if candidate.startswith("{"):
                try:
                    parsed = json.loads(candidate)
                    if isinstance(parsed, dict):
                        return parsed
                except json.JSONDecodeError:
                    continue

    # Last resort: the outermost {...} span.
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    return {"raw": raw}

from __future__ import annotations

import json

import httpx

from .base import BaseLLMProvider


class OllamaProvider(BaseLLMProvider):
    def __init__(self, base_url: str, model: str, timeout_seconds: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds

    async def generate(self, prompt: str, context: dict) -> dict:
        model = context.get("model", self.model)
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.1},
        }

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(f"{self.base_url}/api/generate", json=payload)
            response.raise_for_status()
            body = response.json()

        generated = body.get("response", "{}")
        if isinstance(generated, dict):
            return generated
        try:
            return json.loads(generated)
        except json.JSONDecodeError:
            return {"raw": generated}

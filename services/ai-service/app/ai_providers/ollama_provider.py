"""Ollama's native generate API."""

from __future__ import annotations

import httpx

from .base import BaseLLMProvider, coerce_json


class OllamaProvider(BaseLLMProvider):
    kind = "ollama"

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        timeout_seconds: int,
        extra_headers: dict[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.extra_headers = dict(extra_headers or {})
        self._client = client

    async def generate(self, prompt: str, context: dict) -> dict:
        payload = {
            "model": context.get("model", self.model),
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.1},
        }

        client = self._client or httpx.AsyncClient(timeout=self.timeout_seconds)
        try:
            response = await client.post(
                f"{self.base_url}/api/generate", json=payload, headers=self.extra_headers
            )
            response.raise_for_status()
            body = response.json()
        finally:
            if self._client is None:
                await client.aclose()

        return coerce_json(body.get("response", ""))

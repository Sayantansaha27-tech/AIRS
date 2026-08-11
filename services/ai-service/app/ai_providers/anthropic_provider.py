"""Anthropic Messages API.

Separate from the OpenAI-compatible adapter because the request and response
shapes genuinely differ: a top-level system parameter rather than a system
message, x-api-key rather than a bearer token, and content returned as a list
of blocks.
"""

from __future__ import annotations

import httpx

from .base import BaseLLMProvider, coerce_json

ANTHROPIC_VERSION = "2023-06-01"


class AnthropicProvider(BaseLLMProvider):
    kind = "anthropic"

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str,
        timeout_seconds: int,
        max_tokens: int = 2048,
        extra_headers: dict[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_tokens = max_tokens
        self.extra_headers = dict(extra_headers or {})
        self._client = client

    async def generate(self, prompt: str, context: dict) -> dict:
        payload = {
            "model": context.get("model", self.model),
            "max_tokens": self.max_tokens,
            "temperature": 0.1,
            "system": "Return only strict JSON matching the requested schema. No prose.",
            "messages": [{"role": "user", "content": prompt}],
        }
        headers = {
            "content-type": "application/json",
            "x-api-key": self.api_key,
            "anthropic-version": ANTHROPIC_VERSION,
            **self.extra_headers,
        }

        client = self._client or httpx.AsyncClient(timeout=self.timeout_seconds)
        try:
            response = await client.post(f"{self.base_url}/messages", json=payload, headers=headers)
            response.raise_for_status()
            body = response.json()
        finally:
            if self._client is None:
                await client.aclose()

        blocks = body.get("content", [])
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text")
        return coerce_json(text)

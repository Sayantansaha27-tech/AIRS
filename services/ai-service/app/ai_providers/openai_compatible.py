"""Any server that speaks the OpenAI chat-completions API.

That is most of them: OpenAI itself, vLLM, LM Studio, llama.cpp's server,
Together, Groq, OpenRouter, DeepSeek, Mistral, Anyscale, and most
self-hosted gateways. Adding any of those is a config entry, not a new class.
"""

from __future__ import annotations

import httpx

from .base import BaseLLMProvider, coerce_json


class OpenAICompatibleProvider(BaseLLMProvider):
    kind = "openai_compatible"

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        timeout_seconds: int,
        api_key: str | None = None,
        extra_headers: dict[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.api_key = api_key
        self.extra_headers = dict(extra_headers or {})
        self._client = client

    def _headers(self) -> dict[str, str]:
        headers = {"content-type": "application/json", **self.extra_headers}
        # Local servers routinely need no key at all, so only send one if set.
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def generate(self, prompt: str, context: dict) -> dict:
        payload = {
            "model": context.get("model", self.model),
            "messages": [
                {"role": "system", "content": "Return only strict JSON. No prose, no fences."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }

        client = self._client or httpx.AsyncClient(timeout=self.timeout_seconds)
        try:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=self._headers(),
            )
            if response.status_code == 400 and "response_format" in response.text:
                # Not every OpenAI-compatible server implements JSON mode.
                # Retry without it rather than failing the tier: coerce_json
                # recovers the common shapes anyway.
                payload.pop("response_format", None)
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    headers=self._headers(),
                )
            response.raise_for_status()
            body = response.json()
        finally:
            if self._client is None:
                await client.aclose()

        return coerce_json(body["choices"][0]["message"]["content"])

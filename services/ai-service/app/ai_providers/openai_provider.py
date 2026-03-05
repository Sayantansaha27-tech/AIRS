from __future__ import annotations

import json

import httpx

from .base import BaseLLMProvider


class OpenAIProvider(BaseLLMProvider):
    def __init__(self, base_url: str, api_key: str, model: str, timeout_seconds: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds

    async def generate(self, prompt: str, context: dict) -> dict:
        model = context.get("model", self.model)
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "Return only strict JSON."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        headers = {"Authorization": f"Bearer {self.api_key}"}

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            body = response.json()

        content = body["choices"][0]["message"]["content"]
        if isinstance(content, dict):
            return content
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {"raw": content}

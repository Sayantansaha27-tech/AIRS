from __future__ import annotations

from abc import ABC, abstractmethod


class BaseLLMProvider(ABC):
    @abstractmethod
    async def generate(self, prompt: str, context: dict) -> dict:
        raise NotImplementedError

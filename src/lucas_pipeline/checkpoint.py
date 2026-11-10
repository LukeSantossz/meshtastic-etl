"""Checkpoint do ultimo _id do Mongo processado, para retomar o polling."""
from __future__ import annotations

from typing import Optional


class MemoryCheckpoint:
    """Fallback em memoria, usado nos testes (sem Redis)."""

    def __init__(self) -> None:
        self._value: Optional[str] = None

    def get(self) -> Optional[str]:
        return self._value

    def set(self, value: str) -> None:
        self._value = value


class RedisCheckpoint:
    def __init__(self, redis_client, key: str = "lucas:checkpoint"):
        self._redis = redis_client
        self._key = key

    def get(self) -> Optional[str]:
        value = self._redis.get(self._key)
        if value is None:
            return None
        return value.decode() if isinstance(value, bytes) else value

    def set(self, value: str) -> None:
        self._redis.set(self._key, str(value))

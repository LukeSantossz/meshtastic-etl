"""Deteccao de buracos (silencio prolongado) por no, sem interpolacao."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional


@dataclass
class Gap:
    node_id: str
    start: datetime
    end: datetime
    duration_seconds: float


class GapTracker:
    def __init__(self, threshold_seconds: int, redis_client=None, prefix: str = "lucas"):
        self.threshold_seconds = threshold_seconds
        self.redis_client = redis_client
        self.prefix = prefix
        self._last: Dict[str, datetime] = {}

        if self.redis_client is not None:
            raw = self.redis_client.hgetall(self._redis_key) or {}
            for node_id, value in raw.items():
                if isinstance(node_id, bytes):
                    node_id = node_id.decode("utf-8")
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                self._last[node_id] = datetime.fromisoformat(value)

    @property
    def _redis_key(self) -> str:
        return f"{self.prefix}:gap:last"

    def check(self, node_id: str, received_at: datetime) -> Optional[Gap]:
        last = self._last.get(node_id)
        gap: Optional[Gap] = None

        if last is not None:
            delta = (received_at - last).total_seconds()
            if delta > self.threshold_seconds:
                gap = Gap(node_id=node_id, start=last, end=received_at, duration_seconds=delta)

        if last is None or received_at > last:
            self._last[node_id] = received_at
            if self.redis_client is not None:
                self.redis_client.hset(self._redis_key, node_id, received_at.isoformat())

        return gap

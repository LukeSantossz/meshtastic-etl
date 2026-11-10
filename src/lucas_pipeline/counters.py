"""Contadores de observabilidade do pipeline: em memoria durante a execucao, persistidos no Redis e recarregados no proximo start."""
from __future__ import annotations

from typing import Dict, Optional


class Counters:
    def __init__(self) -> None:
        self.processados = 0
        self.duplicatas_descartadas = 0
        self.timestamps_invalidos = 0
        self.buracos_detectados = 0
        self.quarentena: Dict[str, int] = {}

    def incr(self, field: str, amount: int = 1, reason: Optional[str] = None) -> None:
        if field == "quarentena":
            motivo = reason or "desconhecido"
            self.quarentena[motivo] = self.quarentena.get(motivo, 0) + amount
            return
        if not hasattr(self, field):
            raise ValueError(f"contador desconhecido: {field}")
        setattr(self, field, getattr(self, field) + amount)

    def as_dict(self) -> dict:
        return {
            "processados": self.processados,
            "duplicatas_descartadas": self.duplicatas_descartadas,
            "timestamps_invalidos": self.timestamps_invalidos,
            "buracos_detectados": self.buracos_detectados,
            "quarentena": dict(self.quarentena),
        }

    def merge(self, other: "Counters") -> None:
        self.processados += other.processados
        self.duplicatas_descartadas += other.duplicatas_descartadas
        self.timestamps_invalidos += other.timestamps_invalidos
        self.buracos_detectados += other.buracos_detectados
        for motivo, qtd in other.quarentena.items():
            self.quarentena[motivo] = self.quarentena.get(motivo, 0) + qtd

    def sync_to_redis(self, redis_client, prefix: str = "lucas") -> None:
        base = f"{prefix}:counters"
        redis_client.set(f"{base}:processados", self.processados)
        redis_client.set(f"{base}:duplicatas_descartadas", self.duplicatas_descartadas)
        redis_client.set(f"{base}:timestamps_invalidos", self.timestamps_invalidos)
        redis_client.set(f"{base}:buracos_detectados", self.buracos_detectados)
        for motivo, qtd in self.quarentena.items():
            redis_client.hset(f"{base}:quarentena", motivo, qtd)

    @classmethod
    def load_from_redis(cls, redis_client, prefix: str = "lucas") -> "Counters":
        base = f"{prefix}:counters"
        counters = cls()
        counters.processados = _to_int(redis_client.get(f"{base}:processados"))
        counters.duplicatas_descartadas = _to_int(redis_client.get(f"{base}:duplicatas_descartadas"))
        counters.timestamps_invalidos = _to_int(redis_client.get(f"{base}:timestamps_invalidos"))
        counters.buracos_detectados = _to_int(redis_client.get(f"{base}:buracos_detectados"))
        quarentena_raw = redis_client.hgetall(f"{base}:quarentena") or {}
        counters.quarentena = {_to_str(motivo): _to_int(qtd) for motivo, qtd in quarentena_raw.items()}
        return counters


def _to_int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return int(value)


def _to_str(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)

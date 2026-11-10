"""Validacao de faixa fisica das leituras decodificadas."""
from __future__ import annotations

import math
from typing import Optional, Tuple

RANGES = {
    "temperature": (-10.0, 60.0),
    "humidity": (0.0, 100.0),
    "relative_humidity": (0.0, 100.0),
    "battery_level": (0.0, 100.0),
    "voltage": (2.5, 5.0),
    "channel_utilization": (0.0, 100.0),
    "air_util_tx": (0.0, 100.0),
    "latitude": (-90.0, 90.0),
    "longitude": (-180.0, 180.0),
}


def validate_reading(reading) -> Tuple[bool, Optional[str]]:
    value = reading.value
    metric = reading.metric

    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        return False, f"{metric}_valor_nao_finito"

    if metric == "uptime_seconds":
        if value >= 0:
            return True, None
        return False, "uptime_seconds_negativo"

    bounds = RANGES.get(metric)
    if bounds is None:
        return True, None  # metrica desconhecida, mas numerica e finita: aceita

    low, high = bounds
    if low <= value <= high:
        return True, None
    return False, f"{metric}_fora_de_faixa"

"""Validacao do relogio do no (node_timestamp), sempre subordinado ao received_at."""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

EPOCH_THRESHOLD = 1_000_000_000  # timestamps menores indicam relogio/GPS nao sincronizado
MAX_FUTURE_SECONDS = 24 * 3600
MAX_PAST_SECONDS = 30 * 24 * 3600


def validate_node_timestamp(
    node_timestamp: Optional[int], received_at: datetime
) -> Tuple[bool, Optional[str]]:
    """node_timestamp e apenas um atributo; a serie sempre usa received_at como relogio."""
    if node_timestamp is None:
        return True, None

    if node_timestamp <= 0:
        return False, "timestamp_zero_ou_negativo"

    if node_timestamp < EPOCH_THRESHOLD:
        return False, "timestamp_epoca_1970"

    delta = node_timestamp - received_at.timestamp()

    if delta > MAX_FUTURE_SECONDS:
        return False, "timestamp_futuro"

    if delta < -MAX_PAST_SECONDS:
        return False, "timestamp_muito_antigo"

    return True, None

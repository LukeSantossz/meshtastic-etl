"""Decodifica documentos brutos do Mongo (pacotes Meshtastic/DHT11) em leituras de metrica."""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple


class DecodeError(Exception):
    def __init__(self, reason: str, detail: str = ""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass
class Envelope:
    node_id: str
    packet_id: str
    received_at: datetime
    node_timestamp: Optional[int]
    packet_type: Optional[str]


@dataclass
class MetricReading:
    node_id: str
    packet_id: str
    received_at: datetime
    metric: str
    value: float
    unit: Optional[str]
    node_timestamp: Optional[int]
    packet_type: Optional[str]


def _parse_received_at(doc: dict) -> datetime:
    raw = doc.get("received_at")
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DecodeError("received_at_invalido", raw) from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    doc_id = doc.get("_id")
    if doc_id is not None and hasattr(doc_id, "generation_time"):
        return doc_id.generation_time

    raise DecodeError("received_at_ausente")


def _fallback_packet_id(sender: str, received_at: datetime, packet_type: Optional[str]) -> str:
    base = f"{sender}|{received_at.isoformat()}|{packet_type}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def extract_envelope(doc: dict) -> Envelope:
    payload_data = doc.get("payload_data")
    if not isinstance(payload_data, dict):
        raise DecodeError("payload_data_ausente")

    sender = payload_data.get("sender")
    if not sender:
        raise DecodeError("sender_ausente")

    received_at = _parse_received_at(doc)
    packet_type = payload_data.get("type")
    node_timestamp = payload_data.get("timestamp")

    raw_packet_id = payload_data.get("id")
    if raw_packet_id is None:
        raw_packet_id = payload_data.get("packet_id")
    packet_id = (
        str(raw_packet_id)
        if raw_packet_id is not None
        else _fallback_packet_id(sender, received_at, packet_type)
    )

    return Envelope(
        node_id=str(sender),
        packet_id=packet_id,
        received_at=received_at,
        node_timestamp=node_timestamp,
        packet_type=packet_type,
    )


def _finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _extract_env_metrics(payload: dict) -> List[Tuple[str, float, str]]:
    out: List[Tuple[str, float, str]] = []
    temp = _finite_number(payload.get("temperature"))
    if temp is not None:
        out.append(("temperature", temp, "°C"))
    humid = _finite_number(payload.get("relative_humidity", payload.get("humidity")))
    if humid is not None:
        out.append(("relative_humidity", humid, "%"))
    return out


def _extract_telemetry_metrics(payload: dict) -> List[Tuple[str, float, str]]:
    nested = payload.get("device_metrics")
    source = nested if isinstance(nested, dict) else payload
    fields = (
        ("battery_level", "%"),
        ("voltage", "V"),
        ("channel_utilization", "%"),
        ("air_util_tx", "%"),
        ("uptime_seconds", "s"),
    )
    out: List[Tuple[str, float, str]] = []
    for name, unit in fields:
        value = _finite_number(source.get(name))
        if value is not None:
            out.append((name, value, unit))
    return out


def _extract_position_metrics(payload: dict) -> List[Tuple[str, float, str]]:
    out: List[Tuple[str, float, str]] = []

    lat = _finite_number(payload.get("latitude"))
    if lat is None:
        lat_i = _finite_number(payload.get("latitude_i"))
        lat = lat_i / 1e7 if lat_i is not None else None
    if lat is not None:
        out.append(("latitude", lat, "°"))

    lon = _finite_number(payload.get("longitude"))
    if lon is None:
        lon_i = _finite_number(payload.get("longitude_i"))
        lon = lon_i / 1e7 if lon_i is not None else None
    if lon is not None:
        out.append(("longitude", lon, "°"))

    alt = _finite_number(payload.get("altitude"))
    if alt is not None:
        out.append(("altitude", alt, "m"))
    return out


def _parse_json_object(value: Any) -> Optional[dict]:
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_dht11_text_metrics(payload: Any) -> List[Tuple[str, float, str]]:
    """Classe DHT11: tipo 'text' mas com leitura de temperatura/umidade embutida."""
    candidate: Any = payload
    if isinstance(payload, dict) and "text" in payload:
        candidate = _parse_json_object(payload.get("text")) or payload
    elif isinstance(payload, str):
        candidate = _parse_json_object(payload) or {}

    if not isinstance(candidate, dict):
        return []

    temp = _finite_number(candidate.get("t", candidate.get("temp")))
    humid = _finite_number(candidate.get("u", candidate.get("umid")))

    out: List[Tuple[str, float, str]] = []
    if temp is not None:
        out.append(("temperature", temp, "°C"))
    if humid is not None:
        out.append(("relative_humidity", humid, "%"))
    return out


def _extract_metrics(packet_type: Optional[str], payload: Any) -> List[Tuple[str, float, str]]:
    if packet_type in ("env_metrics", "environment") and isinstance(payload, dict):
        return _extract_env_metrics(payload)
    if packet_type in ("telemetry", "device_metrics") and isinstance(payload, dict):
        return _extract_telemetry_metrics(payload)
    if packet_type == "position" and isinstance(payload, dict):
        return _extract_position_metrics(payload)
    if packet_type == "text":
        # Pode ser um sensor DHT11 disfarcado de texto, ou uma mensagem sem numeros
        # (nesse caso simplesmente nao emitimos metricas, mas o envelope ja foi contado).
        return _extract_dht11_text_metrics(payload)
    return []


def decode_metrics(doc: dict, envelope: Envelope) -> List[MetricReading]:
    payload_data = doc.get("payload_data") or {}
    payload = payload_data.get("payload")
    triples = _extract_metrics(envelope.packet_type, payload)
    return [
        MetricReading(
            node_id=envelope.node_id,
            packet_id=envelope.packet_id,
            received_at=envelope.received_at,
            metric=name,
            value=value,
            unit=unit,
            node_timestamp=envelope.node_timestamp,
            packet_type=envelope.packet_type,
        )
        for name, value, unit in triples
    ]


def decode_document(doc: dict) -> List[MetricReading]:
    envelope = extract_envelope(doc)
    return decode_metrics(doc, envelope)

import pytest

from lucas_pipeline import decode


def _find(packets, packet_id):
    return next(d for d in packets if d.get("payload_data") and d["payload_data"].get("id") == packet_id)


def test_dht11_text_shape_t_u(synthetic_packets):
    doc = _find(synthetic_packets, 1011)
    readings = decode.decode_document(doc)
    by_metric = {r.metric: r for r in readings}

    assert by_metric["temperature"].value == 21
    assert by_metric["temperature"].unit == "°C"
    assert by_metric["relative_humidity"].value == 15
    assert by_metric["relative_humidity"].unit == "%"


def test_telemetry_fields(synthetic_packets):
    doc = _find(synthetic_packets, 1003)
    readings = decode.decode_document(doc)
    metrics = {r.metric: r.value for r in readings}

    assert metrics["battery_level"] == 87
    assert metrics["voltage"] == 4.01
    assert metrics["channel_utilization"] == 12.3
    assert metrics["air_util_tx"] == 3.1
    assert metrics["uptime_seconds"] == 360000


def test_env_metrics_fields(synthetic_packets):
    doc = _find(synthetic_packets, 1001)
    readings = decode.decode_document(doc)
    metrics = {r.metric: r.value for r in readings}

    assert metrics["temperature"] == 24.5
    assert metrics["relative_humidity"] == 61.2


def test_position_scaled_lat_lon(synthetic_packets):
    doc = _find(synthetic_packets, 1005)
    readings = decode.decode_document(doc)
    metrics = {r.metric: r.value for r in readings}

    assert metrics["latitude"] == pytest.approx(-23.355)
    assert metrics["longitude"] == pytest.approx(-46.633)
    assert metrics["altitude"] == 760


def test_plain_text_without_sensor_numbers_has_no_metrics(synthetic_packets):
    doc = _find(synthetic_packets, 1007)
    readings = decode.decode_document(doc)
    assert readings == []


def test_malformed_payload_data_raises(synthetic_packets):
    doc = next(d for d in synthetic_packets if d.get("payload_data") is None)
    with pytest.raises(decode.DecodeError) as exc_info:
        decode.decode_document(doc)
    assert exc_info.value.reason == "payload_data_ausente"


def test_missing_sender_raises():
    doc = {
        "payload_data": {"type": "env_metrics", "payload": {"temperature": 20}},
        "received_at": "2026-01-01T12:00:00Z",
    }
    with pytest.raises(decode.DecodeError) as exc_info:
        decode.decode_document(doc)
    assert exc_info.value.reason == "sender_ausente"


def test_packet_id_extracted_from_payload_data(synthetic_packets):
    doc = _find(synthetic_packets, 1001)
    envelope = decode.extract_envelope(doc)
    assert envelope.packet_id == "1001"
    assert envelope.node_id == "!8fa724e8"


def test_packet_id_fallback_hash_when_missing():
    doc = {
        "payload_data": {"sender": "!noid0001", "type": "text", "payload": "mensagem sem id"},
        "received_at": "2026-01-01T12:12:00Z",
    }
    envelope = decode.extract_envelope(doc)
    assert envelope.packet_id
    assert envelope.node_id == "!noid0001"

    envelope_again = decode.extract_envelope(doc)
    assert envelope_again.packet_id == envelope.packet_id

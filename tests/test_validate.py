from datetime import datetime, timezone

from lucas_pipeline.decode import MetricReading
from lucas_pipeline.validate import validate_reading


def make_reading(metric, value):
    return MetricReading(
        node_id="!node1",
        packet_id="1",
        received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        metric=metric,
        value=value,
        unit=None,
        node_timestamp=None,
        packet_type="env_metrics",
    )


def test_in_range_temperature_is_ok():
    ok, reason = validate_reading(make_reading("temperature", 24.5))
    assert ok
    assert reason is None


def test_out_of_range_temperature_is_quarantined():
    ok, reason = validate_reading(make_reading("temperature", 532.7))
    assert not ok
    assert reason == "temperature_fora_de_faixa"


def test_humidity_above_100_fails():
    ok, reason = validate_reading(make_reading("relative_humidity", 150))
    assert not ok
    assert reason == "relative_humidity_fora_de_faixa"


def test_unknown_metric_finite_value_is_ok():
    ok, reason = validate_reading(make_reading("rssi", -70))
    assert ok
    assert reason is None


def test_non_finite_value_fails():
    ok, reason = validate_reading(make_reading("temperature", float("nan")))
    assert not ok
    assert reason == "temperature_valor_nao_finito"

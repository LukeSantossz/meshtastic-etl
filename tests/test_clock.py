from datetime import datetime, timezone

from lucas_pipeline.clock import validate_node_timestamp

RECEIVED = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_timestamp_zero_is_invalid():
    ok, reason = validate_node_timestamp(0, RECEIVED)
    assert not ok
    assert reason


def test_timestamp_near_1970_is_invalid():
    ok, reason = validate_node_timestamp(500_000, RECEIVED)
    assert not ok
    assert reason == "timestamp_epoca_1970"


def test_good_timestamp_is_ok():
    ts = int(RECEIVED.timestamp()) - 10
    ok, reason = validate_node_timestamp(ts, RECEIVED)
    assert ok
    assert reason is None


def test_missing_timestamp_is_ok_but_not_invalid():
    ok, reason = validate_node_timestamp(None, RECEIVED)
    assert ok
    assert reason is None


def test_far_future_timestamp_is_invalid():
    ts = int(RECEIVED.timestamp()) + 2 * 24 * 3600
    ok, reason = validate_node_timestamp(ts, RECEIVED)
    assert not ok
    assert reason == "timestamp_futuro"


def test_far_past_timestamp_is_invalid():
    ts = int(RECEIVED.timestamp()) - 40 * 24 * 3600
    ok, reason = validate_node_timestamp(ts, RECEIVED)
    assert not ok
    assert reason == "timestamp_muito_antigo"

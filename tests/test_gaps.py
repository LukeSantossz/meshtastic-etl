from datetime import datetime, timedelta, timezone

from lucas_pipeline.gaps import GapTracker

from .doubles import FakeRedis


def test_silence_above_threshold_creates_gap():
    tracker = GapTracker(threshold_seconds=900)
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert tracker.check("!node1", start) is None

    later = start + timedelta(seconds=1800)
    gap = tracker.check("!node1", later)

    assert gap is not None
    assert gap.node_id == "!node1"
    assert gap.start == start
    assert gap.end == later
    assert gap.duration_seconds == 1800


def test_silence_below_threshold_does_not_create_gap():
    tracker = GapTracker(threshold_seconds=900)
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    tracker.check("!node1", start)

    soon = start + timedelta(seconds=60)
    assert tracker.check("!node1", soon) is None


def test_gaps_are_tracked_independently_per_node():
    tracker = GapTracker(threshold_seconds=900)
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    tracker.check("!node1", start)

    later = start + timedelta(seconds=1800)
    assert tracker.check("!node2", later) is None


def test_gap_survives_process_restart_via_redis():
    redis_client = FakeRedis()
    before_restart = GapTracker(threshold_seconds=900, redis_client=redis_client)
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert before_restart.check("!node1", start) is None

    after_restart = GapTracker(threshold_seconds=900, redis_client=redis_client)
    later = start + timedelta(seconds=1800)
    gap = after_restart.check("!node1", later)

    assert gap is not None
    assert gap.node_id == "!node1"
    assert gap.start == start
    assert gap.end == later
    assert gap.duration_seconds == 1800


def test_without_redis_client_restart_loses_last_seen():
    redis_client = FakeRedis()
    before_restart = GapTracker(threshold_seconds=900, redis_client=redis_client)
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    before_restart.check("!node1", start)

    after_restart = GapTracker(threshold_seconds=900)
    later = start + timedelta(seconds=1800)
    assert after_restart.check("!node1", later) is None


def test_prefix_isolates_redis_gap_state():
    redis_client = FakeRedis()
    tracker_a = GapTracker(threshold_seconds=900, redis_client=redis_client, prefix="lucas")
    start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    tracker_a.check("!node1", start)

    tracker_b = GapTracker(threshold_seconds=900, redis_client=redis_client, prefix="outro")
    later = start + timedelta(seconds=1800)
    assert tracker_b.check("!node1", later) is None

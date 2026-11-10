from datetime import datetime, timezone

from lucas_pipeline.checkpoint import MemoryCheckpoint
from lucas_pipeline.counters import Counters
from lucas_pipeline.gaps import GapTracker
from lucas_pipeline.pipeline import Pipeline

from .doubles import FakeQuarantine, FakeRedis, FakeRedisLast, FakeStore


def make_pipeline(redis_client=None, prefix="lucas"):
    return Pipeline(
        collection=None,
        store=FakeStore(),
        redis_last=FakeRedisLast(),
        quarantine=FakeQuarantine(),
        checkpoint=MemoryCheckpoint(),
        counters=Counters(),
        gap_tracker=GapTracker(threshold_seconds=900),
        redis_client=redis_client,
        prefix=prefix,
    )


def make_doc(packet_id, received_at):
    return {
        "payload_data": {
            "sender": "!node1",
            "type": "env_metrics",
            "payload": {"temperature": 24.0, "relative_humidity": 50.0},
            "timestamp": int(received_at.timestamp()),
            "id": packet_id,
        },
        "received_at": received_at.isoformat().replace("+00:00", "Z"),
    }


def test_sync_then_load_restores_scalars_and_quarantine():
    redis_client = FakeRedis()
    counters = Counters()
    counters.incr("processados", 5)
    counters.incr("duplicatas_descartadas", 2)
    counters.incr("timestamps_invalidos", 1)
    counters.incr("buracos_detectados", 3)
    counters.incr("quarentena", 4, reason="payload_invalido")
    counters.incr("quarentena", 1, reason="sensor_fora_do_range")

    counters.sync_to_redis(redis_client, prefix="lucas")
    restored = Counters.load_from_redis(redis_client, prefix="lucas")

    assert restored.processados == 5
    assert restored.duplicatas_descartadas == 2
    assert restored.timestamps_invalidos == 1
    assert restored.buracos_detectados == 3
    assert restored.quarentena == {
        "payload_invalido": 4,
        "sensor_fora_do_range": 1,
    }


def test_load_from_empty_redis_yields_zeros():
    redis_client = FakeRedis()

    restored = Counters.load_from_redis(redis_client, prefix="lucas")

    assert restored.processados == 0
    assert restored.duplicatas_descartadas == 0
    assert restored.timestamps_invalidos == 0
    assert restored.buracos_detectados == 0
    assert restored.quarentena == {}


def test_prefix_is_honored():
    redis_client = FakeRedis()
    counters = Counters()
    counters.incr("processados", 7)
    counters.incr("quarentena", 2, reason="duplicado")
    counters.sync_to_redis(redis_client, prefix="lucas")

    other_prefix = Counters.load_from_redis(redis_client, prefix="outro")

    assert other_prefix.processados == 0
    assert other_prefix.quarentena == {}

    same_prefix = Counters.load_from_redis(redis_client, prefix="lucas")
    assert same_prefix.processados == 7
    assert same_prefix.quarentena == {"duplicado": 2}


def test_process_batch_with_redis_client_syncs_counters():
    redis_client = FakeRedis()
    pipeline = make_pipeline(redis_client=redis_client)
    received_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    doc = make_doc(6001, received_at)

    pipeline.process_batch([doc])

    assert redis_client.get("lucas:counters:processados") == b"2"


def test_process_batch_without_redis_client_does_not_raise():
    pipeline = make_pipeline(redis_client=None)
    received_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    doc = make_doc(6002, received_at)

    pipeline.process_batch([doc])

    assert pipeline.counters.processados == 2


def test_process_batch_sync_is_recoverable_via_load_from_redis():
    redis_client = FakeRedis()
    pipeline = make_pipeline(redis_client=redis_client, prefix="outro")
    received_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    doc = make_doc(6003, received_at)

    pipeline.process_batch([doc])

    recovered = Counters.load_from_redis(redis_client, prefix="outro")
    assert recovered.processados == 2

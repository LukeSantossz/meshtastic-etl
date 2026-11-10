from datetime import datetime, timezone

from lucas_pipeline.checkpoint import MemoryCheckpoint
from lucas_pipeline.counters import Counters
from lucas_pipeline.gaps import GapTracker
from lucas_pipeline.pipeline import Pipeline

from .doubles import FakeQuarantine, FakeRedisLast, FakeStore


def make_pipeline():
    return Pipeline(
        collection=None,
        store=FakeStore(),
        redis_last=FakeRedisLast(),
        quarantine=FakeQuarantine(),
        checkpoint=MemoryCheckpoint(),
        counters=Counters(),
        gap_tracker=GapTracker(threshold_seconds=900),
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


def test_duplicate_packet_is_discarded_without_double_counting():
    pipeline = make_pipeline()
    received_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    doc = make_doc(5001, received_at)

    pipeline.process_document(doc)
    assert pipeline.counters.processados == 2
    assert pipeline.counters.duplicatas_descartadas == 0
    assert len(pipeline.store.serie_rows) == 2

    pipeline.process_document(doc)
    assert pipeline.counters.processados == 2
    assert pipeline.counters.duplicatas_descartadas == 1
    assert len(pipeline.store.serie_rows) == 2


def test_different_packet_ids_both_count():
    pipeline = make_pipeline()
    received_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    pipeline.process_document(make_doc(5002, received_at))
    pipeline.process_document(make_doc(5003, received_at))

    assert pipeline.counters.processados == 4
    assert pipeline.counters.duplicatas_descartadas == 0

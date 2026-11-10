"""Orquestra decode -> validacao de relogio -> dedupe -> validacao de faixa -> gravacao."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterable, Optional, Set, Tuple

from . import decode
from .clock import validate_node_timestamp
from .gaps import GapTracker
from .validate import validate_reading

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        collection,
        store,
        redis_last,
        quarantine,
        checkpoint,
        counters,
        gap_tracker: Optional[GapTracker] = None,
        gap_threshold_seconds: int = 900,
        batch_size: int = 100,
        poll_interval_seconds: int = 5,
        redis_client=None,
        prefix: str = "lucas",
    ):
        self.collection = collection
        self.store = store
        self.redis_last = redis_last
        self.quarantine = quarantine
        self.checkpoint = checkpoint
        self.counters = counters
        self.gap_tracker = gap_tracker or GapTracker(gap_threshold_seconds)
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self.redis_client = redis_client
        self.prefix = prefix
        self._seen_packets: Set[Tuple[str, str]] = set()

    def process_document(self, doc: dict) -> None:
        try:
            envelope = decode.extract_envelope(doc)
        except decode.DecodeError as exc:
            self.counters.incr("quarentena", reason=exc.reason)
            self._quarantine(doc, exc.reason)
            return

        key = (envelope.node_id, envelope.packet_id)
        if key in self._seen_packets:
            self.counters.incr("duplicatas_descartadas")
            return
        self._seen_packets.add(key)

        clock_ok, clock_reason = validate_node_timestamp(envelope.node_timestamp, envelope.received_at)
        if not clock_ok:
            self.counters.incr("timestamps_invalidos")
            logger.debug("node_timestamp invalido node=%s motivo=%s", envelope.node_id, clock_reason)

        readings = decode.decode_metrics(doc, envelope)

        accepted = []
        for reading in readings:
            ok, reason = validate_reading(reading)
            if ok:
                accepted.append(reading)
            else:
                self.counters.incr("quarentena", reason=reason)
                self._quarantine(doc, reason)

        gap = self.gap_tracker.check(envelope.node_id, envelope.received_at)
        if gap is not None:
            self.counters.incr("buracos_detectados")
            self.store.insert_buraco(gap)

        if accepted:
            self.store.insert_serie_batch(accepted)
            for reading in accepted:
                self.redis_last.set(reading)
                self._update_aggregates(reading)
            self.counters.incr("processados", amount=len(accepted))

    def _quarantine(self, doc: dict, reason: str) -> None:
        self.quarantine.insert(doc, reason, datetime.now(timezone.utc))

    def _update_aggregates(self, reading) -> None:
        bucket_hora = reading.received_at.replace(minute=0, second=0, microsecond=0)
        bucket_dia = reading.received_at.date()
        self.store.upsert_agregado_hora(reading.node_id, reading.metric, bucket_hora, reading.value)
        self.store.upsert_agregado_dia(reading.node_id, reading.metric, bucket_dia, reading.value)

    def process_batch(self, docs: Iterable[dict]) -> int:
        count = 0
        for doc in docs:
            self.process_document(doc)
            if "_id" in doc:
                self.checkpoint.set(str(doc["_id"]))
            count += 1
        if self.redis_client is not None:
            self.counters.sync_to_redis(self.redis_client, prefix=self.prefix)
        return count

    def run_once(self) -> int:
        from bson import ObjectId

        query = {}
        last_id = self.checkpoint.get()
        if last_id is not None:
            query["_id"] = {"$gt": ObjectId(last_id)}

        docs = list(self.collection.find(query).sort("_id", 1).limit(self.batch_size))
        if docs:
            self.process_batch(docs)
        return len(docs)

    def run_forever(self) -> None:
        while True:
            n = self.run_once()
            if n:
                logger.info("lote processado tamanho=%s contadores=%s", n, self.counters.as_dict())
            time.sleep(self.poll_interval_seconds)

"""Sinks de destino: MariaDB (serie/agregados/buracos), Redis (ultimo valor) e Mongo (quarentena)."""
from __future__ import annotations

import json
from typing import Iterable


class MariaDBStore:
    def __init__(self, connection, prefix: str = "lucas"):
        self._conn = connection
        self._prefix = prefix

    def _table(self, name: str) -> str:
        return f"{self._prefix}_{name}"

    def ensure_schema(self, schema_sql: str) -> None:
        with self._conn.cursor() as cur:
            for statement in schema_sql.split(";"):
                stmt = statement.strip()
                if stmt:
                    cur.execute(stmt)
        self._conn.commit()

    def insert_serie_batch(self, readings: Iterable) -> int:
        readings = list(readings)
        if not readings:
            return 0

        table = self._table("serie")
        sql = (
            f"INSERT IGNORE INTO {table} "
            "(node_id, packet_id, received_at, metric, value, unit, node_timestamp) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        )
        rows = [
            (r.node_id, r.packet_id, r.received_at, r.metric, r.value, r.unit, r.node_timestamp)
            for r in readings
        ]
        with self._conn.cursor() as cur:
            cur.executemany(sql, rows)
            inserted = cur.rowcount
        self._conn.commit()
        return inserted if inserted is not None and inserted >= 0 else len(rows)

    def _upsert_agregado(self, table: str, bucket_col: str, node_id: str, metric: str, bucket_value, value: float) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT avg_value, min_value, max_value, count_value FROM {table} "
                f"WHERE node_id = %s AND metric = %s AND {bucket_col} = %s",
                (node_id, metric, bucket_value),
            )
            row = cur.fetchone()
            if row is None:
                avg_value, min_value, max_value, count_value = value, value, value, 1
            else:
                count_value = row["count_value"] + 1
                avg_value = (row["avg_value"] * row["count_value"] + value) / count_value
                min_value = min(row["min_value"], value)
                max_value = max(row["max_value"], value)

            cur.execute(
                f"INSERT INTO {table} "
                f"(node_id, metric, {bucket_col}, avg_value, min_value, max_value, count_value) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE avg_value = %s, min_value = %s, max_value = %s, count_value = %s",
                (
                    node_id, metric, bucket_value, avg_value, min_value, max_value, count_value,
                    avg_value, min_value, max_value, count_value,
                ),
            )
        self._conn.commit()

    def upsert_agregado_hora(self, node_id: str, metric: str, bucket_hora, value: float) -> None:
        self._upsert_agregado(self._table("agregado_hora"), "bucket_hora", node_id, metric, bucket_hora, value)

    def upsert_agregado_dia(self, node_id: str, metric: str, bucket_dia, value: float) -> None:
        self._upsert_agregado(self._table("agregado_dia"), "bucket_dia", node_id, metric, bucket_dia, value)

    def insert_buraco(self, gap) -> None:
        table = self._table("buracos")
        sql = (
            f"INSERT IGNORE INTO {table} (node_id, silencio_inicio, silencio_fim, duracao_segundos) "
            "VALUES (%s, %s, %s, %s)"
        )
        with self._conn.cursor() as cur:
            cur.execute(sql, (gap.node_id, gap.start, gap.end, int(gap.duration_seconds)))
        self._conn.commit()


class RedisLastValue:
    def __init__(self, redis_client, prefix: str = "lucas"):
        self._redis = redis_client
        self._prefix = prefix

    def set(self, reading) -> None:
        key = f"{self._prefix}:last:{reading.node_id}:{reading.metric}"
        payload = json.dumps(
            {
                "value": reading.value,
                "unit": reading.unit,
                "received_at": reading.received_at.isoformat(),
            }
        )
        self._redis.set(key, payload)


class MongoQuarantine:
    def __init__(self, collection):
        self._collection = collection

    def insert(self, doc: dict, reason: str, quarantined_at) -> None:
        record = dict(doc)
        record["_motivo_quarentena"] = reason
        record["_quarentena_em"] = quarantined_at.isoformat()
        self._collection.insert_one(record)

"""Test doubles para store/redis_last/quarantine, usados nos testes unitarios (sem Docker)."""


class FakeStore:
    def __init__(self):
        self.serie_rows = []
        self.buracos = []
        self.aggregates_hora = []
        self.aggregates_dia = []

    def insert_serie_batch(self, readings):
        self.serie_rows.extend(readings)
        return len(readings)

    def insert_buraco(self, gap):
        self.buracos.append(gap)

    def upsert_agregado_hora(self, node_id, metric, bucket, value):
        self.aggregates_hora.append((node_id, metric, bucket, value))

    def upsert_agregado_dia(self, node_id, metric, bucket, value):
        self.aggregates_dia.append((node_id, metric, bucket, value))


class FakeRedisLast:
    def __init__(self):
        self.values = {}

    def set(self, reading):
        self.values[(reading.node_id, reading.metric)] = reading


class FakeQuarantine:
    def __init__(self):
        self.items = []

    def insert(self, doc, reason, quarantined_at):
        self.items.append((doc, reason, quarantined_at))


class FakeRedis:
    """Dict-backed stand-in for redis.Redis, covering only get/set/hset/hgetall."""

    def __init__(self):
        self.strings = {}
        self.hashes = {}

    def get(self, key):
        value = self.strings.get(key)
        if value is None:
            return None
        return value.encode("utf-8") if isinstance(value, str) else str(value).encode("utf-8")

    def set(self, key, value):
        self.strings[key] = value

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value

    def hgetall(self, key):
        fields = self.hashes.get(key, {})
        return {
            (k.encode("utf-8") if isinstance(k, str) else k): str(v).encode("utf-8")
            for k, v in fields.items()
        }

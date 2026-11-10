"""Ponto de entrada do pipeline: conecta Mongo/MariaDB/Redis e roda o loop de polling."""
import logging
from pathlib import Path

import pymongo
import pymysql
import redis

from .checkpoint import RedisCheckpoint
from .config import Settings, load_settings
from .counters import Counters
from .gaps import GapTracker
from .pipeline import Pipeline
from .store import MariaDBStore, MongoQuarantine, RedisLastValue

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"


def build_pipeline(settings: Settings) -> Pipeline:
    mongo_client = pymongo.MongoClient(settings.mongo_uri)
    mongo_db = mongo_client[settings.mongo_db]
    collection = mongo_db[settings.mongo_collection]
    quarantine_collection = mongo_db[settings.mongo_quarantine_collection]

    mariadb_conn = pymysql.connect(
        host=settings.mariadb_host,
        port=settings.mariadb_port,
        user=settings.mariadb_user,
        password=settings.mariadb_password,
        database=settings.mariadb_database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

    redis_client = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
    )

    store = MariaDBStore(mariadb_conn, prefix=settings.prefix)
    if settings.ensure_schema and SCHEMA_PATH.exists():
        store.ensure_schema(SCHEMA_PATH.read_text(encoding="utf-8"))

    return Pipeline(
        collection=collection,
        store=store,
        redis_last=RedisLastValue(redis_client, prefix=settings.prefix),
        quarantine=MongoQuarantine(quarantine_collection),
        checkpoint=RedisCheckpoint(redis_client, key=settings.redis_key("checkpoint")),
        counters=Counters.load_from_redis(redis_client, prefix=settings.prefix),
        gap_tracker=GapTracker(
            threshold_seconds=settings.gap_threshold_seconds,
            redis_client=redis_client,
            prefix=settings.prefix,
        ),
        batch_size=settings.batch_size,
        poll_interval_seconds=settings.poll_interval_seconds,
        redis_client=redis_client,
        prefix=settings.prefix,
    )


def main() -> None:
    settings = load_settings()
    pipeline = build_pipeline(settings)
    logger.info("pipeline iniciado prefix=%s colecao=%s", settings.prefix, settings.mongo_collection)
    try:
        pipeline.run_forever()
    except KeyboardInterrupt:
        logger.info("interrompido pelo usuario. contadores finais=%s", pipeline.counters.as_dict())


if __name__ == "__main__":
    main()

"""Configuracao do pipeline, carregada de variaveis de ambiente (.env)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass
class Settings:
    mongo_uri: str
    mongo_db: str
    mongo_collection: str
    mongo_quarantine_collection: str
    mariadb_host: str
    mariadb_port: int
    mariadb_user: str
    mariadb_password: str
    mariadb_database: str
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
    prefix: str
    poll_interval_seconds: int
    gap_threshold_seconds: int
    batch_size: int
    ensure_schema: bool

    def redis_key(self, suffix: str) -> str:
        return f"{self.prefix}:{suffix}"

    def table(self, name: str) -> str:
        return f"{self.prefix}_{name}"


def load_settings(env_file: str = ".env") -> Settings:
    load_dotenv(env_file, override=False)
    prefix = os.getenv("PREFIX", "lucas")

    return Settings(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "iot"),
        mongo_collection=os.getenv("MONGO_COLLECTION", "pacotes_brutos"),
        mongo_quarantine_collection=os.getenv("MONGO_QUARANTINE_COLLECTION", f"{prefix}_quarentena"),
        mariadb_host=os.getenv("MARIADB_HOST", "localhost"),
        mariadb_port=int(os.getenv("MARIADB_PORT", "3306")),
        mariadb_user=os.getenv("MARIADB_USER", "lucas"),
        mariadb_password=os.getenv("MARIADB_PASSWORD", "changeme"),
        mariadb_database=os.getenv("MARIADB_DATABASE", "fatec_iot"),
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        redis_db=int(os.getenv("REDIS_DB", "0")),
        redis_password=os.getenv("REDIS_PASSWORD") or None,
        prefix=prefix,
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "5")),
        gap_threshold_seconds=int(os.getenv("GAP_THRESHOLD_SECONDS", "900")),
        batch_size=int(os.getenv("BATCH_SIZE", "100")),
        ensure_schema=os.getenv("ENSURE_SCHEMA", "0") == "1",
    )

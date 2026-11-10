#!/usr/bin/env python3
"""Carrega os pacotes sinteticos de tests/fixtures/synthetic_packets.json no Mongo local.

Uso: PYTHONPATH=src python scripts/seed_synthetic.py
Util para rodar o pipeline completo via docker-compose sem depender dos sensores reais.
Os documentos sao sinteticos; nao usar o resultado como numero real das 72h de coleta.
"""
import json
from pathlib import Path

from pymongo import MongoClient

from lucas_pipeline.config import load_settings

FIXTURES_PATH = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "synthetic_packets.json"


def main() -> None:
    settings = load_settings()
    with open(FIXTURES_PATH, encoding="utf-8") as fh:
        docs = json.load(fh)

    client = MongoClient(settings.mongo_uri)
    collection = client[settings.mongo_db][settings.mongo_collection]
    result = collection.insert_many(docs)
    print(
        f"Inseridos {len(result.inserted_ids)} documentos sinteticos em "
        f"{settings.mongo_db}.{settings.mongo_collection}"
    )


if __name__ == "__main__":
    main()

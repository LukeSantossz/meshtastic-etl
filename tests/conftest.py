import json
from datetime import datetime
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def synthetic_packets():
    with open(FIXTURES_DIR / "synthetic_packets.json", encoding="utf-8") as fh:
        return json.load(fh)


def parse_received_at(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

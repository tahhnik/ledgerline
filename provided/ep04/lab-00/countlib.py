"""Provided for Lab 00: load payment records and count them the naive way."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from typing import Iterable


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def to_rows(records: Iterable) -> list[dict]:
    """Flatten generator records into the fields a counter needs."""
    return [{"ingested_at": r.ingested_at, "event_time": r.event_time,
             "event_id": r.event["event_id"], "card_id": r.event["card_id"]} for r in records]


def load_rows(path: str) -> list[dict]:
    """Read a JSONL stream written by `python -m ledgerline`."""
    rows = []
    with open(path) as f:
        for line in f:
            rec = json.loads(line)
            e = rec["event"]
            rows.append({"ingested_at": _ts(rec["ingested_at"]), "event_time": _ts(e["event_time"]),
                         "event_id": e["event_id"], "card_id": e["card_id"]})
    return rows


def naive_count_per_card(rows: list[dict], start: datetime, end: datetime) -> dict[str, int]:
    """What a simple batch job does: count whatever arrived during the window."""
    return dict(Counter(r["card_id"] for r in rows if start <= r["ingested_at"] < end))

"""Deterministic synthetic payment stream with controllable faults.

Each record carries the business event plus `ingested_at`: the moment the
pipeline received it. The gap between `event_time` and `ingested_at` is what
makes data late; a repeated `event_id` is a duplicate delivery.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterator

from .contract import CURRENCIES, DEVICES

COUNTRIES = ("SG", "BD", "GB", "US", "DE", "IN")


def iso(ts: datetime) -> str:
    """UTC timestamp with fixed millisecond precision, e.g. 2026-11-09T10:00:00.000Z."""
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class GeneratorConfig:
    n_events: int = 1000
    seed: int = 42
    start: datetime = datetime(2026, 11, 9, 10, 0, tzinfo=timezone.utc)
    events_per_second: float = 2.0
    n_cards: int = 200
    n_merchants: int = 50
    dup_rate: float = 0.0          # share of events delivered twice
    late_rate: float = 0.0         # share of events that arrive late
    max_lateness_s: int = 600      # upper bound on lateness
    bad_rate: float = 0.0          # share of events that break the contract
    hot_card_share: float = 0.0    # share of events from one hot card


@dataclass(frozen=True, order=True)
class Record:
    ingested_at: datetime
    seq: int = field(compare=True)
    event: dict = field(compare=False)

    @property
    def event_time(self) -> datetime:
        return datetime.fromisoformat(self.event["event_time"])


def generate(cfg: GeneratorConfig) -> Iterator[Record]:
    """Yield records in arrival (`ingested_at`) order."""
    rng = random.Random(cfg.seed)
    step = timedelta(seconds=1 / cfg.events_per_second)
    out: list[Record] = []
    seq = 0
    for i in range(cfg.n_events):
        event_time = cfg.start + i * step
        if cfg.hot_card_share and rng.random() < cfg.hot_card_share:
            card = "card_0001"
        else:
            card = f"card_{rng.randint(2, cfg.n_cards):04d}"
        event = {
            "schema_version": 1,
            "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "card_id": card,
            "merchant_id": f"m_{rng.randint(1, cfg.n_merchants):03d}",
            "amount": f"{rng.lognormvariate(3.2, 0.9):.2f}",
            "currency": rng.choice(CURRENCIES),
            "event_time": iso(event_time),
            "device": rng.choice(DEVICES),
            "country": rng.choice(COUNTRIES),
        }
        if cfg.bad_rate and rng.random() < cfg.bad_rate:
            event["currency"] = rng.choice(["usd", "XXX", "US$"])

        delay = timedelta(milliseconds=rng.randint(50, 900))
        if cfg.late_rate and rng.random() < cfg.late_rate:
            delay = timedelta(seconds=rng.randint(61, cfg.max_lateness_s))
        out.append(Record(event_time + delay, seq, event))
        seq += 1

        if cfg.dup_rate and rng.random() < cfg.dup_rate:
            redelivery = delay + timedelta(seconds=rng.randint(1, 120))
            out.append(Record(event_time + redelivery, seq, dict(event)))
            seq += 1

    yield from sorted(out)

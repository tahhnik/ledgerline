"""Provided helper for Lab 04: reset a topic on Kafka and seed deterministic Ledgerline events."""
from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from ledgerline.contract import PaymentAuthorized
from ledgerline.generator import GeneratorConfig, generate
from pydantic import ValidationError

from sink_db import amount_to_cents, get_bootstrap


def recreate_topics(bootstrap: str, topics: list[str], partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_delete = [t for t in topics if t in existing]
    if to_delete:
        for f in admin.delete_topics(to_delete, operation_timeout=15).values():
            try:
                f.result(timeout=15)
            except Exception:
                pass
        deadline = time.time() + 15
        while time.time() < deadline:
            if not (set(admin.list_topics(timeout=5).topics.keys()) & set(to_delete)):
                break
            time.sleep(0.3)

    new_topics = [NewTopic(t, num_partitions=partitions, replication_factor=1) for t in topics]
    for f in admin.create_topics(new_topics, operation_timeout=15).values():
        f.result(timeout=15)

    deadline = time.time() + 20
    while time.time() < deadline:
        meta = admin.list_topics(timeout=5).topics
        if all(
            t in meta
            and not meta[t].error
            and len(meta[t].partitions) == partitions
            and all(p.leader >= 0 for p in meta[t].partitions.values())
            for t in topics
        ):
            return
        time.sleep(0.3)
    raise TimeoutError(f"Topics {topics} did not elect leaders within 20s")


def seed_stream(
    bootstrap: str,
    topic: str = "payments.authorized",
    n_events: int = 200,
    seed: int = 42,
    dup_rate: float = 0.0,
    bad_rate: float = 0.0,
    partitions: int = 3,
) -> dict[str, int]:
    recreate_topics(bootstrap, [topic, f"{topic}.dlq"], partitions=partitions)
    cfg = GeneratorConfig(n_events=n_events, seed=seed, dup_rate=dup_rate, bad_rate=bad_rate)
    records = list(generate(cfg))
    p = Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "partitioner": "murmur2_random",
        }
    )
    seen_valid: set[str] = set()
    valid_cents = 0
    bad_deliveries = 0
    for r in records:
        ev = r.event
        p.produce(topic, key=ev["card_id"].encode(), value=json.dumps(ev).encode())
        p.poll(0)
        try:
            PaymentAuthorized.model_validate(ev)
            if ev["event_id"] not in seen_valid:
                seen_valid.add(ev["event_id"])
                valid_cents += amount_to_cents(ev["amount"])
        except ValidationError:
            bad_deliveries += 1
    p.flush(30)
    return {
        "deliveries": len(records),
        "valid_unique": len(seen_valid),
        "valid_cents": valid_cents,
        "bad_deliveries": bad_deliveries,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Recreate topic and seed Ledgerline events.")
    ap.add_argument("--topic", default="payments.authorized")
    ap.add_argument("-n", "--events", type=int, default=200)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--dup-rate", type=float, default=0.0)
    ap.add_argument("--bad-rate", type=float, default=0.0)
    a = ap.parse_args()
    stats = seed_stream(
        get_bootstrap(),
        topic=a.topic,
        n_events=a.events,
        seed=a.seed,
        dup_rate=a.dup_rate,
        bad_rate=a.bad_rate,
    )
    print(
        f"seeded topic={a.topic} deliveries={stats['deliveries']} "
        f"valid_unique={stats['valid_unique']} bad_deliveries={stats['bad_deliveries']} "
        f"valid_cents={stats['valid_cents']}"
    )


if __name__ == "__main__":
    main()

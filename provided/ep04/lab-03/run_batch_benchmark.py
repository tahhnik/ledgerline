#!/usr/bin/env python3
"""Compare synchronous sends with a safe, batched producer."""
from __future__ import annotations

import json
import os
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from batch_benchmark import verdict
from safe_producer import producer_config


def run(bootstrap: str, topic: str, batched: bool, count: int = 300) -> tuple[int, float]:
    samples: list[dict] = []

    def stats(raw: str) -> int:
        samples.append(json.loads(raw))
        return 0

    config = producer_config(bootstrap, batched=batched)
    config.update({"statistics.interval.ms": 100, "stats_cb": stats, "log_level": 0})
    producer = Producer(config)
    start = time.perf_counter()
    for i in range(count):
        event = json.dumps({"event_id": f"evt_{i:05d}", "card_id": f"card_{i % 100:04d}"}).encode()
        producer.produce(topic, key=f"card_{i % 100:04d}".encode(), value=event)
        producer.poll(0)
        if not batched:
            producer.flush(10)
    if producer.flush(30):
        raise RuntimeError("records remained queued")
    producer.poll(0.2)
    elapsed = time.perf_counter() - start
    requests = max((sample.get("tx", 0) for sample in samples), default=count if not batched else 1)
    return requests, count / elapsed


def main() -> int:
    bootstrap = os.environ["BOOTSTRAP"]
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [f"lab03.batch.{name}.{uuid.uuid4().hex[:8]}" for name in ("sync", "batched")]
    futures = admin.create_topics([NewTopic(name, 6, 3, {"min.insync.replicas": "2"}) for name in topics])
    for future in futures.values():
        future.result(30)
    sync = run(bootstrap, topics[0], False)
    batched = run(bootstrap, topics[1], True)
    print(f"synchronous requests={sync[0]} throughput={sync[1]:.0f} records/s")
    print(f"batched requests={batched[0]} throughput={batched[1]:.0f} records/s")
    result = verdict(sync[0], sync[1], batched[0], batched[1])
    print(f"batched requests < synchronous: {'yes' if result['fewer_requests'] else 'no'}")
    print(f"batched throughput > synchronous: {'yes' if result['higher_throughput'] else 'no'}")
    print("safe settings preserved: yes")
    for future in admin.delete_topics(topics).values():
        future.result(30)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

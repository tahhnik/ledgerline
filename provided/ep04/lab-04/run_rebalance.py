"""Provided helper for Lab 04 Chapter 5: compare eager vs cooperative rebalance with 3 members."""
from __future__ import annotations

import argparse
import json
import threading
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from ledgerline.generator import GeneratorConfig, generate

from idempotent_sink import apply_payment
from rebalance_consumer import make_on_revoke, rebalance_config
from seed_topic import recreate_topics
from sink_db import connect_db, get_bootstrap, reset_db, summary


def seed_even_partitions(bootstrap: str, topic: str, per_partition: int = 100, seed: int = 77) -> int:
    recreate_topics(bootstrap, [topic], partitions=3)
    total = per_partition * 3
    events = [r.event for r in generate(GeneratorConfig(n_events=total, seed=seed))]
    p = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    for idx, ev in enumerate(events):
        p.produce(topic, partition=idx % 3, key=ev["card_id"].encode(), value=json.dumps(ev).encode())
        p.poll(0)
    p.flush(30)
    return total


def run_simulation(bootstrap: str, mode: str, db_path: str = "sink.db") -> dict[str, int]:
    topic = f"lab04.rebalance.{mode}"
    group = f"lab04-rebalance-{mode}-{int(time.time())}"
    seed_even_partitions(bootstrap, topic, per_partition=100, seed=77)
    reset_db(db_path).close()

    lock = threading.Lock()
    assigned_events = [threading.Event() for _ in range(3)]
    pre_rebalance_done = [threading.Event() for _ in range(3)]
    release_c2 = threading.Event()
    c2_closed = threading.Event()

    revoked_members: set[str] = set()
    survivor_revocations = 0
    deliveries_processed = 0

    def worker(idx: int) -> None:
        nonlocal survivor_revocations, deliveries_processed
        name = f"c{idx + 1}"
        pending: dict[int, TopicPartition] = {}
        phase = {"after_c2_left": False}

        if mode == "cooperative":
            cfg = rebalance_config(bootstrap, group)
            user_on_revoke = make_on_revoke(pending)
        else:
            cfg = {
                "bootstrap.servers": bootstrap,
                "group.id": group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "partition.assignment.strategy": "range",
            }
            user_on_revoke = lambda _c, _parts: None

        c = Consumer(cfg)

        def on_assign(_c: Consumer, parts: list[TopicPartition]) -> None:
            if parts:
                assigned_events[idx].set()

        def on_revoke(cons: Consumer, parts: list[TopicPartition]) -> None:
            nonlocal survivor_revocations
            with lock:
                revoked_members.add(name)
                if idx != 1 and phase["after_c2_left"]:
                    survivor_revocations += 1
            user_on_revoke(cons, parts)

        c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
        conn = connect_db(db_path)
        try:
            # Phase 1: wait until all 3 members hold 1 partition, then read 20 messages on each.
            deadline = time.time() + 30
            while not assigned_events[idx].is_set() and time.time() < deadline:
                c.poll(0.2)
            for b in assigned_events:
                b.wait(timeout=20)

            read_here = 0
            while read_here < 20:
                msg = c.poll(1.0)
                if msg is None or msg.error():
                    continue
                ev = json.loads(msg.value())
                with lock:
                    apply_payment(conn, ev, msg.partition(), msg.offset())
                    deliveries_processed += 1
                pending[msg.partition()] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                read_here += 1

            pre_rebalance_done[idx].set()
            for b in pre_rebalance_done:
                b.wait(timeout=20)

            if idx == 1:
                release_c2.wait(timeout=20)
                return

            # Survivors commit their own 20 messages before c2 leaves so any replay comes solely from c2's partition.
            if pending:
                c.commit(offsets=list(pending.values()), asynchronous=False)
                pending.clear()
            phase["after_c2_left"] = True
            release_c2.set()
            c2_closed.wait(timeout=20)

            idle_deadline = time.time() + 4.0
            while time.time() < idle_deadline:
                msg = c.poll(0.5)
                if msg is None or msg.error():
                    continue
                ev = json.loads(msg.value())
                with lock:
                    apply_payment(conn, ev, msg.partition(), msg.offset())
                    deliveries_processed += 1
                pending[msg.partition()] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                idle_deadline = time.time() + 2.5
            if pending:
                c.commit(offsets=list(pending.values()), asynchronous=False)
                pending.clear()
        finally:
            conn.close()
            c.close()
            if idx == 1:
                c2_closed.set()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    s = summary(db_path, "payments_dedup")
    return {
        "survivor_revocations": survivor_revocations,
        "deliveries_processed": deliveries_processed,
        "replayed_deliveries": deliveries_processed - s["unique_events"],
        "sink_unique": s["unique_events"],
        "total_count": s["total_count"],
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Run 3-consumer rebalance experiment.")
    ap.add_argument("--mode", choices=["eager", "cooperative"], required=True)
    ap.add_argument("--db", default="sink.db")
    a = ap.parse_args()
    res = run_simulation(get_bootstrap(), a.mode, str(Path(a.db)))
    print(
        f"mode={a.mode} survivor_revocations={res['survivor_revocations']} "
        f"deliveries_processed={res['deliveries_processed']} "
        f"replayed_deliveries={res['replayed_deliveries']} "
        f"sink_unique={res['sink_unique']} total_count={res['total_count']}"
    )


if __name__ == "__main__":
    main()

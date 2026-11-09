"""Provided helper for Lab 04 Chapter 5: compare eager vs cooperative rebalance with 3 members."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from ledgerline.generator import GeneratorConfig, generate

from idempotent_sink import apply_payment
from rebalance_consumer import make_on_revoke, rebalance_config
from seed_topic import recreate_topics
from sink_db import connect_db, get_bootstrap, reset_db, summary


def produce_even_partitions(bootstrap: str, topic: str, per_partition: int = 100, seed: int = 77) -> int:
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
    recreate_topics(bootstrap, [topic], partitions=3)
    conn = reset_db(db_path)

    survivor_revocations = 0
    deliveries_processed = 0
    watching_rebalance = False

    pending_by_member: list[dict[int, TopicPartition]] = [{}, {}, {}]
    consumers: list[Consumer] = []

    for idx in range(3):
        pending = pending_by_member[idx]
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

        def make_revoke_cb(member_idx: int, cb):
            def _on_revoke(cons: Consumer, parts: list[TopicPartition]) -> None:
                nonlocal survivor_revocations
                if parts and member_idx != 1 and watching_rebalance:
                    survivor_revocations += 1
                cb(cons, parts)

            return _on_revoke

        c.subscribe([topic], on_revoke=make_revoke_cb(idx, user_on_revoke))
        consumers.append(c)

    c1, c2, c3 = consumers
    try:
        # 1. Wait until all 3 members hold exactly 1 partition on the empty topic.
        deadline = time.time() + 25.0
        while time.time() < deadline:
            for c in consumers:
                c.poll(0.05)
            if [len(c.assignment()) for c in consumers] == [1, 1, 1]:
                break
        else:
            raise TimeoutError("3 consumers did not stabilize at 1 partition each")

        # 2. Produce 100 records to each of the 3 partitions (300 unique events total).
        produce_even_partitions(bootstrap, topic, per_partition=100, seed=77)

        # 3. c1 and c3 process all 100 records on their partition and commit; c2 processes 20 and does not commit yet.
        for idx, target in ((0, 100), (2, 100), (1, 20)):
            c = consumers[idx]
            pending = pending_by_member[idx]
            read_here = 0
            step_deadline = time.time() + 15.0
            while read_here < target and time.time() < step_deadline:
                msg = c.poll(0.2)
                if msg is None or msg.error():
                    continue
                ev = json.loads(msg.value())
                apply_payment(conn, ev, msg.partition(), msg.offset())
                deliveries_processed += 1
                pending[msg.partition()] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                read_here += 1
            if idx != 1 and pending:
                c.commit(offsets=list(pending.values()), asynchronous=False)
                pending.clear()

        c2_part = list(pending_by_member[1].keys())[0]

        # 4. Member c2 leaves the group; in cooperative mode its on_revoke commits offset 20.
        watching_rebalance = True
        c2.close()

        # 5. Survivors c1 and c3 rebalance and drain c2's reassigned partition through offset 99.
        drain_deadline = time.time() + 20.0
        drained_c2 = False
        while not drained_c2 and time.time() < drain_deadline:
            for idx in (0, 2):
                c = consumers[idx]
                pending = pending_by_member[idx]
                msg = c.poll(0.1)
                if msg is None or msg.error():
                    continue
                ev = json.loads(msg.value())
                apply_payment(conn, ev, msg.partition(), msg.offset())
                deliveries_processed += 1
                pending[msg.partition()] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                if msg.partition() == c2_part and msg.offset() >= 99:
                    drained_c2 = True

        watching_rebalance = False
        for idx in (0, 2):
            pending = pending_by_member[idx]
            if pending:
                consumers[idx].commit(offsets=list(pending.values()), asynchronous=False)
                pending.clear()
    finally:
        watching_rebalance = False
        conn.close()
        for idx in (0, 2):
            try:
                consumers[idx].close()
            except Exception:
                pass

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

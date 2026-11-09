#!/usr/bin/env python3
"""Drop one committed Produce response and compare unsafe/idempotent retries."""
from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from collections import Counter
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from safe_producer import producer_config


def load_state(path: str = "aws.env") -> dict[str, str]:
    state = {}
    for raw in Path(path).read_text().splitlines():
        if raw and not raw.startswith("#") and "=" in raw:
            name, value = raw.split("=", 1)
            state[name] = value.strip().strip("'\"")
    return state


def ssh(host: str, key: str, command: str) -> None:
    subprocess.run(
        ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no",
         "-o", "UserKnownHostsFile=/dev/null", f"ubuntu@{host}", command],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def wait_topic(admin: AdminClient, topic: str) -> None:
    deadline = time.time() + 30
    while time.time() < deadline:
        meta = admin.list_topics(topic, timeout=10).topics.get(topic)
        if meta and not meta.error and meta.partitions[0].leader >= 0:
            return
        time.sleep(0.5)
    raise TimeoutError(f"{topic} has no leader")


def consume_ids(bootstrap: str, topic: str) -> list[str]:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"retry-audit-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    # The first group join on a fresh cluster can take several seconds, so wait up to
    # 30 s for the first record; after that, stop once the topic has been idle for 3 s.
    ids, idle, seen_any, deadline = [], 0, False, time.time() + 30
    try:
        while (idle < 3) if seen_any else (time.time() < deadline):
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
            elif not msg.error():
                idle, seen_any = 0, True
                event_id = json.loads(msg.value())["event_id"]
                if event_id != "warmup":
                    ids.append(event_id)
    finally:
        consumer.close()
    return ids


def run(bootstrap: str, hosts: dict[int, str], key_file: str,
        admin: AdminClient, idempotent: bool) -> tuple[int, int, int]:
    mode = "safe" if idempotent else "unsafe"
    topic = f"lab03.retry.{mode}.{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=3, config={"min.insync.replicas": "2"})])[topic].result(30)
    wait_topic(admin, topic)

    config = producer_config(bootstrap, batched=True)
    config.update({
        "enable.idempotence": idempotent,
        "retries": 20,
        "request.timeout.ms": 3000,
        "delivery.timeout.ms": 30000,
        "retry.backoff.ms": 250,
        "linger.ms": 250,
        "batch.num.messages": 1000,
        "topic.metadata.refresh.interval.ms": 300000,
        "log_level": 0,
    })
    producer = Producer(config)
    producer.produce(topic, key=b"card_warmup", value=b'{"event_id":"warmup"}')
    if producer.flush(15):
        raise RuntimeError("warmup did not flush")

    leader = producer.list_topics(topic, timeout=10).topics[topic].partitions[0].leader
    host = hosts[leader]
    ssh(host, key_file,
        "sudo rm -f /run/ep04-lab03-response-dropped /run/ep04-lab03-drop-next && "
        "sudo touch /run/ep04-lab03-drop-next")

    delivered, errors = [], []

    def callback(err, msg) -> None:
        (errors if err else delivered).append(str(err) if err else msg.offset())

    for i in range(100):
        value = json.dumps({"event_id": f"evt_{i:04d}", "card_id": "card_0042"}).encode()
        producer.produce(topic, key=b"card_0042", value=value, on_delivery=callback)
    remaining = producer.flush(40)
    if remaining or errors or len(delivered) != 100:
        raise RuntimeError(f"delivery failure: remaining={remaining} errors={len(errors)}")
    ssh(host, key_file, "sudo test -f /run/ep04-lab03-response-dropped")

    ids = consume_ids(bootstrap, topic)
    counts = Counter(ids)
    result = (len(ids), len(counts), sum(n - 1 for n in counts.values()))
    admin.delete_topics([topic])[topic].result(30)
    return result


def main() -> int:
    bootstrap = os.environ["BOOTSTRAP"]
    state = load_state()
    hosts = {i: state[f"PUBLIC_{i}"] for i in (1, 2, 3)}
    admin = AdminClient({"bootstrap.servers": bootstrap})
    unsafe = run(bootstrap, hosts, state["KEY_FILE"], admin, False)
    safe = run(bootstrap, hosts, state["KEY_FILE"], admin, True)
    print(f"unsafe total={unsafe[0]} unique={unsafe[1]} duplicates={unsafe[2]}")
    print(f"safe   total={safe[0]} unique={safe[1]} duplicates={safe[2]}")
    passed = unsafe == (200, 100, 100) and safe == (100, 100, 0)
    print(f"retry invariant: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

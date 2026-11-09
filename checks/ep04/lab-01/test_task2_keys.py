"""Task 2: keys decide partitions; partitions keep order."""
import json
import time
from collections import defaultdict

from confluent_kafka import Consumer, Producer
from kafka_murmur2 import java_partition
from ledgerline.generator import GeneratorConfig, generate

from labenv import BOOTSTRAP
from produce import produce_events, producer_config

EVENTS = [r.event for r in generate(GeneratorConfig(n_events=300, n_cards=25, seed=31))]


def read_all(topic, n, timeout=60):
    """Read n records. The first group join on a new cluster can take a while,
    so wait up to `timeout` seconds overall and skip transient error events."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"verify-{topic}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out, deadline = [], time.time() + timeout
    while len(out) < n and time.time() < deadline:
        m = c.poll(1)
        if m is None or m.error():
            continue
        out.append(m)
    c.close()
    assert len(out) == n, f"only {len(out)} of {n} records came back within {timeout}s"
    return out


def test_producer_config():
    cfg = producer_config(BOOTSTRAP)
    assert cfg["bootstrap.servers"] == BOOTSTRAP
    assert cfg.get("acks") in ("all", "-1", -1)
    assert cfg.get("partitioner") == "murmur2_random", "match the Java clients' partitioner"


def test_each_card_lands_on_one_partition_like_java_clients(fresh_topic):
    topic = fresh_topic(3)
    delivered = produce_events(Producer(producer_config(BOOTSTRAP)), topic, EVENTS)
    assert len(delivered) == 300
    partitions = defaultdict(set)
    for key, partition, _offset in delivered:
        partitions[key].add(partition)
    assert all(len(p) == 1 for p in partitions.values()), "a key was split across partitions"
    assert all(p == {java_partition(k, 3)} for k, p in partitions.items())
    assert {p for ps in partitions.values() for p in ps} == {0, 1, 2}


def test_order_is_preserved_per_card(fresh_topic):
    topic = fresh_topic(3)
    produce_events(Producer(producer_config(BOOTSTRAP)), topic, EVENTS)
    seen = defaultdict(list)
    for m in read_all(topic, 300):
        seen[m.key().decode()].append(m.value())
    sent = defaultdict(list)
    for e in EVENTS:
        sent[e["card_id"]].append(e)
    for card, values in seen.items():
        assert [json.loads(v)["event_id"] for v in values] == [e["event_id"] for e in sent[card]]

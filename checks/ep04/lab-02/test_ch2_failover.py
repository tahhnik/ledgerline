"""Chapter 2: the cluster healed after the crashes, and no record was lost."""
from confluent_kafka import Consumer, TopicPartition

from conftest import BOOTSTRAP, topic_meta


def test_every_partition_has_a_leader_and_full_isr(admin):
    for name in ("payments.authorized", "lab02.failover"):
        t = topic_meta(admin, name)
        assert t is not None and not t.error, f"create {name} (Chapters 1 and 2)"
        for p in t.partitions.values():
            assert p.leader >= 0, f"{name}[{p.id}] has no leader"
            assert len(p.isrs) == 3, f"{name}[{p.id}] ISR is {p.isrs}: restart every stopped broker"


def test_failover_topic_kept_all_200_records(admin):
    t = topic_meta(admin, "lab02.failover")
    assert t is not None and not t.error, "create lab02.failover (Chapter 2)"
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "lab02-check"})
    total = 0
    for p in t.partitions:
        low, high = c.get_watermark_offsets(TopicPartition("lab02.failover", p), timeout=10)
        total += high - low
    c.close()
    assert total == 200, f"lab02.failover holds {total} records, expected 100 before + 100 after the crash"

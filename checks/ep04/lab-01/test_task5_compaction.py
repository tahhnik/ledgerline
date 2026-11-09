"""Task 5: a compacted topic behaves like a table keyed by card."""
import time

from confluent_kafka import Consumer, Producer

from card_status import read_table, set_status
from labenv import BOOTSTRAP

COMPACT = {"cleanup.policy": "compact", "segment.ms": "5000",
           "min.cleanable.dirty.ratio": "0.01", "delete.retention.ms": "1000"}


def write_history(topic):
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for card, status in [("card_0001", "active"), ("card_0002", "active"), ("card_0003", "active"),
                         ("card_0001", "frozen"), ("card_0002", "closed"), ("card_0003", None)]:
        set_status(p, topic, card, status)
    p.flush(10)


def test_table_view_keeps_latest_value_and_honours_tombstones(fresh_topic):
    topic = fresh_topic(1, COMPACT)
    write_history(topic)
    assert read_table(BOOTSTRAP, topic) == {"card_0001": "frozen", "card_0002": "closed"}


def test_tombstone_has_a_null_value(fresh_topic):
    topic = fresh_topic(1, COMPACT)
    write_history(topic)
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"t-{topic}", "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    last = None
    for _ in range(6):
        last = c.poll(10)
    c.close()
    assert last.key() == b"card_0003" and last.value() is None


def test_compaction_removes_superseded_records(fresh_topic):
    """Slow: waits for the log cleaner (up to ~90 s)."""
    topic = fresh_topic(1, COMPACT)
    write_history(topic)
    time.sleep(6)                                  # let the active segment age past segment.ms
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    set_status(p, topic, "card_0009", "active")    # a newer record rolls the old segment
    p.flush(10)
    deadline = time.time() + 90
    while time.time() < deadline:
        c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"c-{time.time()}",
                      "auto.offset.reset": "earliest"})
        c.subscribe([topic])
        records = []
        while (m := c.poll(3)) is not None:
            records.append((m.key(), m.value()))
        c.close()
        keys = [k for k, _ in records]
        if keys.count(b"card_0001") == 1 and keys.count(b"card_0002") == 1:
            # Superseded values are gone; the latest value per key survives.
            assert (b"card_0001", b"frozen") in records
            # The tombstone is kept for at least delete.retention.ms so that
            # consumers can observe the delete; a later cleaning pass removes it.
            assert (b"card_0003", None) in records
            return
        time.sleep(5)
    raise AssertionError(f"compaction did not run; records still present: {records}")

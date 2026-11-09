"""Chapter 4: audit.py finds acknowledged payments that cannot be read."""
from confluent_kafka import Producer

from conftest import lab_file, topic_meta
from labenv import BOOTSTRAP


def test_audit_counts_lost_ids():
    from audit import audit
    r = audit(["a", "b", "c"], {"a", "x"})
    assert (r["acked"], r["readable"], r["lost"]) == (3, 2, 2)
    assert r["lost_ids"] == ["b", "c"]


def test_read_payment_ids_reads_every_partition(fresh_topic):
    import json
    from audit import read_payment_ids
    topic = fresh_topic()
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for i in range(30):
        p.produce(topic, key=f"card_{i % 5}".encode(), value=json.dumps({"event_id": f"pay_{i}"}).encode())
    p.flush(15)
    assert read_payment_ids(BOOTSTRAP, topic) == {f"pay_{i}" for i in range(30)}


def test_incident_reproduced(admin):
    from audit import audit, read_payment_ids
    assert topic_meta(admin, "lab02.incident") is not None, "run the Chapter 4 incident"
    acked = lab_file("acked-incident.txt")
    assert len(acked) == 200, "the incident run should have 200 acknowledged payments"
    assert audit(acked, read_payment_ids(BOOTSTRAP, "lab02.incident"))["lost"] == 200

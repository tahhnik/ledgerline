"""Chapter 3 checks: poison-pill validation and dead-letter topic routing."""
from __future__ import annotations

import uuid

from confluent_kafka import Consumer

from dlq_router import consume_with_dlq
from labenv import BOOTSTRAP
from seed_topic import seed_stream
from sink_db import reset_db, summary


def _read_dlq(topic: str, expected_count: int) -> list:
    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"dlq-check-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([f"{topic}.dlq"])
    out = []
    try:
        while len(out) < expected_count:
            msg = c.poll(5.0)
            if msg is None:
                break
            if not msg.error():
                out.append(msg)
    finally:
        c.close()
    return out


def test_poison_pills_routed_to_dlq_with_headers(fresh_topic, tmp_path):
    topic = fresh_topic(partitions=3, with_dlq=True)
    expected = seed_stream(BOOTSTRAP, topic=topic, n_events=300, seed=9, bad_rate=0.04)
    db = tmp_path / "ch3.db"
    reset_db(db).close()
    group = f"ch3-dlq-{uuid.uuid4().hex[:6]}"

    report = consume_with_dlq(BOOTSTRAP, topic, group, str(db))
    assert report["valid"] == expected["valid_unique"]
    assert report["dlq"] == expected["bad_deliveries"]
    assert report["reasons"].get("currency") == expected["bad_deliveries"]

    s = summary(db, "payments_dedup")
    assert s["rows"] == expected["valid_unique"]
    assert s["total_cents"] == expected["valid_cents"]

    dlq_msgs = _read_dlq(topic, expected["bad_deliveries"])
    assert len(dlq_msgs) == expected["bad_deliveries"]
    headers = dict(dlq_msgs[0].headers() or [])
    assert headers.get("error_field") == b"currency"
    assert "source_partition" in headers
    assert "source_offset" in headers

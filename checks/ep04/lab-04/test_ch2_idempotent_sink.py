"""Chapter 2 checks: idempotent sink deduplicates retries and full topic replays."""
from __future__ import annotations

import uuid

import pytest

from idempotent_sink import apply_payment, consume_idempotent
from labenv import BOOTSTRAP
from seed_topic import seed_stream
from sink_db import connect_db, reset_db, summary


def test_apply_payment_deduplicates_by_event_id(tmp_path):
    db = tmp_path / "unit.db"
    conn = reset_db(db)
    ev = {"event_id": "evt_00000001", "card_id": "card_0007", "amount": "15.25"}
    assert apply_payment(conn, ev, 0, 0) is True
    assert apply_payment(conn, ev, 0, 1) is False
    conn.close()

    s = summary(db, "payments_dedup")
    assert s["rows"] == 1
    assert s["unique_events"] == 1
    assert s["total_count"] == 1
    assert s["total_cents"] == 1525


def test_crash_resume_and_replay_preserve_exact_totals(fresh_topic, tmp_path):
    topic = fresh_topic(partitions=3)
    expected = seed_stream(BOOTSTRAP, topic=topic, n_events=200, seed=42, dup_rate=0.05)
    db = tmp_path / "ch2.db"
    reset_db(db).close()
    group = f"ch2-idem-{uuid.uuid4().hex[:6]}"

    with pytest.raises(RuntimeError, match="crash"):
        consume_idempotent(BOOTSTRAP, topic, group, str(db), crash_after=50)
    consume_idempotent(BOOTSTRAP, topic, group, str(db), crash_after=None)

    s1 = summary(db, "payments_dedup")
    assert s1["rows"] == expected["valid_unique"] == 200
    assert s1["total_count"] == 200
    assert s1["total_cents"] == expected["valid_cents"]

    # Replay the entire topic with a fresh group against the same sink DB.
    consume_idempotent(BOOTSTRAP, topic, f"{group}-replay", str(db), crash_after=None)
    s2 = summary(db, "payments_dedup")
    assert s2 == s1
    conn = connect_db(db)
    conn.close()

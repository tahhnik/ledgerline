"""Chapter 4 checks: async batch consumer with aiokafka + idempotent sink + DLQ."""
from __future__ import annotations

import asyncio
import uuid

import pytest

from async_consumer import consume_async
from labenv import BOOTSTRAP
from seed_topic import seed_stream
from sink_db import reset_db, summary


def test_aiokafka_batch_consumer_crash_and_reconcile(fresh_topic, tmp_path):
    topic = fresh_topic(partitions=3, with_dlq=True)
    expected = seed_stream(
        BOOTSTRAP, topic=topic, n_events=300, seed=9, dup_rate=0.05, bad_rate=0.03
    )
    db = tmp_path / "ch4.db"
    reset_db(db).close()
    group = f"ch4-aio-{uuid.uuid4().hex[:6]}"

    with pytest.raises(RuntimeError, match="crash"):
        asyncio.run(consume_async(BOOTSTRAP, topic, group, str(db), batch_size=100, crash_after=100))

    res = asyncio.run(consume_async(BOOTSTRAP, topic, group, str(db), batch_size=100, crash_after=None))
    s = summary(db, "payments_dedup")
    assert s["unique_events"] == expected["valid_unique"]
    assert s["total_count"] == expected["valid_unique"]
    assert s["total_cents"] == expected["valid_cents"]
    assert res["dlq_Unique_or_sent"] if "dlq_Unique_or_sent" in res else res["dlq"] >= expected["bad_deliveries"]
    assert s["unique_events"] + expected["bad_deliveries"] == 300

"""Chapter 1 checks: the crash window (commit-before-write vs write-before-commit)."""
from __future__ import annotations

import uuid

import pytest

from labenv import BOOTSTRAP
from naive_consumer import consume_naive, naive_consumer_config
from seed_topic import seed_stream
from sink_db import reset_db, summary


def test_naive_consumer_config_disables_auto_commit():
    cfg = naive_consumer_config(BOOTSTRAP, "test-group")
    assert cfg["bootstrap.servers"] == BOOTSTRAP
    assert cfg["group.id"] == "test-group"
    assert cfg.get("auto.offset.reset") == "earliest"
    assert cfg.get("enable.auto.commit") is False


def test_write_before_commit_duplicates_on_crash(fresh_topic, tmp_path):
    topic = fresh_topic(partitions=3)
    seed_stream(BOOTSTRAP, topic=topic, n_events=200, seed=11)
    db = tmp_path / "ch1_dup.db"
    reset_db(db).close()
    group = f"ch1-dup-{uuid.uuid4().hex[:6]}"

    with pytest.raises(RuntimeError, match="crash"):
        consume_naive(BOOTSTRAP, topic, group, str(db), mode="at_least_once", crash_after=50)
    consume_naive(BOOTSTRAP, topic, group, str(db), mode="at_least_once", crash_after=None)

    s = summary(db, "payments_raw")
    assert s["unique_events"] == 200
    assert s["rows"] == 250
    assert s["total_count"] == 250


def test_commit_before_write_loses_records_on_crash(fresh_topic, tmp_path):
    topic = fresh_topic(partitions=3)
    seed_stream(BOOTSTRAP, topic=topic, n_events=200, seed=11)
    db = tmp_path / "ch1_loss.db"
    reset_db(db).close()
    group = f"ch1-loss-{uuid.uuid4().hex[:6]}"

    with pytest.raises(RuntimeError, match="crash"):
        consume_naive(BOOTSTRAP, topic, group, str(db), mode="at_most_once", crash_after=50)
    consume_naive(BOOTSTRAP, topic, group, str(db), mode="at_most_once", crash_after=None)

    s = summary(db, "payments_raw")
    assert s["rows"] == 150
    assert s["unique_events"] == 150
    assert s["total_count"] == 150

"""Chapter 5 checks: cooperative rebalance config and on_revoke offset commit."""
from __future__ import annotations

from confluent_kafka import TopicPartition

from labenv import BOOTSTRAP
from rebalance_consumer import make_on_revoke, rebalance_config
from run_rebalance import run_simulation


def test_rebalance_config_uses_cooperative_sticky():
    cfg = rebalance_config(BOOTSTRAP, "rebalance-test")
    assert cfg["bootstrap.servers"] == BOOTSTRAP
    assert cfg["group.id"] == "rebalance-test"
    assert cfg.get("enable.auto.commit") is False
    assert cfg.get("partition.assignment.strategy") == "cooperative-sticky"


def test_make_on_revoke_commits_pending_offsets_and_clears():
    committed = []

    class DummyConsumer:
        def commit(self, offsets=None, asynchronous=True):
            committed.append((offsets, asynchronous))

    pending = {0: TopicPartition("payments.authorized", 0, 20)}
    cb = make_on_revoke(pending)
    cb(DummyConsumer(), [TopicPartition("payments.authorized", 0)])
    assert len(committed) == 1
    offsets, async_flag = committed[0]
    assert async_flag is False
    assert offsets == [TopicPartition("payments.authorized", 0, 20)]
    assert pending == {}


def test_cooperative_rebalance_avoids_survivor_revocation_and_replay(tmp_path):
    db = tmp_path / "ch5.db"
    res = run_simulation(BOOTSTRAP, "cooperative", str(db))
    assert res["survivor_revocations"] == 0
    assert res["replayed_deliveries"] == 0
    assert res["sink_unique"] == 300
    assert res["total_count"] == 300

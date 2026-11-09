"""Chapter 1: three brokers and a replicated payments topic."""
from conftest import topic_meta


def test_cluster_has_three_brokers(admin):
    assert len(admin.list_topics(timeout=10).brokers) == 3, "all three brokers must be running"


def test_payments_topic_is_replicated_three_times(admin):
    t = topic_meta(admin, "payments.authorized")
    assert t is not None and not t.error, "create payments.authorized (Chapter 1)"
    assert len(t.partitions) == 3
    for p in t.partitions.values():
        assert len(p.replicas) == 3, f"partition {p.id} has {len(p.replicas)} replicas, want 3"

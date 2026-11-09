"""Task 4: the short-retention topic you configured with the CLI."""
from confluent_kafka.admin import ConfigResource


def test_retention_topic_is_configured(admin):
    res = ConfigResource(ConfigResource.Type.TOPIC, "lab01.retention")
    try:
        cfg = admin.describe_configs([res])[res].result(timeout=10)
    except Exception:
        raise AssertionError("create lab01.retention with kafka-topics --create (Task 4)")
    assert cfg["cleanup.policy"].value == "delete"
    assert int(cfg["retention.ms"].value) <= 60_000
    assert int(cfg["segment.ms"].value) <= 15_000

"""Chapter 5: durable defaults on the producer and the topic; zero loss."""
from confluent_kafka.admin import ConfigResource

from conftest import lab_file
from labenv import BOOTSTRAP


def test_durable_config():
    from durable_producer import durable_config
    cfg = durable_config(BOOTSTRAP)
    assert cfg["acks"] == "all"
    assert cfg["enable.idempotence"] is True


def test_payments_topic_has_durable_settings(admin):
    res = ConfigResource("topic", "payments.authorized")
    cfg = admin.describe_configs([res])[res].result(timeout=10)
    assert cfg["min.insync.replicas"].value == "2"
    assert cfg["unclean.leader.election.enable"].value == "false"


def test_durable_rerun_lost_nothing():
    from audit import audit, read_payment_ids
    assert len(lab_file("acked-durable-down.txt")) == 0, "no acks while only the leader is in sync"
    acked = lab_file("acked-durable.txt")
    assert len(acked) == 200
    assert audit(acked, read_payment_ids(BOOTSTRAP, "lab02.incident-durable"))["lost"] == 0

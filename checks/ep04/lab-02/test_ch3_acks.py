"""Chapter 3: durable_producer.py configures acks and counts acknowledgements."""
from confluent_kafka import Producer

from labenv import BOOTSTRAP


def test_producer_config_passes_acks_through():
    from durable_producer import producer_config
    for acks in ("0", "1", "all"):
        cfg = producer_config(BOOTSTRAP, acks)
        assert cfg["acks"] == acks
        assert cfg["bootstrap.servers"] == BOOTSTRAP
    assert int(cfg["delivery.timeout.ms"]) <= 30000, "fail fast: a short delivery timeout"


def test_send_payments_counts_every_ack(fresh_topic):
    from durable_producer import producer_config, send_payments
    topic = fresh_topic(config={"min.insync.replicas": "2"})
    payments = [{"event_id": f"pay_{i}", "card_id": f"card_{i % 3}"} for i in range(20)]
    report = send_payments(Producer(producer_config(BOOTSTRAP, "all")), topic, payments)
    assert sorted(report.acked) == sorted(p["event_id"] for p in payments)
    assert sum(report.failed.values()) == 0


def test_acks_experiment_recorded():
    from conftest import lab_file
    assert len(lab_file("acked-all.txt")) == 0, "acks=all must be refused while 2 brokers are down"
    assert len(lab_file("acked-one.txt")) == 50, "acks=1 is accepted by the leader alone"

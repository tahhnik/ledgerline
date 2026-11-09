from batch_benchmark import verdict
from safe_producer import producer_config


def test_batching_keeps_safety_settings():
    sync = producer_config("broker:9094", batched=False)
    batch = producer_config("broker:9094", batched=True)
    for key in ("enable.idempotence", "acks", "max.in.flight.requests.per.connection", "retries"):
        assert sync[key] == batch[key]
    assert batch["linger.ms"] == 20
    assert batch["batch.size"] == 65536
    assert batch["compression.type"] == "lz4"


def test_benchmark_verdict_requires_fewer_requests_and_more_throughput():
    assert verdict(300, 10.0, 12, 800.0) == {
        "fewer_requests": True,
        "higher_throughput": True,
        "passed": True,
    }
    assert verdict(10, 100.0, 12, 90.0)["passed"] is False

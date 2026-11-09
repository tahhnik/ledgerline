from safe_producer import producer_config, publish_events


def test_retry_safety_is_explicit():
    config = producer_config("broker:9094")
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["max.in.flight.requests.per.connection"] == 5
    assert config["retries"] >= 10
    assert config["partitioner"] == "murmur2_random"


def test_delivery_metadata_proves_one_partition_per_card(fresh_topic):
    topic = fresh_topic()
    events = [{"event_id": f"evt_{i:04d}", "card_id": "card_0042"} for i in range(20)]
    delivered = publish_events(None, topic, events)
    assert len(delivered) == 20
    assert len({item.partition for item in delivered}) == 1
    assert [item.event_id for item in delivered] == [event["event_id"] for event in events]

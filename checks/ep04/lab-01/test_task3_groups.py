"""Task 3: a consumer group commits its position and resumes from it."""
from confluent_kafka import Consumer, Producer, TopicPartition
from ledgerline.generator import GeneratorConfig, generate

from labenv import BOOTSTRAP
from consume import consume, consumer_config
from produce import produce_events, producer_config


def test_consumer_config():
    cfg = consumer_config(BOOTSTRAP, "g")
    assert cfg["group.id"] == "g"
    assert str(cfg["enable.auto.commit"]).lower() == "false"
    assert cfg["auto.offset.reset"] == "earliest"


def test_group_resumes_where_it_committed(fresh_topic):
    topic = fresh_topic(3)
    events = [r.event for r in generate(GeneratorConfig(n_events=300, seed=32))]
    produce_events(Producer(producer_config(BOOTSTRAP)), topic, events)
    group = f"g-{topic}"
    first = consume(BOOTSTRAP, topic, group, max_records=100)
    rest = consume(BOOTSTRAP, topic, group, max_records=1000, idle_timeout=5)
    assert len(first) == 100
    assert len(rest) == 200, "the second run must start after the committed offsets"
    ids = [m["event_id"] for m in first + rest]
    assert len(set(ids)) == 300, "records were read twice or skipped"


def test_lag_is_zero_after_consuming_everything(fresh_topic, admin):
    topic = fresh_topic(3)
    events = [r.event for r in generate(GeneratorConfig(n_events=150, seed=33))]
    produce_events(Producer(producer_config(BOOTSTRAP)), topic, events)
    group = f"g-{topic}"
    consume(BOOTSTRAP, topic, group, max_records=1000, idle_timeout=5)
    probe = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group})
    tps = [TopicPartition(topic, p) for p in range(3)]
    committed = probe.committed(tps, timeout=10)
    for tp in committed:
        _low, high = probe.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)
        assert tp.offset == high, f"partition {tp.partition}: committed {tp.offset}, end {high}"
    probe.close()

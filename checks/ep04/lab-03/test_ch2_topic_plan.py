from confluent_kafka.admin import ConfigResource

from key_plan import CHOSEN_KEY
from topic_plan import TOPIC, policy, recommended_partitions


def test_sizing_and_policy_are_explicit():
    assert recommended_partitions(3000, 750, 0.25) == 6
    plan = policy()
    assert plan == {
        "key": "card_id",
        "partitions": 6,
        "replication_factor": 3,
        "min.insync.replicas": "2",
        "retention.ms": "604800000",
        "cleanup.policy": "delete",
    }
    assert CHOSEN_KEY == plan["key"]


def test_live_topic_matches_the_plan(admin):
    meta = admin.list_topics(TOPIC, timeout=10).topics[TOPIC]
    assert not meta.error
    assert len(meta.partitions) == 6
    assert all(len(part.replicas) == 3 for part in meta.partitions.values())
    resource = ConfigResource(ConfigResource.Type.TOPIC, TOPIC)
    configs = admin.describe_configs([resource])[resource].result(20)
    assert configs["min.insync.replicas"].value == "2"
    assert configs["retention.ms"].value == "604800000"
    assert configs["cleanup.policy"].value == "delete"

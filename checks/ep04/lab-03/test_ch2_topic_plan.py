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


def test_topic_request_is_accepted_by_the_client(monkeypatch):
    """Regression: NewTopic's 4th positional argument is replica_assignment, not config.
    Building the request must not trip the client's own validation."""
    import topic_plan
    sent = []

    class FakeTopics:
        topics = {}

    class FakeAdmin:
        def __init__(self, conf):
            pass

        def list_topics(self, timeout=None):
            return FakeTopics()

        def create_topics(self, topics):
            sent.extend(topics)
            raise SystemExit("captured")

    monkeypatch.setattr(topic_plan, "AdminClient", FakeAdmin)
    try:
        topic_plan.ensure_topic("unused:9092")
    except SystemExit:
        pass
    (topic,) = sent
    assert topic.replica_assignment is None, "config was passed positionally as replica_assignment"
    assert topic.replication_factor == 3 and topic.num_partitions == 6
    assert topic.config.get("min.insync.replicas") == "2"

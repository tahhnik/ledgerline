"""Task 1: the topic you created with the CLI."""


def test_payments_topic_exists_with_three_partitions(admin):
    md = admin.list_topics(timeout=5).topics
    assert "payments.authorized" in md, "create it with kafka-topics --create (Task 1)"
    assert len(md["payments.authorized"].partitions) == 3


def test_scratch_topic_exists(admin):
    md = admin.list_topics(timeout=5).topics
    assert "lab01.scratch" in md, "create lab01.scratch for CLI experiments (Task 1)"

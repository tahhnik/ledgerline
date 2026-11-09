"""Shared fixtures. Checks import the learner's files from the lab folder:
LAB_DIR, or the current directory (run pytest from ~/lab)."""
import os
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, os.environ.get("LAB_DIR", os.getcwd()))
sys.path.insert(0, str(Path(__file__).parent))

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from labenv import BOOTSTRAP  # noqa: E402


@pytest.fixture(scope="session")
def admin():
    a = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        a.list_topics(timeout=5)
    except Exception:
        pytest.exit("Kafka is not reachable on localhost:9092. Run: docker compose up -d --wait", 2)
    return a


def wait_for_leaders(admin, topic, partitions, timeout=15):
    """A new topic's partitions need a leader before writes succeed. Producing earlier
    makes the client retry, and retries without idempotence can reorder records."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = admin.list_topics(topic, timeout=5).topics.get(topic)
        if t and not t.error and len(t.partitions) == partitions and \
                all(pm.leader >= 0 for pm in t.partitions.values()):
            return
        time.sleep(0.2)
    raise TimeoutError(f"{topic}: partitions have no leader after {timeout}s")


@pytest.fixture
def fresh_topic(admin):
    """A new, empty topic per test so tests never see each other's data."""
    created = []

    def make(partitions=3, config=None):
        name = f"test.{uuid.uuid4().hex[:8]}"
        fs = admin.create_topics([NewTopic(name, partitions, 1, config=config or {})])
        fs[name].result(timeout=10)
        created.append(name)
        wait_for_leaders(admin, name, partitions)
        return name

    yield make
    if created:
        for f in admin.delete_topics(created).values():
            f.result(timeout=10)

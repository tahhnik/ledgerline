"""Shared fixtures. Checks import the learner's files from the lab folder:
LAB_DIR, or the current directory (run pytest from ~/lab)."""
import os
import sys
import time
import uuid
from pathlib import Path

import pytest

LAB_DIR = Path(os.environ.get("LAB_DIR", os.getcwd()))
sys.path.insert(0, str(LAB_DIR))
sys.path.insert(0, str(Path(__file__).parent))

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from labenv import BOOTSTRAP  # noqa: E402


@pytest.fixture(scope="session")
def admin():
    a = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        a.list_topics(timeout=10)
    except Exception:
        pytest.exit(f"Kafka is not reachable at {BOOTSTRAP}. Run: source ~/.bashrc", 2)
    return a


def topic_meta(admin, topic):
    return admin.list_topics(topic, timeout=10).topics.get(topic)


def wait_for_leaders(admin, topic, partitions, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = topic_meta(admin, topic)
        if t and not t.error and len(t.partitions) == partitions and \
                all(pm.leader >= 0 for pm in t.partitions.values()):
            return
        time.sleep(0.5)
    raise TimeoutError(f"{topic}: partitions have no leader after {timeout}s")


@pytest.fixture
def fresh_topic(admin):
    """A new replicated topic per test, deleted afterwards."""
    created = []

    def make(partitions=3, config=None):
        name = f"test.{uuid.uuid4().hex[:8]}"
        admin.create_topics([NewTopic(name, partitions, 3, config=config or {})])[name].result(timeout=15)
        created.append(name)
        wait_for_leaders(admin, name, partitions)
        return name

    yield make
    if created:
        for f in admin.delete_topics(created).values():
            f.result(timeout=15)


def lab_file(name):
    path = LAB_DIR / name
    assert path.exists(), f"{name} not found in {LAB_DIR}: run the chapter's commands from ~/lab"
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]

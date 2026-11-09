"""Lab 03 checks import learner files from LAB_DIR or the current directory."""
from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

LAB_DIR = Path(os.environ.get("LAB_DIR", os.getcwd()))
sys.path.insert(0, str(LAB_DIR))
sys.path.insert(0, str(Path(__file__).parent))

from labenv import BOOTSTRAP  # noqa: E402


@pytest.fixture(scope="session")
def admin():
    client = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        client.list_topics(timeout=10)
    except Exception:
        pytest.exit(f"Kafka is not reachable at {BOOTSTRAP}. Run: source ~/.bashrc", 2)
    return client


def wait_for_topic(admin, topic: str, partitions: int, timeout: int = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        meta = admin.list_topics(topic, timeout=10).topics.get(topic)
        if meta and not meta.error and len(meta.partitions) == partitions and \
                all(part.leader >= 0 for part in meta.partitions.values()):
            return
        time.sleep(0.5)
    raise TimeoutError(f"{topic} did not get {partitions} leaders")


@pytest.fixture
def fresh_topic(admin):
    created = []

    def make(partitions: int = 6):
        name = f"test.lab03.{uuid.uuid4().hex[:8]}"
        admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=3,
                                       config={"min.insync.replicas": "2"})])[name].result(30)
        wait_for_topic(admin, name, partitions)
        created.append(name)
        return name

    yield make
    for future in admin.delete_topics(created).values():
        future.result(30)

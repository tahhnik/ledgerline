"""Shared fixtures for Lab 04 checks. Imports learner modules from LAB_DIR / cwd."""
from __future__ import annotations

import os
import sys
import time
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, os.environ.get("LAB_DIR", os.getcwd()))
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "provided/ep04/lab-04"))
sys.path.insert(0, str(Path(__file__).parent))

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from labenv import BOOTSTRAP  # noqa: E402


@pytest.fixture(scope="session")
def admin() -> AdminClient:
    a = AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        a.list_topics(timeout=8)
    except Exception:
        pytest.exit(f"Kafka is not reachable on {BOOTSTRAP}. Check your EC2 broker and aws.env.", 2)
    return a


def wait_for_leaders(admin: AdminClient, topic: str, partitions: int, timeout: int = 20) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = admin.list_topics(topic, timeout=5).topics.get(topic)
        if (
            t
            and not t.error
            and len(t.partitions) == partitions
            and all(pm.leader >= 0 for pm in t.partitions.values())
        ):
            return
        time.sleep(0.2)
    raise TimeoutError(f"{topic}: partitions have no leader after {timeout}s")


@pytest.fixture
def fresh_topic(admin: AdminClient):
    created: list[str] = []

    def make(partitions: int = 3, with_dlq: bool = False) -> str:
        name = f"test.lab04.{uuid.uuid4().hex[:8]}"
        topics = [name, f"{name}.dlq"] if with_dlq else [name]
        fs = admin.create_topics([NewTopic(t, partitions, 1) for t in topics])
        for t, f in fs.items():
            f.result(timeout=15)
            created.append(t)
            wait_for_leaders(admin, t, partitions)
        return name

    yield make
    if created:
        for f in admin.delete_topics(created).values():
            try:
                f.result(timeout=10)
            except Exception:
                pass

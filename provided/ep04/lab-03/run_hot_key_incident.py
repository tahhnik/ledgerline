#!/usr/bin/env python3
"""Publish the same skewed workload under merchant and card keys, then audit it."""
from __future__ import annotations

import os
import uuid

from confluent_kafka.admin import AdminClient, NewTopic

from partition_audit import audit
from safe_producer import publish_events
from workload import transactions


def main() -> int:
    bootstrap = os.environ["BOOTSTRAP"]
    admin = AdminClient({"bootstrap.servers": bootstrap})
    events = transactions()
    topics = {field: f"lab03.hot.{field}.{uuid.uuid4().hex[:8]}"
              for field in ("merchant_id", "card_id")}
    futures = admin.create_topics([
        NewTopic(topic, num_partitions=6, replication_factor=3, config={"min.insync.replicas": "2"}) for topic in topics.values()
    ])
    for future in futures.values():
        future.result(30)

    reports = {}
    for field, topic in topics.items():
        delivered = publish_events(bootstrap, topic, events, key_field=field)
        reports[field] = audit(events, delivered)

    merchant = reports["merchant_id"]
    card = reports["card_id"]
    print("merchant key counts:", " ".join(f"p{p}={n}" for p, n in merchant.counts.items()))
    print(f"merchant key max/mean: {merchant.max_mean:.2f}")
    print(f"hot merchant: {merchant.dominant_merchant}")
    print("card key counts:", " ".join(f"p{p}={n}" for p, n in card.counts.items()))
    print(f"card key max/mean: {card.max_mean:.2f}")
    cleared = merchant.alert and not card.alert
    print(f"alert cleared: {'yes' if cleared else 'no'}")
    for future in admin.delete_topics(list(topics.values())).values():
        future.result(30)
    return 0 if cleared and merchant.dominant_merchant == "merchant_hot" else 1


if __name__ == "__main__":
    raise SystemExit(main())

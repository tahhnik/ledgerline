"""Provided for Lab 01: send Ledgerline payments with your produce.py and summarise
where they landed. --no-key sends the same payments without keys, for comparison."""
import argparse
import json
import os
from collections import Counter

from confluent_kafka import Producer

from ledgerline.generator import GeneratorConfig, generate

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--events", type=int, default=300)
    ap.add_argument("--topic", default="payments.authorized")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--no-key", action="store_true", help="send without keys")
    a = ap.parse_args()
    events = [r.event for r in generate(GeneratorConfig(n_events=a.events, n_cards=25, seed=a.seed))]
    if a.no_key:
        delivered = []
        p = Producer({"bootstrap.servers": BOOTSTRAP})
        for e in events:
            p.produce(a.topic, value=json.dumps(e).encode(),
                      on_delivery=lambda err, m: delivered.append(("", m.partition(), m.offset())))
            p.poll(0)
        p.flush(30)
    else:
        from produce import produce_events, producer_config
        delivered = produce_events(Producer(producer_config(BOOTSTRAP)), a.topic, events)
    per_partition = Counter(p for _, p, _ in delivered)
    print(f"delivered {len(delivered)} records to {a.topic}")
    for part in sorted(per_partition):
        print(f"  partition {part}: {per_partition[part]} records")
    card = events[0]["card_id"]
    where = sorted({p for k, p, _ in delivered if k == card})
    print(f"  records keyed {card!r} -> partitions {where or 'none (records have no key)'}")


if __name__ == "__main__":
    main()

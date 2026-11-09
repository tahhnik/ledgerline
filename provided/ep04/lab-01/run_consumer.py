"""Provided for Lab 01: run your consume.py from the command line, or (with --forever)
as a long-running group member that prints every partition assignment."""
import argparse
import os
import signal
import sys

from confluent_kafka import Consumer

from consume import consume, consumer_config

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092")


def run_forever(topic: str, group: str, name: str) -> None:
    def on_assign(_c, parts):
        print(f"[{name}] assigned {sorted(p.partition for p in parts) or 'nothing'}", flush=True)

    def on_revoke(_c, parts):
        print(f"[{name}] revoked {sorted(p.partition for p in parts)}", flush=True)

    c = Consumer(consumer_config(BOOTSTRAP, group))
    c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        while True:
            msg = c.poll(1.0)
            if msg is not None and not msg.error():
                c.commit(message=msg, asynchronous=False)
    finally:
        c.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="payments.authorized")
    ap.add_argument("--group", default="lab01-reader")
    ap.add_argument("--max", type=int, default=50)
    ap.add_argument("--forever", action="store_true")
    ap.add_argument("--name", default="consumer")
    a = ap.parse_args()
    if a.forever:
        run_forever(a.topic, a.group, a.name)
        return
    records = consume(BOOTSTRAP, a.topic, a.group, a.max, idle_timeout=5)
    print(f"read {len(records)} records as group '{a.group}'")


if __name__ == "__main__":
    main()

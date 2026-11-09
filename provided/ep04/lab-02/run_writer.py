"""Provided for Lab 02: send Ledgerline payments with your durable_producer.py and count
what the cluster acknowledged. Acknowledged payment IDs are saved to --out, one per line,
so audit.py can later check that each one is still readable."""
import argparse
import os

from confluent_kafka import Producer

from ledgerline.generator import GeneratorConfig, generate

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9094")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="payments.authorized")
    ap.add_argument("-n", "--events", type=int, default=200)
    ap.add_argument("--acks", default="all", help="0, 1 or all (ignored with --durable)")
    ap.add_argument("--durable", action="store_true", help="use durable_config() (Chapter 5)")
    ap.add_argument("--seed", type=int, default=2)
    ap.add_argument("--out", default="acked.txt")
    a = ap.parse_args()
    from durable_producer import producer_config, send_payments
    if a.durable:
        from durable_producer import durable_config
        cfg = durable_config(BOOTSTRAP)
    else:
        cfg = producer_config(BOOTSTRAP, a.acks)
    payments = [r.event for r in generate(GeneratorConfig(n_events=a.events, n_cards=25, seed=a.seed))]
    report = send_payments(Producer(cfg), a.topic, payments)
    with open(a.out, "w") as f:
        f.writelines(pid + "\n" for pid in report.acked)
    print(f"{a.topic}: acked {len(report.acked)}, failed {sum(report.failed.values())}")
    for reason, count in sorted(report.failed.items()):
        print(f"  failed {count}: {reason}")
    print(f"acknowledged IDs saved to {a.out}")


if __name__ == "__main__":
    main()

"""Provided for Lab 02: compare the payments the producer was told were saved (--acked)
with the payments a fresh reader can find in the topic, using your audit.py."""
import argparse
import os

BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9094")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="payments.authorized")
    ap.add_argument("--acked", default="acked.txt")
    a = ap.parse_args()
    from audit import audit, read_payment_ids
    with open(a.acked) as f:
        acked = [line.strip() for line in f if line.strip()]
    readable = read_payment_ids(BOOTSTRAP, a.topic)
    result = audit(acked, readable)
    print(f"{a.topic}: acked {result['acked']}, readable {result['readable']}, "
          f"lost {result['lost']}")
    for pid in result["lost_ids"][:3]:
        print(f"  lost: {pid}")


if __name__ == "__main__":
    main()

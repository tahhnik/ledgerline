"""Task 2 (provided): profile a generated stream.

    python starter/profile_stream.py data/stream.jsonl
"""
import json
import sys
from collections import Counter
from datetime import datetime


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def main(path):
    rows = [json.loads(line) for line in open(path)]
    ids = Counter(r["event"]["event_id"] for r in rows)
    delays = [(ts(r["ingested_at"]) - ts(r["event"]["event_time"])).total_seconds() for r in rows]
    late = sum(d > 60 for d in delays)
    print(f"records delivered      {len(rows)}")
    print(f"unique business events {len(ids)}")
    print(f"duplicate deliveries   {sum(c - 1 for c in ids.values())}")
    print(f"late (> 60 s)          {late}")
    print(f"max lateness           {max(delays):.0f} s")
    times = [ts(r["event"]["event_time"]) for r in rows]
    print(f"event_time range       {min(times):%H:%M:%S} to {max(times):%H:%M:%S} UTC")


if __name__ == "__main__":
    main(sys.argv[1])

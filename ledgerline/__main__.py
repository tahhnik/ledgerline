"""CLI: python -m ledgerline [options] > events.jsonl

Writes one JSON object per line: {"ingested_at": ..., "event": {...}}.
"""
from __future__ import annotations

import argparse
import json
import sys

from .generator import GeneratorConfig, generate, iso


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="ledgerline", description="Synthetic Ledgerline payments")
    p.add_argument("-n", "--events", type=int, default=1000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dup-rate", type=float, default=0.0)
    p.add_argument("--late-rate", type=float, default=0.0)
    p.add_argument("--max-lateness", type=int, default=600, help="seconds")
    p.add_argument("--bad-rate", type=float, default=0.0)
    p.add_argument("--hot-card-share", type=float, default=0.0)
    p.add_argument("--events-only", action="store_true", help="emit bare events, no envelope")
    a = p.parse_args(argv)
    cfg = GeneratorConfig(n_events=a.events, seed=a.seed, dup_rate=a.dup_rate,
                          late_rate=a.late_rate, max_lateness_s=a.max_lateness,
                          bad_rate=a.bad_rate, hot_card_share=a.hot_card_share)
    for r in generate(cfg):
        row = r.event if a.events_only else {
            "ingested_at": iso(r.ingested_at), "event": r.event}
        sys.stdout.write(json.dumps(row) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

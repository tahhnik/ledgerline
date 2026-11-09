"""Generator tests: determinism and fault knobs."""
from collections import Counter
from datetime import timedelta

from ledgerline.contract import PaymentAuthorized
from ledgerline.generator import GeneratorConfig, generate


def test_same_seed_same_stream():
    cfg = GeneratorConfig(n_events=200, seed=7)
    assert list(generate(cfg)) == list(generate(cfg))


def test_clean_stream_is_valid_unique_and_in_order():
    records = list(generate(GeneratorConfig(n_events=500, seed=1)))
    assert len(records) == 500
    ids = [r.event["event_id"] for r in records]
    assert len(set(ids)) == 500
    for r in records:
        PaymentAuthorized.model_validate(r.event)
    arrivals = [r.ingested_at for r in records]
    assert arrivals == sorted(arrivals)


def test_duplicates_reuse_event_id():
    records = list(generate(GeneratorConfig(n_events=1000, dup_rate=0.05, seed=2)))
    counts = Counter(r.event["event_id"] for r in records)
    dups = sum(c - 1 for c in counts.values())
    assert 30 <= dups <= 70           # about 5% of 1000
    assert len(counts) == 1000        # unique business events unchanged


def test_late_events_arrive_after_their_event_time_window():
    cfg = GeneratorConfig(n_events=1000, late_rate=0.10, max_lateness_s=600, seed=3)
    records = list(generate(cfg))
    delays = [r.ingested_at - r.event_time for r in records]
    late = [d for d in delays if d > timedelta(seconds=60)]
    assert 60 <= len(late) <= 140     # about 10%
    assert max(delays) <= timedelta(seconds=600 + 5)


def test_bad_values_fail_the_contract():
    records = list(generate(GeneratorConfig(n_events=1000, bad_rate=0.02, seed=4)))
    bad = 0
    for r in records:
        try:
            PaymentAuthorized.model_validate(r.event)
        except Exception:
            bad += 1
    assert 8 <= bad <= 35


def test_hot_card_share():
    records = list(generate(GeneratorConfig(n_events=2000, hot_card_share=0.3, seed=5)))
    top_card, top_count = Counter(r.event["card_id"] for r in records).most_common(1)[0]
    assert 0.25 <= top_count / 2000 <= 0.35

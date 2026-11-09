"""Task 3: count transactions per card correctly despite duplicates and late data."""
from collections import Counter
from datetime import datetime, timedelta, timezone

from ledgerline.generator import GeneratorConfig, generate

from counts import correct_count_per_card, naive_count_per_card, to_rows

START = datetime(2026, 11, 9, 10, 0, tzinfo=timezone.utc)
WINDOW = (START, START + timedelta(minutes=5))
FOREVER = START + timedelta(days=1)


def truth(cfg):
    """Ground truth: unique business events whose event_time is in the window."""
    seen, out = set(), Counter()
    for r in generate(cfg):
        eid = r.event["event_id"]
        if eid in seen:
            continue
        seen.add(eid)
        if WINDOW[0] <= r.event_time < WINDOW[1]:
            out[r.event["card_id"]] += 1
    return dict(out)


def test_correct_count_matches_truth_with_duplicates_and_late_events():
    cfg = GeneratorConfig(n_events=2000, dup_rate=0.05, late_rate=0.10, n_cards=20, seed=11)
    rows = to_rows(generate(cfg))
    assert correct_count_per_card(rows, *WINDOW, as_of=FOREVER) == truth(cfg)


def test_naive_count_is_wrong_on_the_same_data():
    cfg = GeneratorConfig(n_events=2000, dup_rate=0.05, late_rate=0.10, n_cards=20, seed=11)
    rows = to_rows(generate(cfg))
    assert naive_count_per_card(rows, *WINDOW) != truth(cfg)


def test_as_of_excludes_events_that_had_not_arrived_yet():
    cfg = GeneratorConfig(n_events=2000, late_rate=0.30, n_cards=20, seed=12)
    rows = to_rows(generate(cfg))
    at_close = correct_count_per_card(rows, *WINDOW, as_of=WINDOW[1])
    later = correct_count_per_card(rows, *WINDOW, as_of=FOREVER)
    assert sum(at_close.values()) < sum(later.values())   # late events arrive after the window closes


def test_duplicates_alone_do_not_change_the_count():
    clean = GeneratorConfig(n_events=1500, n_cards=20, seed=13)
    dupes = GeneratorConfig(n_events=1500, n_cards=20, dup_rate=0.2, seed=13)
    assert sum(correct_count_per_card(to_rows(generate(dupes)), *WINDOW, as_of=FOREVER).values()) == \
        sum(truth(clean).values())

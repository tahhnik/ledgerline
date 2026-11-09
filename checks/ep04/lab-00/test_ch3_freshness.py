"""Chapter 3: when does an event become visible to the model?"""
from datetime import datetime, timedelta, timezone

from freshness import visible_at, worst_case_staleness

T = datetime(2026, 11, 9, 0, 1, tzinfo=timezone.utc)


def test_nightly_batch():
    # Runs at 00:00 every day and takes 40 minutes.
    v = visible_at(T, interval=timedelta(days=1), run_duration=timedelta(minutes=40))
    assert v == datetime(2026, 11, 10, 0, 40, tzinfo=timezone.utc)


def test_micro_batch():
    v = visible_at(T, interval=timedelta(minutes=5), run_duration=timedelta(seconds=20))
    assert v == datetime(2026, 11, 9, 0, 5, 20, tzinfo=timezone.utc)


def test_event_exactly_on_a_boundary_waits_for_the_next_run():
    on_boundary = datetime(2026, 11, 9, 0, 5, tzinfo=timezone.utc)
    v = visible_at(on_boundary, interval=timedelta(minutes=5), run_duration=timedelta(seconds=20))
    assert v == datetime(2026, 11, 9, 0, 10, 20, tzinfo=timezone.utc)


def test_worst_case_staleness():
    assert worst_case_staleness(timedelta(days=1), timedelta(minutes=40)) == timedelta(hours=24, minutes=40)
    assert worst_case_staleness(timedelta(minutes=5), timedelta(seconds=20)) == timedelta(minutes=5, seconds=20)

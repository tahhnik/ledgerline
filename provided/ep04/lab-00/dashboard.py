"""Break it (provided): the fraud team's 'transactions per card' dashboard.

    python starter/dashboard.py data/incident.jsonl
"""
import sys
from datetime import datetime, timedelta, timezone

from counts import correct_count_per_card, load_rows, naive_count_per_card

START = datetime(2026, 11, 9, 10, 0, tzinfo=timezone.utc)
END = START + timedelta(minutes=5)
AS_OF = START + timedelta(hours=1)          # the report is rebuilt an hour later


def main(path):
    rows = load_rows(path)
    naive = naive_count_per_card(rows, START, END)
    try:
        correct = correct_count_per_card(rows, START, END, as_of=AS_OF)
    except NotImplementedError:
        correct = None
    print(f"window 10:00-10:05 UTC   naive total = {sum(naive.values())}")
    if correct is None:
        print("correct total: not implemented yet (finish Task 3)")
        return
    print(f"                          correct total = {sum(correct.values())}")
    print("\ncard        naive  correct")
    for card in sorted(set(naive) | set(correct), key=lambda c: (-abs(naive.get(c, 0) - correct.get(c, 0)), c))[:8]:
        print(f"{card}   {naive.get(card, 0):5}  {correct.get(card, 0):7}")


if __name__ == "__main__":
    main(sys.argv[1])

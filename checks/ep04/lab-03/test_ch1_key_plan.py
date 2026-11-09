from key_plan import CHOSEN_KEY, compare_keys
from workload import transactions


def test_card_is_the_ordering_key():
    assert CHOSEN_KEY == "card_id"


def test_hot_merchant_spreads_when_keyed_by_card():
    report = compare_keys(transactions(), partitions=6)
    assert report["merchant_id"].max_mean >= 3.0
    assert report["card_id"].max_mean < 1.5
    assert report["merchant_id"].max_mean > report["card_id"].max_mean * 2

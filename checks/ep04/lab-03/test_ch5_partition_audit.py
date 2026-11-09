from partition_audit import audit
from workload import distribution, transactions


def deliveries(events, key_field):
    expected = distribution(events, key_field, 6)
    offsets = {partition: 0 for partition in expected}
    out = []
    from partition_audit import Delivery
    from workload import java_partition
    for event in events:
        partition = java_partition(event[key_field], 6)
        out.append(Delivery(event["event_id"], partition, offsets[partition]))
        offsets[partition] += 1
    return out


def test_audit_finds_and_clears_the_hot_merchant():
    events = transactions()
    merchant = audit(events, deliveries(events, "merchant_id"))
    card = audit(events, deliveries(events, "card_id"))
    assert merchant.alert is True
    assert merchant.dominant_merchant == "merchant_hot"
    assert merchant.max_mean >= 3.0
    assert card.alert is False
    assert card.max_mean < 1.5

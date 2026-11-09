"""Contract tests for the v1 payment.authorized event."""
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from ledgerline.contract import PaymentAuthorized, json_schema


def valid(**overrides):
    event = {
        "schema_version": 1,
        "event_id": "6f1c2a8e-3b7d-4c1e-9a2f-0d5e8b7c6a41",
        "card_id": "card_0042",
        "merchant_id": "m_017",
        "amount": "42.50",
        "currency": "USD",
        "event_time": "2026-11-09T10:15:00Z",
        "device": "ios",
        "country": "SG",
    }
    event.update(overrides)
    return event


def test_valid_event_parses():
    e = PaymentAuthorized.model_validate(valid())
    assert e.card_id == "card_0042"
    assert e.event_time.tzinfo is not None


@pytest.mark.parametrize("field,value", [
    ("amount", "0"),                     # must be positive
    ("amount", "-5"),
    ("currency", "XYZ"),                 # not an accepted ISO 4217 code
    ("card_id", "42"),                   # wrong id format
    ("device", "fax"),
    ("country", "Singapore"),            # must be ISO 3166-1 alpha-2
    ("event_time", "2026-11-09T10:15:00"),  # naive timestamp: timezone required
    ("schema_version", 2),
])
def test_invalid_values_are_rejected(field, value):
    with pytest.raises(ValidationError):
        PaymentAuthorized.model_validate(valid(**{field: value}))


def test_missing_required_field_is_rejected():
    event = valid()
    del event["event_id"]
    with pytest.raises(ValidationError):
        PaymentAuthorized.model_validate(event)


def test_unknown_fields_are_rejected():
    with pytest.raises(ValidationError):
        PaymentAuthorized.model_validate(valid(risk_score=0.9))


def test_event_time_is_normalised_to_utc():
    e = PaymentAuthorized.model_validate(valid(event_time="2026-11-09T16:15:00+06:00"))
    assert e.event_time == datetime(2026, 11, 9, 10, 15, tzinfo=timezone.utc)
    assert e.event_time.utcoffset() == timedelta(0)


def test_json_schema_is_exported():
    schema = json_schema()
    assert schema["title"] == "PaymentAuthorized"
    assert set(schema["required"]) >= {"event_id", "card_id", "amount", "event_time"}

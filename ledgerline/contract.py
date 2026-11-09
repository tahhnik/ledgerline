"""v1 contract for the `payment.authorized` event.

Every Ledgerline pipeline (batch and streaming) reads this one contract, so a
change here is a change to every consumer. Evolve it deliberately.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

CURRENCIES = ("USD", "EUR", "GBP", "SGD", "BDT")
DEVICES = ("web", "ios", "android", "pos")


class PaymentAuthorized(BaseModel):
    """One authorized card payment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    event_id: str = Field(min_length=8, description="Unique business id; the deduplication key")
    card_id: str = Field(pattern=r"^card_\d{4}$")
    merchant_id: str = Field(pattern=r"^m_\d{3}$")
    amount: Decimal = Field(gt=0, max_digits=10, decimal_places=2)
    currency: Literal[CURRENCIES]  # type: ignore[valid-type]
    event_time: AwareDatetime = Field(description="When the payment happened (UTC)")
    device: Literal[DEVICES]  # type: ignore[valid-type]
    country: str = Field(pattern=r"^[A-Z]{2}$", description="ISO 3166-1 alpha-2")

    @field_validator("event_time")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        return v.astimezone(timezone.utc)


def json_schema() -> dict:
    """JSON Schema for the contract, for docs and non-Python consumers."""
    return PaymentAuthorized.model_json_schema()

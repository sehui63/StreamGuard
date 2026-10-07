"""Data models used by the generator and its internal test metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Scenario(str, Enum):
    NORMAL = "NORMAL"
    HIGH_AMOUNT = "HIGH_AMOUNT"
    RAPID_REPEAT = "RAPID_REPEAT"
    DUPLICATE = "DUPLICATE"
    LATE_EVENT = "LATE_EVENT"


@dataclass(frozen=True)
class UserProfile:
    user_id: str
    usual_city: str
    avg_amount: int
    preferred_categories: tuple[str, ...]
    preferred_payment_method: str
    device_ids: tuple[str, ...]


@dataclass(frozen=True)
class MerchantProfile:
    merchant_id: str
    merchant_category: str
    city: str
    avg_amount: int
    min_amount: int
    max_amount: int


@dataclass(frozen=True)
class Transaction:
    """The Raw Event contract. It intentionally has exactly ten fields."""

    transaction_id: str
    user_id: str
    merchant_id: str
    merchant_category: str
    amount: int
    currency: str
    event_time: datetime
    city: str
    device_id: str
    payment_method: str

    def to_dict(self) -> dict[str, str | int]:
        return {
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "merchant_id": self.merchant_id,
            "merchant_category": self.merchant_category,
            "amount": self.amount,
            "currency": self.currency,
            "event_time": self.event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "city": self.city,
            "device_id": self.device_id,
            "payment_method": self.payment_method,
        }


@dataclass(frozen=True)
class GroundTruthRecord:
    """Internal metadata; never serialized with the Raw Event."""

    output_index: int
    transaction_id: str
    scenario: Scenario


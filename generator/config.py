"""Central configuration for the synthetic transaction generator."""

from __future__ import annotations

from dataclasses import dataclass


CITIES: tuple[str, ...] = (
    "Seoul",
    "Busan",
    "Incheon",
    "Daegu",
    "Daejeon",
    "Gwangju",
)

MERCHANT_CATEGORIES: tuple[str, ...] = (
    "restaurant",
    "grocery",
    "convenience",
    "electronics",
    "fashion",
    "travel",
    "entertainment",
    "online_marketplace",
)

PAYMENT_METHODS: tuple[str, ...] = (
    "credit_card",
    "debit_card",
    "mobile_pay",
)


@dataclass(frozen=True)
class AmountRange:
    """Typical KRW transaction range for one merchant category."""

    minimum: int
    average: int
    maximum: int


CATEGORY_AMOUNT_RANGES: dict[str, AmountRange] = {
    "convenience": AmountRange(1_500, 15_000, 100_000),
    "grocery": AmountRange(10_000, 60_000, 300_000),
    "restaurant": AmountRange(8_000, 50_000, 300_000),
    "fashion": AmountRange(15_000, 120_000, 800_000),
    "electronics": AmountRange(50_000, 600_000, 2_500_000),
    "entertainment": AmountRange(10_000, 70_000, 500_000),
    "online_marketplace": AmountRange(5_000, 100_000, 1_000_000),
    "travel": AmountRange(50_000, 700_000, 3_000_000),
}


@dataclass(frozen=True)
class GeneratorConfig:
    """Values that control profile variety and normal-transaction behaviour."""

    user_count: int = 500
    merchant_count: int = 100
    preferred_city_probability: float = 0.88
    preferred_category_probability: float = 0.75
    preferred_payment_probability: float = 0.82
    normal_amount_sigma: float = 0.40
    rapid_repeat_interval_seconds: int = 5
    late_event_delay_min_minutes: int = 5
    late_event_delay_max_minutes: int = 15
    high_amount_over_max_multiplier_min: float = 1.20
    high_amount_over_max_multiplier_max: float = 2.00

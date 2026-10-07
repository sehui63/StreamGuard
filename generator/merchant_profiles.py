"""Create merchant profiles whose amount distributions vary by category."""

from __future__ import annotations

import random

try:
    from .config import CATEGORY_AMOUNT_RANGES, CITIES, MERCHANT_CATEGORIES
    from .models import MerchantProfile
except ImportError:  # Supports `python app.py` from the generator directory.
    from config import CATEGORY_AMOUNT_RANGES, CITIES, MERCHANT_CATEGORIES
    from models import MerchantProfile


def create_merchant_profiles(count: int, rng: random.Random) -> list[MerchantProfile]:
    """Return merchants with category-specific, not uniform, spend ranges."""
    profiles: list[MerchantProfile] = []

    for number in range(1, count + 1):
        # Cycling guarantees every category is represented even with a small count.
        category = MERCHANT_CATEGORIES[(number - 1) % len(MERCHANT_CATEGORIES)]
        amount_range = CATEGORY_AMOUNT_RANGES[category]
        average = int(
            min(
                max(rng.normalvariate(amount_range.average, amount_range.average * 0.15), amount_range.minimum),
                amount_range.maximum,
            )
        )
        profiles.append(
            MerchantProfile(
                merchant_id=f"merchant_{number:03d}",
                merchant_category=category,
                city=rng.choice(CITIES),
                avg_amount=average,
                min_amount=amount_range.minimum,
                max_amount=amount_range.maximum,
            )
        )

    return profiles


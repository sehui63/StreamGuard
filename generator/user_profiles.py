"""Create non-identifying user profiles for transaction generation."""

from __future__ import annotations

import random

try:
    from .config import CITIES, MERCHANT_CATEGORIES, PAYMENT_METHODS
    from .models import UserProfile
except ImportError:  # Supports `python app.py` from the generator directory.
    from config import CITIES, MERCHANT_CATEGORIES, PAYMENT_METHODS
    from models import UserProfile


def create_user_profiles(count: int, rng: random.Random) -> list[UserProfile]:
    """Return profiles with varied cities, spend levels, preferences, and devices."""
    profiles: list[UserProfile] = []
    next_device_number = 1

    for number in range(1, count + 1):
        category_count = rng.randint(2, 4)
        device_count = rng.randint(1, 3)
        device_ids = tuple(
            f"device_{device_number:04d}"
            for device_number in range(next_device_number, next_device_number + device_count)
        )
        next_device_number += device_count

        # A right-skewed distribution keeps very high average spend uncommon.
        avg_amount = int(min(max(rng.lognormvariate(10.55, 0.48), 8_000), 250_000))
        profiles.append(
            UserProfile(
                user_id=f"user_{number:03d}",
                usual_city=rng.choice(CITIES),
                avg_amount=avg_amount,
                preferred_categories=tuple(rng.sample(MERCHANT_CATEGORIES, category_count)),
                preferred_payment_method=rng.choice(PAYMENT_METHODS),
                device_ids=device_ids,
            )
        )

    return profiles


"""Generate realistic Raw Transaction events and separate scenario ground truth."""

from __future__ import annotations

import math
import random
from datetime import UTC, datetime, timedelta

try:
    from .config import CITIES, MERCHANT_CATEGORIES, PAYMENT_METHODS, GeneratorConfig
    from .merchant_profiles import create_merchant_profiles
    from .models import GroundTruthRecord, MerchantProfile, Scenario, Transaction, UserProfile
    from .user_profiles import create_user_profiles
except ImportError:  # Supports `python app.py` from the generator directory.
    from config import CITIES, MERCHANT_CATEGORIES, PAYMENT_METHODS, GeneratorConfig
    from merchant_profiles import create_merchant_profiles
    from models import GroundTruthRecord, MerchantProfile, Scenario, Transaction, UserProfile
    from user_profiles import create_user_profiles


class TransactionGenerator:
    """Stateful generator that keeps emitted transactions and their ground truth separate."""

    def __init__(self, config: GeneratorConfig | None = None, seed: int | None = None) -> None:
        self.config = config or GeneratorConfig()
        self.rng = random.Random(seed)
        self.user_profiles = create_user_profiles(self.config.user_count, self.rng)
        self.merchant_profiles = create_merchant_profiles(self.config.merchant_count, self.rng)
        self._users_by_id = {profile.user_id: profile for profile in self.user_profiles}
        self._merchants_by_id = {profile.merchant_id: profile for profile in self.merchant_profiles}
        self._emitted_transactions: list[Transaction] = []
        self.ground_truth: list[GroundTruthRecord] = []
        self._transaction_number = 0
        self._output_number = 0
        self._base_time = datetime.now(UTC).replace(microsecond=0)
        self._rapid_repeat_user: UserProfile | None = None
        self._rapid_repeat_start: datetime | None = None
        self._rapid_repeat_count = 0

    def generate_transactions(self, count: int, scenario: Scenario = Scenario.NORMAL) -> list[Transaction]:
        """Generate exactly `count` output events for a single requested scenario."""
        if count < 1:
            raise ValueError("count must be at least 1")

        transactions: list[Transaction] = []
        if scenario is Scenario.DUPLICATE:
            for index in range(count):
                if index % 2 == 0:
                    transactions.append(self.generate_transaction(Scenario.NORMAL))
                else:
                    duplicate = self._generate_duplicate(transactions[-1])
                    self._record_output(duplicate, Scenario.DUPLICATE)
                    transactions.append(duplicate)
            return transactions

        for index in range(count):
            transactions.append(self.generate_transaction(scenario))
        return transactions

    def generate_transaction(self, scenario: Scenario = Scenario.NORMAL) -> Transaction:
        """Generate one Raw Event and store only its scenario in internal ground truth."""
        if scenario is Scenario.DUPLICATE:
            transaction = self._generate_duplicate()
        elif scenario is Scenario.RAPID_REPEAT:
            transaction = self._generate_rapid_repeat()
        else:
            transaction = self._generate_normal_transaction()
            if scenario is Scenario.HIGH_AMOUNT:
                transaction = self._with_high_amount(transaction)
            elif scenario is Scenario.LATE_EVENT:
                transaction = self._with_late_event_time(transaction)

        self._record_output(transaction, scenario)
        return transaction

    def _generate_normal_transaction(self, user: UserProfile | None = None, event_time: datetime | None = None) -> Transaction:
        selected_user = user or self.rng.choice(self.user_profiles)
        city = self._choose_city(selected_user)
        merchant = self._choose_merchant(selected_user, city)
        amount = self._normal_amount(selected_user, merchant)
        return Transaction(
            transaction_id=self._next_transaction_id(),
            user_id=selected_user.user_id,
            merchant_id=merchant.merchant_id,
            merchant_category=merchant.merchant_category,
            amount=amount,
            currency="KRW",
            event_time=event_time or self._next_event_time(),
            city=merchant.city,
            device_id=self.rng.choice(selected_user.device_ids),
            payment_method=self._choose_payment_method(selected_user),
        )

    def _generate_duplicate(self, source: Transaction | None = None) -> Transaction:
        if source is not None:
            return source
        if not self._emitted_transactions:
            # Preserve the duplicate contract even when called directly on a new generator.
            original = self._generate_normal_transaction()
            self._emitted_transactions.append(original)
        return self.rng.choice(self._emitted_transactions)

    def _generate_rapid_repeat(self) -> Transaction:
        if self._rapid_repeat_user is None:
            self._rapid_repeat_user = self.rng.choice(self.user_profiles)
            self._rapid_repeat_start = self._next_event_time()
            self._rapid_repeat_count = 0

        assert self._rapid_repeat_start is not None
        event_time = self._rapid_repeat_start + timedelta(
            seconds=self._rapid_repeat_count * self.config.rapid_repeat_interval_seconds
        )
        self._rapid_repeat_count += 1
        return self._generate_normal_transaction(self._rapid_repeat_user, event_time)

    def _choose_city(self, user: UserProfile) -> str:
        if self.rng.random() < self.config.preferred_city_probability:
            return user.usual_city
        return self.rng.choice(tuple(city for city in CITIES if city != user.usual_city))

    def _choose_merchant(self, user: UserProfile, city: str) -> MerchantProfile:
        preferred_category = (
            self.rng.choice(user.preferred_categories)
            if self.rng.random() < self.config.preferred_category_probability
            else self.rng.choice(MERCHANT_CATEGORIES)
        )
        category_and_city = [
            merchant
            for merchant in self.merchant_profiles
            if merchant.merchant_category == preferred_category and merchant.city == city
        ]
        category_only = [
            merchant for merchant in self.merchant_profiles if merchant.merchant_category == preferred_category
        ]
        return self.rng.choice(category_and_city or category_only or self.merchant_profiles)

    def _choose_payment_method(self, user: UserProfile) -> str:
        if self.rng.random() < self.config.preferred_payment_probability:
            return user.preferred_payment_method
        alternatives = tuple(method for method in PAYMENT_METHODS if method != user.preferred_payment_method)
        return self.rng.choice(alternatives)

    def _normal_amount(self, user: UserProfile, merchant: MerchantProfile) -> int:
        # Geometric mean lets user spending and merchant price characteristics both influence the result.
        target = math.sqrt(user.avg_amount * merchant.avg_amount)
        sampled = self.rng.lognormvariate(math.log(target), self.config.normal_amount_sigma)
        bounded = min(max(sampled, merchant.min_amount), merchant.max_amount)
        return max(1_000, int(round(bounded / 100) * 100))

    def _with_high_amount(self, transaction: Transaction) -> Transaction:
        merchant = self._merchants_by_id[transaction.merchant_id]
        multiplier = self.rng.uniform(
            self.config.high_amount_over_max_multiplier_min,
            self.config.high_amount_over_max_multiplier_max,
        )
        # A high amount is category-relative: it must exceed this merchant's normal upper bound.
        high_amount = int(round(merchant.max_amount * multiplier / 100) * 100)
        return Transaction(
            transaction_id=transaction.transaction_id,
            user_id=transaction.user_id,
            merchant_id=transaction.merchant_id,
            merchant_category=transaction.merchant_category,
            amount=high_amount,
            currency=transaction.currency,
            event_time=transaction.event_time,
            city=transaction.city,
            device_id=transaction.device_id,
            payment_method=transaction.payment_method,
        )

    def _with_late_event_time(self, transaction: Transaction) -> Transaction:
        delay = self.rng.randint(
            self.config.late_event_delay_min_minutes,
            self.config.late_event_delay_max_minutes,
        )
        return Transaction(
            transaction_id=transaction.transaction_id,
            user_id=transaction.user_id,
            merchant_id=transaction.merchant_id,
            merchant_category=transaction.merchant_category,
            amount=transaction.amount,
            currency=transaction.currency,
            event_time=transaction.event_time - timedelta(minutes=delay),
            city=transaction.city,
            device_id=transaction.device_id,
            payment_method=transaction.payment_method,
        )

    def _next_transaction_id(self) -> str:
        self._transaction_number += 1
        return f"txn_{self._transaction_number:06d}"

    def _next_event_time(self) -> datetime:
        event_time = self._base_time + timedelta(seconds=self._transaction_number)
        return event_time

    def _record_output(self, transaction: Transaction, scenario: Scenario) -> None:
        self._output_number += 1
        self._emitted_transactions.append(transaction)
        self.ground_truth.append(
            GroundTruthRecord(
                output_index=self._output_number,
                transaction_id=transaction.transaction_id,
                scenario=scenario,
            )
        )

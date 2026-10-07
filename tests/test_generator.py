"""Unit tests for the synthetic transaction generator."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from generator.models import Scenario
from generator.transaction_generator import TransactionGenerator


class TransactionGeneratorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.generator = TransactionGenerator(seed=20261008)

    def test_transaction_has_exactly_ten_raw_event_fields(self) -> None:
        transaction = self.generator.generate_transaction()
        self.assertEqual(len(transaction.to_dict()), 10)

    def test_profile_counts_and_device_counts_match_configuration(self) -> None:
        self.assertEqual(len(self.generator.user_profiles), 500)
        self.assertEqual(len(self.generator.merchant_profiles), 100)
        self.assertTrue(all(1 <= len(user.device_ids) <= 3 for user in self.generator.user_profiles))

    def test_normal_transaction_ids_are_unique(self) -> None:
        transactions = self.generator.generate_transactions(100, Scenario.NORMAL)
        transaction_ids = [transaction.transaction_id for transaction in transactions]
        self.assertEqual(len(transaction_ids), len(set(transaction_ids)))

    def test_currency_is_krw(self) -> None:
        transaction = self.generator.generate_transaction()
        self.assertEqual(transaction.currency, "KRW")

    def test_merchant_category_matches_merchant_profile(self) -> None:
        transaction = self.generator.generate_transaction()
        merchant = next(
            merchant
            for merchant in self.generator.merchant_profiles
            if merchant.merchant_id == transaction.merchant_id
        )
        self.assertEqual(transaction.merchant_category, merchant.merchant_category)

    def test_device_belongs_to_transaction_user(self) -> None:
        transaction = self.generator.generate_transaction()
        user = next(user for user in self.generator.user_profiles if user.user_id == transaction.user_id)
        self.assertIn(transaction.device_id, user.device_ids)

    def test_normal_amount_is_within_merchant_normal_range(self) -> None:
        for transaction in self.generator.generate_transactions(100, Scenario.NORMAL):
            merchant = next(
                merchant
                for merchant in self.generator.merchant_profiles
                if merchant.merchant_id == transaction.merchant_id
            )
            self.assertLessEqual(transaction.amount, merchant.max_amount)

    def test_high_amount_exceeds_its_merchant_normal_range(self) -> None:
        transaction = self.generator.generate_transaction(Scenario.HIGH_AMOUNT)
        merchant = next(
            merchant
            for merchant in self.generator.merchant_profiles
            if merchant.merchant_id == transaction.merchant_id
        )
        self.assertGreater(transaction.amount, merchant.max_amount)

    def test_duplicate_scenario_reuses_a_transaction_id(self) -> None:
        original = self.generator.generate_transaction(Scenario.NORMAL)
        duplicate = self.generator.generate_transaction(Scenario.DUPLICATE)
        self.assertEqual(duplicate.transaction_id, original.transaction_id)
        self.assertEqual(self.generator.ground_truth[-1].scenario, Scenario.DUPLICATE)

    def test_rapid_repeat_uses_one_user_and_short_event_time_intervals(self) -> None:
        transactions = self.generator.generate_transactions(4, Scenario.RAPID_REPEAT)
        self.assertEqual(len({transaction.user_id for transaction in transactions}), 1)
        intervals = [
            later.event_time - earlier.event_time
            for earlier, later in zip(transactions, transactions[1:])
        ]
        self.assertTrue(all(interval == timedelta(seconds=5) for interval in intervals))

    def test_late_event_has_a_past_event_time(self) -> None:
        transaction = self.generator.generate_transaction(Scenario.LATE_EVENT)
        self.assertLess(transaction.event_time, datetime.now(UTC) - timedelta(minutes=4))


if __name__ == "__main__":
    unittest.main()

"""Console entry point for generating Raw Transaction JSON Lines."""

from __future__ import annotations

import argparse
import json
import time
from .kafka_producer import TransactionKafkaProducer

try:
    from .models import Scenario
    from .transaction_generator import TransactionGenerator
except ImportError:  # Supports `python app.py` from the generator directory.
    from models import Scenario
    from transaction_generator import TransactionGenerator


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate StreamGuard synthetic transaction events.")
    parser.add_argument("--count", type=int, default=20, help="Number of JSON events to print (default: 20).")
    parser.add_argument(
        "--scenario",
        choices=[scenario.value for scenario in Scenario],
        default=Scenario.NORMAL.value,
        help="Scenario to generate (default: NORMAL).",
    )
    parser.add_argument("--seed", type=int, default=None, help="Optional seed for reproducible output.")
    parser.add_argument("--events-per-second", type=float, default=0, help="Optional output rate; 0 prints immediately.")
    parser.add_argument(
    "--output",
    choices=["console", "kafka"],
    default="console",
    help="Output destination (default: console).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    if args.count < 1:
        raise SystemExit("--count must be at least 1")

    if args.events_per_second < 0:
        raise SystemExit("--events-per-second cannot be negative")

    generator = TransactionGenerator(seed=args.seed)
    scenario = Scenario(args.scenario)

    delay_seconds = (
        1 / args.events_per_second
        if args.events_per_second
        else 0
    )

    producer = None

    if args.output == "kafka":
        producer = TransactionKafkaProducer()

    try:
        for transaction in generator.generate_transactions(
            args.count,
            scenario,
        ):
            if args.output == "console":
                print(
                    json.dumps(
                        transaction.to_dict(),
                        ensure_ascii=False,
                    )
                )
            else:
                producer.send(transaction)

            if delay_seconds:
                time.sleep(delay_seconds)

    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()


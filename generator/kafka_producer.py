from __future__ import annotations
import json
import os
from kafka import KafkaProducer
from .models import Transaction


class TransactionKafkaProducer:
    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.getenv("KAFKA_TOPIC", "transactions")
        self.delivered_count = 0
        self.delivery_errors: list[str] = []
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
        )

    def send(self, transaction: Transaction) -> None:
        value = json.dumps(
            transaction.to_dict(),
            ensure_ascii=False,
        ).encode("utf-8")

        delivery_future = self.producer.send(
            self.topic,
            key=transaction.user_id.encode("utf-8"),
            value=value,
        )
        delivery_future.add_callback(self._delivery_report)
        delivery_future.add_errback(self._delivery_failed)

    def close(self) -> None:
        """Flush pending events and report whether every event reached Kafka."""
        try:
            self.producer.flush(timeout=10)
        finally:
            self.producer.close(timeout=10)
        if self.delivery_errors:
            raise RuntimeError("Kafka delivery failed: " + "; ".join(self.delivery_errors))

        print(
            f"Kafka delivery succeeded: {self.delivered_count} message(s) "
            f"to topic '{self.topic}'."
        )

    def _delivery_report(self, record_metadata) -> None:
        self.delivered_count += 1

    def _delivery_failed(self, error) -> None:
        self.delivery_errors.append(str(error))

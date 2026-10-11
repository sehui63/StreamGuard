from __future__ import annotations
import json
from confluent_kafka import Producer
from .models import Transaction


class TransactionKafkaProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions",
    ) -> None:
        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
            }
        )

    def send(self, transaction: Transaction) -> None:
        value = json.dumps(
            transaction.to_dict(),
            ensure_ascii=False,
        ).encode("utf-8")

        self.producer.produce(
            topic=self.topic,
            key=transaction.user_id.encode("utf-8"),
            value=value,
            callback=self._delivery_report,
        )

        # Kafka 내부 delivery callback 처리 기회를 줌
        self.producer.poll(0)

    def close(self) -> None:
        # 아직 전송되지 않은 메시지가 있으면 전부 전송하고 종료함
        self.producer.flush()

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err is not None:
            print(f"Kafka delivery failed: {err}")
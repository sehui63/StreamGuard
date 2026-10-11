# Phase 2 — Kafka 연결

이 단계는 Generator가 만든 Raw Transaction JSON을 Kafka의 `transactions` 토픽으로 전송하고, Kafka Consumer로 확인하는 단계다.

## 실행 순서

```powershell
python -m pip install -r requirements.txt
docker compose up -d
docker compose exec -T kafka bash /opt/streamguard/kafka/create_topics.sh
```

Kafka Consumer를 별도 터미널에서 먼저 실행한다.

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic transactions `
  --offset latest `
  --max-messages 10 `
  --formatter-property print.key=true `
  --formatter-property key.separator=" | "
```

그다음 Producer를 실행한다.

```powershell
python app.py --count 10 --output kafka --seed 42
```

## 완료 검증

```powershell
python app.py --count 300 --output kafka --seed 42
python app.py --count 100 --output kafka --seed 1
python app.py --count 100 --output kafka --seed 2
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh `
  --bootstrap-server localhost:9092 `
  --topic transactions
```

- Consumer에서 `user_id` key와 Transaction JSON을 확인한다.
- Producer 종료 후 재실행해도 전송 성공 요약이 출력돼야 한다.
- 마지막 offset은 각 전송 건수만큼 증가해야 한다.

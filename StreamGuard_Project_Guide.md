# 1. 프로젝트 개요

## 1.1 프로젝트명

**StreamGuard — Real-Time Fraud Detection Data Pipeline**

실시간으로 발생하는 거래 이벤트를 Kafka로 수집하고, Spark Structured Streaming으로 처리하고, 룰 기반 이상 거래 탐지를 수행한 뒤 Delta Lake와 PostgreSQL에 저장하고, Airflow와 dbt를 이용해 분석 파이프라인을 자동화하는 프로젝트임.

## 1.2 프로젝트를 만드는 이유

단순히 CSV 파일을 읽고 변환한 뒤 DB에 적재하는 수준이 아니라, 실제 데이터 엔지니어링 환경에서 자주 등장하는 문제를 직접 다뤄보기 위함임.

이 프로젝트를 통해 다음 문제를 직접 다뤄야 함.

- 지속적으로 발생하는 이벤트 데이터를 어떻게 수집할 것인지 결정해야 함.
- 실시간 처리와 배치 처리의 차이를 이해해야 함.
- 동일한 거래가 중복으로 들어왔을 때 어떻게 처리할지 설계해야 함.
- 이벤트가 늦게 도착했을 때 어떻게 처리할지 고민해야 함.
- 원본 데이터와 정제 데이터를 어떤 방식으로 분리해서 저장할지 결정해야 함.
- 이상 거래 판별 결과를 어떻게 저장하고 재사용할지 결정해야 함.
- 분석용 데이터를 어떻게 모델링할지 결정해야 함.
- 데이터 처리 작업을 어떤 순서로 자동 실행할지 구성해야 함.
- 파이프라인이 실패했을 때 어디서 실패했는지 알 수 있어야 함.
- 전체 프로젝트를 다른 사람이 재현할 수 있어야 함.

즉, 단순 기능 구현보다 **실제 데이터 파이프라인을 설계하고 운영하는 사고방식**을 보여주는 것이 핵심임.

---

# 2. 프로젝트 최종 목표

최종적으로 아래 구조가 동작해야 함.

```text
Transaction Generator
        |
        v
      Kafka
        |
        v
Spark Structured Streaming
        |
        +----------------------+
        |                      |
        v                      v
 Bronze Layer             Fraud Detection
 (Raw Events)                  |
        |                      |
        v                      |
 Silver Layer <---------------+
 (Clean / Validated)
        |
        v
 Gold / Analytics Layer
        |
        +------------+
        |            |
        v            v
     dbt         PostgreSQL
        |            |
        +-------> Analytics
                    |
                    v
                Dashboard

Airflow
  |
  +--> batch validation
  +--> dbt run
  +--> aggregation
  +--> scheduled jobs
  +--> maintenance tasks
```

최종 프로젝트에서 최소한 다음 항목을 구현해야 함.

- Python 기반 가짜 거래 데이터 생성기 구현해야 함.
- Kafka Producer 구현해야 함.
- Kafka Topic에 거래 이벤트가 지속적으로 들어가야 함.
- Spark Structured Streaming으로 Kafka 데이터를 읽어야 함.
- JSON schema를 정의하고 정상적으로 파싱해야 함.
- 기본 데이터 정제 로직을 구현해야 함.
- 중복 거래 처리 전략을 구현해야 함.
- 늦게 도착한 이벤트를 다루기 위한 event time과 watermark 개념을 적용해야 함.
- Rule-based fraud detection을 구현해야 함.
- Raw / Clean / Fraud 데이터를 Delta Lake에 저장해야 함.
- 분석용 PostgreSQL 환경을 구성해야 함.
- dbt 모델을 구성해야 함.
- Airflow로 배치성 작업과 dbt 작업을 orchestration 해야 함.
- 최소한의 데이터 품질 검증을 넣어야 함.
- 최소한의 모니터링 또는 상태 확인 방법을 제공해야 함.
- Docker Compose 기반으로 실행 가능해야 함.
- README에 실행 방법과 아키텍처를 설명해야 함.

---

# 3. 프로젝트에서 다루지 않을 범위

처음부터 너무 많은 기능을 넣으면 프로젝트가 완성되지 않을 가능성이 높음.

따라서 1차 구현에서는 아래 기능을 필수 범위에서 제외함.

- 머신러닝 기반 Fraud Detection은 필수가 아님.
- 복잡한 Feature Engineering은 필수가 아님.
- Kubernetes 배포는 필수가 아님.
- 실제 금융 데이터 연동은 하지 않음.
- 실제 카드 결제 시스템과 연결하지 않음.
- 고가용성 Kafka Cluster 구성은 필수가 아님.
- Spark Cluster의 대규모 분산 환경 구성은 필수가 아님.
- 실시간 Feature Store는 구현하지 않음.
- Great Expectations 기반 재사용형 데이터 품질 프레임워크는 별도 확장으로 남겨둠.

먼저 하나의 완성된 end-to-end pipeline을 만드는 것이 우선임.

---

# 4. 이 프로젝트에서 확인해야 하는 핵심 질문

프로젝트를 진행하면서 아래 질문에 답할 수 있어야 함.

- 왜 Kafka를 사용했는가?
- Kafka 없이 구현하면 어떤 문제가 생기는가?
- Batch ingestion과 Streaming ingestion의 차이는 무엇인가?
- 왜 Spark Structured Streaming을 사용했는가?
- Python Consumer만으로 처리하지 않은 이유는 무엇인가?
- 동일한 transaction이 두 번 들어오면 어떻게 처리해야 하는가?
- event time과 processing time의 차이는 무엇인가?
- 늦게 도착한 이벤트를 어떻게 처리해야 하는가?
- watermark가 왜 필요한가?
- Delta Lake를 사용하는 이유는 무엇인가?
- Bronze / Silver / Gold Layer를 왜 나누는가?
- dbt는 Spark와 어떤 역할 차이가 있는가?
- Airflow는 Streaming Engine이 아닌데 왜 사용하는가?
- Fraud Detection 결과가 잘못됐을 때 어떻게 추적할 수 있는가?
- Pipeline이 중간에 실패하면 어떻게 복구할 것인가?
- 데이터가 10배 증가하면 어느 부분이 병목이 되는가?
- 데이터가 정상적으로 적재됐다는 것을 어떻게 확인할 것인가?

위 질문을 구현 과정에서 계속 문서화해야 함.

---

# 5. 사용 기술과 도입 이유

## 5.1 Python

### 사용하는 이유

거래 데이터 생성기와 Kafka Producer를 구현하기 위해 사용함.

Python을 사용하면 가짜 데이터를 빠르게 만들 수 있고 Kafka Client Library와도 쉽게 연동 가능함.

### 구현해야 하는 것

- 거래 이벤트 생성 로직 구현해야 함.
- 정상 거래와 의심 거래를 일정 비율로 생성해야 함.
- JSON 형태로 직렬화해야 함.
- Kafka Producer를 통해 topic으로 전송해야 함.
- 환경 변수 기반으로 Kafka broker 주소를 설정할 수 있어야 함.
- 전송 실패 시 최소한의 예외 처리가 있어야 함.

---

## 5.2 Apache Kafka

### 사용하는 이유

계속 발생하는 거래 이벤트를 Producer와 Consumer 사이에서 안정적으로 전달하기 위해 사용함.

실제 운영 환경에서는 이벤트 생산 속도와 처리 속도가 항상 같지 않음. Kafka를 사용하면 Producer와 Consumer를 분리할 수 있고 처리 속도의 차이를 흡수할 수 있음.

또한 topic과 partition 개념을 통해 이후 확장 가능성을 설명할 수 있음.

### 구현해야 하는 것

- Kafka broker 실행해야 함.
- transaction용 topic 생성해야 함.
- Python Producer가 해당 topic으로 메시지를 보내야 함.
- Spark Structured Streaming이 해당 topic을 읽어야 함.
- key를 사용할 경우 어떤 값을 key로 잡을지 이유를 정해야 함.
- 최소한 transaction_id 또는 user_id를 key 후보로 검토해야 함.

### 완료 조건

아래가 가능하면 Kafka 단계 완료로 판단함.

```text
Python Generator
    ↓
Kafka Topic
    ↓
Consumer 또는 Kafka CLI로 메시지 확인
```

거래 JSON 메시지가 연속적으로 Kafka에 들어오는 것을 직접 확인해야 함.

---

## 5.3 Apache Spark Structured Streaming

### 사용하는 이유

Kafka로 유입되는 거래 데이터를 실시간에 가깝게 처리하기 위해 사용함.

단순 Python Consumer보다 아래 개념을 연습하기 좋음.

- streaming dataframe
- schema enforcement
- event time
- window
- watermark
- stateful processing
- checkpoint
- fault recovery

이 프로젝트에서 가장 중요한 기술 중 하나임.

### 구현해야 하는 것

- Kafka Source 설정해야 함.
- Kafka value를 string으로 변환해야 함.
- JSON schema를 적용해야 함.
- 잘못된 JSON 처리 전략을 정해야 함.
- timestamp를 Spark timestamp type으로 변환해야 함.
- 필요한 컬럼을 정제해야 함.
- 중복 제거 전략을 구현해야 함.
- watermark를 적용해야 함.
- Fraud Rule을 적용해야 함.
- Delta Lake Sink를 구성해야 함.
- checkpoint location을 지정해야 함.

### 완료 조건

Kafka에 들어간 거래 데이터가 Spark Streaming DataFrame으로 정상 처리되고 정제된 결과가 지속적으로 출력되면 1차 완료임.

그 다음 Delta Lake에 지속적으로 저장되면 Spark 단계의 최소 완료 상태로 판단함.

---

## 5.4 Delta Lake

### 사용하는 이유

Raw / Clean / Fraud 결과를 안정적으로 저장하기 위해 사용함.

단순 Parquet 파일보다 transaction log를 제공하기 때문에 데이터 변경과 상태 관리에 유리함.

이 프로젝트에서는 데이터 레이어를 구분하기 위한 저장소 역할을 수행함.

### 구현해야 하는 것

최소 아래 3개 테이블 또는 저장 경로를 구성해야 함.

```text
bronze_transactions
silver_transactions
fraud_transactions
```

Gold까지 확장할 경우 다음과 같이 구성 가능함.

```text
bronze_transactions
silver_transactions
gold_transaction_metrics
fraud_transactions
```

### Bronze Layer

Kafka에서 받은 원본 이벤트를 최대한 그대로 저장해야 함. 원본 추적을 위한 역할임.

### Silver Layer

schema와 type이 정리되고 invalid record와 duplicate가 처리된 데이터를 저장해야 함. 분석과 Fraud Detection에 사용할 수 있는 상태여야 함.

### Gold Layer

Dashboard와 dbt에서 바로 사용하기 좋은 집계 결과를 저장하는 용도로 사용할 수 있음. 필수는 아니지만 최종 단계에서는 구현하는 것이 좋음.

### 완료 조건

Spark가 실행될 때 Delta Table 또는 Delta Path에 데이터가 계속 추가되고, 재실행 후에도 데이터가 정상 조회돼야 함.

---

## 5.5 PostgreSQL

### 사용하는 이유

분석용 데이터와 Dashboard 연동용 저장소로 사용함.

Delta Lake가 데이터 레이크 영역을 담당한다면 PostgreSQL은 최종 분석 계층의 간단한 Warehouse 역할을 담당한다고 볼 수 있음.

### 구현해야 하는 것

아래와 같은 분석용 테이블을 고려해야 함.

```text
fact_transactions
fact_fraud_transactions
dim_users
dim_merchants
daily_transaction_metrics
daily_fraud_metrics
```

처음부터 모든 테이블을 만들 필요는 없음.

최소 아래 정도부터 시작하는 것이 좋음.

```text
fact_transactions
fact_fraud_transactions
daily_fraud_metrics
```

---

## 5.6 dbt

### 사용하는 이유

분석용 SQL Transformation을 코드 형태로 관리하기 위해 사용함.

Spark에서 모든 집계를 처리하게 만들 수도 있지만 역할을 분리하기 위해 dbt를 사용함.

Spark는 대량 스트리밍 처리와 정제에 집중함. dbt는 분석용 모델링과 SQL Transformation에 집중함.

### 구현해야 하는 것

예시 모델은 다음과 같음.

```text
stg_transactions.sql
stg_fraud_transactions.sql
fct_transactions.sql
fct_fraud_transactions.sql
mart_daily_fraud_summary.sql
mart_merchant_risk_summary.sql
```

### dbt Test

최소 아래 테스트를 적용하는 것이 좋음.

- transaction_id not_null
- transaction_id unique
- amount not_null
- amount >= 0
- user_id not_null
- merchant_id not_null
- fraud flag allowed values 확인

### 완료 조건

`dbt run`이 성공해야 함.

`dbt test`가 성공해야 함.

PostgreSQL에서 최종 mart table을 조회할 수 있어야 함.

---

## 5.7 Apache Airflow

### 사용하는 이유

Spark Streaming 자체를 Airflow로 계속 실행하기 위한 용도가 아님.

Airflow는 정해진 순서에 따라 실행해야 하는 배치 작업을 관리하기 위해 사용함.

예를 들어 아래 작업을 담당할 수 있음.

```text
데이터 상태 확인
    ↓
집계 작업
    ↓
PostgreSQL 적재
    ↓
dbt run
    ↓
dbt test
    ↓
결과 확인
```

### 구현해야 하는 것

최종적으로 하나 이상의 DAG를 구현해야 함.

예시임.

```text
streamguard_analytics_pipeline
```

Task 예시는 다음과 같음.

```text
check_delta_data
    ↓
load_to_postgres
    ↓
dbt_run
    ↓
dbt_test
    ↓
validate_output
```

### 완료 조건

Airflow UI에서 DAG가 정상적으로 보여야 함.

수동 실행 또는 Schedule 실행 시 모든 Task가 성공해야 함.

실패한 Task가 무엇인지 UI에서 확인 가능해야 함.

---

## 5.8 Docker / Docker Compose

### 사용하는 이유

프로젝트를 다른 사람이 동일한 환경에서 실행할 수 있도록 하기 위함임.

"내 컴퓨터에서는 됨" 수준의 프로젝트를 피하기 위해 필요함.

### Docker Compose에 포함할 후보

- Kafka
- Kafka Controller 또는 Kafka 운영에 필요한 구성요소
- PostgreSQL
- Airflow Webserver
- Airflow Scheduler
- Spark 관련 컨테이너
- 필요 시 Python Generator

초기 구현에서는 모든 서비스를 한 번에 Docker에 넣지 않아도 됨. 단계별로 Docker Compose에 추가해야 함.

### 완료 조건

최종적으로 아래와 같은 흐름이 가능해야 함.

```bash
docker compose up -d
```

이후 필요한 초기화 명령 몇 개만 수행하면 전체 프로젝트를 실행할 수 있어야 함.

---

# 6. 가짜 거래 데이터 설계

실제 금융 거래 데이터가 없기 때문에 Python으로 synthetic transaction을 생성함.

단순 random 숫자만 생성하면 Fraud Rule을 테스트하기 어려움.

따라서 **정상 패턴과 비정상 패턴을 의도적으로 포함한 데이터 생성기**를 만들어야 함.

## 6.1 기본 Transaction Schema

예시 schema는 아래와 같음.

```json
{
  "transaction_id": "txn_20261007_000001",
  "user_id": "user_00125",
  "card_id": "card_00482",
  "merchant_id": "merchant_0032",
  "merchant_category": "electronics",
  "amount": 152000,
  "currency": "KRW",
  "event_time": "2026-10-07T12:30:20.123Z",
  "country": "KR",
  "city": "Seoul",
  "device_id": "device_01392",
  "payment_method": "credit_card",
  "ip_address": "203.0.113.10"
}
```

## 6.2 필수 컬럼과 이유

### transaction_id

거래를 식별하는 고유 ID임. 중복 거래 탐지와 재처리 전략을 연습하기 위해 반드시 필요함.

### user_id

거래를 수행한 사용자 식별자임. 짧은 시간 내 반복 거래 탐지에 사용함.

### card_id

카드 식별자임. 하나의 사용자가 여러 카드를 가진 상황을 표현할 수 있음.

### merchant_id

가맹점 식별자임. 특정 가맹점에서 Fraud 비율이 높은지 분석할 때 사용함.

### merchant_category

가맹점 유형임.

예시는 아래와 같음.

- grocery
- restaurant
- electronics
- fashion
- travel
- entertainment
- convenience
- online_marketplace

### amount

거래 금액임. 고액 거래 Rule에 사용함.

### event_time

실제 거래가 발생한 시간임. Spark의 event time과 watermark 구현을 위해 핵심 컬럼임.

### country / city

지역 기반 이상 거래 탐지에 활용할 수 있음. 초기에는 분석용으로 사용하고 이후 Fraud Rule 확장 시 활용 가능함.

### device_id

짧은 시간 동안 여러 사용자가 동일 device를 사용하는 패턴 등을 확장할 수 있음. 초기에는 분석용 컬럼으로 사용하면 됨.

---

# 7. 정상 거래 데이터 생성 전략

정상 거래는 완전히 random하게 만들지 않는 것이 좋음.

사용자마다 어느 정도 패턴을 가지도록 생성해야 함.

예를 들어 user profile을 미리 생성함.

```json
{
  "user_id": "user_001",
  "home_city": "Seoul",
  "average_amount": 35000,
  "preferred_categories": [
    "grocery",
    "restaurant",
    "convenience"
  ]
}
```

거래 생성 시 대부분은 사용자 profile과 비슷한 패턴으로 발생하도록 해야 함.

- 평균 거래 금액 주변에서 거래 발생함.
- 사용자가 자주 사용하는 merchant category에서 거래 발생함.
- 대부분 home city 또는 인접 지역에서 거래 발생함.
- 거래 시간대도 지나치게 비현실적이지 않게 생성함.

이렇게 해야 Fraud Rule을 적용했을 때 정상과 비정상의 차이가 생김.

---

# 8. 의심 거래 데이터 생성 전략

Fraud Detection을 테스트하기 위해 의도적인 이상 거래 패턴을 생성해야 함.

초기에는 전체 거래의 약 2~5% 정도를 이상 패턴으로 생성하는 방식으로 시작 가능함. 비율 자체보다 Rule을 검증할 수 있는 데이터가 일정하게 생성되는 것이 중요함.

## 8.1 High Amount Fraud

사용자의 일반적인 거래 금액보다 지나치게 높은 거래를 생성함.

```text
평균 거래 금액: 30,000원
의심 거래 금액: 1,500,000원
```

초기 Rule 예시는 아래와 같음.

```text
amount >= 1,000,000
```

나중에는 사용자 평균 대비 비율 기반으로 확장 가능함.

## 8.2 Rapid Repeat Fraud

같은 사용자가 매우 짧은 시간에 여러 번 결제하는 패턴임.

```text
user_001
12:00:01  120,000원
12:00:05  130,000원
12:00:10  150,000원
12:00:20  180,000원
```

Spark Window를 이용해 일정 시간 내 거래 횟수를 계산하는 데 사용할 수 있음.

## 8.3 Duplicate Transaction

동일한 transaction_id를 다시 전송하는 상황을 의도적으로 만들어야 함.

```text
txn_001
txn_002
txn_003
txn_002
```

Silver Layer에서는 동일 transaction_id가 중복 저장되지 않도록 처리해야 함.

## 8.4 Late Arrival Event

event_time은 과거인데 Kafka에는 늦게 도착하도록 이벤트를 생성함.

현재 시간이 12:10인데 아래 event_time을 가진 이벤트를 보내는 식임.

```text
event_time = 12:03
```

이를 통해 watermark와 late event 처리 방식을 테스트해야 함.

## 8.5 Location Anomaly

사용자가 매우 짧은 시간에 현실적으로 이동하기 어려운 지역에서 거래하는 패턴임.

```text
12:00 Seoul
12:05 New York
```

초기 MVP에서는 구현하지 않아도 됨. Fraud Rule 확장 단계에서 구현 가능함.

---

# 9. Fraud Detection 전략

처음부터 Machine Learning을 사용하지 않음.

먼저 설명 가능한 Rule-based Detection을 구현해야 함.

## 9.1 초기 필수 Rule

### Rule 1 — High Amount

```text
amount >= threshold
```

### Rule 2 — Rapid Transactions

```text
N분 이내 동일 user_id가 X건 이상 거래
```

예시는 다음과 같음.

```text
5분 이내 5회 이상 거래
```

## 9.2 Fraud 결과 컬럼

Silver 또는 Fraud 데이터에 아래 컬럼을 추가할 수 있음.

```text
is_fraud
fraud_rule
fraud_score
detected_at
```

예시임.

```json
{
  "is_fraud": true,
  "fraud_rule": "HIGH_AMOUNT",
  "fraud_score": 0.8
}
```

초기 fraud_score는 정교한 확률 값이 아니어도 됨. Rule severity 표현용 값으로 사용할 수 있음.

---

# 10. 단계별 구현 계획

## Phase 0. 개발 환경 준비

### 목표

개발에 필요한 기본 환경을 준비하는 단계임.

### 구현해야 하는 것

- Git Repository 생성해야 함.
- Python virtual environment 또는 Docker 기반 환경을 정해야 함.
- 기본 폴더 구조 생성해야 함.
- `.env` 관리 방식 정해야 함.
- `.gitignore` 작성해야 함.
- Docker 설치 확인해야 함.
- Docker Compose 실행 확인해야 함.

### 완료 조건

아래가 성공해야 함.

```bash
docker compose version
python --version
git status
```

Repository가 정상적으로 생성되고 README가 있어야 함.

### 다음 단계로 넘어가는 기준

개발 환경 문제가 없는 상태여야 함.

---

## Phase 1. Transaction Generator 구현

### 목표

가짜 거래 데이터를 지속적으로 생성하는 프로그램을 만드는 단계임.

### 구현해야 하는 것

- transaction schema 정의해야 함.
- user profile 생성해야 함.
- merchant 목록 생성해야 함.
- 정상 거래 생성해야 함.
- high amount 거래 생성해야 함.
- duplicate 거래 생성 옵션 만들어야 함.
- late arrival 거래 생성 옵션 만들어야 함.
- console에 JSON 출력 가능해야 함.

### 완료 조건

최소 1,000건 정도의 거래를 생성해도 프로그램이 정상 실행돼야 함.

transaction_id가 기본적으로 unique 해야 함.

Fraud Scenario를 설정하면 이상 거래가 일정 비율로 생성돼야 함.

### 왜 필요한가

후속 단계인 Kafka, Spark, Fraud Detection을 외부 데이터 없이 반복적으로 테스트하기 위해 필요함.

---

## Phase 2. Kafka 연결

### 목표

생성된 거래 데이터를 Kafka로 전송하는 단계임.

### 구현해야 하는 것

- Kafka Docker 환경 구성해야 함.
- transaction topic 생성해야 함.
- Python Kafka Producer 구현해야 함.
- JSON Serialization 구현해야 함.
- 메시지 전송 성공 여부 확인해야 함.
- Kafka CLI 또는 테스트 Consumer로 메시지 확인해야 함.

### 완료 조건

Generator를 실행하면 Kafka topic에 거래가 지속적으로 들어가야 함.

최소 수백 건 이상 연속 전송했을 때 오류가 없어야 함.

Producer를 재시작해도 정상적으로 다시 전송 가능해야 함.

### 다음 단계로 넘어가는 기준

Kafka 안에 거래 데이터가 정상적으로 쌓이는 것을 확인해야 함. Spark는 아직 없어도 됨.

---

## Phase 3. Spark Structured Streaming 연결

### 목표

Kafka 데이터를 Spark가 실시간으로 읽고 구조화된 데이터로 변환하는 단계임.

### 구현해야 하는 것

- Spark에서 Kafka source 연결해야 함.
- Kafka value decode 해야 함.
- JSON schema 적용해야 함.
- event_time parsing 해야 함.
- invalid record 처리해야 함.
- 필요한 column casting 해야 함.
- console sink로 결과 확인해야 함.
- checkpoint 경로 구성해야 함.

### 완료 조건

Kafka Producer를 실행하면 몇 초 이내에 Spark에서 거래 데이터가 확인돼야 함.

JSON이 정상적으로 struct 형태로 파싱돼야 함.

잘못된 데이터 하나가 들어왔다고 전체 Streaming Query가 종료되면 안 됨.

### 다음 단계로 넘어가는 기준

Kafka -> Spark 흐름이 최소 10분 이상 안정적으로 동작해야 함.

---

## Phase 4. Delta Lake Bronze 저장

### 목표

Kafka 원본 데이터를 Bronze Layer에 보존하는 단계임.

### 구현해야 하는 것

- Delta Lake dependency 설정해야 함.
- Bronze Path 또는 Table 구성해야 함.
- checkpoint 별도 설정해야 함.
- append 방식으로 저장해야 함.
- 저장된 데이터를 다시 읽을 수 있어야 함.

### 완료 조건

Kafka에 보낸 데이터가 Bronze Delta에 저장돼야 함.

Spark 재시작 후에도 기존 데이터가 유지돼야 함.

Delta 데이터를 조회했을 때 transaction_id, event_time, payload가 정상적으로 보여야 함.

---

## Phase 5. Silver Layer 정제

### 목표

분석과 Fraud Detection에 사용할 수 있는 정제된 거래 테이블을 만드는 단계임.

### 구현해야 하는 것

- null validation 해야 함.
- amount type 검증해야 함.
- negative amount 처리해야 함.
- event_time 검증해야 함.
- transaction_id duplicate 제거해야 함.
- 필요한 schema normalization 해야 함.
- invalid row 처리 정책 정해야 함.
- Silver Delta에 저장해야 함.

### 완료 조건

Silver Table에서 동일 transaction_id가 중복으로 저장되지 않아야 함.

필수 컬럼이 null인 데이터가 Silver에 들어오지 않아야 함.

비정상 row 처리 방식이 README에 문서화돼야 함.

---

## Phase 6. Event Time / Watermark 적용

### 목표

Streaming 시스템에서 늦게 도착하는 이벤트를 처리하는 방법을 구현하는 단계임.

### 구현해야 하는 것

- event_time 기준 Streaming 처리해야 함.
- watermark 설정해야 함.
- window aggregation 하나 이상 구현해야 함.
- late event를 Generator에서 의도적으로 만들어야 함.
- watermark 범위 안의 late event와 범위 밖의 late event 차이를 확인해야 함.

### 완료 조건

의도적으로 늦은 이벤트를 전송했을 때 설정한 정책에 따라 처리 결과가 달라지는 것을 확인해야 함.

README에 아래 내용을 설명할 수 있어야 함.

- event time이 무엇인지
- processing time과 무엇이 다른지
- watermark를 왜 설정했는지
- watermark 값을 왜 해당 값으로 정했는지

---

## Phase 7. Rule-based Fraud Detection

### 목표

실시간 거래에서 의심 패턴을 탐지하는 단계임.

### 구현해야 하는 것

최소 다음 Rule을 구현해야 함.

```text
HIGH_AMOUNT
RAPID_REPEAT
```

가능하면 추가로 다음을 구현할 수 있음.

```text
DUPLICATE_TRANSACTION
UNUSUAL_LOCATION
```

Fraud 결과는 별도 Delta Table에 저장해야 함.

### 완료 조건

Generator에서 의도적으로 fraud event를 만들었을 때 Spark가 이를 탐지해야 함.

어떤 Rule 때문에 탐지됐는지 fraud_rule 컬럼에서 확인 가능해야 함.

False Positive 가능성과 Rule 한계를 README에 적어야 함.

---

## Phase 8. PostgreSQL 분석 계층

### 목표

Dashboard와 SQL 분석에 사용할 데이터를 PostgreSQL에 제공하는 단계임.

### 구현해야 하는 것

최소 아래 테이블을 구성해야 함.

```text
fact_transactions
fact_fraud_transactions
daily_fraud_metrics
```

집계 예시는 다음과 같음.

- 날짜별 총 거래 건수
- 날짜별 총 거래 금액
- 날짜별 Fraud 건수
- Fraud 비율
- Merchant별 Fraud 건수
- 사용자별 Fraud 건수
- 평균 거래 금액

### 완료 조건

PostgreSQL SQL Query로 위 지표를 조회할 수 있어야 함.

---

## Phase 9. dbt 도입

### 목표

분석용 SQL Transformation을 코드와 테스트로 관리하는 단계임.

### 구현해야 하는 것

- dbt project 생성해야 함.
- source 정의해야 함.
- staging model 만들어야 함.
- fact model 만들어야 함.
- mart model 만들어야 함.
- schema test 추가해야 함.

### 완료 조건

아래 명령이 성공해야 함.

```bash
dbt run
dbt test
```

dbt lineage를 설명할 수 있어야 함.

---

## Phase 10. Airflow 도입

### 목표

배치성 작업을 자동화하는 단계임.

### 구현해야 하는 것

DAG를 구성해야 함.

```text
check_source
    ↓
load_analytics_data
    ↓
dbt_run
    ↓
dbt_test
    ↓
validate_output
```

Streaming Job 자체를 Airflow로 반복 실행하는 구조는 피하는 것이 좋음.

### 완료 조건

Airflow UI에서 DAG가 정상 실행돼야 함.

Task 실패 시 어느 작업이 실패했는지 확인 가능해야 함.

DAG 재실행이 가능해야 함.

---

## Phase 11. Dashboard

### 목표

처리된 거래 데이터와 Fraud 결과를 시각적으로 확인하는 단계임.

### 보여줄 항목

최소 아래 지표를 고려함.

- Total Transactions
- Total Transaction Amount
- Fraud Transactions
- Fraud Rate
- Fraud by Merchant
- Fraud by Rule
- Transactions Over Time
- Fraud Over Time
- Top Risk Users
- Top Risk Merchants

Dashboard 도구는 Power BI, Metabase, Superset, Grafana 또는 간단한 Streamlit 중 하나를 사용할 수 있음.

프로젝트 핵심은 Dashboard가 아니므로 지나치게 많은 시간을 쓰지 않는 것이 좋음.

### 완료 조건

최소 1개의 화면에서 주요 KPI와 Fraud Trend를 확인할 수 있어야 함.

---

## Phase 12. Docker Compose 통합

### 목표

프로젝트를 재현 가능한 형태로 만드는 단계임.

### 구현해야 하는 것

가능한 범위에서 아래 구성요소를 Compose에 포함해야 함.

```text
Kafka
Spark
PostgreSQL
Airflow
Generator
```

dbt는 Airflow 컨테이너 또는 별도 컨테이너에서 실행 가능함.

### 완료 조건

README만 보고 다른 사람이 프로젝트를 실행할 수 있어야 함.

환경 변수를 `.env.example`에 제공해야 함.

Secret이나 Password를 Git에 직접 올리면 안 됨.

---

## Phase 13. 최종 문서화

### 목표

프로젝트를 면접에서 설명할 수 있는 상태로 만드는 단계임.

### 반드시 포함해야 하는 문서

- 프로젝트 목적
- Architecture Diagram
- Tech Stack
- 각 기술을 선택한 이유
- Transaction Schema
- Fraud Rules
- 실행 방법
- Docker 실행 방법
- dbt 실행 방법
- Airflow DAG 설명
- 데이터 흐름
- Known Limitations
- Future Improvements
- Screenshots
- Troubleshooting
- Interview Questions

### 완료 조건

Repository를 처음 보는 사람이 README만 보고 프로젝트 목적과 구조를 이해할 수 있어야 함.

---

# 11. 단계별 완료 기준 요약

| 단계 | 완료 기준 |
|---|---|
| Phase 0 | Git, Python, Docker 환경 정상임 |
| Phase 1 | Synthetic transaction 생성 가능함 |
| Phase 2 | Kafka Topic에 이벤트 지속 입력 가능함 |
| Phase 3 | Spark가 Kafka 이벤트를 실시간 파싱함 |
| Phase 4 | Bronze Delta에 원본 이벤트 저장됨 |
| Phase 5 | Silver에 정제 및 중복 제거된 데이터 저장됨 |
| Phase 6 | Watermark와 Late Event 동작을 직접 확인함 |
| Phase 7 | 최소 2개 Fraud Rule이 실제로 탐지됨 |
| Phase 8 | PostgreSQL에서 분석 SQL 실행 가능함 |
| Phase 9 | dbt run / dbt test 성공함 |
| Phase 10 | Airflow DAG 전체 성공함 |
| Phase 11 | Fraud Dashboard 확인 가능함 |
| Phase 12 | Docker Compose로 재현 가능함 |
| Phase 13 | README와 Architecture 문서 완성됨 |

---

# 12. 권장 프로젝트 폴더 구조

```text
streamguard/
│
├── README.md
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── generator/
│   ├── app.py
│   ├── transaction_generator.py
│   ├── user_profiles.py
│   ├── merchant_data.py
│   └── requirements.txt
│
├── kafka/
│   ├── create_topics.sh
│   └── config/
│
├── spark/
│   ├── jobs/
│   │   ├── bronze_stream.py
│   │   ├── silver_stream.py
│   │   └── fraud_detection.py
│   ├── schemas/
│   │   └── transaction_schema.py
│   ├── rules/
│   │   └── fraud_rules.py
│   └── utils/
│       └── spark_session.py
│
├── storage/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── checkpoints/
│
├── dbt/
│   └── streamguard_dbt/
│       ├── models/
│       │   ├── staging/
│       │   ├── marts/
│       │   └── schema.yml
│       └── dbt_project.yml
│
├── airflow/
│   ├── dags/
│   │   └── streamguard_pipeline.py
│   └── logs/
│
├── postgres/
│   ├── init/
│   └── sql/
│
├── dashboard/
│
├── tests/
│   ├── test_generator.py
│   ├── test_fraud_rules.py
│   └── test_schema.py
│
├── docs/
│   ├── architecture.md
│   ├── data_model.md
│   ├── fraud_rules.md
│   └── troubleshooting.md
│
└── scripts/
    ├── start.sh
    ├── stop.sh
    └── reset.sh
```

초기부터 모든 폴더를 완성할 필요는 없음. Phase 진행에 따라 하나씩 추가하면 됨.

---

# 13. 데이터 품질 최소 요구사항

프로젝트 04 수준의 Data Quality Framework는 아직 구현하지 않음.

하지만 StreamGuard 안에서도 최소한의 데이터 검증은 필요함.

다음 검증은 구현하는 것이 좋음.

- transaction_id null 여부 확인해야 함.
- user_id null 여부 확인해야 함.
- merchant_id null 여부 확인해야 함.
- amount가 숫자인지 확인해야 함.
- amount가 음수가 아닌지 확인해야 함.
- event_time parsing 가능한지 확인해야 함.
- duplicate transaction_id 제거해야 함.
- 허용되지 않은 currency 값 처리해야 함.

잘못된 데이터를 무조건 삭제하기보다 별도 invalid/dead-letter 영역으로 보내는 방식도 고려 가능함.

---

# 14. 장애 및 예외 상황 테스트

정상 상황만 테스트하면 부족함.

최소 아래 상황을 일부러 만들어봐야 함.

- Kafka가 잠시 내려갔다가 다시 올라오는 상황
- Producer가 재시작되는 상황
- Spark Job이 재시작되는 상황
- Duplicate transaction 발생
- 잘못된 JSON 발생
- amount가 null인 거래 발생
- event_time이 잘못된 거래 발생
- 매우 늦은 이벤트 발생
- dbt test 실패
- PostgreSQL 연결 실패

모든 장애를 완벽하게 자동 복구할 필요는 없음.

하지만 어떤 문제가 발생했고 시스템이 어떻게 반응했는지 설명할 수 있어야 함.

---

# 15. 프로젝트에서 특히 중요하게 기록해야 하는 것

개발 중 Architecture Decision을 계속 기록해야 함.

예시는 다음과 같음.

```text
왜 Kafka를 선택했는가?
왜 Redis Stream이 아닌가?
왜 Spark Structured Streaming인가?
왜 Flink가 아닌가?
왜 Delta Lake인가?
왜 PostgreSQL인가?
왜 dbt가 필요한가?
왜 Airflow가 필요한가?
```

모든 기술 비교를 깊게 할 필요는 없음.

최소한 현재 기술을 선택한 이유와 장단점을 설명할 수 있어야 함.

---

# 16. 성공한 프로젝트의 기준

코드가 동작한다고 프로젝트가 끝난 것이 아님.

최종적으로 아래 질문에 대부분 "예"라고 답할 수 있어야 함.

- 거래 이벤트가 실시간으로 생성되는가?
- Kafka에 지속적으로 들어가는가?
- Spark가 실시간으로 읽는가?
- Raw 데이터가 보존되는가?
- Clean 데이터가 별도로 저장되는가?
- Duplicate가 처리되는가?
- Late Event를 설명할 수 있는가?
- Watermark가 실제로 적용돼 있는가?
- Fraud Rule이 실제로 동작하는가?
- Fraud 결과를 별도 저장하는가?
- 분석용 테이블이 있는가?
- dbt model이 있는가?
- dbt test가 있는가?
- Airflow DAG가 있는가?
- Dashboard에서 결과를 볼 수 있는가?
- Docker로 실행 가능한가?
- README만 보고 다른 사람이 실행할 수 있는가?
- Architecture Diagram이 있는가?
- 장애 상황을 일부 테스트했는가?
- 각 기술을 왜 사용했는지 설명할 수 있는가?

대부분 충족되면 프로젝트를 포트폴리오에 넣을 수 있는 수준으로 판단 가능함.

---

# 17. 처음부터 구현하지 말아야 하는 것

다음 항목은 프로젝트 진행을 늦출 가능성이 높으므로 초기에는 제외하는 것이 좋음.

- ML Fraud Detection
- Kubernetes
- Terraform
- AWS 전체 배포
- Schema Registry
- Kafka Connect
- 복잡한 CI/CD
- 복잡한 사용자 인증
- Microservice 분리
- 고가용성 Kafka Cluster
- Spark Multi-node Cluster
- 실시간 Push Notification
- 너무 많은 Fraud Rule

먼저 핵심 pipeline을 끝까지 연결한 뒤 하나씩 확장해야 함.

---

# 18. 향후 확장 아이디어

## QualityLens 통합

프로젝트 04에서 다루는 별도 데이터 품질 및 Drift Monitoring 기능을 추가 가능함.

예시는 다음과 같음.

- Great Expectations
- Evidently
- Schema Drift
- Distribution Drift
- Historical Quality Metrics
- Quality Alert

현재 프로젝트가 완료되기 전에는 구현하지 않음.

## Machine Learning Fraud Detection

Rule 기반 Detection이 안정적으로 동작한 뒤 고려할 수 있음.

예시는 다음과 같음.

- Isolation Forest
- Logistic Regression
- XGBoost
- Autoencoder

ML 자체보다 Feature Pipeline과 Serving 구조가 더 복잡해질 수 있으므로 후순위로 둠.

## Cloud Deployment

로컬 환경에서 프로젝트가 안정적으로 완료된 이후 일부 구성요소만 Cloud에 올릴 수 있음.

예시는 다음과 같음.

- Object Storage
- Managed PostgreSQL
- Managed Kafka
- Airflow
- Dashboard

전체를 클라우드로 옮기는 것보다 최소 하나의 구성요소를 실제 배포해 보는 것이 우선임.

---

# 19. 권장 개발 순서

최종적으로 아래 순서대로 진행해야 함.

```text
1. Repository / Docker 기본 환경
        ↓
2. Transaction Generator
        ↓
3. Kafka Producer
        ↓
4. Kafka Topic 검증
        ↓
5. Spark Structured Streaming
        ↓
6. Bronze Delta
        ↓
7. Silver Cleaning
        ↓
8. Duplicate Handling
        ↓
9. Event Time / Watermark
        ↓
10. Fraud Rule
        ↓
11. Fraud Delta Table
        ↓
12. PostgreSQL
        ↓
13. dbt
        ↓
14. Airflow
        ↓
15. Dashboard
        ↓
16. Docker Compose 통합
        ↓
17. README / Architecture / Test
```

순서를 가능한 한 지키는 것이 좋음.

앞 단계가 불안정한 상태에서 다음 기술을 추가하면 디버깅 난이도가 급격히 증가함.

---

# 20. 첫 번째 구현 목표

가장 처음 목표는 아래만 구현하면 됨.

```text
Python Transaction Generator
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
Console Output
```

이 단계에서 Delta Lake, dbt, Airflow, PostgreSQL을 생각할 필요 없음.

먼저 거래 이벤트 한 건이 아래 경로를 따라 실제로 이동하는지 확인해야 함.

```text
Python에서 생성됨
    ↓
Kafka에 들어감
    ↓
Spark가 읽음
    ↓
JSON이 정상 파싱됨
```

여기까지 성공하면 첫 번째 milestone 완료임.

---

# 21. 두 번째 구현 목표

첫 번째 milestone 완료 후 아래 구조를 구현함.

```text
Kafka
    ↓
Spark
    ↓
Bronze Delta
    ↓
Silver Delta
```

이 단계에서 데이터 정제와 중복 제거를 확실하게 구현해야 함.

---

# 22. 세 번째 구현 목표

두 번째 milestone 완료 후 Fraud Detection을 추가함.

```text
Silver Transactions
        ↓
Fraud Rules
        ↓
Fraud Transactions
```

이 단계까지 성공하면 StreamGuard의 핵심 기능은 구현된 상태임.

---

# 23. 네 번째 구현 목표

이후 Analytics Stack을 추가함.

```text
Delta
    ↓
PostgreSQL
    ↓
dbt
    ↓
Dashboard
```

이 단계에서 분석 가능한 프로젝트가 됨.

---

# 24. 다섯 번째 구현 목표

마지막으로 운영 요소를 추가함.

```text
Airflow
Docker Compose
Data Quality Check
Monitoring
Documentation
Deployment
```
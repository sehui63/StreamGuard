# Phase 1 — Synthetic Transaction Generator

이 문서는 StreamGuard의 첫 구현 단계 기준이다. 이 단계는 Python으로 현실적인 Raw Transaction을 만들고 콘솔에 JSON Lines로 출력하는 데만 집중한다.

Kafka, Spark, Airflow, dbt, PostgreSQL, Delta Lake는 이 단계의 구현 범위가 아니다.

## 목표

- 메모리에서 User Profile 500명과 Merchant Profile 100개를 생성한다.
- 두 프로필의 관계를 반영한 정상 거래를 생성한다.
- 이후 파이프라인 검증용 `HIGH_AMOUNT`, `RAPID_REPEAT`, `DUPLICATE`, `LATE_EVENT` 시나리오를 생성한다.
- Raw Transaction에는 fraud label을 넣지 않는다.
- Generator 내부에는 별도 Ground Truth를 보관한다.

## 파일 구조와 책임

```text
generator/
├── app.py                     # CLI, JSON Lines 콘솔 출력
├── config.py                  # 도시·카테고리·금액 범위·확률 설정
├── models.py                  # Profile, Raw Transaction, Scenario, Ground Truth 타입
├── user_profiles.py           # 비식별 사용자 프로필 생성
├── merchant_profiles.py       # 카테고리별 가맹점 프로필 생성
└── transaction_generator.py   # 프로필 기반 거래 및 시나리오 생성

tests/
└── test_generator.py          # 생성기 단위 테스트
```

모든 구현은 Python 표준 라이브러리만 사용한다.

## 내부 프로필

### User Profile

500명의 비식별 사용자를 생성한다. 각 사용자는 아래 속성을 가진다.

```text
user_id
usual_city
avg_amount
preferred_categories
preferred_payment_method
device_ids  # 1~3개
```

실명·연락처 등 실제 개인정보는 생성하지 않는다. 사용자별 도시, 평균 금액, 선호 카테고리, 결제 수단, 기기 목록은 서로 다르게 생성된다.

### Merchant Profile

100개의 가맹점을 생성한다. 각 가맹점은 아래 속성을 가진다.

```text
merchant_id
merchant_category
city
avg_amount
min_amount
max_amount
```

카테고리는 다음 값만 사용한다.

```text
restaurant, grocery, convenience, electronics,
fashion, travel, entertainment, online_marketplace
```

도시는 `Seoul`, `Busan`, `Incheon`, `Daegu`, `Daejeon`, `Gwangju`만 사용한다.

카테고리마다 다른 정상 거래 금액 범위를 둔다. 정상 거래는 해당 카테고리의 `max_amount`를 넘지 않으며, `HIGH_AMOUNT`는 반드시 그 상한선을 초과한다.

| Category | 평균 금액 | 정상 거래 상한 |
|---|---:|---:|
| `convenience` | 15,000원 | 100,000원 |
| `grocery` | 60,000원 | 300,000원 |
| `restaurant` | 50,000원 | 300,000원 |
| `fashion` | 120,000원 | 800,000원 |
| `electronics` | 600,000원 | 2,500,000원 |
| `entertainment` | 70,000원 | 500,000원 |
| `online_marketplace` | 100,000원 | 1,000,000원 |
| `travel` | 700,000원 | 3,000,000원 |

## Raw Transaction 계약

콘솔에 출력하는 Raw Transaction은 아래 **정확히 10개 필드**만 가진다.

```text
transaction_id
user_id
merchant_id
merchant_category
amount
currency
event_time
city
device_id
payment_method
```

`currency`는 항상 `KRW`이며, `event_time`은 UTC ISO-8601 형식(`YYYY-MM-DDTHH:MM:SSZ`)이다. `is_fraud`, `fraud_type`, `scenario`는 Raw Event에 포함하지 않는다.

## 정상 거래 생성 원칙

- 보통 사용자의 `usual_city`에서 거래한다.
- 사용자의 `preferred_categories`에 해당하는 가맹점을 더 높은 확률로 선택한다.
- 사용자의 기본 결제 수단을 더 높은 확률로 선택한다.
- 사용자의 등록 `device_ids` 중 하나만 사용한다.
- 금액은 사용자 평균 금액과 가맹점 평균 금액의 영향을 함께 받는다.
- 금액은 로그 정규 분포를 사용해 만들며, 대부분 일반적인 금액이고 큰 금액은 드물다.

확률, 금액 범위, 지연 시간, 고액 배수 등 조정 가능한 값은 `generator/config.py`의 `GeneratorConfig`에 모아 둔다.

## 테스트 시나리오와 Ground Truth

| 시나리오 | Raw Event 특성 | 목적 |
|---|---|---|
| `NORMAL` | 일반 사용자 패턴을 따른다 | 기준 데이터 |
| `HIGH_AMOUNT` | 해당 가맹점 카테고리의 정상 거래 상한선을 초과한다 | 고액 Fraud Rule 검증 |
| `RAPID_REPEAT` | 같은 사용자의 이벤트 시간이 5초 간격으로 생성된다 | Window 기반 반복 거래 탐지 검증 |
| `DUPLICATE` | 이전에 출력한 이벤트와 같은 `transaction_id`를 재출력한다 | 중복 제거 파이프라인 검증 |
| `LATE_EVENT` | 생성 시점보다 5~15분 과거의 `event_time`을 가진다 | Event Time / Watermark 검증 |

시나리오는 `GroundTruthRecord(output_index, transaction_id, scenario)`로 Generator 내부의 `ground_truth` 목록에 분리해 보관한다. 특히 중복 이벤트는 동일 ID가 여러 번 나타날 수 있으므로, transaction ID 하나만 key로 하는 mapping 대신 출력 순번을 함께 기록한다.

## 실행 방법

저장소 루트에서 실행한다.

```bash
python app.py --count 20 --scenario NORMAL
```

재현 가능한 결과가 필요하면 `--seed`를 사용한다.

```bash
python app.py --count 10 --scenario HIGH_AMOUNT --seed 42
python app.py --count 10 --scenario RAPID_REPEAT --seed 42
python app.py --count 10 --scenario DUPLICATE --seed 42
python app.py --count 10 --scenario LATE_EVENT --seed 42
```

`--events-per-second 2`처럼 출력 속도를 설정할 수 있다. `DUPLICATE` 모드에서는 원본 이벤트와 중복 이벤트가 교대로 출력되어, 지정한 출력 개수를 유지한다.

## 완료 기준

- [x] User Profile 500명과 Merchant Profile 100개를 생성한다.
- [x] Raw Transaction이 정확히 10개 필드를 가진다.
- [x] 정상 거래의 ID는 고유하다.
- [x] 가맹점 ID와 카테고리, 사용자와 기기 관계가 일관된다.
- [x] 고액·반복·중복·지연 도착 시나리오를 생성할 수 있다.
- [x] Raw Event와 Ground Truth를 분리한다.
- [x] 콘솔에서 JSON Lines를 확인할 수 있다.
- [x] 핵심 생성 규칙에 대한 단위 테스트가 있다.

## 검증 명령

```bash
python -m unittest discover -s tests -v
```

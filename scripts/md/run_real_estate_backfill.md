# 📄 run_real_estate_backfill.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** `data_orchestration` Airflow DAG가 매일 자동으로 수행하는 부동산 실거래가 수집 로직(`fetch_real_estate_recent` / `upsert_real_estate`)을, 과거의 특정 기간에 대해 수동으로 재실행(백필)할 수 있게 해주는 CLI 스크립트다. 입력받은 시작일~종료일 구간의 날짜 하나하나를 각각 "기준일(as_of_date)"로 삼아, DAG와 동일하게 그 기준일로부터 최근 `LOOKBACK_DAYS`(90일)를 재확인하고, 실제로 변경된 계약일만 모아 마지막에 한 번에 테이블 upsert를 수행한다.
- **현재 구현 상태:** 완료. 과거 로직(월 단위 기본 백필, `_months_ago`)이 주석으로 보존되어 있으며 현재는 CLI 인자 미지정 시 최근 7일(`BACKFILL_DEFAULT_DAYS`)을 기본값으로 사용하도록 대체됨. 명시적 TODO 주석은 없다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `ingestion.Real_Estate` (`fetch_real_estate_recent`, `upsert_real_estate`) — `data_orchestration.py` DAG가 사용하는 것과 동일한 함수를 그대로 재사용.
- **외부 패키지:** 없음(순수 표준 라이브러리만 사용) — `logging`, `sys`, `datetime`(`date`, `datetime`, `timedelta`). 주석 처리된 `calendar` import는 과거 `_months_ago()` 함수용으로 삭제되지 않고 남아 있음.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `_parse_ctrt_day_arg` | CLI로 받은 날짜 문자열을 `YYYYMMDD` 또는 `YYYY-MM-DD` 형식으로 파싱해 `date`로 변환. 두 형식 모두 실패하면 로그 남기고 `SystemExit(1)` | `value: str`, `label: str` | `date`; 실패 시 프로세스 종료(exit code 1) |
| `main` | 전체 실행 진입점: CLI 인자 파싱 → 기준일 구간 반복 → `fetch_real_estate_recent` 호출 → 변경분 취합 → `upsert_real_estate` 호출 | CLI 인자 0개(기본값 사용) 또는 2개(`시작일`, `종료일`) | `None`; `__main__`에서 직접 호출 |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** N/A — Airflow에 의해 스케줄링되지 않는 수동 CLI 스크립트. `run_real_estate_backfill.bat`을 통해 실행되며, 배치파일이 사용자로부터 시작일/종료일을 입력받아 인자로 전달(입력 없이 Enter만 치면 인자 없이 실행되어 기본값 적용).
- **Source (Input):** 서울 열린데이터광장 실거래가 API(`fetch_real_estate_recent` 경유, 계약일 단위 조회), CLI 인자(시작일/종료일, 생략 가능).
- **Target (Output):** `data_orchestration.py` DAG와 동일한 DuckDB 테이블 `real_estate`(`REAL_ESTATE_TABLE_NAME = "real_estate"`).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. CLI 인자가 2개 이상이면 `_parse_ctrt_day_arg`로 시작일/종료일 파싱, 시작일이 종료일보다 늦으면 오류 로그 후 `SystemExit(1)`.
  2. 인자가 없으면 기본값 적용: 종료일=어제, 시작일=어제로부터 `BACKFILL_DEFAULT_DAYS(7)`일 전(최근 1주일, 어제 포함 7일).
  3. 시작일부터 종료일까지 하루씩 순회하며 각 날짜를 `as_of_date`로 `fetch_real_estate_recent(None, lookback_days=90, as_of_date=as_of)` 호출, 반환된 `changed_ctrt_days`를 `set`에 누적(중복 자동 제거).
  4. 구간 반복이 끝난 후 변경된 계약일이 하나도 없으면 로그만 남기고 조기 종료(`return`).
  5. 변경된 계약일이 있으면 정렬 후 `upsert_real_estate(None, "real_estate", sorted_changed_days)`를 한 번만 호출해 전체 반영.
  - 참고: 서울 열린데이터광장 API가 계약일 범위(fromDate~toDate) 조회를 지원하지 않아(위치 기반 경로 세그먼트 파라미터), `fetch_real_estate_recent` 내부적으로 하루씩 반복 호출하는 구조이며, 이 스크립트는 그 함수를 여러 기준일에 대해 다시 반복 호출하는 이중 루프 구조를 갖는다(기준일 루프 × 내부 lookback 루프).

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] 시작일~종료일 범위를 기준일 단위로 반복하며 DAG와 동일한 lookback 재확인 로직 재사용
- [x] `YYYYMMDD`/`YYYY-MM-DD` 두 날짜 형식 자동 인식 및 형식 오류 시 명확한 종료 처리
- [x] 변경된 계약일을 `set`으로 누적해 중복 upsert 방지, 마지막에 한 번만 upsert 수행(구간이 겹쳐도 중복 반영 없음)
- [x] 과거 3개월 기본 백필 로직(`BACKFILL_MONTHS`, `_months_ago`)을 최근 7일 기본값으로 대체(주석으로 이전 로직 보존)
- [ ] [TODO] 기준일 구간이 넓을 경우(예: 수개월치 백필) 기준일마다 90일 lookback을 반복 조회하므로 API 호출 횟수가 `구간일수 × 90`에 비례해 증가한다 — 대량 백필 시 실행 시간이 매우 길어질 수 있음을 사용자가 인지해야 한다(코드 내 명시된 TODO는 아니며 운영상 주의사항으로 별도 6번 항목에도 기재).

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 기준일 구간이 겹치는 경우(예: 90일보다 좁은 간격의 여러 기준일)에도 `fetch_real_estate_recent` 내부의 지문(fingerprint) 비교로 실제 변경이 없는 날짜는 자동으로 스킵되므로 중복 재저장이 발생하지 않는다(코드 주석에 명시).
- 시작일이 종료일보다 늦으면 즉시 `SystemExit(1)`로 종료되며, 호출한 `.bat` 파일도 동일한 errorlevel로 종료된다.
- 백필 대상 기간이 길수록 API 호출량이 `구간일수 × LOOKBACK_DAYS(90)`에 비례해 늘어나 실행 시간이 오래 걸릴 수 있다 — 별도의 진행률 표시나 병렬화는 없고 순차 실행이다.
- `LOOKBACK_DAYS`와 `REAL_ESTATE_TABLE_NAME` 값은 `data_orchestration.py` DAG와 동일하게 맞춰야 한다는 점이 코드 주석에 명시되어 있다 — 둘 중 하나만 변경하면 DAG와 백필 스크립트의 동작이 어긋날 수 있다.
- 별도의 재시도 정책은 없다 — API 호출 실패 시 예외가 그대로 전파되어 스크립트가 중단된다.

# 📄 backfill_mno_sno.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 이미 적재되어 있는 `lakehouse.fact_apt_transactions`의 기존 행에 `mno`/`sno`(지번) 값만 채워 넣는 1회성 수동 백필 스크립트. `Real_Estate_Transform.py`의 Silver 변환 로직(필터/컬럼 매핑/캐스팅/dedup 키)을 그대로 재사용해 Bronze를 다시 정제하고, 기존 행과 동일한 자연키로 매칭되는 행에 한해 `mno`/`sno` 두 컬럼만 UPDATE한다. 새 행 INSERT는 하지 않는 순수 보강(추가/보강) 목적의 독립 실행 스크립트다.
- **현재 구현 상태:** 완료된 1회성 유틸리티 스크립트로 보인다. `Real_Estate_Transform.py`의 로직/스키마/파이프라인 동작 자체는 변경하지 않는다고 명시되어 있고(docstring), 백필 전후 전체 행 수 불변 여부를 자체 검증(assert 대신 WARN 로그)하는 안전장치와, 백필 직후 Iceberg 스냅샷 정리(`expire_snapshots`)까지 포함해 마무리가 잘 되어 있다. TODO/FIXME 주석은 없다. 다만 정식 TODO는 아니지만, `Real_Estate_Transform.py`의 로직 변경 시 이 스크립트의 복제된 로직(select/필터/dedup 키)이 함께 갱신되지 않으면 두 스크립트가 서서히 어긋날 수 있는 구조적 위험이 있다(6번 참고).

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음(프로젝트 내부 모듈 직접 import 없음). 대신 `Real_Estate_Transform.py`의 Silver 정제 로직(필터/컬럼 매핑/캐스팅/dropDuplicates 키)을 코드 수준에서 그대로 복제해 재사용하고 있다(주석에 명시).
- **외부 패키지:** `python-dotenv`(`load_dotenv` — env/.env 로드), `pyspark.sql`(`SparkSession`, `functions as F`), 표준 라이브러리 `os`, `pathlib.Path`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| 환경 변수 로드 블록 (1번) | `_PROJECT_ROOT/env/.env` 또는 `/opt/airflow/project/env/.env` 중 존재하는 첫 경로에서 `.env`를 로드(로컬/Airflow 컨테이너 겸용), `override=False` | `_ENV_CANDIDATES: list[Path]` | 환경변수 세팅(부수효과) |
| SparkSession 생성 블록 (2번) | `Real_Estate_Transform.py`와 동일한 Iceberg/S3A 설정으로 SparkSession 1회 기동(앱 이름만 `Real_Estate_Backfill_Mno_Sno`로 다름) | 다수의 `.config(...)`, env var 오버라이드 가능 | `SparkSession` 전역 `spark` |
| 컬럼 보장 및 사전 카운트 블록 (3번) | `fact_apt_transactions`에 `mno`/`sno` 컬럼이 없으면 `ALTER TABLE ADD COLUMNS`로 추가, 백필 전 `mno IS NULL`인 행 수와 전체 행 수를 조회/출력 | 없음 | `before_null_cnt`, `total_cnt`(stdout 로그) |
| Bronze 재정제 블록 (4번) | `Real_Estate_Transform.py`와 동일한 필터(`BLDG_USG == "아파트"`)/컬럼 매핑/캐스팅/`dropDuplicates` 키로 Bronze 전체를 재정제하고, `mno`가 null이거나 공백인 행은 제외 | `BRONZE_PATH`(와일드카드 전체 스캔) | `source_df`(임시 뷰 `mno_sno_backfill_source`) |
| MERGE INTO 블록 (5번) | 자연키(`deal_date`, `sgg_cd`, `dong_cd`, `apt_name`, `price_ten_thousand`, `exclusive_area_m2`, `floor`, 전부 `IS NOT DISTINCT FROM`) 매칭 + `target.mno IS NULL`일 때만 `mno`/`sno` 두 컬럼 UPDATE. WHEN NOT MATCHED 절 없음(INSERT 안 함) | 없음(SQL 문자열 실행) | Iceberg 테이블 갱신(부수효과) |
| 결과 검증 블록 (6번) | 백필 후 `mno IS NULL` 행 수, 전체 행 수를 재조회해 백필 성공 건수 계산, 전체 행 수 불변 여부를 경고 로그로 확인, 채워진 행 상위 5건 출력 | 없음 | stdout 로그 |
| 스냅샷 정리 블록 (7번) | Iceberg `CALL lakehouse.system.expire_snapshots(...)`로 `retain_last=1`, `older_than=now()` 설정으로 이전 스냅샷(옛 스키마 파일 포함) 전부 만료 | 없음 | 실행 결과 `show()`, `spark.stop()` |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** N/A — Airflow DAG가 아닌 수동 1회성 실행 스크립트(`python src/transformation/silver/backfill_mno_sno.py`로 직접 실행). 스케줄에 등록되어 있지 않다.
- **Source (Input):** MinIO(S3A) Bronze parquet 전체 와일드카드 — `s3a://{RAW}/real_estate/year=*/month=*/day=*/*.parquet`, 그리고 기존 Iceberg 테이블 `lakehouse.fact_apt_transactions`(백필 대상).
- **Target (Output):** `lakehouse.fact_apt_transactions`의 `mno`/`sno` 컬럼만 UPDATE(신규 행 없음), 백필 후 스냅샷 정리로 이전(옛 스키마) 데이터 파일 만료.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. env 로드(`.env` 후보 경로 순회) 후 `Real_Estate_Transform.py`와 동일한 Iceberg/S3A 설정으로 SparkSession 기동.
  2. `fact_apt_transactions`에 `mno`/`sno` 컬럼이 없으면 스키마 진화로 추가, 백필 전 상태(null 건수/전체 건수) 기록.
  3. Bronze 전체를 `Real_Estate_Transform.py`와 동일한 규칙(아파트 필터, 컬럼 매핑, 방어적 캐스팅, `deal_date` null 제외, `(deal_date, sgg_cd, dong_cd, apt_name, price_ten_thousand, exclusive_area_m2, floor)` 키로 dedup)으로 재정제하고, `mno`가 비어있는 행은 소스에서 제외.
  4. 기존 fact 행과 위 자연키(`IS NOT DISTINCT FROM`으로 NULL-safe 매칭)가 일치하고 `target.mno IS NULL`인 행에 대해서만 `mno`/`sno` UPDATE(신규 INSERT 없음).
  5. 백필 전후 `mno IS NULL` 건수 차이로 신규 채움 건수를 계산하고, 전체 행 수가 불변인지 검증(불변이 아니면 WARN 로그로 이상 신호).
  6. `expire_snapshots`로 이전 스냅샷(옛 스키마 파일)을 만료시켜, `mno`/`sno` 없는 옛 parquet 파일이 남아 Iceberg 메타데이터를 거치지 않고 직접 glob하는 다운스트림 스크립트(`pipeline_apt_name.py` 등)에서 스키마 불일치를 일으키는 것을 방지.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] `Real_Estate_Transform.py`의 Silver 정제 로직을 재사용/복제해 기존 fact 행과 매칭 가능한 소스 생성
- [x] `mno`/`sno` 두 컬럼만 UPDATE하고 신규 INSERT는 하지 않는 순수 보강 방식(MERGE INTO + WHEN MATCHED만 사용)
- [x] 백필 전후 전체 행 수 불변 검증(UPDATE만 수행됐는지 확인하는 자체 안전장치)
- [x] 백필 직후 Iceberg 스냅샷 만료(`expire_snapshots`, retain_last=1)로 옛 스키마 데이터 파일 정리(다운스트림 직접 glob 스크립트와의 스키마 불일치 방지)
- [ ] [TODO] 코드 내 명시된 TODO 주석은 없음. 개선 제안: `Real_Estate_Transform.py`의 Silver 정제 로직(필터/컬럼 매핑/dedup 키)이 이 스크립트에 그대로 복제되어 있어, 원본 로직이 변경될 때 이 스크립트가 함께 갱신되지 않으면 매칭 자연키가 어긋나 백필이 조용히 실패(매칭 0건)할 위험이 있다 — 공통 함수로 추출해 두 스크립트가 같은 코드를 참조하도록 리팩터링을 고려할 만하다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 1회성 수동 백필 스크립트이므로 다른 Gold/Silver 태스크와 동시 실행을 상정하지 않는다(주석에 명시) — 그럼에도 메모리 설정은 무제한이 아니라 `Real_Estate_Transform.py`와 동일한 보수적 값(3g 등)을 사용한다.
- MERGE 매칭 조건이 전부 `IS NOT DISTINCT FROM`(NULL-safe)으로 되어 있어, 자연키의 일부 컬럼 값이 NULL인 기존 행도 정확히 매칭될 수 있다 — 일반 `=` 비교였다면 NULL 컬럼이 있는 행은 영원히 매칭되지 않았을 것이다.
- `WHEN MATCHED AND target.mno IS NULL THEN UPDATE`이므로, 이미 `mno`가 채워진 행은 재실행해도 덮어쓰지 않는다 — 반복 실행에 안전(idempotent)하다.
- `expire_snapshots`는 Iceberg의 copy-on-write 특성상 이전 스냅샷(옛 파일)을 완전히 제거하므로, 실행 후에는 백필 이전 시점으로 롤백할 방법이 없다(`retain_last=1`로 최신 스냅샷 하나만 남김) — 되돌릴 수 없는 파괴적 작업이므로 백필 결과를 검증(6번 블록의 로그)한 뒤 실행 여부를 판단해야 한다.
- Bronze 재정제 시 `mno`가 비어있거나 공백 문자열인 행은 소스에서 제외되므로, 애초에 API 응답에 지번 정보가 없는 거래는 이 백필로도 채워지지 않는다.

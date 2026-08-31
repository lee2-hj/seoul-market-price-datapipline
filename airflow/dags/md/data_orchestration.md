# 📄 data_orchestration.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 서울시 아파트 매매 실거래가 원본(Bronze) 수집부터 Silver(Iceberg) 정제, Gold 레이어 데이터 마트 빌드, Elasticsearch 색인까지 이어지는 전체 파이프라인을 매일 자동 실행하는 Airflow TaskFlow DAG(`data_orchestration`)이다. 계약일(CTRT_DAY) 기준으로 매일 "오늘부터 최근 90일 전까지"를 다시 확인해, 서울 열린데이터광장 API의 뒤늦은 신고/정정 반영을 놓치지 않도록 설계됐다. Silver 변환 이후 모든 Gold 마트 빌드 태스크(`main_mart` → `apt_summary_mart` → 동/평형대 계열 4종 → apt_name Elasticsearch 색인 → `apt_rtt_mart` → `apt_mkt_trends_mart`)를 하나의 선형 체인으로 묶어 순차 실행시킨다.
- **현재 구현 상태:** 완료(운영 배포 반영 수준). 다만 `GOLD_MART_POOL`("gold_mart_serial_pool", slots=1)은 DAG 코드가 자동 생성하지 않고 배포 시 `airflow pools set` 명령을 수동으로 한 번 실행해야 하며(헤더 주석 및 코드 주석에 명시), Pool이 없으면 관련 태스크는 실패하지 않고 무기한 대기 상태(큐잉)로 멈춘다는 운영상 함정이 있다. 이는 TODO라기보다 "배포 절차 문서화 필요"에 해당하는 항목이다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:**
  - `ingestion.Real_Estate` (`fetch_real_estate_recent`, `upsert_real_estate`) — 실거래가 원본 수집 및 DuckDB 테이블 upsert
  - `transformation.gold.pipeline_apt_name` (`run_apt_name_es_pipeline`) — apt_name Elasticsearch 색인 파이프라인
  - `airflow/config/paths.toml` (경로 `AIRFLOW_PATHS_CONFIG` 환경변수, 기본값 `/opt/airflow/config/paths.toml`) — Silver/Gold 각 스크립트의 절대경로 및 `.env` 경로 설정을 담은 외부 설정 파일(코드 대신 여기서 로드)
- **외부 패키지:**
  - `airflow.decorators` (`dag`, `task`) — TaskFlow API 기반 DAG/Task 정의
  - `pendulum` — DAG `start_date` 및 각 태스크의 `base_date`/`as_of_date`(Asia/Seoul) 계산
  - `python-dotenv` (`load_dotenv`) — `paths.toml`의 `[env].dotenv_path`에 지정된 프로젝트 공통 `.env` 로드
  - `tomllib` (표준 라이브러리, Python 3.11+) — `paths.toml` 파싱
  - `subprocess`, `threading`, `signal`, `os`, `logging` (표준 라이브러리) — PySpark/DuckDB 서브프로세스를 스트리밍 로그와 함께 실행하고, SIGTERM 시 자식 프로세스 그룹 전체를 강제 종료하기 위한 저수준 제어

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `data_orchestration` (DAG) | 서울 아파트 실거래가 수집·Silver·Gold 전체 오케스트레이션 | `schedule="0 0 * * *"`(매일 0시, Asia/Seoul), `catchup=False`, `max_active_runs=1`, `default_args={"retries":1,"retry_delay":5분}` | 태스크 그래프 실행 |
| `_run_streaming_subprocess` | PySpark/DuckDB 서브프로세스를 `Popen`+PIPE로 실행하며 stdout/stderr를 실시간 로깅. `MALLOC_ARENA_MAX=1`/`PYTHONUNBUFFERED=1` 주입, `setsid`로 새 프로세스 그룹 분리, SIGTERM 수신 시 `os.killpg`로 자식 그룹 전체 강제 종료, 선택적으로 `RLIMIT_AS` 메모리 상한 적용 | `cmd: list[str]`, `log_label: str`, `extra_env: dict\|None`, `memory_limit_mb: int\|None` | 반환값 없음(`None`); `returncode != 0`이면 `subprocess.CalledProcessError` 발생 |
| `task_fetch_real_estate` (Task) | 계약일 기준 오늘부터 90일 전까지 하루씩 재조회, 변경분만 parquet 재저장 | 없음(내부에서 `REAL_ESTATE_FETCH_LOOKBACK_DAYS=90` 사용) | `list[str]`(변경된 계약일 YYYYMMDD 목록) → `task_upsert_real_estate`, `task_transform_silver_real_estate`로 전달 |
| `task_upsert_real_estate` (Task) | 변경된 계약일만 DuckDB 테이블(`real_estate`)에서 기존 행 삭제 후 재삽입(교체) | `changed_ctrt_days: list[str]` | `str`(테이블명); Pool 미적용 |
| `task_transform_silver_real_estate` (Task, Pool 적용) | `Real_Estate_Transform.py`(PySpark)를 서브프로세스로 실행해 Iceberg Silver 레이어 적재. 변경 계약일이 없으면 스킵 | `changed_ctrt_days: list[str]`, `pool=GOLD_MART_POOL, pool_slots=1` | `None` → `task_build_gold_main_mart` |
| `task_build_gold_main_mart` (Task, Pool 적용) | `main_mart.py` 실행 — [자치구x법정동x단지x거래일x면적] 원자적 Gold 마트(`dm_main`) 빌드, 카카오 지오코딩 좌표 부여 | `dummy_input: None`(의존성 강제용), `pool=GOLD_MART_POOL` | `None` → `task_build_gold_apt_summary_mart` |
| `task_build_gold_apt_summary_mart` (Task, Pool 적용) | `build_apt_recent_trade_mart.py` 실행 — 아파트별 최근 90일 거래지표 + 건축물대장 기본정보 마트 빌드 | `dummy_input: None`, `pool=GOLD_MART_POOL` | `None` → `task_build_gold_dm_dong_pyeong_price_avg` |
| `task_build_gold_dm_dong_pyeong_price_avg` (Task, Pool 적용) | [동x평형대] 계열 4종 중 1번째, 동 단위 평균가 마트 빌드 | `dummy_input: None`, `pool=GOLD_MART_POOL` | `None` → `task_build_gold_dm_apt_price_avg` |
| `task_build_gold_dm_apt_price_avg` (Task, Pool 적용) | 4종 중 2번째, 단지 전체 평균가 마트 빌드 | `dummy_input: None`, `pool=GOLD_MART_POOL` | `None` → `task_build_gold_dm_apt_pyeong_price` |
| `task_build_gold_dm_apt_pyeong_price` (Task, Pool 적용) | 4종 중 3번째, 단지x평형별 평균가 마트 빌드 | `dummy_input: None`, `pool=GOLD_MART_POOL` | `None` → `task_build_gold_dm_apt_flr_price` |
| `task_build_gold_dm_apt_flr_price` (Task, Pool 적용) | 4종 중 4번째(마지막), 단지x층수별 평균가 마트 빌드 | `dummy_input: None`, `pool=GOLD_MART_POOL` | `None` → `task_index_apt_name_es` |
| `task_index_apt_name_es` (Task) | `run_apt_name_es_pipeline()`을 워커 프로세스 안에서 직접 호출(서브프로세스 아님)해 Elasticsearch `apt_name` 인덱스 색인 | `dummy_input: None`; Pool 미적용 | `dict`(색인 결과, XCom에 실림) → `task_build_gold_apt_rtt_mart` |
| `task_build_gold_apt_rtt_mart` (Task, Pool 적용) | `apt_rtt_mart.py`(Polars+DuckDB) 실행 — 아파트 거래동향 마트 빌드, `DUCKDB_MEMORY_LIMIT=3GB`/`RLIMIT_AS=4608MB` 적용 | `dummy_input: dict\|None`, `pool=GOLD_MART_POOL`, `extra_env=_DUCKDB_ENV`, `memory_limit_mb=4608` | `None` → `task_build_gold_apt_mkt_trends_mart` |
| `task_build_gold_apt_mkt_trends_mart` (Task, Pool 적용, DAG 마지막) | `apt_mkt_trends_mart.py`(Polars+DuckDB) 실행 — 아파트 시장동향 마트 빌드 | `dummy_input: None`, `pool=GOLD_MART_POOL`, 동일 DuckDB 메모리 설정 | `None` (체인 종료) |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** `schedule="0 0 * * *"`(매일 자정, Asia/Seoul), `start_date=2024-01-01`, `catchup=False`, `max_active_runs=1`(동시 실행 시 동일 Iceberg 테이블에 커밋 충돌 방지).
- **Source (Input):** 서울 열린데이터광장 실거래가 API(계약일 단위 조회, `ingestion.Real_Estate.fetch_real_estate_recent` 경유), Bronze parquet 원본(연/월/일 파티션), `paths.toml`에 정의된 각 Silver/Gold 스크립트 절대경로.
- **Target (Output):** DuckDB 테이블 `real_estate`(Bronze→테이블 upsert), Iceberg Silver 레이어(`Real_Estate_Transform.py` 결과), S3 Lake `mart/` 경로 하위의 Gold 마트들(`dm_main`, `apt_summary`, `dm_dong_pyeong_price_avg`, `dm_apt_price_avg`, `dm_apt_pyeong_price`, `dm_apt_flr_price`, `apt_rtt`, `apt_mkt_trends`), Elasticsearch `apt_name` 인덱스.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `task_fetch_real_estate`: 오늘부터 90일 전까지 계약일별 재조회 → 실제 변경된 계약일만 골라 parquet 재저장, `changed_ctrt_days` 반환.
  2. `task_upsert_real_estate`: 변경 계약일만 DuckDB `real_estate` 테이블에서 DELETE 후 재삽입.
  3. `task_transform_silver_real_estate`: 변경 계약일이 있으면 `Real_Estate_Transform.py`를 콤마 구분 인자로 1회 실행(SparkSession 1회 기동, 여러 날짜 일괄 처리)해 Iceberg Silver 반영. 변경 없으면 스킵.
  4. Silver 완료 후 Gold 마트 8개 태스크가 리턴값(대부분 `None`)을 체인으로 이어받으며 순차 실행 — 예전엔 PySpark 체인과 DuckDB 체인이 두 갈래로 병렬 실행됐으나, 8GB 단일 VM 메모리 제약으로 전부 단일 선형 체인으로 재구성됨.
  5. `_run_streaming_subprocess`가 모든 서브프로세스 실행을 담당: stdout/stderr를 별도 스레드로 동시에 읽어 데드락 방지, SIGTERM 수신 시 자식 프로세스 그룹 전체를 SIGKILL하여 JVM/DuckDB 고아 프로세스 방지.
  6. `task_index_apt_name_es`는 유일하게 서브프로세스가 아니라 같은 워커 프로세스 내에서 함수 직접 호출.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] 계약일 기준 90일 lookback 재조회 및 변경분만 재저장하는 증분 수집 로직
- [x] Silver→Gold 전체를 단일 선형 체인으로 통합해 8GB VM에서 동시 다중 프로세스 실행을 구조적으로 차단
- [x] `gold_mart_serial_pool`(slots=1) 및 서브프로세스별 메모리 상한(`RLIMIT_AS`)으로 3중 메모리 안전장치 구성
- [x] SIGTERM 핸들러로 자식 프로세스 그룹 강제 종료(JVM/DuckDB 고아 프로세스 방지)
- [ ] [TODO] `GOLD_MART_POOL`이 DAG 코드로 자동 생성되지 않는다는 점이 배포 문서/헬스체크에 반영되어 있는지 확인 필요 — Pool 미생성 시 태스크가 실패하지 않고 무기한 대기하므로 장애 감지가 늦어질 수 있다(코드 주석에서 명시된 운영 리스크).
- [ ] [TODO] Gold 마트 태스크 8개가 모두 거의 동일한 보일러플레이트(오늘 날짜 계산 → 스크립트 경로 조회 → `_run_streaming_subprocess` 호출 → 로깅)를 반복하고 있어, 공통 헬퍼 함수로 추출하면 유지보수성이 개선될 것으로 보인다(코드 품질 관점 제안, 코드 내 명시된 TODO는 아님).

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- `default_args`에 `retries=1`, `retry_delay=5분`이 전역 설정되어 있어 태스크 실패 시 5분 후 1회 자동 재시도한다.
- `max_active_runs=1`로 동시 DAG 실행을 막는다(Iceberg 동시 커밋 충돌 방지 목적, 코드 주석에 명시).
- Gold 마트 관련 태스크는 전부 `gold_mart_serial_pool`(slot=1)을 공유해 물리적으로 한 번에 하나만 실행되도록 강제된다 — 이 Pool은 배포 시 `airflow pools set gold_mart_serial_pool 1 ...` 명령으로 수동 생성해야 하며 DAG 파싱 시점에 자동 생성되지 않는다.
- `_run_streaming_subprocess`는 `MALLOC_ARENA_MAX=1`(glibc 전용, Linux 컨테이너에서만 의미 있음)과 `PYTHONUNBUFFERED=1`을 자식 프로세스 환경에 주입해 메모리 부풀림과 로그 유실을 방지한다.
- `memory_limit_mb`(RLIMIT_AS)는 PySpark(JVM) 프로세스에는 절대 적용하지 않는다 — JVM은 실제 RSS보다 훨씬 큰 가상 주소 공간을 예약해두는 특성이 있어 기동 자체가 실패할 수 있다는 점이 코드 주석에 명시되어 있다. DuckDB(`apt_rtt_mart`, `apt_mkt_trends_mart`)에만 3GB 설정의 1.5배(4608MB)로 적용.
- `task_upsert_real_estate`는 단일 PK가 없는 테이블 구조상 계약일 단위로 DELETE 후 전체 재삽입하는 방식이며, 부분 갱신이 아니다.

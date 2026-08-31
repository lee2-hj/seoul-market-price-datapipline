# 📄 Real_Estate.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 서울 열린데이터광장의 부동산(아파트) 실거래가 API(`tbLnOpendataRtmsV`)를 호출해 원본(Bronze) 데이터를 계약일(CTRT_DAY) 기준 연/월/일 파티션으로 MinIO(S3)에 parquet으로 적재하고(`fetch_real_estate`, `fetch_real_estate_recent`), 그 parquet을 DuckDB 테이블로 업서트(계약일 단위 전체 교체)하는(`upsert_real_estate`) Bronze 계층 수집 모듈이다.
- **현재 구현 상태:** 핵심 흐름(페이지네이션 수집 -> 파티션 저장 -> 지문 비교를 통한 중복 방지 -> 테이블 교체 적재)은 동작하도록 구현되어 있으나, 코드 내 명시된 TODO가 두 곳 남아 있어 "완료"로 보기는 이르다. ① `PAGE_SIZE = 1000`이 실제 인증키로 검증된 공식 최대 허용 건수인지 재확인 필요(26번째 줄 주석), ② `FILTER_PARAM_ORDER`에서 `BLDG_NM` 이후 필터 순서가 공식 문서로 재검증되지 않음(44번째 줄 주석). 또한 `upsert_real_estate`가 사용하는 테이블의 명확한 단일/복합 기본키가 아직 확정되지 않아(417번째 줄 주석) 계약일(CTRT_DAY) 단위 DELETE 후 INSERT 방식으로 우회 구현되어 있다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `utils.raw_pipeline`의 `create_session_with_retry`(재시도 HTTP 세션 생성), `ensure_connection`(DuckDB 연결에 S3 설정을 보장) — 두 함수 모두 내부적으로 `utils.connect`(MinIO 접속 설정)와 `config.paths`(.env 로드)에 의존한다.
- **외부 패키지:** `polars`(API 응답 row를 DataFrame으로 변환/미리보기/parquet 저장), `duckdb`(`DuckDBPyConnection` 타입 및 SQL 실행), 표준 라이브러리 `hashlib`(지문 해시), `json`(응답 파싱/정규화), `logging`, `os`(환경변수), `time`(요청 간격/재시도 대기), `datetime`(`date`, `datetime`, `timedelta` — 최근 N일 계산).

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `fetch_real_estate` | probe 요청으로 총 건수 파악 후 PAGE_SIZE 단위로 전체 페이지를 조회하고, CTRT_DAY별로 그룹핑/필터링하여 기존 저장분과 지문이 다른 날짜만 parquet으로 (재)저장 | `con: DuckDBPyConnection\|None`, `service_key: str\|None`(미지정 시 env `KEY`), `ctrt_day_from/ctrt_day_to: str\|None`(YYYYMMDD, 동일값이면 서버 필터 적용) | `dict`(`raw_path`, `checked_ctrt_days`, `changed_ctrt_days`) |
| `fetch_real_estate_recent` | `as_of_date` 기준 `lookback_days`일 동안 하루 단위로 `fetch_real_estate`를 반복 호출해 뒤늦은 신고/정정 반영 여부를 매일 재확인 | `con`, `service_key`, `lookback_days: int = 90`, `as_of_date: date\|None`(기본 오늘) | `dict`(`raw_path`, `checked_ctrt_days`, `changed_ctrt_days` 전체 누적) |
| `upsert_real_estate` | `changed_ctrt_days`에 있는 계약일만 대상으로 테이블을 최초 생성(없으면)하고, 각 날짜의 기존 행을 DELETE 후 Bronze parquet 전체로 INSERT(트랜잭션) | `con`, `table_name: str`, `changed_ctrt_days: list[str]` | `str`(전달받은 `table_name` 그대로 반환) |
| `_fetch_page` / `_build_page_url` / `_parse_real_estate_json` | 페이지 단위 HTTP 호출, URL 조립(경로 세그먼트 방식), 이 API 전용 JSON 응답 파싱(SERVICE_NAME 래퍼 유무 분기) | `service_key`, `start_index`, `end_index`, `filters` 등 | `tuple[int, list[dict]]`(전체건수, row 목록) 또는 예외 |
| `_fingerprint_rows` / `_read_existing_day_rows` | 행 목록을 정규화된 JSON 문자열 정렬 후 SHA-256 해시로 지문화, 기존 저장 parquet을 읽어 dict 리스트로 반환 | `rows: list[dict]` / `con`, `ctrt_day: str` | `str`(해시) / `list[dict]\|None` |
| `_group_rows_by_ctrt_day` / `_filter_ctrt_days` / `_build_page_ranges` / `_build_filter_segments` | 각각 계약일별 그룹핑, 기간 필터링, START/END 인덱스 구간 생성, 필터 딕셔너리를 경로 세그먼트 리스트로 변환 | 내부 헬퍼, 파라미터는 함수명 참고 | 각각 `dict`/`dict`/`list[tuple[int,int]]`/`list[str]` |
| `_create_table_if_not_exists` / `_replace_ctrt_day` | 테이블 생성(모든 컬럼 VARCHAR + created_at, PK 없음), 특정 계약일 행을 DELETE 후 parquet에서 INSERT(BEGIN/COMMIT/ROLLBACK) | `con`, `table_name`, `columns` / `con`, `table_name`, `ctrt_day` | `None` |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** 이 파일 자체는 Airflow DAG가 아니라 순수 Python 함수 모듈이다(스케줄 정의 없음). 실제 스케줄은 이 함수들을 호출하는 오케스트레이션(DAG) 쪽에서 결정되며, 주석상 "데일리 배치는 `ctrt_day_from == ctrt_day_to`로 매일 최근 90일을 재확인"하는 방식으로 쓰임이 명시되어 있다. N/A(이 파일 단독으로는 트리거 없음).
- **Source (Input):** 서울 열린데이터광장 실거래가 API(`http://openapi.seoul.go.kr:8088/{인증키}/json/tbLnOpendataRtmsV/...`), 인증키는 환경변수 `KEY`. `upsert_real_estate`의 입력은 `fetch_real_estate*`가 만든 S3 parquet 경로(`s3://{RAW}/real_estate/year=*/month=*/day=*/real_estate_raw.parquet`).
- **Target (Output):** Bronze parquet — `s3://{RAW 환경변수}/real_estate/year=YYYY/month=MM/day=DD/real_estate_raw.parquet`(계약일별 전체 덮어쓰기). 이후 `upsert_real_estate`가 호출자가 지정한 DuckDB `table_name`에 계약일 단위로 DELETE+INSERT.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `ensure_connection`으로 DuckDB에 S3(MinIO) 설정 보장.
  2. probe 요청(1건)으로 `list_total_count` 확인 후, 이미 받은 건수 다음부터 `PAGE_SIZE`(1000) 단위로 전체 페이지를 순회 조회(`_build_page_ranges`). 페이지 호출은 요청 간 최소 `REQUEST_INTERVAL_SECONDS`(0.5초) 대기, HTTP 레벨 재시도는 `create_session_with_retry` 세션이, 응답의 논리적 오류(RESULT.CODE)는 `_fetch_page` 내부에서 `MAX_PAGE_ATTEMPTS`(3회) 추가 재시도.
  3. 개별 페이지가 재시도 소진 후에도 실패하면 그 구간만 `failed_ranges`에 기록하고 전체 수집은 계속 진행(부분 실패 허용).
  4. 전체 row를 CTRT_DAY(계약일) 기준으로 그룹핑(`_group_rows_by_ctrt_day`) 후 필요시 기간 필터(`_filter_ctrt_days`, 백필용).
  5. 각 계약일마다 기존 저장 parquet과 새 데이터의 지문(SHA-256)을 비교해 동일하면 저장을 건너뛰고(`skipped_ctrt_days`), 다르면 해당 날짜 폴더 전체를 덮어쓴다(`changed_ctrt_days`).
  6. `upsert_real_estate`는 `changed_ctrt_days`만 대상으로 DuckDB 테이블에 반영 — 최초 실행 시 테이블 생성 후, 각 계약일에 대해 트랜잭션으로 기존 행 DELETE, Bronze parquet 전체 INSERT(계약일 단위 원자적 전체 교체 방식, 단일 PK가 없어 채택된 대안 전략).

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] probe + 페이지네이션 기반 전체 데이터 수집 및 부분 실패 허용 로직
- [x] 계약일 기준 지문(fingerprint) 비교로 불필요한 재적재/후속 처리 방지
- [x] 계약일 단위 원자적 교체(DELETE+INSERT, 트랜잭션) 방식의 upsert
- [ ] [TODO] `PAGE_SIZE`(1000)가 실제 API 공식 문서 기준 최대 허용 건수인지 실제 인증키로 재확인(코드 25번째 줄 주석)
- [ ] [TODO] `FILTER_PARAM_ORDER`의 `BLDG_NM` 이후(THING_AMT ~ OPBIZ_RESTAGNT_SGG_NM) 순서가 공식 API 문서와 정확히 일치하는지 재검증(코드 44번째 줄 주석)
- [ ] [TODO] `upsert_real_estate` 대상 테이블의 명확한 단일/복합 기본키 확정 필요(코드 417번째 줄 주석 — 현재는 계약일 단위 전체 교체로 우회)

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 이 API는 쿼리스트링이 아니라 URL 경로 세그먼트로 필터를 전달하며, 뒤쪽 필터를 쓰려면 앞쪽 자리를 반드시 공백문자(" ")로 채워야 한다(빈 문자열 사용 시 "//"가 생겨 API가 파싱 실패).
- CTRT_DAY(계약일)는 위치 기반 파라미터라 범위 조회를 지원하지 않는다 — `ctrt_day_from == ctrt_day_to`일 때만 서버 필터가 적용되고, 그 외에는 전체를 받아 클라이언트 측에서 사후 필터링한다.
- 요청 하나마다 최소 `REQUEST_INTERVAL_SECONDS`(0.5초) 대기해 서버의 DoS 오인 차단을 방지한다.
- `_parse_real_estate_json`은 정상 응답(SERVICE_NAME 래퍼 있음)과 "0건"(SERVICE_NAME 래퍼 없이 최상위 RESULT, CODE=INFO-200) 응답 구조가 다르다는 점을 반드시 구분해야 하며, 이를 혼동하면 정상적인 0건 상황을 에러로 오판할 수 있다.
- 페이지 하나의 최종 실패는 전체 수집을 중단시키지 않고 해당 구간만 누락 처리되므로, 로그(`failed_ranges`)를 통해 데이터 누락 여부를 반드시 확인해야 한다.
- `upsert_real_estate`의 DELETE+INSERT는 하나의 트랜잭션(BEGIN/COMMIT/ROLLBACK)으로 묶여 있어 중간 실패 시 롤백되지만, 단일 기본키가 없는 구조이므로 계약일(CTRT_DAY) 자체를 원자적 교체 단위로 삼는다는 설계 제약이 있다.
- `_default_raw_path`/`_raw_path_for_ctrt_day`는 환경변수 `RAW`(버킷명)에 의존하므로 미설정 시 잘못된 경로(`s3://None/...`)가 생성될 수 있다.

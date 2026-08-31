# 📄 apt_rtt_mart.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 서울시 아파트 거래동향(RTT: Real-estate Trade Trend) Gold 데이터 마트를 생성하는 단독 실행 배치 스크립트. `dim_apartment`/`fact_apt_transactions`(S3 Lake의 Iceberg 원본)를 DuckDB(httpfs)로 직접 조회해 최근 90일 아파트 매매 거래 건별(자치구/법정동/거래일자/층수/거래가/평/거래건수) 데이터를 MinIO의 `mart/RTT/base_date=YYYY-MM-DD` 경로에 Parquet(Snappy)으로 저장한다. `apt_mkt_trends_mart.py`와 거의 동일한 구조(DuckDB+Polars, 브로드캐스트 조인, base_date 파티션 Upsert)를 가지되, 최종 컬럼 구성과 레코드 키 구성이 다르다(RTT는 원본 컬럼명 그대로 출력하며 apt_name/mno/sno/exclusive_area_m2를 키에 사용).
- **현재 구현 상태:** 코드는 완성되어 있으나, docstring에 "이 스크립트는 코드만 작성된 상태이며, 실제로 실행해 MinIO에 데이터를 적재하거나 Airflow DAG에 태스크로 연동하는 작업은 별도로 진행되지 않았다"고 명시되어 있어 **미실행/미연동 상태**로 판단된다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음(완전히 자기완결적인 단일 파일 스크립트).
- **외부 패키지:** `duckdb`(httpfs 확장, S3/MinIO 직접 조회 및 브로드캐스트 조인용 TEMP TABLE), `polars`(pl, 컬럼 파생·Upsert·해시 비교), `python-dotenv`(`load_dotenv`), 표준 라이브러리 `os`, `sys`, `datetime`, `pathlib`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `load_config()` | `env/.env`(로컬) 또는 `/opt/airflow/project/env/.env`(Airflow) 경로 중 존재하는 곳에서 환경변수 로드 후 dict 반환 | 없음 | `dict`(s3_endpoint/s3_access_key/s3_secret_key/lake_bucket) |
| `get_duckdb_connection(config)` | httpfs 확장 로드 + S3(MinIO) 접속 설정 + 메모리/스레드/스필 디렉터리 설정을 적용한 DuckDB 커넥션 생성 | `config: dict` | `duckdb.DuckDBPyConnection` |
| `load_dim_apartment_broadcast(con, lake_bucket)` | `dim_apartment` parquet을 TEMP TABLE `dim_apartment_bc`로 구체화(브로드캐스트 조인 build side) | `con`, `lake_bucket: str` | `None` |
| `fetch_joined_raw(con, lake_bucket, as_of_date, start_date)` | `fact_apt_transactions`를 deal_date_day 파티션 프루닝 + 필터(취소건 제외, 금액/면적>0)로 걸러 `dim_apartment_bc`와 INNER JOIN | `con`, `lake_bucket`, `as_of_date`, `start_date` | `pl.DataFrame` |
| `shape_rtt_columns(raw_df)` | 원본 컬럼명을 유지한 채 평(exclusive_area_m2/3.30578), `trade_count=1`, `base_date`(=deal_date 문자열) 파생 | `raw_df: pl.DataFrame` | `pl.DataFrame` |
| `_with_record_key(df)` | `KEY_COLUMNS`(sgg_cd/dong_cd/apt_name/mno/sno/deal_date/floor/exclusive_area_m2)를 이어붙인 `record_key` 부여 | `df: pl.DataFrame` | `pl.DataFrame` |
| `_row_fingerprint(df)` | record_key 정렬 후 `hash_rows(seed=0)`로 행 지문 리스트 생성 | `df: pl.DataFrame` | `list` |
| `_partition_path(lake_bucket, day_str)` | base_date 파티션 S3 경로 조립 | `lake_bucket`, `day_str` | `str` |
| `read_existing_partition(con, lake_bucket, day_str)` | 기존 파티션을 `hive_partitioning=false`로 읽고 없으면 `None` | `con`, `lake_bucket`, `day_str` | `pl.DataFrame \| None` |
| `write_partition_file(con, lake_bucket, day_str, df)` | 고정 파일명(`data.parquet`)으로 파티션 통째 (재)기록(Parquet/Snappy) | `con`, `lake_bucket`, `day_str`, `df` | `None` |
| `upsert_partition(con, lake_bucket, day_str, new_day_df)` | 파티션 단위 Insert/Update/Skip 처리 | `con`, `lake_bucket`, `day_str`, `new_day_df` | `str`("insert"/"update"/"skip") |
| `main()` | CLI 인자(AS_OF_DATE, 생략 시 오늘)로 조회기간 산정 → 조인/변환 → base_date 그룹별 Upsert 반복 → 스키마/샘플/카운트 로그 | `sys.argv[1]`(선택) | `None` |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** Airflow 연동 미완료(docstring 명시). 현재는 CLI 독립 실행(`python src/transformation/gold/apt_rtt_mart.py [AS_OF_DATE]`).
- **Source (Input):** `s3://{LAKE}/dim_apartment/data/*.parquet`, `s3://{LAKE}/fact_apt_transactions/data/**/*.parquet` (DuckDB httpfs로 직접 조회). 접속 정보는 `env/.env` 또는 OS 환경변수.
- **Target (Output):** `s3://{LAKE}/mart/RTT/base_date=YYYY-MM-DD/data.parquet` (Parquet, Snappy). base_date는 실행일이 아닌 각 행의 실제 거래일자(deal_date).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `main()`이 CLI 인자(또는 오늘)로 `as_of_date`/`start_date`(90일 전) 산정.
  2. `load_config()` → `get_duckdb_connection()`으로 MinIO 접속 DuckDB 커넥션 준비.
  3. `load_dim_apartment_broadcast()`로 dim_apartment TEMP TABLE 구체화.
  4. `fetch_joined_raw()`가 predicate/projection pushdown 서브쿼리로 fact_apt_transactions를 필터링(취소건 제외, 금액/면적 양수) 후 (sgg_cd, dong_cd, apt_name) 기준 INNER JOIN.
  5. `shape_rtt_columns()`가 Polars로 pyeong/trade_count/base_date를 파생(RTT는 컬럼명을 리네이밍하지 않고 sgg_cd/dong_cd 등 원본명을 그대로 유지 — apt_mkt_trends_mart.py와의 주요 차이점).
  6. `mart_df.partition_by("base_date")`로 거래일자별 그룹을 나누고 각 그룹을 `upsert_partition()`으로 Insert/Update/Skip 처리(KEY_COLUMNS 기준 record_key + row fingerprint 비교).
  7. 처리 건수/최종 스키마/샘플 20건/총 레코드 수를 로그로 출력 후 커넥션 종료.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] DuckDB Broadcast Join 기반 조회(predicate/projection pushdown 최적화)
- [x] base_date(거래일자) 파티션 단위 Insert/Update/Skip 멱등적 Upsert(KEY_COLUMNS + row fingerprint)
- [x] OOM(SIGKILL) 대응 DuckDB memory_limit/temp_directory 명시적 설정
- [ ] [TODO] docstring에 명시된 대로 실제 실행/데이터 적재 검증과 Airflow DAG 연동이 아직 되어 있지 않다 - 운영 반영 전 통합 테스트 및 DAG 등록 필요.
- [ ] [TODO] `apt_mkt_trends_mart.py`와 코드 구조(로드/커넥션/브로드캐스트/파티션 Upsert 로직)가 거의 동일하게 중복되어 있다 - 공통 유틸 모듈로 추출하면 유지보수 시 두 파일을 매번 동시에 수정해야 하는 부담을 줄일 수 있어 보인다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- `KEY_COLUMNS`에 거래가(trade_amount에 해당하는 price_ten_thousand)를 포함하지 않는다 - 가격 정정 시 "새 거래"로 오판되어 옛 레코드가 지워지지 않고 누적되는 문제가 로컬 테스트에서 실제로 재현되었다고 명시.
- `read_existing_partition()`은 `hive_partitioning=false`로 읽어야 한다 - 그렇지 않으면 경로의 `base_date=` 문자열이 Hive 파티션 컬럼으로 자동 인식되어 신규 데이터와 컬럼 수가 어긋나 병합/지문 비교가 깨진다.
- DuckDB `memory_limit`을 명시하지 않으면 호스트 전체 메모리 기준 기본 한도가 잡혀 OOM(SIGKILL)으로 죽을 수 있다(Airflow에서 실제 재현 이력 있음) - 기본값 3GB.
- fact_apt_transactions에는 접수번호 같은 단일 PK가 없어 (단지+지번+거래일자+층+전용면적) 조합을 레코드 고유 키로 사용한다.
- 조회기간 내 거래가 0건인 날짜는 기존 파티션이 있어도 삭제하지 않는다(보수적 동작).
- 파일명을 고정값(`data.parquet`)으로 둬 같은 base_date를 여러 번 갱신해도 파일이 누적되지 않는다.

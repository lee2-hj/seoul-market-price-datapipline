# 📄 apt_mkt_trends_mart.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 서울시 아파트 시장 동향(MKT_TRENDS) Gold 데이터 마트를 생성하는 단독 실행 배치 스크립트. `dim_apartment`(차원)와 `fact_apt_transactions`(사실 테이블, S3 Lake에 Iceberg로 적재)를 DuckDB(httpfs)로 직접 조회해, 최근 90일 아파트 매매 거래를 자치구/법정동/지번(mno·sno) 단위 건별 레코드(자치구코드·자치구명·법정동코드·법정동명·아파트명·mno·sno·계약일자·층수·거래건수·거래금액·평·평단가)로 정리한 뒤 MinIO(S3 Lake)의 `mart/apt_mkt_trends/base_date=YYYY-MM-DD/data.parquet` 경로에 Parquet(Snappy)으로 저장한다. PySpark 없이 Polars + DuckDB만으로 동작하며(건별 트랜잭션 결과라 대규모 셔플이 불필요), base_date(=계약일자) 파티션 단위로 Insert/Update/Skip을 판별하는 멱등적 Upsert 로직을 자체 구현하고 있다.
- **현재 구현 상태:** 코드 자체는 완성된 형태이나, 모듈 docstring에 "이 스크립트는 코드만 작성된 상태이며, 실제로 실행해 MinIO에 데이터를 적재하거나 Airflow DAG에 태스크로 연동하는 작업은 별도로 진행되지 않았다"라고 명시되어 있어 **실행/운영 연동 전 단계(미검증)** 로 판단된다. 별도의 TODO/FIXME 주석은 없다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음(같은 폴더의 `apt_rtt_mart.py`와 동일한 패턴을 그대로 재사용하고 있으나 실제 import 관계는 아니며, 완전히 자기완결적인 단일 파일 스크립트).
- **외부 패키지:** `duckdb`(httpfs 확장으로 S3/MinIO 직접 조회, 브로드캐스트 조인용 TEMP TABLE), `polars`(pl, 컬럼 파생·Upsert·해시 비교), `python-dotenv`(`load_dotenv`, 환경변수 로드), 표준 라이브러리 `os`, `sys`, `datetime`, `pathlib`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `load_config()` | `env/.env`(로컬) 또는 `/opt/airflow/project/env/.env`(Airflow 컨테이너) 중 존재하는 경로를 로드하고 S3 접속 정보를 dict로 반환 | 없음 | `dict`(s3_endpoint/s3_access_key/s3_secret_key/lake_bucket) |
| `get_duckdb_connection(config)` | httpfs 확장 설치 후 S3(MinIO) 접속 설정 및 메모리 한도(`DUCKDB_MEMORY_LIMIT` 기본 3GB)/스레드/스필 디렉터리를 적용한 DuckDB 커넥션 생성 | `config: dict` | `duckdb.DuckDBPyConnection` |
| `load_dim_apartment_broadcast(con, lake_bucket)` | `dim_apartment` parquet을 TEMP TABLE `dim_apartment_bc`로 구체화(브로드캐스트 조인의 build side) | `con`, `lake_bucket: str` | `None` (테이블 부작용) |
| `fetch_joined_raw(con, lake_bucket, as_of_date, start_date)` | `fact_apt_transactions`를 deal_date_day 파티션 프루닝 + 필터(취소건 제외, 금액/면적>0, mno 존재)로 걸러 `dim_apartment_bc`와 INNER JOIN | `con`, `lake_bucket`, `as_of_date`, `start_date` | `pl.DataFrame`(조인 결과 원본) |
| `shape_mkt_trends_columns(raw_df)` | 컬럼 리네이밍(cgg_cd/cgg_nm/stdg_cd/stdg_nm), 평(전용면적/3.30578) 및 평단가 파생, `trade_count=1` 부여, `base_date`(=deal_date 문자열) 생성 | `raw_df: pl.DataFrame` | `pl.DataFrame`(최종 스키마 + base_date) |
| `drop_technical_columns(df)` | 저장 스키마에 남기지 않을 `base_date`/`record_key` 컬럼 제거(존재하는 것만 안전하게 drop) | `df: pl.DataFrame` | `pl.DataFrame` |
| `_with_record_key(df)` | `RECORD_KEY_COLUMNS`(cgg_cd/stdg_cd/mno/sno/deal_date/floor/pyeong)를 `\|`로 이어붙인 `record_key` 컬럼 부여(Null은 빈 문자열 처리) | `df: pl.DataFrame` | `pl.DataFrame` |
| `_row_fingerprint(df)` | record_key 기준 정렬 후 `hash_rows(seed=0)`로 행 지문 리스트 생성(내용 동일성 비교용) | `df: pl.DataFrame` | `list` |
| `_partition_path(lake_bucket, day_str)` | base_date 파티션의 S3 저장 경로 문자열 조립 | `lake_bucket`, `day_str` | `str` |
| `read_existing_partition(con, lake_bucket, day_str)` | 기존 base_date 파티션을 `hive_partitioning=false`로 읽어옴. 없으면 예외를 잡아 `None` 반환 | `con`, `lake_bucket`, `day_str` | `pl.DataFrame \| None` |
| `write_partition_file(con, lake_bucket, day_str, df)` | 지정 파티션 경로에 고정 파일명(`data.parquet`)으로 Parquet(Snappy) 통째로 (재)기록 | `con`, `lake_bucket`, `day_str`, `df` | `None` |
| `upsert_partition(con, lake_bucket, day_str, new_day_df)` | 파티션 단위 Insert/Update/Skip 판별 및 실행(신규 없음→Insert, 내용 동일→Skip, 그 외→record_key 기준 병합 Update) | `con`, `lake_bucket`, `day_str`, `new_day_df: pl.DataFrame` | `str`("insert"/"update"/"skip") |
| `main()` | CLI 인자(AS_OF_DATE, 생략 시 오늘)로 조회기간 산정 → 설정/커넥션 준비 → 조인/변환 → base_date별 그룹 Upsert 반복 → 스키마/샘플/카운트 로그 출력 | `sys.argv[1]`(선택, YYYY-MM-DD) | `None`(표준출력 로그, S3 부작용) |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** Airflow DAG 연동은 아직 이루어지지 않은 상태(docstring 명시). 현재는 CLI로 직접 실행하는 독립 배치 스크립트(`python src/transformation/gold/apt_mkt_trends_mart.py [AS_OF_DATE]`).
- **Source (Input):** S3 Lake(MinIO)의 `s3://{LAKE}/dim_apartment/data/*.parquet`와 `s3://{LAKE}/fact_apt_transactions/data/**/*.parquet`(Iceberg 테이블 원본 파일을 DuckDB httpfs로 직접 글롭 조회). 접속 정보는 `env/.env` 또는 OS 환경변수(`S3_END_POINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `LAKE`).
- **Target (Output):** `s3://{LAKE}/mart/apt_mkt_trends/base_date=YYYY-MM-DD/data.parquet` (Parquet, Snappy 압축). base_date는 실행일이 아니라 각 거래 행의 실제 계약일자(deal_date).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `main()`이 CLI 인자(또는 오늘)로 `as_of_date`를 정하고 `LOOKBACK_DAYS=90`일 전을 `start_date`로 계산.
  2. `load_config()` → `get_duckdb_connection()`으로 MinIO 접속 DuckDB 커넥션 생성(SSL 여부는 endpoint 스킴으로 자동 판별).
  3. `load_dim_apartment_broadcast()`로 `dim_apartment`를 TEMP TABLE로 구체화(브로드캐스트 조인 build side).
  4. `fetch_joined_raw()`가 predicate/projection pushdown이 적용된 서브쿼리로 `fact_apt_transactions`를 필터링(최근 90일, 취소건 제외, 금액/면적 양수, mno 존재)한 뒤 `dim_apartment_bc`와 (sgg_cd, dong_cd, apt_name) 기준 INNER JOIN.
  5. `shape_mkt_trends_columns()`가 Polars로 최종 컬럼(cgg_cd/cgg_nm/stdg_cd/stdg_nm/apt_name/mno/sno/deal_date/floor/trade_amount/pyeong/trade_count/pyeong_amt)과 `base_date`를 파생.
  6. `mart_df.partition_by("base_date")`로 계약일자별 그룹을 나누고, 각 그룹에 대해 `upsert_partition()`을 호출해 기존 파티션과 비교(`_with_record_key`+`_row_fingerprint`)해 Insert/Update/Skip 중 하나로 처리.
  7. 처리 건수(Insert/Update/Skip)와 최종 스키마·샘플 20건·총 레코드 수를 로그로 출력 후 커넥션 종료.
- **아파트 특정 조인 키 설계:** 물리적 조인은 dim_apartment/fact가 공유하는 (sgg_cd, dong_cd, apt_name)으로 수행하지만, 비즈니스 상 "아파트(필지)를 특정"하는 키는 (cgg_cd, stdg_cd, mno, sno) 4개 컬럼이며 `RECORD_KEY_COLUMNS`(=APT_IDENTITY_COLUMNS + deal_date/floor/pyeong)의 기반이 된다. 거래금액은 가격 정정 시 오판(중복 누적)을 막기 위해 의도적으로 키에서 제외.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] DuckDB + Polars 기반 Broadcast Join(브로드캐스트 TEMP TABLE) 및 partition/projection pushdown 최적화 조회
- [x] base_date(계약일자) 파티션 단위 Insert/Update/Skip 멱등적 Upsert 로직(record_key + row fingerprint 비교)
- [x] OOM(SIGKILL) 대응을 위한 DuckDB memory_limit/temp_directory 명시적 설정
- [ ] [TODO] docstring에 명시된 대로 아직 실제 실행/데이터 적재 검증과 Airflow DAG 태스크 연동이 되어 있지 않으므로, 운영 반영 전 별도 통합 테스트 및 DAG 등록 작업이 필요하다.
- [ ] [TODO] `read_existing_partition()`/DuckDB 쿼리 실행에서 예외를 `except Exception: return None`으로 광범위하게 처리하고 있어, 파티션 부재(404)가 아닌 다른 종류의 오류(권한, 네트워크 등)도 동일하게 "파티션 없음"으로 오인될 위험이 있다 - 예외 유형을 좁혀 구분하는 것이 안전해 보인다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- `RECORD_KEY_COLUMNS`에 거래금액(trade_amount)을 포함하지 않는다 - 포함 시 가격 정정 케이스가 "새 거래"로 오판되어 옛 레코드가 누적되는 문제가 실제로 재현된 바 있다(apt_rtt_mart.py에서 동일 문제 확인).
- `read_existing_partition()`은 반드시 `hive_partitioning=false`로 읽어야 한다 - 그렇지 않으면 경로의 `base_date=` 문자열을 DuckDB가 Hive 파티션 컬럼으로 자동 인식해 저장 스키마에 없는 컬럼이 되살아나며, 신규 수집 데이터와 컬럼 수가 어긋나 병합/지문 비교가 깨진다.
- DuckDB `memory_limit`을 명시하지 않으면 컨테이너 cgroup이 아닌 호스트 전체 물리 메모리 기준으로 기본 한도를 잡아 OOM(SIGKILL)으로 죽을 수 있다 - 기본값 3GB(GCP e2-standard-2, Gold 마트 순차 실행 전제)로 설정.
- `mno IS NOT NULL AND TRIM(mno) <> ''` 필터로 지번이 없는 거래는 애초에 대상에서 제외된다(아파트 특정 조인 키의 핵심 컬럼이기 때문).
- 조회기간 내 거래가 0건인 날짜(base_date)는 기존 파티션이 있어도 삭제하지 않고 그대로 둔다(보수적 동작) - 취소된 거래가 사라지는 것은 정상 시나리오로 간주.
- 파일명을 고정값(`data.parquet`)으로 두어 같은 base_date를 여러 번 Update해도 파일이 누적되지 않고 교체된다.

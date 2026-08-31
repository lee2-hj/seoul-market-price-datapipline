# 📄 dong_pyeong_common.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** [동 x 평형대] 계열 Gold 마트 4종(`dm_dong_pyeong_price_avg.py`, `dm_apt_price_avg.py`, `dm_apt_pyeong_price.py`, `dm_apt_flr_price.py`)이 공통으로 사용하는 준비 로직 모듈. 환경변수 로드/SparkSession 기동 → Silver(`dim_apartment`, `fact_apt_transactions`) 로딩 → 1차 정제(취소건/0원/0㎡ 제외) → `dim_apartment` 브로드캐스트 조인 → Bronze 지번(MNO/SNO) 보조 조회 → 카카오맵 지오코딩(4단계 Fallback) → 평형대(`pyeong_grp`)/층수그룹(`flr_grp`)/공급평수/평당가 파생 컬럼 계산까지 끝낸 `common_df`를 담은 `GoldMartContext`를 생성해 각 마트 스크립트에 제공한다. 자체적으로는 실행 진입점이 없는 라이브러리 모듈이다.
- **현재 구현 상태:** 대체로 완료. 각 처리 단계마다 과거 겪었던 실제 장애(예: `FileStreamSink.hasMetadata`의 S3A FileNotFoundException, Windows에서 Python 워커 콜백 실패, Spark SQL의 작은따옴표 이스케이프 버그, VOID 타입 컬럼으로 인한 Parquet 저장 실패)에 대한 회피 로직과 상세한 주석이 남아 있어 성숙도가 높다. 다만 카카오맵 API 호출이 순차 for-loop(비동시성)로 이루어지고, 지오코딩 실패 시 좌표가 `None`으로 남는 등 일부 구조적 한계가 있다(6절 참고). 명시적 TODO/FIXME 주석은 없음.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음(다른 프로젝트 내부 모듈을 import하지 않는 최하위 공통 유틸 모듈). 이 파일 자체가 4개 `dm_*.py` 마트 스크립트에 의해 `from transformation.gold.dong_pyeong_common import ...` 형태로 임포트된다.
- **외부 패키지:** `requests`(카카오맵 REST API 호출), `requests.adapters.HTTPAdapter` + `urllib3.util.retry.Retry`(429/5xx 재시도), `python-dotenv`(`load_dotenv`), `pyspark.sql`(`DataFrame`, `SparkSession`, `functions as F`, `broadcast`), `pyspark.sql.types`(`BooleanType`, `DoubleType`, `StringType`, `StructField`, `StructType`), `pyspark.sql.window.Window`. 표준 라이브러리로 `os`, `re`, `dataclasses.dataclass`, `datetime`(`date`, `datetime`, `timedelta`), `pathlib.Path`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `GoldMartContext` (dataclass) | 4개 마트 스크립트가 공유하는 컨텍스트 컨테이너. `spark`, `common_df`, `mart_paths`(마트명→S3 경로 dict), `base_date_str`, `run_timestamp`를 담는다. | 필드: `spark: SparkSession`, `common_df: DataFrame`, `mart_paths: dict[str, str]`, `base_date_str: str`, `run_timestamp: datetime` | N/A(데이터 클래스) |
| `apt_select_cols(ctx)` | 마트 ②~④(단지 단위) 공통 select 컬럼 리스트(`base_date`, `cgg_cd`, `cgg_nm`, `stdg_cd`, `stdg_nm`, `bldg_nm`, `latitude`, `longitude`, `is_exact_location`, `mno`, `sno`, `updated_at`)를 생성. | `ctx: GoldMartContext` | `list`(Column 표현식 리스트) |
| `_load_env()` | `<project_root>/env/.env`와 `/opt/airflow/project/env/.env` 두 경로를 순서대로 시도해 존재하는 첫 파일을 `override=False`로 로드. | 없음 | `None`(부수효과: 환경변수 설정) |
| `_create_spark_session(lake_bucket, s3_access_key, s3_secret_key, s3_end_point)` | Iceberg+S3A 설정이 적용된 `SparkSession`을 생성. `S3_END_POINT`의 스킴(`https://` 유무)으로 SSL 사용 여부 자동 판별. 메모리 설정(driver.memory=3g 등)은 8GB VM 전제로 보수적으로 고정, 환경변수로 오버라이드 가능. | `lake_bucket, s3_access_key, s3_secret_key, s3_end_point: str \| None` | `SparkSession` |
| `_bronze_month_paths(raw_bucket, start, end)` | 조회기간에 해당하는 Bronze `year=/month=/day=*/*.parquet` 경로 리스트를 월 단위로 생성. | `raw_bucket: str`, `start, end: date` | `list[str]` |
| `_build_jibun_lookup(spark, raw_bucket, start_date, base_date)` | Bronze 원본에서 (자치구+법정동+단지명)별 최빈 (MNO, SNO) 조합을 대표 지번으로 뽑아 dict로 반환. `raw_bucket` 없거나 파티션이 없으면 빈 dict(best-effort 폴백). | `spark: SparkSession`, `raw_bucket: str \| None`, `start_date, base_date: date` | `dict[tuple, tuple]` (키: `(sgg_cd, dong_cd, apt_name)`, 값: `(mno, sno)`) |
| `_clean_bldg_nm(name)` | 단지명을 카카오 검색어로 정제(로마자→숫자 변환, 괄호 제거, 특수문자 공백화, 말미 '아파트' 제거, 공백 정리). | `name: str \| None` | `str` |
| `_format_jibun(mno, sno)` | `MNO`/`SNO` 원본 문자열을 `"22"` 또는 `"22-1"` 형태로 변환, 본번 없거나 0이면 `None`. | `mno, sno` | `str \| None` |
| `_is_apartment_category(doc)` | 카카오 검색 결과 문서가 아파트 카테고리(`PM9` 또는 카테고리명에 "아파트" 포함)인지 판별. | `doc: dict` | `bool` |
| `_address_matches_dong(doc, sgg_nm, dong_nm)` | 문서 주소에 구/동 이름이 모두 포함되는지 확인. | `doc, sgg_nm, dong_nm` | `bool` |
| `_first_verified_match(docs, sgg_nm, dong_nm)` | 주소+카테고리 검증을 통과하는 첫 문서 반환. | `docs: list[dict], sgg_nm, dong_nm` | `dict \| None` |
| `_geocode_apartments(distinct_apt_rows, jibun_lookup, kakao_api_key)` | 고유 단지 목록을 순회하며 4단계 Fallback(지번+단지명 키워드 → 지번 주소검색 → 단지명 키워드 → 법정동 대표좌표)으로 지오코딩. 메모리 캐시로 동일 단지 재요청 차단. | `distinct_apt_rows`(Row 리스트), `jibun_lookup: dict`, `kakao_api_key: str \| None` | `list[tuple]` (`sgg_cd, dong_cd, apt_name, latitude, longitude, is_exact_location, mno, sno`) |
| `_sql_literal(value)` | Python 값을 Spark SQL `VALUES` 절 리터럴 문자열로 변환(작은따옴표는 백슬래시 이스케이프, float은 `D` 접미사). | `value` | `str` |
| `_geocode_rows_to_df(spark, geocode_rows)` | 지오코딩 결과 튜플 리스트를 `spark.createDataFrame` 대신 순수 Spark SQL `VALUES` 절로 DataFrame화(Windows Python 워커 콜백 문제 회피), `_GEOCODE_SCHEMA`로 명시 캐스팅(VOID 타입 방지). | `spark: SparkSession, geocode_rows: list[tuple]` | `DataFrame` |
| `build_gold_mart_context(base_date)` | 전체 준비 단계를 조합하는 메인 함수. 환경변수 로드 → SparkSession 생성 → Silver 읽기(파티션 프루닝 적용) → 1차 정제 → 브로드캐스트 조인 → 지번 조회 → 지오코딩 → 좌표 조인 → 평형대/층수그룹/공급평수/평당가 파생 → `common_df.cache()` + `count()` 실행. | `base_date: date` | `GoldMartContext` |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** N/A - 이 파일은 Airflow DAG나 CLI 진입점이 없는 순수 라이브러리 모듈이며, `build_gold_mart_context()`가 각 `dm_*.py` 마트 스크립트에서 호출될 때만 동작한다.
- **Source (Input):** Iceberg 카탈로그 `lakehouse.dim_apartment`(구/동/단지명 차원), `lakehouse.fact_apt_transactions`(`deal_date` 기준 최근 `LOOKBACK_DAYS`(90)일 파티션 프루닝, 거래정보), Bronze 원본 `s3a://{RAW}/real_estate/year=/month=/day=*/*.parquet`(지번 MNO/SNO 보조 조회용), 카카오맵 REST API(`keyword.json`, `address.json`), 환경변수(`env/.env` 또는 `/opt/airflow/project/env/.env`: `S3_END_POINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `LAKE`, `RAW`, `KAKAO_MAP_REST_API_KEY`, 각종 `SPARK_*` 오버라이드).
- **Target (Output):** 직접 저장하지 않음. `GoldMartContext.mart_paths`에 4개 마트별 S3 경로(`s3a://{LAKE}/mart/{마트명}/base_date=YYYY-MM-DD`)만 사전 계산해 반환하며, 실제 Parquet 쓰기는 각 `dm_*.py` 스크립트가 수행한다.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `_load_env()`로 환경변수 로드, 필수 값(`S3_END_POINT` 등) 조회. `KAKAO_MAP_REST_API_KEY` 없으면 경고 로그 후 좌표 전부 NULL로 진행.
  2. `_create_spark_session()`으로 Iceberg+S3A 설정된 SparkSession 생성(로컬 MinIO/HTTP와 배포 HTTPS 엔드포인트 양쪽 지원, 8GB VM 기준 메모리 보수적 설정).
  3. `dim_apartment`(브로드캐스트 대상)와 `fact_apt_transactions`(`deal_date` 범위로 파티션 프루닝, 필요 컬럼만 select) 로딩.
  4. 취소건(`cancel_date` 존재) 및 금액/면적 0 이하 레코드 제외(1차 정제).
  5. `dim_apartment`를 `broadcast()`로 조인(Shuffle 최소화), 단지명/동명 `trim()` 처리 → `joined_df.cache()`(이후 distinct collect와 집계에서 재사용).
  6. `_build_jibun_lookup()`으로 Bronze에서 (자치구+법정동+단지명)별 최빈 (MNO, SNO) 조회 테이블 생성(RAW 버킷 없으면 빈 dict, best-effort).
  7. `joined_df`에서 고유 단지 목록을 `distinct().collect()`로 드라이버에 수집 → `_geocode_apartments()`로 카카오맵 4단계 Fallback 지오코딩 실행(단지당 최대 1회 API 호출, 메모리 캐시로 중복 차단).
  8. 지오코딩 결과를 `_geocode_rows_to_df()`로 DataFrame화(Spark SQL VALUES 절 방식) → `broadcast()` left join으로 `joined_df`에 좌표/정확도/지번 병합.
  9. `pyeong_grp`(전용면적 기준 10/20/30/40+ 평형대 구간), `flr_grp`(층수 LOW/MID/HIGH), `supply_pyeong`(전용면적×1.3/3.30578), `pyeong_amt`(거래금액/공급평수) 파생 컬럼 계산 → `common_df.cache()` + `count()`로 즉시 실행(다음 단계에서 재사용).
  10. `GoldMartContext`(spark, common_df, mart_paths, base_date_str, run_timestamp)를 반환.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] Silver 읽기/조인/1차 정제/카카오맵 4단계 지오코딩/지번 보조 조회/평형대·층수그룹·평당가 파생 컬럼까지 포함한 공통 준비 로직 완성.
- [x] Windows 로컬 개발 환경에서 `spark.createDataFrame(python_list, schema)`가 Python 워커 콜백 실패로 죽는 문제를 Spark SQL `VALUES` 절 방식으로 우회(`_geocode_rows_to_df`).
- [x] Spark SQL 파서가 `''`(작은따옴표 두 번)를 이스케이프로 인식하지 못해 아포스트로피가 포함된 단지명(예: "역삼I'PARK")이 깨지는 버그를 백슬래시 이스케이프(`_sql_literal`)로 수정.
- [x] 지오코딩 결과가 전부 NULL일 때 컬럼 타입이 VOID로 추론되어 Parquet 저장이 실패하는 문제를 `_GEOCODE_SCHEMA` 명시 캐스팅으로 해결.
- [ ] [TODO] 명시적 TODO 주석은 없음. `_geocode_apartments()`가 고유 단지 목록을 순차 for-loop로 하나씩 호출하는 구조라(동시성 없음), 단지 수가 매우 많아지면 배치 실행 시간이 선형으로 늘어날 수 있다 - 추후 비동기/병렬 호출 또는 배치 API 검토 여지가 있다(코드 품질 개선 제안).
- [ ] [TODO] `distinct_apt_rows`를 `.collect()`로 드라이버 메모리에 전부 적재하므로, 고유 단지 수가 매우 커질 경우 드라이버 메모리 부담이 될 수 있다(현재 3g로 보수적 설정된 것과 상충 가능성) - 규모 커질 때 재검토 필요.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 4개 `dm_*.py` 마트 스크립트는 이 모듈의 `build_gold_mart_context()`를 각자 독립적으로 호출한다 - SparkSession을 공유하지 않으므로 Silver 읽기/조인과 카카오맵 지오코딩(외부 API 호출)이 마트마다 반복된다. 이는 GCP 메모리 제약(OOM 방지)을 위해 의도적으로 받아들인 비용이라고 주석에 명시되어 있다.
- 카카오맵 API 호출에는 `Retry(total=3, backoff_factor=1.0, status_forcelist=[429,500,502,503,504])`가 적용되어 일시적 오류에 재시도하지만, 그 외 예외(타임아웃, 파싱 오류 등)는 `except Exception`으로 흡수되어 좌표가 `(None, None, False)`로 남고 배치 전체는 죽지 않는다(단, 해당 단지는 좌표 없이 저장됨).
- `is_exact_location`은 지오코딩 1~3단계(지번/단지명 매칭 성공) 성공 시에만 True이고, 4단계(법정동 대표 좌표) 폴백 시 False - 이 값이 `dm_dong_pyeong_price_avg.py`의 `bool_and` 집계 기준이 된다.
- `fact_apt_transactions`는 `PARTITIONED BY (days(deal_date))`로 만들어져 있어 `deal_date` 필터가 Iceberg 파티션 프루닝에 활용된다 - 필터 조건을 바꿀 때 이 최적화가 깨지지 않도록 주의해야 한다.
- Bronze 지번 조회는 `BLDG_USG == "아파트"` 및 `CTRT_DAY` 범위로 필터링하며, RAW 버킷이 없거나 해당 기간 파티션이 없으면 조용히 빈 dict를 반환(예외로 배치를 죽이지 않는 best-effort 설계) - 이 경우 지오코딩은 자동으로 2~4단계로만 진행된다.
- SparkSession 메모리 설정(`spark.driver.memory=3g` 등)은 GCP e2-standard-2(8GB) VM에서 "이 프로세스 혼자 4.5~5GB를 쓴다"는 전제로 고정된 값이므로, 다른 프로세스와 동시 실행 시 OOM 위험이 있다(Airflow 순차 실행 체이닝에 의존).

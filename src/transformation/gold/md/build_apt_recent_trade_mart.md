# 📄 build_apt_recent_trade_mart.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 아파트별 최근 90일 거래 지표(총 거래금액/평단가 합계/최근 거래가/거래건수 등)와 건축물 기본정보(세대수, 사용승인일)를 결합한 Gold 데이터 마트(`dm_apt_recent_trade`)를 생성하는 단독 실행 PySpark 배치 스크립트. `dim_apartment`/`fact_apt_transactions`(Iceberg, s3a)를 Spark로 집계하고, 공공데이터포털 국토교통부 건축HUB `getBrRecapTitleInfo` API를 ThreadPoolExecutor로 병렬 호출해 아파트별 세대수/사용승인일을 보강한 뒤 Broadcast Join으로 최종 결과를 만들어 `mart/dm_apt_recent_trade/base_date=YYYY-MM-DD` 경로에 Parquet(overwrite)으로 저장한다.
- **현재 구현 상태:** 완료된 형태의 코드로 판단된다. API 호출 실패/타임아웃/429에 대한 재시도·백오프, 일일 트래픽 한도 초과 시 안전 종료, ANSI 모드 날짜 파싱 예외 대응(`try_to_timestamp` 활용) 등 실제 운영 중 발생한 문제들에 대한 방어 로직이 코드와 주석에 구체적으로 반영되어 있다. 별도 TODO/FIXME 주석은 없다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음(자기완결적 단일 파일 스크립트, 오케스트레이션 로직 없이 `build_dong_pyeong_mart.py`와 유사한 스타일로 독립 실행).
- **외부 패키지:** `pyspark`(`SparkSession`, `DataFrame`, `Row`, `functions as F`, `broadcast`, `types` - Iceberg 카탈로그 + Hadoop S3A/MinIO 연동), `requests`(건축HUB REST API 호출), `python-dotenv`(`load_dotenv`), 표준 라이브러리 `os`, `sys`, `threading`, `time`, `concurrent.futures`(`ThreadPoolExecutor`, `as_completed`), `datetime`, `pathlib`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `DailyTrafficExceededError` | 공공데이터포털 일일 트래픽 한도(`DAILY_TRAFFIC_LIMIT=10000`) 초과 시 발생시키는 커스텀 예외 | - | Exception |
| `ApiCallLimiter` | 스레드-세이프 API 호출 카운터. `increment()`가 한도 초과 시 `DailyTrafficExceededError` 발생 | `max_calls: int` | `None` |
| `ApiRateLimiter` | 여러 스레드 간 API 호출 최소 간격(`API_REQUEST_INTERVAL_SECONDS=1.0`)을 강제하는 스로틀러 | `min_interval: float` | `None`(`wait()` 호출 시 필요시 sleep) |
| `load_config()` | `env/.env` 로드 후 S3/DATA_GO_KR_KEY 등 필요한 환경변수를 dict로 반환 | 없음 | `dict` |
| `create_spark_session(config)` | Iceberg(`lakehouse` 카탈로그, hadoop 타입) + S3A/MinIO 연동 설정이 적용된 SparkSession 생성(local[2], driver 3g 등 GCP e2-standard-2 메모리 제약 반영) | `config: dict` | `SparkSession` |
| `load_dim_apartment(spark)` | `lakehouse.dim_apartment`에서 조인/출력에 필요한 컬럼만 선택 | `spark: SparkSession` | `DataFrame` |
| `aggregate_recent_trades(spark, base_date, start_date)` | 최근 90일 `fact_apt_transactions`를 필터링(취소건 제외, 금액/면적 양수) 후 아파트 단위(sgg_cd+dong_cd+apt_name)로 총 거래금액/총 평단가/총 평/최근 거래가/최근 평/거래건수/대표 mno·sno 집계 | `spark`, `base_date: date`, `start_date: date` | `DataFrame`(1차 집계) |
| `collect_apartment_keys(agg_df)` | 1차 집계된 고유 아파트 식별 키(+mno/sno)를 Driver로 collect | `agg_df: DataFrame` | `list[Row]` |
| `_format_jibun_param(value)` | mno/sno 원본 값을 건축HUB API가 요구하는 4자리 zero-padded 문자열로 변환 | `value` | `str` |
| `_normalize_use_approval_date(value)` | 공백/빈 문자열을 `None`으로 정규화(ANSI 모드 파싱 예외 방지) | `value` | `str \| None` |
| `fetch_building_info(row, api_key)` | 건축HUB `getBrRecapTitleInfo` 단건 호출. 429/5xx/타임아웃은 지수 백오프로 `API_MAX_RETRIES=3`회까지 재시도, 실패 시 None으로 채운 dict 반환(전체 배치는 계속 진행) | `row: Row`, `api_key: str` | `dict` |
| `fetch_building_info_parallel(apartment_rows, api_key)` | `ThreadPoolExecutor(max_workers=1)`로 병렬(사실상 순차) 호출을 수행하고 `DailyTrafficExceededError` 발생 시 남은 future를 취소 후 예외를 상위로 전파 | `apartment_rows: list[Row]`, `api_key: str` | `list[dict]` |
| `_sql_literal(value)` | 파이썬 값을 Spark SQL VALUES 절 리터럴 문자열로 변환 | `value` | `str` |
| `build_api_dataframe(spark, api_results)` | API 조회 결과(list[dict])를 `spark.createDataFrame` 대신 SQL VALUES 절로 소형 DataFrame으로 변환(Windows 환경 파이썬 워커 콜백 문제 회피) | `spark`, `api_results: list[dict]` | `DataFrame`(BUILDING_API_SCHEMA) |
| `build_gold_mart(agg_df, dim_df, api_df)` | agg_df에 dim_df(inner)·api_df(left)를 broadcast join으로 결합, `use_approval_date`의 8/6/4자리 부분 정밀도(yyyyMMdd/yyyyMM/yyyy)를 `try_to_timestamp`로 안전 파싱해 최종 컬럼 구성 | `agg_df`, `dim_df`, `api_df: DataFrame` | `DataFrame`(최종 Gold 마트) |
| `main()` | CLI 인자(BASE_DATE, 생략 시 오늘)로 조회기간 산정 → SparkSession/집계/API 조회/조인 → overwrite 저장 → 스키마/샘플/카운트 로그, 트래픽 한도 초과 시 `sys.exit(1)` | `sys.argv[1]`(선택) | `None` |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** 별도 Airflow DAG 스케줄 정의는 이 파일에 없음(오케스트레이션 로직 없는 독립 배치 스크립트). CLI로 직접 실행(`python src/transformation/gold/build_apt_recent_trade_mart.py [BASE_DATE]`).
- **Source (Input):** `lakehouse.dim_apartment`, `lakehouse.fact_apt_transactions`(s3a://{LAKE}/ 하위 Iceberg 테이블) + 외부 API `https://apis.data.go.kr/1613000/BldRgstHubService/getBrRecapTitleInfo`(공공데이터포털 건축HUB, `DATA_GO_KR_KEY` 필요).
- **Target (Output):** `s3a://{LAKE}/mart/dm_apt_recent_trade/base_date=YYYY-MM-DD` 경로에 Parquet(overwrite 모드).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `main()`이 CLI 인자(또는 오늘)로 `base_date`/`start_date`(90일 전) 결정, `load_config()`로 환경변수 로드, `create_spark_session()`으로 Iceberg+S3A 연동 SparkSession 생성.
  2. `load_dim_apartment()`로 차원 로드, `aggregate_recent_trades()`로 최근 90일 거래를 아파트 단위 1차 집계(`.cache()`로 재사용).
  3. `collect_apartment_keys()`로 고유 아파트 식별 키를 Driver로 가져와 `fetch_building_info_parallel()`을 통해 건축HUB API를 스레드풀(사실상 1개 워커, 스로틀 1초 간격)로 호출 - 일일 트래픽 한도 초과 시 `DailyTrafficExceededError`를 잡아 캐시 해제 및 저장 없이 `sys.exit(1)`로 종료.
  4. `build_api_dataframe()`로 API 결과를 소형 DataFrame으로 변환.
  5. `build_gold_mart()`가 agg_df에 dim_df(inner)·api_df(left)를 broadcast join하고, `use_approval_date`의 정밀도(8/6/4자리)에 따라 안전하게 파싱해 `use_approval_date`(문자열, 가변 포맷)와 `use_approval_year`(정수)를 파생.
  6. 최종 결과를 `.cache()`한 뒤 `mart_path`에 overwrite 모드로 저장, 스키마/샘플 20건/총 레코드 수를 로그로 출력 후 `spark.stop()`.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] 아파트 단위 최근 90일 거래 지표 1차 집계(총 거래금액/평단가/평/최근값/거래건수)
- [x] 건축HUB API 병렬 호출 + 재시도(지수 백오프)/429 스로틀링/일일 트래픽 한도 방어
- [x] `use_approval_date` 부분 정밀도(6/4자리 등 불완전한 날짜) 안전 파싱(ANSI 모드 CANNOT_PARSE_TIMESTAMP 예외 회피)
- [x] Windows 로컬 환경에서 `spark.createDataFrame(list, schema)` 파이썬 워커 콜백 문제 회피(SQL VALUES 절 사용)
- [ ] [TODO] `API_MAX_WORKERS=1`로 사실상 순차 호출이라 아파트 수가 많을수록 배치 시간이 선형으로 늘어난다(주석상 429 방지를 위한 의도적 축소) - 처리 시간이 문제가 될 경우 API 호출 캐싱/증분 방식(이미 조회한 아파트는 재호출 생략) 등의 개선 여지가 있어 보인다.
- [ ] [TODO] Airflow DAG 연동 로직이 이 파일에는 없다 - 실제 스케줄링/오케스트레이션 설정이 별도 DAG 파일에 되어 있는지 확인이 필요하다(이 스크립트만으로는 트리거 조건을 알 수 없음).

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 건축HUB API 호출은 `API_MAX_WORKERS=1`, `API_REQUEST_INTERVAL_SECONDS=1.0`로 강하게 스로틀링되어 있다(429 방지를 위해 기존 10에서 1로 축소한 이력 명시).
- `API_MAX_RETRIES=3`, 지수 백오프(1s, 2s, 4s...)로 재시도하며 재시도 소진 후에도 실패하면 해당 아파트는 `household_count`/`use_approval_date`가 `None`으로 채워지고 배치 전체는 계속 진행(단건 실패가 전체를 막지 않음).
- `DAILY_TRAFFIC_LIMIT=10000`건 초과 시 `DailyTrafficExceededError`가 발생하며, 이 경우 진행 중이던 캐시(`agg_df.unpersist()`)를 정리하고 저장 없이 `sys.exit(1)`로 종료한다(부분 저장 없음).
- `use_approval_date`는 원본 데이터 정밀도가 8/6/4자리로 혼재되어 있어 `DateType`이 아닌 가변 포맷 문자열(`StringType`)로 저장하며, 파싱 실패 시 `NULL`로 안전 처리한다.
- SparkSession 메모리 설정(`spark.driver.memory=3g` 등)은 GCP e2-standard-2(8GB RAM) VM에서 Gold 마트가 순차 실행된다는 전제(`gold_mart_serial_pool`) 하에 튜닝된 값 - 다른 프로세스와 동시 실행 시 OOM 위험이 있을 수 있다.
- User-Agent 헤더를 명시하지 않으면 건축HUB 서버가 봇으로 간주해 503을 반환하는 사례가 있어 브라우저 UA를 고정값으로 지정하고 있다.
- 저장은 `overwrite` 모드이므로 같은 base_date로 재실행하면 해당 경로 전체가 최신 결과로 교체된다(파티션 단위 Upsert가 아님 - apt_rtt_mart.py/apt_mkt_trends_mart.py와 달리 증분 병합 로직이 없다).

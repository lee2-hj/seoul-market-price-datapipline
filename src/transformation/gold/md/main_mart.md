# 📄 main_mart.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** Silver(Iceberg `dim_apartment`, `fact_apt_transactions`) → Gold 마트 `dm_main` 생성 스크립트. Gold 레이어의 다른 마트들보다 먼저 실행되는 "최우선" 마트로, [자치구 x 법정동 x 단지 x 거래일자 x 면적] 단위의 원자적(atomic) 집계(거래건수/거래금액 합계/평당가 합계 + 좌표)를 생성해 하위 마트들이 필요로 하는 공통 정제 데이터의 기초를 제공하는 취지로 만들어졌다. 오케스트레이션 로직은 없는 단독 실행 PySpark 배치 스크립트이며, `build_dong_pyeong_mart.py`/`build_apt_recent_trade_mart.py`와 동일한 스타일(모듈 최상위 스코프에서 순차 실행)로 작성되었다.
- **현재 구현 상태:** 완료. 번호가 매겨진 주석 섹션(1~8)으로 CLI 인자 처리부터 저장까지 전체 흐름이 순서대로 문서화되어 있다. `dong_pyeong_common.py`와 상당 부분 로직이 중복되어 있으나(지오코딩, 정제, 조인 로직 등), 이 파일은 함수/클래스로 캡슐화하지 않고 모듈 최상위 스코프에 절차적으로 나열되어 있다는 점이 `dong_pyeong_common.py` 기반 마트들과의 구조적 차이다. 명시적 TODO/FIXME 주석은 없음.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음 - 다른 프로젝트 내부 모듈(예: `dong_pyeong_common`)을 import하지 않고 모든 로직(환경변수 로드, SparkSession 생성, 지오코딩 등)을 이 파일 안에 자체적으로 구현한다.
- **외부 패키지:** `python-dotenv`(`load_dotenv`), `pyspark.sql`(`SparkSession`, `functions as F`, `broadcast`), `pyspark.sql.types`(`BooleanType`, `DoubleType`, `IntegerType`, `LongType`, `StringType`, `StructField`, `StructType`), `pyspark.sql.window.Window`, `requests`(카카오맵 API 호출, 파일 중간에서 임포트), `requests.adapters.HTTPAdapter`, `urllib3.util.retry.Retry`. 표준 라이브러리 `os`, `re`, `sys`, `datetime`(`datetime`, `timedelta`), `pathlib.Path`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| 모듈 최상위 절차(섹션 1~8) | 함수로 감싸지 않고 모듈 로드 시 순차 실행되는 절차적 스크립트. 섹션 1: BASE_DATE 결정, 2: 환경변수 로드, 3: SparkSession 생성, 4: Silver 읽기, 5: 1차 정제, 6: 브로드캐스트 조인, 7: 카카오맵 지오코딩, 8: 최종 집계+저장. | 없음(전역 스코프) | N/A |
| `_clean_bldg_nm(name)` | 단지명을 카카오 검색어로 정제(로마자→숫자, 괄호/특수문자 제거, 말미 '아파트' 제거). `dong_pyeong_common.py`와 동일 로직. | `name: str \| None` | `str` |
| `_format_jibun(mno, sno)` | `MNO`/`SNO`를 `"22"`/`"22-1"` 형태로 변환. | `mno, sno` | `str \| None` |
| `_kakao_search(url, query)` | 카카오 REST API GET 호출 후 `documents` 리스트 반환(재시도 세션 사용). | `url: str, query: str` | `list[dict]` |
| `_is_apartment_category(doc)` | 카테고리 코드/명으로 아파트 여부 판별. | `doc: dict` | `bool` |
| `_address_matches_dong(doc, sgg_nm, dong_nm)` | 주소에 구/동 이름 포함 여부 확인. | `doc, sgg_nm, dong_nm` | `bool` |
| `_first_verified_match(docs, sgg_nm, dong_nm)` | 주소+카테고리 검증 통과하는 첫 문서 반환. | `docs, sgg_nm, dong_nm` | `dict \| None` |
| `_geocode_apartment(sgg_nm, dong_nm, jibun, bldg_nm)` | 3~4단계 Fallback(지번+단지명 → 지번주소 → 단지명 → 법정동)으로 단일 단지 지오코딩, `(latitude, longitude)` 튜플만 반환(`is_exact_location` 플래그 없음 - `dong_pyeong_common.py`와의 차이점). 예외는 내부에서 흡수. | `sgg_nm, dong_nm, jibun, bldg_nm: str` | `tuple[float\|None, float\|None]` |
| `_sql_literal(value)` | Python 값을 Spark SQL VALUES 리터럴로 변환(작은따옴표 백슬래시 이스케이프, float에 `D` 접미사). | `value` | `str` |
| `jibun_lookup` (전역 변수) | `joined_df`에서 (자치구+법정동+단지명)별 최빈 (mno, sno) 조합을 뽑은 dict. Bronze를 다시 스캔하지 않고 이미 읽은 `fact_df`(Silver, mno/sno 컬럼 포함)만으로 계산. | 없음(스크립트 실행 중 생성) | `dict[tuple, tuple]` |
| `main_mart_df` (전역 변수) | 최종 집계 결과. `[sgg_cd, sgg_nm, dong_cd, dong_nm, apt_name, deal_date, exclusive_area_m2, mno, sno, latitude, longitude]` 단위로 `deal_cnt`, `total_thing_amt`, `total_pyeong_amt` 집계 후 컬럼명 변환(`cgg_cd`, `stdg_cd`, `bldg_nm`, `area` 등). | 없음 | `DataFrame` |
| 저장부(`main_mart_df.write...`) | `MART_PATH`(`s3a://{LAKE}/mart/dm_main/base_date=YYYY-MM-DD`)에 `overwrite` 모드 Parquet 저장, 이후 `spark.stop()`. | 없음 | `None`(부수효과: S3 저장) |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** 이 파일 자체에는 DAG/cron 정의 없음(N/A). Airflow 오케스트레이션(`data_orchestration.py` 등)에서 다른 Gold 마트들보다 먼저 실행되도록 배치되는 것으로 추정되며, CLI로 단독 실행도 가능하다(`python src/transformation/gold/main_mart.py [BASE_DATE]`).
- **Source (Input):** Iceberg 카탈로그 `lakehouse.dim_apartment`(구/동/단지명), `lakehouse.fact_apt_transactions`(`deal_date` 기준 최근 90일 파티션 프루닝, `mno`/`sno` 포함 - Bronze 재조회 불필요), 카카오맵 REST API, 환경변수(`S3_END_POINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `LAKE`, `KAKAO_MAP_REST_API_KEY`, `SPARK_*`).
- **Target (Output):** `s3a://{LAKE}/mart/dm_main/base_date=YYYY-MM-DD` 경로에 Parquet 저장(`overwrite` 모드, 멱등적). RDB 적재 없음.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `sys.argv[1]`로 `BASE_DATE` 결정(없으면 오늘), 조회기간은 `BASE_DATE` 포함 최근 90일(`LOOKBACK_DAYS`).
  2. `env/.env` 또는 `/opt/airflow/project/env/.env`에서 환경변수 로드(`override=False`).
  3. Iceberg+S3A 설정 SparkSession 생성(`dong_pyeong_common.py`의 `_create_spark_session`과 거의 동일한 설정값, 8GB VM 기준 `driver.memory=3g` 등).
  4. `dim_apartment`(브로드캐스트용 컬럼만), `fact_apt_transactions`(`deal_date` 범위 필터, `mno`/`sno` 포함 select).
  5. 취소건(`cancel_date`) 제외, 금액/면적 0 이하 제외.
  6. `dim_apartment`를 `broadcast()` 조인(구+동+단지명 키), 단지명/동명 `trim()` → `joined_df.cache()`.
  7. `joined_df` 내 `mno`/`sno`로 단지별 최빈 지번(`jibun_lookup`) 계산 → 고유 단지 목록(`distinct_apt_rows`) 수집 → 순차 for-loop로 `_geocode_apartment()` 호출(단지당 1회, 메모리 캐시로 중복 차단) → 결과를 Spark SQL `VALUES` 절로 DataFrame화(`geocode_schema`로 VOID 타입 방지 캐스팅) → `broadcast()` left join.
  8. `pyeong_amt`(거래금액/공급평수) 파생 → `[sgg_cd, sgg_nm, dong_cd, dong_nm, apt_name, deal_date, exclusive_area_m2, mno, sno, latitude, longitude]` 기준 groupBy → `deal_cnt`, `total_thing_amt`, `total_pyeong_amt` 집계 → 최종 컬럼명(`base_date`, `cgg_cd`, `cgg_nm`, `stdg_cd`, `stdg_nm`, `bldg_nm`, `area` 등)으로 select → Parquet 저장 → `spark.stop()`.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] [자치구 x 법정동 x 단지 x 거래일자 x 면적] 단위 원자적 집계 및 카카오맵 지오코딩(3~4단계 Fallback), Parquet 저장 구현 완료.
- [x] `fact_apt_transactions`에 Silver 스키마 진화로 이미 `mno`/`sno`가 포함되어 있어, `dong_pyeong_common.py`(및 그 전신인 `build_dong_pyeong_mart.py`)와 달리 Bronze 원본을 별도로 재스캔하지 않는 최적화가 적용되어 있다(주석에 명시).
- [ ] [TODO] 명시적 TODO 주석은 없음. 이 파일은 `dong_pyeong_common.py`와 지오코딩/정제/조인 로직이 상당 부분 중복(`_clean_bldg_nm`, `_format_jibun`, `_is_apartment_category`, `_address_matches_dong`, `_first_verified_match`, `_sql_literal` 등 동일 함수 재정의)되어 있다 - 공통 유틸로 분리해 두 파일이 공유하도록 리팩터링하면 유지보수성이 개선될 것으로 보인다.
- [ ] [TODO] 모듈 전체가 함수로 캡슐화되지 않고 전역 스코프에서 절차적으로 실행되므로, 단위 테스트 작성이나 부분 재사용이 어렵다 - `dong_pyeong_common.py`처럼 `build()`/`run()` 함수 분리 또는 `if __name__ == "__main__":` 가드 도입을 고려할 필요가 있다.
- [ ] [TODO] `import requests` 등 일부 외부 패키지 임포트가 파일 최상단이 아니라 섹션 7(234행 부근) 중간에 위치해 있다 - PEP 8 관례상 파일 최상단으로 이동하는 것이 코드 가독성에 좋다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- `dm_main`은 다른 Gold 마트들보다 "먼저" 실행되어야 하는 최우선 마트로 설계되었다(주석에 명시) - Airflow 태스크 의존성 설정 시 이 순서를 반드시 지켜야 한다.
- `_geocode_apartment()`는 `dong_pyeong_common.py`의 `_geocode_apartments()`와 달리 `is_exact_location` 플래그를 반환하지 않는다(좌표만 반환) - 이 마트의 출력 스키마에는 정확도 플래그 컬럼이 없다.
- `MART_PATH`가 `base_date=YYYY-MM-DD` 파티션 경로에 `overwrite` 모드로 저장되므로 재실행 시 멱등적이다(같은 base_date로 몇 번 실행해도 누적되지 않음).
- 카카오맵 API 호출은 `Retry(total=3, backoff_factor=1.0, status_forcelist=[429,500,502,503,504])`로 일시 오류에 재시도하며, 그 외 예외는 `_geocode_apartment()` 내부에서 흡수되어 배치 전체를 죽이지 않는다(해당 단지는 좌표 `None`으로 남음).
- `KAKAO_MAP_REST_API_KEY`가 없으면 경고 로그만 남기고 좌표가 전부 NULL로 채워진 채 진행된다.
- SparkSession 메모리 설정은 GCP e2-standard-2(8GB) VM에서 이 프로세스가 단독으로 4.5~5GB를 쓴다는 전제로 고정되어 있다(`gold_mart_serial_pool`로 순차 실행되는 것을 전제).

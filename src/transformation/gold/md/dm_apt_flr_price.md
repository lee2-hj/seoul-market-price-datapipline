# 📄 dm_apt_flr_price.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** [동 x 평형대] 계열 Gold 마트 4종 중 하나(④)로, MinIO S3 Lake `mart/dm_apt_flr_price/` 경로에 대응하는 단독 실행 PySpark 배치 스크립트. 공통 준비 모듈 `dong_pyeong_common.py`가 만들어주는 `common_df`(Silver 조인 + 카카오맵 지오코딩 + 평형대/층수 그룹/평당가 파생까지 끝난 데이터프레임)를 입력받아, [단지 x 층수 그룹(flr_grp)] 단위로 최근 90일 거래건수/총 거래금액/총 평당금액/최근 거래가·평당가·계약일자·거래층수를 집계해 Parquet으로 저장한다.
- **현재 구현 상태:** 완료된 형태. 파일 자체는 매우 짧고(69줄) 핵심 로직 대부분을 공통 모듈에 위임하는 구조이며, 별도의 TODO/FIXME 주석이나 예외 처리 미비 없이 스크립트 최상위 레벨 코드로 즉시 실행되도록 작성되어 있다(함수 정의 후 모듈 레벨에서 바로 `build_gold_mart_context()`→`run()`→`spark.stop()` 호출).

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `transformation.gold.dong_pyeong_common` 모듈의 `APT_GROUP_COLS`(단지 단위 공통 groupBy 키), `GoldMartContext`(dataclass: spark/common_df/mart_paths/base_date_str/run_timestamp), `apt_select_cols`(공통 select 컬럼 헬퍼), `build_gold_mart_context`(환경변수 로드→SparkSession 생성→Silver 읽기/조인→카카오맵 지오코딩→평형대/층수 그룹 및 평당가 파생까지 수행하는 공통 준비 함수). 이 공통 모듈은 `dim_apartment`/`fact_apt_transactions`(Iceberg, s3a), Bronze 원본(지번 보조 조회), 카카오맵 REST API(`dapi.kakao.com`)까지 함께 다룬다.
- **외부 패키지:** `pyspark`(`DataFrame`, `functions as F`, `types.IntegerType/LongType`), 표준 라이브러리 `sys`, `datetime`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `MART_NAME` | 마트명 상수(`"dm_apt_flr_price"`) - `ctx.mart_paths` 딕셔너리 키 및 로그 메시지에 사용 | - | `str` |
| `build(ctx)` | `ctx.common_df`를 `APT_GROUP_COLS + ["flr_grp"]`(단지+층수 그룹) 기준 groupBy 후 거래건수(`deal_cnt`)/총 거래금액(`total_thing_amt`)/총 평당금액(`total_pyeong_amt`)/최근 거래가·평당가(`max_by(..., deal_date)`)/최근 계약일자(`recent_deal_date`)/최근 거래층수(`recent_floor`)를 집계, `apt_select_cols(ctx)` 공통 컬럼과 함께 select | `ctx: GoldMartContext` | `DataFrame`(집계 결과) |
| `run(ctx)` | `build(ctx)` 결과를 `ctx.mart_paths[MART_NAME]` 경로에 `overwrite` 모드로 Parquet 저장 | `ctx: GoldMartContext` | `None`(저장 부작용, 완료 로그 출력) |
| 모듈 최상위 실행 블록 | `sys.argv[1]`(BASE_DATE, 생략 시 오늘)로 `_base_date` 결정 → `build_gold_mart_context(_base_date)` 호출 → `run(_ctx)` → `_ctx.spark.stop()` → 완료 로그 출력 | `sys.argv[1]`(선택, YYYY-MM-DD) | `None`(모듈 import 시점에 즉시 실행) |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** 이 파일 자체에는 Airflow DAG 정의가 없다(`data_orchestration.py`가 이 스크립트를 태스크로 순차 체이닝한다고 `dong_pyeong_common.py` 주석에 언급되어 있으나, 정확한 cron 주기는 이 파일 범위 밖). CLI 직접 실행 방식: `python src/transformation/gold/dm_apt_flr_price.py [BASE_DATE]`.
- **Source (Input):** 직접적인 I/O는 없고, `build_gold_mart_context(base_date)`가 반환하는 `GoldMartContext.common_df`를 입력으로 사용한다. 그 `common_df`의 원천은 `lakehouse.dim_apartment`/`lakehouse.fact_apt_transactions`(Iceberg) + Bronze 원본(`real_estate/year=.../month=.../day=*/*.parquet`, 지번 보조 조회용) + 카카오맵 API(위경도/정확도).
- **Target (Output):** `ctx.mart_paths["dm_apt_flr_price"]` = `s3a://{LAKE}/mart/dm_apt_flr_price/base_date=YYYY-MM-DD` 경로에 Parquet(overwrite 모드).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. 모듈 최상위에서 CLI 인자(또는 오늘)로 `_base_date` 결정.
  2. `build_gold_mart_context(_base_date)` 호출 - 내부적으로 환경변수 로드, SparkSession(Iceberg+S3A) 생성, `dim_apartment`/`fact_apt_transactions` 조인, 지오코딩, 평형대(`pyeong_grp`)/층수그룹(`flr_grp`)/평당가(`pyeong_amt`) 파생까지 끝낸 `common_df`를 담은 `GoldMartContext` 반환.
  3. `run(_ctx)` → `build(_ctx)`가 `APT_GROUP_COLS + flr_grp` 기준으로 집계(거래건수/총액/평당총액/최근값들) 후 `apt_select_cols`(base_date/구·동·단지명/좌표/지번/updated_at) 공통 컬럼과 결합.
  4. 결과를 `mart_paths[MART_NAME]`에 overwrite로 저장.
  5. `spark.stop()`으로 세션 종료 후 완료 로그 출력("MinIO S3 Lake 전용, RDB 적재 없음").

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] [단지 x 층수 그룹] 단위 최근 90일 거래건수/총 거래금액/총 평당금액/최근 거래가·평당가·계약일자·거래층수 집계
- [x] 공통 준비 로직(`dong_pyeong_common.py`)을 통한 Silver 조인/지오코딩/평형대·층수 그룹 파생 재사용
- [ ] [TODO] 이 파일 자체에는 TODO/FIXME 주석이 없으나, `dong_pyeong_common.py` 주석에 따르면 [동 x 평형대] 계열 4개 마트가 SparkSession을 공유하지 않아 Silver 읽기/조인과 카카오맵 API 호출이 마트마다 반복된다(메모리 안전을 위한 의도적 트레이드오프로 명시됨) - 외부 API 호출 비용이 커질 경우 지오코딩 결과 캐싱(예: 별도 저장소에 위경도 결과 영속화) 등의 개선을 검토할 수 있어 보인다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 이 스크립트는 다른 `dm_*.py` 마트와 SparkSession을 공유하지 않는다 - GCP 메모리 제약으로 Airflow에서 마트를 하나씩 완전히 종료 후 다음 마트를 순차 실행해야 하기 때문(동시 실행 시 OOM 위험).
- `flr_grp`(층수 그룹: LOW/MID/HIGH, 5층/15층 기준) 및 평형대(`pyeong_grp`)는 `dong_pyeong_common.py`에서 전용면적/층수 기준으로 이미 분류되어 `common_df`에 포함되어 들어온다 - 이 파일에서는 재분류 로직이 없다.
- 저장은 `overwrite` 모드이므로 같은 base_date로 재실행하면 해당 경로 전체가 최신 결과로 교체된다(파티션 단위 증분 Upsert가 아님).
- 좌표(latitude/longitude)는 카카오맵 API 키(`KAKAO_MAP_REST_API_KEY`)가 없으면 전부 NULL로 채워진다(공통 모듈 경고 로그 발생).
- RDB(관계형 DB) 적재는 하지 않으며 MinIO S3 Lake 전용으로 저장됨을 완료 로그에서 명시하고 있다.

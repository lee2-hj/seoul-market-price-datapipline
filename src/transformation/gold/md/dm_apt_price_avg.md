# 📄 dm_apt_price_avg.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** [동 x 평형대] 계열 Gold 마트 4종 중 하나(②)로, MinIO S3 Lake `mart/dm_apt_price_avg/` 경로에 대응하는 단독 실행 PySpark 배치 스크립트. 공통 준비 모듈 `dong_pyeong_common.py`가 만들어주는 `common_df`를 입력받아 [단지] 단위(층수/평형대 세분 없이 전체)로 최근 90일 거래건수/총 거래금액/총 평당금액/최근 거래가·평당가를 집계해 Parquet으로 저장한다. `dm_apt_flr_price.py`와 거의 동일한 구조이나 groupBy 키에 `flr_grp`가 없고(단지 전체 단위), 출력 컬럼에도 `recent_deal_date`/`recent_floor`가 없다는 점이 차이.
- **현재 구현 상태:** 완료된 형태. 파일이 짧고(67줄) 로직 대부분을 공통 모듈에 위임하며, 모듈 최상위 레벨에서 바로 실행되는 구조. 별도 TODO/FIXME 주석이나 예외 처리 미비는 발견되지 않는다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `transformation.gold.dong_pyeong_common`의 `APT_GROUP_COLS`(단지 단위 공통 groupBy 키: sgg_cd/sgg_nm/dong_cd/dong_nm/apt_name/latitude/longitude/is_exact_location/mno/sno), `GoldMartContext`(dataclass), `apt_select_cols`(공통 select 컬럼: base_date/cgg_cd/cgg_nm/stdg_cd/stdg_nm/bldg_nm/좌표/mno/sno/updated_at), `build_gold_mart_context`(환경변수 로드 → SparkSession 생성 → Silver 읽기/조인 → 카카오맵 지오코딩 → 평형대/층수 그룹 및 평당가 파생까지 수행). 이 공통 모듈은 `dim_apartment`/`fact_apt_transactions`(Iceberg), Bronze 원본(지번 보조 조회), 카카오맵 REST API를 함께 다룬다.
- **외부 패키지:** `pyspark`(`DataFrame`, `functions as F`, `types.IntegerType/LongType`), 표준 라이브러리 `sys`, `datetime`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `MART_NAME` | 마트명 상수(`"dm_apt_price_avg"`) - `ctx.mart_paths` 키 및 로그 메시지에 사용 | - | `str` |
| `build(ctx)` | `ctx.common_df`를 `APT_GROUP_COLS`(단지 단위) 기준으로만 groupBy 후 거래건수(`deal_cnt`)/총 거래금액(`total_thing_amt`)/총 평당금액(`total_pyeong_amt`)/최근 거래가·평당가(`max_by(값, deal_date)`)를 집계, `apt_select_cols(ctx)` 공통 컬럼과 함께 select | `ctx: GoldMartContext` | `DataFrame`(집계 결과) |
| `run(ctx)` | `build(ctx)` 결과를 `ctx.mart_paths[MART_NAME]` 경로에 `overwrite` 모드로 Parquet 저장 | `ctx: GoldMartContext` | `None`(저장 부작용, 완료 로그 출력) |
| 모듈 최상위 실행 블록 | `sys.argv[1]`(BASE_DATE, 생략 시 오늘)로 `_base_date` 결정 → `build_gold_mart_context(_base_date)` 호출 → `run(_ctx)` → `_ctx.spark.stop()` → 완료 로그 출력 | `sys.argv[1]`(선택, YYYY-MM-DD) | `None`(모듈 import 시점에 즉시 실행) |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** 이 파일 자체에는 Airflow DAG 정의가 없다(`dong_pyeong_common.py` 주석에 따르면 `data_orchestration.py`가 [동 x 평형대] 마트 4종을 완전히 독립된 프로세스로 순차 실행하도록 체이닝한다고 언급되어 있으나, 정확한 cron 주기는 이 파일 범위 밖). CLI 직접 실행: `python src/transformation/gold/dm_apt_price_avg.py [BASE_DATE]`.
- **Source (Input):** 직접적인 I/O는 없고, `build_gold_mart_context(base_date)`가 반환하는 `GoldMartContext.common_df`를 입력으로 사용. 그 원천은 `lakehouse.dim_apartment`/`lakehouse.fact_apt_transactions`(Iceberg) + Bronze 원본(지번 보조 조회용) + 카카오맵 API(위경도/정확도).
- **Target (Output):** `ctx.mart_paths["dm_apt_price_avg"]` = `s3a://{LAKE}/mart/dm_apt_price_avg/base_date=YYYY-MM-DD` 경로에 Parquet(overwrite 모드).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. 모듈 최상위에서 CLI 인자(또는 오늘)로 `_base_date` 결정.
  2. `build_gold_mart_context(_base_date)` 호출 - 환경변수 로드, SparkSession(Iceberg+S3A) 생성, `dim_apartment`/`fact_apt_transactions` 최근 90일 조인, 지오코딩, 평형대/층수그룹/평당가 파생까지 끝낸 `common_df`를 담은 `GoldMartContext` 반환.
  3. `run(_ctx)` → `build(_ctx)`가 `APT_GROUP_COLS`(단지 단위, 층수/평형대 세분 없음) 기준으로 집계(거래건수/총액/평당총액/최근 거래가·평당가) 후 `apt_select_cols` 공통 컬럼과 결합.
  4. 결과를 `mart_paths[MART_NAME]`에 overwrite로 저장.
  5. `spark.stop()`으로 세션 종료 후 완료 로그 출력("MinIO S3 Lake 전용, RDB 적재 없음").

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] [단지] 전체 단위 최근 90일 거래건수/총 거래금액/총 평당금액/최근 거래가·평당가 집계
- [x] 공통 준비 로직(`dong_pyeong_common.py`)을 통한 Silver 조인/지오코딩/평형대·층수 그룹 파생 재사용
- [ ] [TODO] 이 파일 자체에는 TODO/FIXME 주석이 없으나, `dm_apt_flr_price.py`/`dm_apt_pyeong_price.py`/`dm_dong_pyeong_price_avg.py`와 groupBy 키(층수/평형대 유무)만 다를 뿐 집계 로직 구조가 거의 동일하게 반복되고 있다 - 집계 컬럼 리스트를 파라미터화한 공통 헬퍼로 통합하면 4개 마트 파일 간 중복을 줄일 수 있어 보인다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 이 스크립트는 다른 `dm_*.py` 마트와 SparkSession을 공유하지 않는다 - GCP 메모리 제약으로 Airflow에서 마트를 하나씩 완전히 종료 후 다음 마트를 순차 실행해야 하기 때문(동시 실행 시 OOM 위험). 그 대가로 Silver 읽기/조인과 카카오맵 지오코딩(외부 API 호출)이 마트마다 반복 발생한다.
- `APT_GROUP_COLS`에는 mno/sno/latitude/longitude/is_exact_location이 집계 대상이 아니라 단지별 고정 속성으로 groupBy 키에 포함되어 있다.
- 저장은 `overwrite` 모드이므로 같은 base_date로 재실행하면 해당 경로 전체가 최신 결과로 교체된다(파티션 단위 증분 Upsert가 아님).
- 좌표(latitude/longitude)는 카카오맵 API 키(`KAKAO_MAP_REST_API_KEY`)가 없으면 전부 NULL로 채워진다(공통 모듈 경고 로그 발생).
- RDB(관계형 DB) 적재는 하지 않으며 MinIO S3 Lake 전용으로 저장됨을 완료 로그에서 명시하고 있다.

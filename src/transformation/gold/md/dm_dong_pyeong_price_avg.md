# 📄 dm_dong_pyeong_price_avg.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** Gold 마트 ① `DM_DONG_PYEONG_PRICE_AVG`를 생성하는 단독 실행 PySpark 배치 스크립트. `dong_pyeong_common.py`의 공통 정제 데이터프레임(`common_df`)을 [동] 단위(자치구+법정동, `sgg_cd/sgg_nm/dong_cd/dong_nm`)로 그룹핑해 최근 90일 거래건수, 동 안 단지들의 평균 좌표, 정확 좌표 여부(`is_exact_location`, 보수적 `bool_and` 집계), 거래금액/평당가 합계를 집계해 MinIO S3 Lake `mart/dm_dong_pyeong_price_avg/` 경로에 Parquet으로 저장한다.
- **현재 구현 상태:** 완료. 집계(`build`)와 저장(`run`)이 분리되어 있고 CLI 인자 처리부터 SparkSession 종료까지 일관된 흐름을 갖춘다. TODO/FIXME 주석은 없음. `dm_apt_pyeong_price.py`와 동일하게 모듈 최상위 스코프에서 즉시 실행되는 구조.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `transformation.gold.dong_pyeong_common`에서 `GoldMartContext`, `build_gold_mart_context`만 가져온다(단지 단위 마트와 달리 `APT_GROUP_COLS`/`apt_select_cols`는 사용하지 않음 - 동 단위 집계라 단지 속성 컬럼이 groupBy 키에 필요 없기 때문).
- **외부 패키지:** `pyspark.sql`(`DataFrame`, `functions as F`), `pyspark.sql.types`(`IntegerType`, `LongType`). 표준 라이브러리 `sys`, `datetime`. 인프라(Spark/S3/카카오맵) 연동은 전부 `dong_pyeong_common.py`에 위임.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `MART_NAME` | 마트 식별자 상수(`"dm_dong_pyeong_price_avg"`). | `str` 상수 | N/A |
| `build(ctx)` | `common_df`를 `sgg_cd, sgg_nm, dong_cd, dong_nm`으로 그룹핑해 `deal_cnt`(건수), `latitude`/`longitude`(단지 좌표 평균, 소수 6자리 반올림), `is_exact_location`(`bool_and` - 그룹 내 전 단지가 정확 매칭일 때만 True), `total_thing_amt`/`total_pyeong_amt`(거래금액/평당가 합계)를 집계한다. | `ctx: GoldMartContext` | `DataFrame`(동 단위 집계 결과, `base_date`/`cgg_cd`/`cgg_nm`/`stdg_cd`/`stdg_nm` 컬럼명으로 최종 select) |
| `run(ctx)` | `ctx.mart_paths[MART_NAME]` 경로에 `build(ctx)` 결과를 `overwrite` 모드 Parquet으로 저장. | `ctx: GoldMartContext` | `None` (부수효과: S3 Lake 저장) |
| 모듈 최하단 실행부 | CLI 인자(`BASE_DATE`) 파싱 → `build_gold_mart_context` 호출 → `run` 실행 → `spark.stop()`. | CLI 인자 `BASE_DATE`(옵션, `YYYY-MM-DD`) | 프로세스 종료 |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** 이 파일 자체에는 DAG/cron 정의 없음(N/A) - Airflow 오케스트레이션 파일(`data_orchestration.py` 등)에서 다른 Gold 마트와 순차 실행되도록 체이닝되는 것으로 추정되며, 단독 CLI 실행도 가능하다.
- **Source (Input):** `dong_pyeong_common.build_gold_mart_context(base_date)`가 반환하는 `GoldMartContext.common_df`(Silver `dim_apartment`+`fact_apt_transactions` 조인, 카카오맵 지오코딩, 평형대/평당가 파생까지 끝난 데이터).
- **Target (Output):** `s3a://{LAKE}/mart/dm_dong_pyeong_price_avg/base_date=YYYY-MM-DD` 경로에 Parquet 저장(`overwrite`). RDB 적재 없음.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. CLI 인자 `BASE_DATE` 파싱(없으면 오늘 날짜).
  2. `build_gold_mart_context(_base_date)`로 공통 컨텍스트(`common_df` 포함) 생성.
  3. `run(_ctx)` → `build(_ctx)`로 [동] 단위 그룹핑/집계 수행. 좌표는 동 내 단지 좌표들의 평균(대표 좌표, 지도 핀용)으로 산출하고, `is_exact_location`은 그 평균에 들어간 모든 단지가 지오코딩 1~3단계(정확 매칭)였을 때만 True로 표기(하나라도 4단계 법정동 폴백이 섞이면 False - 대표 좌표 정밀도를 보장할 수 없다고 보는 보수적 판단).
  4. 최종 컬럼을 `base_date`, `cgg_cd`(=sgg_cd), `cgg_nm`(=sgg_nm), `stdg_cd`(=dong_cd), `stdg_nm`(=dong_nm), `deal_cnt`, `latitude`, `longitude`, `is_exact_location`, `total_thing_amt`, `total_pyeong_amt`로 select하여 저장.
  5. `spark.stop()`으로 세션 종료.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] [동] 단위 최근 90일 거래건수/평균좌표/정확도 플래그/금액·평당가 합계 집계 및 Parquet 저장 구현 완료.
- [x] `avg_thing_amt`/`avg_pyeong_amt`(평균) 대신 `total_thing_amt`/`total_pyeong_amt`(합계) 컬럼을 채택 - 코드 주석에 명시된 설계 결정.
- [ ] [TODO] 명시적 TODO 주석은 없음. `dm_apt_pyeong_price.py`와 마찬가지로 `if __name__ == "__main__":` 가드가 없어 모듈을 임포트만 해도 배치가 즉시 실행되는 구조 - 재사용/테스트 관점에서 개선 여지.
- [ ] [TODO] `run()`에 쓰기 실패에 대한 명시적 예외 처리가 없음 - Airflow 태스크 재시도 정책에 의존.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 다른 `dm_*.py` 마트와 SparkSession을 공유하지 않고 완전히 독립 프로세스로 순차 실행된다(GCP 메모리 제약).
- `is_exact_location`은 `bool_and`(AND 집계)로 계산되므로 그룹 내 단 하나의 단지라도 지오코딩 4단계(법정동 폴백)로 떨어지면 전체 동의 `is_exact_location`이 False가 된다 - 매우 보수적인 기준.
- 좌표는 "동 안 단지들의 평균"이라는 근사값이므로 실제 법정동 중심 좌표와 다를 수 있다(지도 핀 표시용으로만 적합, 정밀 위치 조회 용도 아님).
- `overwrite` 모드로 `base_date` 파티션 경로를 통째로 교체하므로 재실행 시 멱등적이다.

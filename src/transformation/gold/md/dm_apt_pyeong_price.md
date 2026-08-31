# 📄 dm_apt_pyeong_price.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** Gold 마트 ③ `DM_APT_PYEONG_PRICE`를 생성하는 단독 실행 PySpark 배치 스크립트. `dong_pyeong_common.py`가 만들어 둔 공통 정제 데이터프레임(`common_df`)을 [단지 x 평형대(`pyeong_grp`)] 단위로 그룹핑해 최근 90일 거래건수/거래금액 합계/평당가 합계/최근 거래정보를 집계하고, MinIO S3 Lake `mart/dm_apt_pyeong_price/` 경로에 Parquet으로 저장한다. 다른 `dm_*.py` 마트와 SparkSession을 공유하지 않고 완전히 독립된 프로세스로 실행된다(GCP 메모리 제약으로 Airflow가 마트를 순차 실행).
- **현재 구현 상태:** 완료. `build()`(집계 로직)와 `run()`(저장) 함수가 명확히 분리되어 있고, 모듈 최하단에서 CLI 인자 파싱 -> 컨텍스트 생성 -> 실행 -> `spark.stop()`까지 일관되게 처리된다. TODO/FIXME 주석은 없다. 다만 스크립트 최하단부가 모듈 임포트 시점에 바로 실행되는 구조(전역 스코프 실행)라 단위 테스트나 재사용 관점에서는 개선 여지가 있다(5절 참고).

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `transformation.gold.dong_pyeong_common`에서 `APT_GROUP_COLS`(단지 단위 groupBy 키), `GoldMartContext`(dataclass), `apt_select_cols`(공통 select 컬럼 생성 함수), `build_gold_mart_context`(Silver 읽기~지오코딩까지 끝낸 컨텍스트 생성 함수)를 가져와 사용한다.
- **외부 패키지:** `pyspark.sql`(`DataFrame`, `functions as F`), `pyspark.sql.types`(`IntegerType`, `LongType`). 표준 라이브러리로 `sys`, `datetime`을 사용한다. Spark/S3/카카오맵 등 실제 인프라 연동은 전부 `dong_pyeong_common.py`에 위임되어 있고, 이 파일 자체는 순수 DataFrame 집계 로직만 가진다.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `MART_NAME` | 마트 식별자 상수(`"dm_apt_pyeong_price"`). `ctx.mart_paths`에서 저장 경로를 조회하는 키로 쓰인다. | `str` 상수 | N/A |
| `build(ctx)` | `ctx.common_df`를 `APT_GROUP_COLS + pyeong_grp` 기준으로 그룹핑해 거래건수(`deal_cnt`), 거래금액 합계(`total_thing_amt`), 평당가 합계(`total_pyeong_amt`), 최근 거래가/평당가/계약일자/공급평수(`recent_*`, `max_by(값, deal_date)` 패턴)를 집계한다. | `ctx: GoldMartContext` | `DataFrame`(단지 x 평형대 단위 집계 결과) |
| `run(ctx)` | `ctx.mart_paths[MART_NAME]` 경로를 조회해 `build(ctx)` 결과를 `overwrite` 모드로 Parquet 저장하고 완료 로그를 출력한다. | `ctx: GoldMartContext` | `None` (부수효과: S3 Lake에 Parquet 저장) |
| 모듈 최하단 실행부 | `sys.argv[1]`이 있으면 `YYYY-MM-DD`로 파싱해 `_base_date`로 사용, 없으면 오늘 날짜 사용 → `build_gold_mart_context(_base_date)` 호출 → `run(_ctx)` 실행 → `_ctx.spark.stop()`. | CLI 인자 `BASE_DATE`(옵션) | 프로세스 종료(별도 반환값 없음) |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** Airflow DAG(`data_orchestration.py`로 추정, 이 파일 자체에는 DAG 정의 없음)에서 다른 Gold 마트들과 순차 체이닝되어 트리거되는 것으로 보이나, 이 파일 자체는 CLI로 직접 실행 가능한 단독 배치 스크립트다. 별도 cron 설정은 이 파일에 없음(N/A, Airflow 오케스트레이션 파일 쪽 소관).
- **Source (Input):** 직접적인 Silver/S3 읽기는 없고, `dong_pyeong_common.build_gold_mart_context(base_date)`가 반환하는 `GoldMartContext.common_df`(이미 `dim_apartment` + `fact_apt_transactions` 조인, 카카오맵 지오코딩, 평형대/공급평수/평당가 파생까지 끝난 데이터)를 입력으로 사용한다.
- **Target (Output):** `s3a://{LAKE}/mart/dm_apt_pyeong_price/base_date=YYYY-MM-DD` 경로에 Parquet으로 저장(`overwrite` 모드). RDB 적재는 없음.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. CLI 인자로 `BASE_DATE`를 받으면 파싱, 없으면 오늘 날짜 사용.
  2. `build_gold_mart_context(_base_date)` 호출 → SparkSession 기동, Silver 읽기/조인/지오코딩/파생컬럼까지 끝난 `common_df`를 담은 `GoldMartContext` 생성.
  3. `run(_ctx)` 호출 → `build(_ctx)`로 [단지 x 평형대] 집계 수행 → `apt_select_cols(ctx)`(base_date/구·동·단지명/좌표/지번/updated_at)와 `pyeong_grp`, 집계 컬럼을 select하여 최종 스키마 확정 → Parquet 저장.
  4. `_ctx.spark.stop()`으로 SparkSession 종료(다음 마트가 메모리를 확보할 수 있도록 프로세스 자체가 종료됨).

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] [단지 x 평형대] 단위 최근 90일 거래건수/금액합계/평당가합계/최근거래정보 집계 및 Parquet 저장 구현 완료.
- [ ] [TODO] 코드 내 명시적 TODO는 없음. 다만 모듈 최하단 실행부(60~68행)가 `if __name__ == "__main__":` 가드 없이 모듈 최상위 스코프에서 바로 실행되므로, 이 파일을 다른 모듈에서 `import`하면 의도치 않게 배치가 실행된다. 테스트 용이성과 안전한 재사용을 위해 `if __name__ == "__main__":` 가드 추가를 권장한다.
- [ ] [TODO] `run()`이 예외 처리 없이 바로 `write.mode("overwrite").parquet(...)`을 호출하므로, S3 쓰기 실패 시 스택트레이스만 남고 별도 재시도/알림 로직은 없다(Airflow 태스크 재시도 정책에 전적으로 의존).

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 다른 `dm_*.py` 마트 스크립트와 SparkSession을 공유하지 않는다 - GCP e2-standard-2(8GB) 메모리 제약 때문에 Airflow에서 마트를 하나씩 완전히 순차 실행해야 하며, 이 스크립트는 매번 독립적으로 뜨고 죽는다.
- `overwrite` 모드로 `base_date=YYYY-MM-DD` 파티션 경로 전체를 덮어쓰므로, 같은 `base_date`로 재실행해도 멱등적으로 최신 결과만 남는다(누적/중복 없음).
- `common_df`는 이미 거래취소건 제외, 금액/면적 0 이하 제거 등 1차 정제가 끝난 데이터이므로 이 파일 자체에는 별도 데이터 검증 로직이 없다 - 정제 룰이 바뀌면 `dong_pyeong_common.py`를 수정해야 한다.
- `recent_*` 컬럼들은 `F.max_by(값, deal_date)` 패턴으로 "최신 계약일자 기준 값"을 뽑는데, 동일 `deal_date`에 여러 건이 있을 경우 `max_by`의 동점 처리 규칙(Spark 구현에 따름, 명시적 tie-break 없음)에 의존한다.

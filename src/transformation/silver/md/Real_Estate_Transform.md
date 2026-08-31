# 📄 Real_Estate_Transform.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 부동산 실거래가 Bronze(MinIO S3의 parquet, `Real_Estate.py`가 적재)를 읽어 Silver(Apache Iceberg) 테이블 `lakehouse.dim_apartment`(아파트 마스터)와 `lakehouse.fact_apt_transactions`(거래 팩트)로 정제/적재하는 단독 실행 PySpark 배치 스크립트. 오케스트레이션 로직 없이 `sys.argv`로 BULK(전체 백필)/단일 날짜/다중 날짜(콤마 구분) 3가지 모드를 지원한다.
- **현재 구현 상태:** 진행 중이며 다수의 실측 버그 수정 이력이 코드 주석에 상세히 기록되어 있다 — (1) dedup 키에 mno/sno(지번)가 빠져 서로 다른 세대 거래가 잘못 합쳐진 사례(16건 확인) 수정, (2) 가격을 dedup 키에서 제외해 MERGE_CARDINALITY_VIOLATION 방지, (3) fact 테이블 MERGE에 파티션 프루닝 조건을 추가하지 않으면 다중 날짜 배치 중 MinIO 커넥션 폭주로 두 번 잡이 죽은 장애(2026-08-25) 이력, (4) 예전에는 append 방식이라 재실행마다 중복 누적(활성 행 830,427건 중 665,643건이 중복이었음)되던 문제를 MERGE INTO로 전환해 해결. 이런 실측 근거들로 볼 때 핵심 로직은 상당히 성숙했으나, "TODO" 명시 주석은 없지만 하드코딩된 메모리/파티션 튜닝값(VM 스펙 의존)과 자연키 설계가 향후 데이터 특성 변화(예: API가 동/호수를 구분하기 시작하는 경우)에 취약할 수 있는 진행형 코드다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음(다른 프로젝트 내부 Python 모듈을 import하지 않음). 대신 `Real_Estate.py`(Bronze 적재)가 만든 S3 경로 규칙(`real_estate/year=*/month=*/day=*/`)과 컬럼 스키마(`CGG_CD`, `BLDG_USG` 등)에 암묵적으로 의존한다.
- **외부 패키지:** `pyspark.sql`(`SparkSession`, `functions as F`, `broadcast`, `Window`), `pyspark.sql.types.DecimalType`, `pyspark.sql.utils.AnalysisException`, 표준 라이브러리 `os`, `sys`, `datetime`(`datetime`, `timedelta`). 실행 시 Maven Central에서 `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0`, `org.apache.hadoop:hadoop-aws:3.4.1` jar를 자동 다운로드(`spark.jars.packages`).

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| 모드 결정 로직 (모듈 최상단) | `sys.argv[1]`이 `"BULK"`면 전체 기간 와일드카드 처리, `"YYYYMMDD"`(단일) 또는 콤마 구분 다중 날짜면 해당 날짜(들)만 처리, 인자 없으면 어제 날짜 기본값. 날짜 형식은 `datetime.strptime`으로 즉시 검증 | `sys.argv[1]: str` | `BULK_MODE: bool`, `target_dates: list[str]` |
| `_bronze_path_for` | 계약일(YYYYMMDD) 하나의 Bronze S3A 파티션 경로 생성 | `ctrt_day: str` | `str`(`s3a://{RAW_BUCKET}/real_estate/year=.../month=.../day=.../`) |
| `_process_bronze_partition` | Bronze 하나(BULK 와일드카드 또는 특정 날짜)를 읽어 Silver 정제 -> `dim_apartment` MERGE -> `fact_apt_transactions` MERGE까지 끝까지 처리하는 핵심 단위 함수. 날짜별 원본 부재는 `AnalysisException(PATH_NOT_FOUND)`로 감지해 해당 날짜만 스킵 | `bronze_path: str`, `is_bulk: bool`(kw-only), `label: str`(kw-only, 로그/날짜 리터럴용) | `None`(부수효과로 Iceberg 테이블에 MERGE 반영) |
| SparkSession 생성 블록 | Iceberg 카탈로그(`lakehouse`, hadoop 타입) + S3A/S3FileIO 이중 설정으로 MinIO 연동. 드라이버 메모리/셔플 파티션/브로드캐스트 임계값/파일 파티션 크기를 8GB VM 제약에 맞춰 환경변수로 오버라이드 가능하게 구성 | 다수의 `spark.config(...)`, 기본값은 env var로 오버라이드 가능(`SPARK_DRIVER_MEMORY` 등) | `SparkSession` 전역 객체 `spark` |
| 테이블 존재 보장 블록 | `lakehouse.dim_apartment`, `lakehouse.fact_apt_transactions` Iceberg 테이블을 `CREATE TABLE IF NOT EXISTS`로 생성, 기존 `fact_apt_transactions`에 `mno`/`sno` 컬럼이 없으면 `ALTER TABLE ... ADD COLUMNS`로 스키마 진화 | 없음(스크립트 상단에서 1회 실행) | `None` |
| 모드별 실행 블록 (6번) | `BULK_MODE`면 `_process_bronze_partition`을 와일드카드 경로로 1회 호출, 아니면 `target_dates`를 순회하며 날짜마다 호출 | 없음 | `None` |
| 검증 출력 블록 (7번) | 처리 종료 후 `dim_apartment`/`fact_apt_transactions` 상위 5건을 `show()`로 출력, `spark.stop()` | 없음 | `None`(stdout 로그) |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** 이 파일 자체에는 Airflow DAG 정의가 없다(단독 실행 스크립트). 주석상 데일리 배치/BULK 백필/다중 날짜(오케스트레이션의 `task_fetch_real_estate`가 돌려주는 `changed_ctrt_days`) 등 여러 방식으로 외부에서 `python Real_Estate_Transform.py [BULK|YYYYMMDD|YYYYMMDD,...]` 형태로 호출되는 것을 전제로 설계되어 있다.
- **Source (Input):** MinIO(S3A) Bronze parquet — `s3a://{RAW}/real_estate/year=*/month=*/day=*/*.parquet`(BULK) 또는 특정 날짜 파티션. 환경변수 `S3_END_POINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `RAW`, `LAKE` 필요.
- **Target (Output):** Iceberg 테이블 `lakehouse.dim_apartment`(sgg_cd/dong_cd/apt_name 기준 MERGE), `lakehouse.fact_apt_transactions`(deal_date 파티셔닝, 자연키 기준 MERGE).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. 모드 판정(BULK/단일/다중 날짜) 및 Bronze 대상 경로 결정.
  2. SparkSession 1회 기동(Iceberg+S3A 설정), 대상 Iceberg 테이블 존재/스키마(mno, sno 컬럼) 보장.
  3. `_process_bronze_partition`에서: `BLDG_USG == "아파트"` 필터 -> 컬럼 매핑(`CGG_CD`->`sgg_cd` 등) 및 방어적 캐스팅(`try_cast`, 날짜 파싱 실패 시 null) -> `deal_date` null 레코드 제외 -> `price_per_m2` 파생 컬럼 계산 -> `(deal_date, sgg_cd, dong_cd, apt_name, exclusive_area_m2, floor, mno, sno)` 키로 `row_number()` dedup(가격 내림차순으로 최고가 대표 선정).
  4. `dim_apartment_source`를 만들어 `(sgg_cd, dong_cd, apt_name)` 키로 Iceberg `MERGE INTO`(UPDATE SET */INSERT *) 수행.
  5. 방금 MERGE된 최신 `dim_apartment`를 다시 읽어 `broadcast` 조인으로 `fact_apt_transactions_source` 생성.
  6. `fact_apt_transactions`에 자연키(`sgg_cd`, `dong_cd`, `apt_name`, `mno`/`sno`(coalesce NULL-safe), `deal_date`, `floor`, `exclusive_area_m2`) 기준 `MERGE INTO`(가격 정정은 UPDATE, 신규 거래는 INSERT). BULK가 아닐 때는 `target.deal_date = DATE'...'` 리터럴 조건을 추가해 Iceberg 파티션 프루닝을 강제.
  7. 모든 날짜 처리 후 두 테이블 상위 5건을 검증 출력하고 `spark.stop()`.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] BULK/단일 날짜/다중 날짜 3가지 실행 모드 및 SparkSession 1회 재사용 최적화
- [x] mno/sno를 포함한 dedup 키 수정(서로 다른 세대 거래 유실 버그 수정, 16건 확인)
- [x] 가격을 dedup 키/자연키에서 제외하여 MERGE_CARDINALITY_VIOLATION 방지
- [x] fact_apt_transactions를 append에서 MERGE INTO로 전환하여 재실행 시 중복 누적 방지(665,643건 중복 실측 수정)
- [x] 비BULK 모드에서 `target.deal_date` 리터럴 조건으로 Iceberg 파티션 프루닝 강제(MinIO 커넥션 폭주 장애 재발 방지)
- [x] 기존 운영 테이블에 mno/sno 컬럼이 없을 경우 스키마 진화(ADD COLUMNS)로 안전하게 추가
- [ ] [TODO] 코드 내 명시된 TODO 주석은 없음. 개선 제안: 메모리/파티션 튜닝값(3g, 8, 5m, 64m 등)이 특정 VM 스펙(GCP e2-standard-2, 8GB)에 강하게 결합되어 있어, 인프라 변경 시 환경변수 오버라이드를 잊으면 성능/안정성 문제가 재발할 수 있다 — 인프라 스펙과 튜닝값 매핑을 별도 설정 문서로 분리 관리하는 것을 고려할 만하다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- Iceberg 런타임 버전은 `iceberg-spark-runtime-4.0_2.13:1.11.0`으로 고정되어야 한다 — 최신 Spark 4.1/4.2 API와 호환되지 않는 미해결 버그(apache/iceberg#15238)가 있어 Spark 4.0.x 조합을 유지해야 한다.
- 메모리 설정(driver.memory=3g 등)은 "Airflow DAG가 Silver/Gold 태스크를 순차 실행(gold_mart_serial_pool)한다"는 전제하에 이 프로세스가 4.5~5GB 여유 메모리를 혼자 쓴다고 가정한 값이다. 대규모 백필 시에는 `SPARK_DRIVER_MEMORY` 환경변수로 일시적으로 올려야 한다.
- `S3_END_POINT`는 로컬(스킴 없음, MinIO)과 배포(HTTPS 스킴 포함) 양쪽 형식을 지원해야 하므로 SSL 여부를 하드코딩하지 않고 스킴으로 동적 판단한다 — 하드코딩 시 로컬에서 SSL 핸드셰이크 오류가 재현된다.
- `fact_apt_transactions` MERGE 조건에서 `target.deal_date = source.deal_date`만으로는 Iceberg가 정적으로 파티션을 좁히지 못하므로, 비BULK 모드에서는 반드시 `label`(YYYYMMDD)에서 만든 날짜 리터럴 조건을 추가해야 한다 — 빠지면 다중 날짜 배치에서 MinIO 커넥션이 누적되어 장애(Connection refused)가 재현된 이력이 있다(2026-08-25).
- dedup/자연키 설계상 "같은 지번+층+면적+날짜, 가격만 다른 두 행"은 가격이 더 높은 쪽을 대표로 남긴다 — 결정적 규칙이 필요해 채택된 것으로, 실제 발생 빈도는 3.5년 전체 데이터 중 3건으로 극히 드물다고 명시되어 있다.
- 데일리/다중 날짜 모드에서 특정 날짜에 Bronze 원본이 아예 없는 경우 `AnalysisException`의 메시지에 `"PATH_NOT_FOUND"`가 포함되는지로만 판단해 해당 날짜를 건너뛴다 — 그 외 예외는 파이프라인 실패로 전파된다.

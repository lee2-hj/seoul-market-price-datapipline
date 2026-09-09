# -*- coding: utf-8 -*-
"""
부동산 실거래가 Bronze(MinIO S3 JSON) -> Silver(Apache Iceberg) 변환 스크립트
- 단독 실행 가능한 PySpark 배치 스크립트 (오케스트레이션 로직 없음)
"""

import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import DecimalType
from pyspark.errors import AnalysisException
from pyspark.sql.window import Window


# =====================================================================================
# 1. 처리 모드 및 대상 날짜 결정
#    - sys.argv[1] == "BULK": 일별 재기동 없이 전체 기간을 한 번에 백필하는 대용량
#      고속 일괄 처리 모드 (연/월/일 파티션 전체를 와일드카드로 한 번에 스캔)
#    - sys.argv[1] == "YYYYMMDD"(단일): 해당 날짜의 일별 파티션만 처리 (데일리 배치용)
#    - sys.argv[1] == "YYYYMMDD,YYYYMMDD,..."(콤마 구분): 그 날짜들만 반복 처리한다 -
#      data_orchestration.py의 task_fetch_real_estate가 돌려주는 changed_ctrt_days
#      (뒤늦게 신고/정정되어 실제로 바뀐 계약일 목록)를 그대로 이 형식으로 받기 위한 모드다.
#      SparkSession은 BULK와 마찬가지로 1회만 기동해서 날짜 수만큼 재사용한다(날짜마다
#      기동하면 매번 JVM/Iceberg 카탈로그 초기화 비용이 반복돼 느리다).
#    - 인자 없음: 어제 날짜를 기본값으로 사용 (데일리 배치용)
# =====================================================================================
CUTOVER_APARTMENT_KEY_V2 = len(sys.argv) > 1 and sys.argv[1].upper() == "CUTOVER_APARTMENT_KEY_V2"
PREPARE_APARTMENT_KEY_V2 = len(sys.argv) > 1 and sys.argv[1].upper() == "PREPARE_APARTMENT_KEY_V2"
BULK_MODE = len(sys.argv) > 1 and sys.argv[1].upper() == "BULK"
target_dates: list[str] = []  # YYYYMMDD 문자열 목록 (BULK_MODE가 아닐 때만 채워짐)

if CUTOVER_APARTMENT_KEY_V2:
    print("[INFO] 처리 모드: CUTOVER_APARTMENT_KEY_V2")
elif PREPARE_APARTMENT_KEY_V2:
    print("[INFO] 처리 모드: PREPARE_APARTMENT_KEY_V2 (기존 테이블 변경 없음)")
elif BULK_MODE:
    print("[INFO] 처리 모드: BULK (전체 기간 일괄 백필, SparkSession 1회 기동)")
else:
    if len(sys.argv) > 1:
        raw_arg = sys.argv[1].strip()
        target_dates = [d.strip() for d in raw_arg.split(",") if d.strip()]
    else:
        target_dates = [(datetime.now() - timedelta(days=1)).strftime("%Y%m%d")]

    # 형식 검증(YYYYMMDD) - 여기서 한 번에 걸러서, 잘못된 날짜 하나 때문에 한참 뒤(Bronze
    # 경로 조립 시점)에야 알아채는 대신 즉시 명확한 에러로 종료한다.
    for _d in target_dates:
        datetime.strptime(_d, "%Y%m%d")

    if len(target_dates) == 1:
        print(f"[INFO] 처리 모드: 단일 날짜 ({target_dates[0]})")
    else:
        print(f"[INFO] 처리 모드: 다중 날짜 목록 {len(target_dates)}건 (SparkSession 1회 기동): {target_dates}")


# =====================================================================================
# 2. 환경 변수 로드 (MinIO/S3 접속 정보, 버킷 경로)
# =====================================================================================
S3_END_POINT = os.getenv("S3_END_POINT", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
RAW_BUCKET = os.getenv("RAW")
LAKE_BUCKET = os.getenv("LAKE")

LAKE_WAREHOUSE = f"s3a://{LAKE_BUCKET}/"
DIM_APARTMENT_CURRENT_PATH = f"s3a://{LAKE_BUCKET}/dim_apartment_current"
FACT_APT_TRANSACTIONS_CURRENT_PATH = f"s3a://{LAKE_BUCKET}/fact_apt_transactions_current"


def _export_current_dim_apartment() -> None:
    """Iceberg 현재 스냅샷을 DuckDB Gold 작업용 Parquet 스냅샷으로 게시한다."""
    (
        spark.table("lakehouse.dim_apartment")
        .select(
            "apartment_id", "sgg_cd", "sgg_nm", "dong_cd", "dong_nm",
            "apt_name", "mno", "sno", "build_year",
        )
        .write.mode("overwrite")
        .parquet(DIM_APARTMENT_CURRENT_PATH)
    )
    print(f"[INFO] dim_apartment 현재 스냅샷 게시 완료: {DIM_APARTMENT_CURRENT_PATH}")
    (
        spark.table("lakehouse.fact_apt_transactions")
        .withColumn("deal_date_day", F.date_format("deal_date", "yyyy-MM-dd"))
        .write.mode("overwrite")
        .partitionBy("deal_date_day")
        .parquet(FACT_APT_TRANSACTIONS_CURRENT_PATH)
    )
    print(f"[INFO] fact_apt_transactions 현재 스냅샷 게시 완료: {FACT_APT_TRANSACTIONS_CURRENT_PATH}")

# S3_END_POINT는 로컬(MinIO, 스킴 없이 "host:port")과 배포(HTTPS 스킴 포함,
# 예: "https://storage.googleapis.com") 양쪽 형식을 그대로 받는다. SSL 사용 여부는
# 이 스킴으로 판단해야 한다 - 하드코딩하면 로컬(HTTP 전용 MinIO)에서 SSL 핸드셰이크를
# 시도하다가 "Unsupported or unrecognized SSL message" 오류로 실패한다.
S3_USE_SSL = S3_END_POINT.strip().lower().startswith("https://")
ENDPOINT_CLEAN = S3_END_POINT.replace("https://", "").replace("http://", "")

# Iceberg의 S3FileIO는 (Hadoop S3A와 달리) endpoint에 스킴이 붙은 URL을 요구한다.
_S3_SCHEME = "https" if S3_USE_SSL else "http"
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL") or f"{_S3_SCHEME}://{ENDPOINT_CLEAN}"


def _bronze_path_for(ctrt_day: str) -> str:
    """계약일(YYYYMMDD) 하나의 Bronze 파티션 경로. 실제 적재 경로는 RAW 버킷 아래
    real_estate/ 하위에 연/월/일 Hive 스타일로 저장돼 있다."""
    year, month, day = ctrt_day[:4], ctrt_day[4:6], ctrt_day[6:8]
    return f"s3a://{RAW_BUCKET}/real_estate/year={year}/month={month}/day={day}/"


BULK_BRONZE_PATH = f"s3a://{RAW_BUCKET}/real_estate/year=*/month=*/day=*/*.parquet"


# =====================================================================================
# 3. SparkSession 생성 (MinIO S3A 연동 + Iceberg 카탈로그 설정) - 모드에 상관없이 1회만 기동
# =====================================================================================
spark = (
    SparkSession.builder
    .appName("Real_Estate_Bronze_to_Silver")
    # --- Iceberg(카탈로그/실행엔진 확장) + Hadoop-AWS(S3A 파일시스템) 런타임 jar ---
    # SparkSession 실행 시 Maven Central에서 자동 다운로드되어 클래스패스에 추가된다.
    # (버전은 pyspark 4.0.4에 번들된 Hadoop 3.4.1 / Scala 2.13 조합에 맞춘 것.
    #  iceberg-spark-runtime은 현재 Spark 4.1/4.2 API 변경과 호환되지 않는 미해결
    #  버그가 있어(apache/iceberg#15238) Spark 4.0.x 조합으로 고정해야 한다.)
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
        "org.apache.hadoop:hadoop-aws:3.4.1",
    )
    # --- 8GB VM(GCP e2-standard-2, 2 vCPU) 메모리 안전 설정 ---
    # Airflow 상주 프로세스가 이미 2.5~3GB를 쓰고 있어 이 프로세스가 쓸 수 있는 여유 메모리는
    # 4.5~5GB뿐이다. Airflow DAG가 Silver/Gold 태스크를 전부 순차 실행하도록 바꿔서
    # (gold_mart_serial_pool) 이 프로세스 혼자 그 예산을 쓴다는 전제 하에, JVM 오버헤드
    # (Metaspace/스레드 스택/오프힙 등, 보통 heap의 20~40%)까지 감안해 힙 자체는 보수적으로
    # 3g로 잡는다(예전엔 4g였는데, 그때는 이 VM 제약이 없는 환경 기준이었다 - BULK/다중 날짜
    # 모드에서 Parquet 압축 버퍼 등으로 OutOfMemoryError가 났던 걸 고치려고 4g로 올렸던
    # 이력이 있다. 매일 도는 증분 배치는 changed_ctrt_days가 보통 며칠 수준이라 3g로도
    # 충분하지만, 수십~수백 일치를 한 번에 처리하는 대규모 백필은 이 VM을 그 시간 동안
    # 독점해도 되는 시점에 SPARK_DRIVER_MEMORY=5g처럼 일시적으로 올려서 실행할 것 - 그래서
    # 하드코딩 대신 환경변수로 오버라이드 가능하게 뒀다).
    .config("spark.master", os.getenv("SPARK_MASTER", "local[2]"))
    .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
    # 드라이버로 collect()되는 결과 총량 상한 - 넘으면 OOM 대신 SparkException으로 바로
    # 실패해 원인을 알 수 있다(jibun_lookup 등 이 스크립트의 여러 .collect() 호출을 방어).
    .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "1g"))
    # 기본값 200은 로컬 2코어 환경에는 과도하다 - 셔플 파티션마다 스케줄링/버퍼 오버헤드가
    # 붙어 코어 수 대비 파티션이 지나치게 많으면 오히려 메모리를 더 쓴다. vCPU 수(2)의 4배
    # 정도로 낮춘다.
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    # Spark가 자동으로 브로드캐스트하는 임계값을 낮춰, 의도치 않은 큰 테이블이 드라이버
    # 메모리로 수집되다 OOM나는 걸 막는다(코드의 명시적 broadcast() 호출은 이 설정과 무관하게
    # 항상 적용되므로 dim_apartment 브로드캐스트 조인은 그대로 동작한다).
    .config("spark.sql.autoBroadcastJoinThreshold", os.getenv("SPARK_AUTO_BROADCAST_THRESHOLD", "5m"))
    # 파일 스캔 파티션 하나의 최대 크기 - 기본 128m보다 낮춰 파티션당 메모리 사용량을 줄인다
    # (그만큼 파티션/태스크 수는 늘지만, 이 VM에서는 메모리 안전이 처리 속도보다 우선이다).
    .config("spark.sql.files.maxPartitionBytes", os.getenv("SPARK_MAX_PARTITION_BYTES", "64m"))
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", LAKE_WAREHOUSE)
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT_URL)
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakehouse.s3.access-key-id", S3_ACCESS_KEY)
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", S3_SECRET_KEY)
    # AWS SDK v2 클라이언트는 리전이 없으면 초기화에 실패하므로 더미 값을 지정한다
    # (MinIO는 리전을 사용하지 않지만 클라이언트 생성 시 값 자체는 필요하다).
    .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
    # --- MinIO(S3 호환) 접속을 위한 Hadoop S3A 설정 ---
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_CLEAN)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    # Windows 로컬 실행에서는 Hadoop native DLL 불일치로 디스크 버퍼 생성이 실패할 수 있다.
    # S3A 업로드 블록을 메모리에 두어 NativeIO.access0 의존을 피한다.
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if S3_USE_SSL else "false")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # 배포시 오류 해결을 위해 
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# =====================================================================================
# 4. Iceberg 테이블 존재 보장 (dim_apartment/fact_apt_transactions) - 모드/날짜 수와
#    무관하게 딱 한 번만 실행한다. CREATE TABLE IF NOT EXISTS는 멱등적이라 여러 날짜를
#    반복 처리하는 다중 날짜 모드에서 날짜마다 다시 불러도 무해하지만, 굳이 반복할 이유가
#    없어 루프 밖(처리 시작 전)으로 뺐다.
# =====================================================================================
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.dim_apartment (
        sgg_cd     STRING,
        sgg_nm     STRING,
        dong_cd    STRING,
        dong_nm    STRING,
        apt_name   STRING,
        build_year STRING,
        mno        STRING,
        sno        STRING,
        apartment_id STRING
    )
    USING iceberg
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.fact_apt_transactions (
        deal_date           DATE,
        sgg_cd              STRING,
        dong_cd             STRING,
        apt_name            STRING,
        price_ten_thousand  DECIMAL(15,2),
        exclusive_area_m2   DECIMAL(10,2),
        price_per_m2        DECIMAL(15,2),
        floor               INT,
        deal_type           STRING,
        cancel_date         STRING,
        agent_sgg_nm        STRING,
        mno                 STRING,
        sno                 STRING,
        apartment_match_status STRING
    )
    USING iceberg
    PARTITIONED BY (days(deal_date))
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.fact_apt_transactions_quarantine (
        deal_date DATE, sgg_cd STRING, dong_cd STRING, apt_name STRING,
        mno STRING, sno STRING, floor INT, exclusive_area_m2 DECIMAL(10,2),
        apartment_match_status STRING, quarantined_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(deal_date))
""")

# 이미 만들어져 있던(운영 중인) fact_apt_transactions에는 mno/sno 컬럼이 없을 수 있다.
# CREATE TABLE IF NOT EXISTS는 기존 테이블 스키마를 바꾸지 않으므로, 여기서 컬럼 존재
# 여부를 확인해 없을 때만 Iceberg 스키마 진화(ADD COLUMNS)로 추가한다 - 기존 행은
# 전혀 건드리지 않고(새 컬럼은 자동으로 NULL) 매 실행(BULK/데일리/다중 날짜)마다 안전하게 반복 가능하다.
_existing_fact_columns = set(spark.table("lakehouse.fact_apt_transactions").columns)
_required_fact_columns = {
    "mno": "STRING",
    "sno": "STRING",
    "apartment_match_status": "STRING",
}
_missing_fact_columns = {
    name: data_type
    for name, data_type in _required_fact_columns.items()
    if name not in _existing_fact_columns
}
if _missing_fact_columns:
    print("[INFO] fact_apt_transactions에 mno/sno 컬럼이 없어 스키마 진화(ADD COLUMNS)로 추가합니다.")
    spark.sql(f"""
        ALTER TABLE lakehouse.fact_apt_transactions
        ADD COLUMNS (
            {", ".join(f"{name} {data_type}" for name, data_type in _missing_fact_columns.items())}
        )
    """)


def _prepare_apartment_key_v2() -> None:
    """Build non-destructive v2/preview tables before the production cutover."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.dim_apartment_v2 (
            sgg_cd STRING, sgg_nm STRING, dong_cd STRING, dong_nm STRING,
            apt_name STRING, build_year STRING, mno STRING, sno STRING,
            apartment_id STRING
        ) USING iceberg
    """)
    spark.sql("""
        INSERT OVERWRITE lakehouse.dim_apartment_v2
        SELECT
            f.sgg_cd, max(d.sgg_nm), f.dong_cd, max(d.dong_nm), f.apt_name,
            max(d.build_year), f.mno, f.sno,
            sha2(concat_ws('|', coalesce(f.sgg_cd, ''), coalesce(f.dong_cd, ''),
                coalesce(f.apt_name, ''), coalesce(f.mno, ''), coalesce(f.sno, '')), 256)
        FROM lakehouse.fact_apt_transactions f
        LEFT JOIN lakehouse.dim_apartment d
          ON f.sgg_cd = d.sgg_cd
         AND f.dong_cd = d.dong_cd
         AND f.apt_name = d.apt_name
        GROUP BY f.sgg_cd, f.dong_cd, f.apt_name, f.mno, f.sno
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.fact_apt_quality_v2_preview (
            apartment_match_status STRING, row_count BIGINT
        ) USING iceberg
    """)
    spark.sql("""
        INSERT OVERWRITE lakehouse.fact_apt_quality_v2_preview
        SELECT apartment_match_status, count(*) AS row_count
        FROM (
            SELECT CASE
                WHEN coalesce(trim(f.sgg_cd), '') = ''
                  OR coalesce(trim(f.dong_cd), '') = ''
                  OR coalesce(trim(f.apt_name), '') = '' THEN 'INVALID_JOIN_KEY'
                WHEN d.apartment_id IS NULL THEN 'UNMATCHED'
                ELSE 'MATCHED'
            END AS apartment_match_status
            FROM lakehouse.fact_apt_transactions f
            LEFT JOIN lakehouse.dim_apartment_v2 d
              ON f.sgg_cd = d.sgg_cd
             AND f.dong_cd = d.dong_cd
             AND f.apt_name = d.apt_name
             AND coalesce(f.mno, '') = coalesce(d.mno, '')
             AND coalesce(f.sno, '') = coalesce(d.sno, '')
        ) quality
        GROUP BY apartment_match_status
    """)

    source_count_row = spark.sql("""
        SELECT count(*) AS count FROM (
            SELECT DISTINCT sgg_cd, dong_cd, apt_name, mno, sno
            FROM lakehouse.fact_apt_transactions
        )
    """).first()
    if source_count_row is None:
        raise RuntimeError("source_count 조회 결과가 없습니다.")
    source_count = source_count_row["count"]
    v2_count = spark.table("lakehouse.dim_apartment_v2").count()
    if source_count != v2_count:
        raise RuntimeError(f"v2 검증 실패: source={source_count}, v2={v2_count}")
    print(f"[INFO] dim_apartment_v2 검증 완료: {v2_count}건")
    spark.table("lakehouse.fact_apt_quality_v2_preview").show(truncate=False)
    print("[INFO] 기존 dim/fact는 변경하지 않았습니다.")


if PREPARE_APARTMENT_KEY_V2:
    _prepare_apartment_key_v2()
    spark.stop()
    sys.exit(0)


def _cutover_apartment_key_v2() -> None:
    preview = {
        row["apartment_match_status"]: row["row_count"]
        for row in spark.table("lakehouse.fact_apt_quality_v2_preview").collect()
    }
    fact_count = spark.table("lakehouse.fact_apt_transactions").count()
    if preview.get("MATCHED", 0) != fact_count or sum(preview.values()) != fact_count:
        raise RuntimeError(f"전환 중단: preview={preview}, fact_count={fact_count}")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.dim_apartment_backup_20260908
        USING iceberg AS SELECT * FROM lakehouse.dim_apartment
    """)
    backup_count = spark.table("lakehouse.dim_apartment_backup_20260908").count()
    current_count = spark.table("lakehouse.dim_apartment").count()
    if backup_count != current_count:
        raise RuntimeError(f"백업 검증 실패: backup={backup_count}, current={current_count}")

    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.dim_apartment
        USING iceberg AS SELECT * FROM lakehouse.dim_apartment_v2
    """)
    spark.sql("""
        MERGE INTO lakehouse.fact_apt_transactions target
        USING (
            SELECT f.deal_date, f.sgg_cd, f.dong_cd, f.apt_name, f.mno, f.sno,
                   f.floor, f.exclusive_area_m2,
                   CASE
                     WHEN coalesce(trim(f.sgg_cd), '') = ''
                       OR coalesce(trim(f.dong_cd), '') = ''
                       OR coalesce(trim(f.apt_name), '') = '' THEN 'INVALID_JOIN_KEY'
                     WHEN d.apartment_id IS NULL THEN 'UNMATCHED'
                     ELSE 'MATCHED'
                   END AS apartment_match_status
            FROM lakehouse.fact_apt_transactions f
            LEFT JOIN lakehouse.dim_apartment d
              ON f.sgg_cd = d.sgg_cd
             AND f.dong_cd = d.dong_cd
             AND f.apt_name = d.apt_name
             AND coalesce(f.mno, '') = coalesce(d.mno, '')
             AND coalesce(f.sno, '') = coalesce(d.sno, '')
        ) source
        ON target.deal_date = source.deal_date
       AND target.sgg_cd = source.sgg_cd
       AND target.dong_cd = source.dong_cd
       AND target.apt_name = source.apt_name
       AND coalesce(target.mno, '') = coalesce(source.mno, '')
       AND coalesce(target.sno, '') = coalesce(source.sno, '')
       AND target.floor <=> source.floor
       AND target.exclusive_area_m2 <=> source.exclusive_area_m2
        WHEN MATCHED THEN UPDATE SET
          target.apartment_match_status = source.apartment_match_status
    """)
    remaining_nulls = spark.table("lakehouse.fact_apt_transactions").filter(
        F.col("apartment_match_status").isNull()
    ).count()
    if remaining_nulls:
        raise RuntimeError(f"품질 백필 검증 실패: NULL={remaining_nulls}")
    print(f"[INFO] 운영 전환 완료: backup={backup_count}, dim_v2={spark.table('lakehouse.dim_apartment').count()}, fact={fact_count}")


if CUTOVER_APARTMENT_KEY_V2:
    _cutover_apartment_key_v2()
    _export_current_dim_apartment()
    spark.stop()
    sys.exit(0)


# =====================================================================================
# 5. Bronze 원천 하나(BULK 전체 와일드카드 또는 날짜 하나)를 Silver 정제 + dim_apartment
#    MERGE + fact_apt_transactions append까지 끝까지 처리하는 단위 함수.
#    BULK 모드는 이 함수를 한 번(wildcard 경로)만 호출하고, 단일/다중 날짜 모드는 날짜마다
#    이 함수를 반복 호출한다(SparkSession은 3번에서 이미 만든 것을 계속 재사용) - 그래야
#    각 날짜의 dim_apartment MERGE 결과가 바로 다음 날짜의 fact 조인에도 반영된다(아래
#    5-4에서 매번 최신 dim_apartment를 다시 읽는 이유).
# =====================================================================================
def _process_bronze_partition(bronze_path: str, *, is_bulk: bool, label: str) -> None:
    print(f"[INFO] [{label}] Bronze 원천 데이터 경로: {bronze_path}")

    if is_bulk:
        # BULK 모드는 전체 기간을 대상으로 하므로 원본이 없을 걱정이 없어 존재 확인 없이
        # 바로 읽는다.
        bronze_df = spark.read.parquet(bronze_path)
    else:
        # 데일리/다중 날짜 배치 시 특정 날짜에 원본 파일이 아예 없는 경우가 흔하므로,
        # PySpark 공개 API만으로 존재 여부를 판단한다: 경로가 없으면 spark.read.parquet가
        # 던지는 AnalysisException(PATH_NOT_FOUND)만 그 날짜만 건너뛰고(다중 날짜 모드라면
        # 나머지 날짜는 계속 처리), 그 밖의 예외는 파이프라인 실패로 그대로 전파한다.
        # (spark._jvm.org.apache... 같은 Hadoop Java API 직접 접근은 정적 분석기가
        #  동적 속성 체인을 해석하지 못해 오탐을 일으키므로 사용하지 않는다.)
        try:
            bronze_df = spark.read.parquet(bronze_path)
        except AnalysisException as e:
            if "PATH_NOT_FOUND" not in str(e):
                raise
            print(f"[SKIP] [{label}] 해당 날짜에 Bronze 원본 파일이 없어 건너뜁니다: {bronze_path}")
            return

    # -----------------------------------------------------------------------------
    # 5-1. Silver 정제: 아파트 매매 필터링 + 컬럼 매핑 + 타입/포맷 방어적 캐스팅
    # -----------------------------------------------------------------------------
    silver_df = (
        bronze_df
        .filter(F.col("BLDG_USG") == "아파트")
        .select(
            F.col("CGG_CD").cast("string").alias("sgg_cd"),
            F.col("CGG_NM").cast("string").alias("sgg_nm"),
            F.col("STDG_CD").cast("string").alias("dong_cd"),
            F.col("STDG_NM").cast("string").alias("dong_nm"),
            F.col("BLDG_NM").cast("string").alias("apt_name"),
            F.col("ARCH_YR").cast("string").alias("build_year"),
            # 계약일자(YYYYMMDD) -> DATE 방어적 변환 (형식 오류 시 null)
            F.to_date(F.col("CTRT_DAY").cast("string"), "yyyyMMdd").alias("deal_date"),
            # 거래금액(만원) -> DECIMAL(15,2) Safe Casting, null은 0.00 처리
            # (pyspark.sql.functions에 try_cast가 없어 SQL try_cast를 F.expr로 사용)
            F.coalesce(
                F.expr("try_cast(THING_AMT as decimal(15,2))"),
                F.lit(0.00)
            ).alias("price_ten_thousand"),
            # 전용면적(㎡) -> DECIMAL(10,2) Safe Casting
            F.expr("try_cast(ARCH_AREA as decimal(10,2))").alias("exclusive_area_m2"),
            F.expr("try_cast(FLR as int)").alias("floor"),
            F.col("DCLR_SE").cast("string").alias("deal_type"),
            F.col("RTRCN_DAY").cast("string").alias("cancel_date"),
            F.col("OPBIZ_RESTAGNT_SGG_NM").cast("string").alias("agent_sgg_nm"),
            # 지번(본번/부번). 기존 컬럼/로직은 그대로 두고 끝에 추가만 한다 - MNO/SNO는
            # Bronze 원본에는 있었지만 지금까지 Silver에는 반영되지 않았던 필드다.
            # [NULL/빈 문자열 정규화] Bronze 원본은 지번이 없는 거래를 "실제 NULL"과 "빈
            # 문자열('')" 양쪽으로 뒤섞어 내려보낸다. 아래 5-4 중복 제거는 Window
            # partitionBy(...) 로 mno/sno "원본값 그대로" 같은지를 비교하는데(NULL과 ''은
            # 서로 다른 값으로 취급되어 별도 그룹이 됨), 반면 5-6의 fact_apt_transactions
            # MERGE INTO는 ON 절에서 coalesce(mno, '') = coalesce(mno, '')로 NULL-safe하게
            # 비교한다(NULL과 ''을 같은 값으로 취급). 이 두 비교 기준이 서로 다르면, 지번이
            # 없어 mno가 어떤 행은 NULL로 어떤 행은 ''으로 들어온 "서로 다른 두 세대"의 거래가
            # 5-4에서는 별개로 남았다가 5-6의 MERGE에서는 같은 자연키로 오판되어 "타겟 1행에
            # 소스 2행이 매칭"되는 MERGE_CARDINALITY_VIOLATION으로 배치가 죽는다(GCP 운영
            # 환경에서 실제로 재현됨). 여기서 공백/빈 문자열을 NULL로 미리 정규화해 두 비교
            # 기준을 일치시킨다 - 이후 모든 단계(5-4 dedup, dim/geocoding의 mno/sno 처리,
            # 5-6 MERGE)가 항상 같은 "지번 없음" 표현(NULL)만 보게 된다.
            F.nullif(F.trim(F.col("MNO").cast("string")), F.lit("")).alias("mno"),
            F.nullif(F.trim(F.col("SNO").cast("string")), F.lit("")).alias("sno"),
        )
        # 5-2. 계약일자가 유효하지 않은(null) 레코드는 제외
        .filter(F.col("deal_date").isNotNull())
        # 5-3. 파생 컬럼: ㎡당 단가 (면적이 0 또는 null이면 0.00)
        .withColumn(
            "price_per_m2",
            F.when(
                F.col("exclusive_area_m2").isNotNull() & (F.col("exclusive_area_m2") > 0),
                (F.col("price_ten_thousand") / F.col("exclusive_area_m2"))
            ).otherwise(F.lit(0.00)).cast(DecimalType(15, 2))
        )
        # 5-4. 원천 중복 제거: BULK 모드는 여러 날짜 파티션을 한 번에 스캔하므로
        # 동일 거래가 중복 집계되지 않도록 핵심 식별 컬럼 기준으로 중복을 제거한다
        # (단일/다중 날짜 모드는 한 번에 하루치만 읽으므로 사실상 그 하루 안에서의 dedup이다)
        # [버그 수정 1] 이전에는 mno/sno(지번)가 이 키에서 빠져 있어서, 같은 날 같은 단지에서
        # 계약일+가격+전용면적+층이 우연히 완전히 같은 "서로 다른 두 세대"가 거래되면 서로
        # 다른 실거래인데도 하나로 잘못 합쳐져 조용히 유실됐다(실측: 3.5년 전체 데이터 중
        # 16건 확인 - 올림픽파크포레온 2025-10-09, 래미안원베일리 여러 건 등).
        # [버그 수정 2] 반대로 거래가(price_ten_thousand)는 이 키에서 뺐다 - fact_apt_
        # transactions MERGE INTO(5-6)의 자연키(자치구+법정동+단지명+지번+계약일+층+
        # 전용면적)가 가격을 의도적으로 제외하고 "같은 거래의 가격 정정"으로 다루는 것과
        # 반드시 맞춰야 한다. 같은 지번+층+면적+날짜인데 가격만 다른 두 행이 Bronze에 실제로
        # 존재하는 경우가 있는데(API가 동/호수까지는 구분해 주지 않는 한계 + 당일 정정 등),
        # 예전처럼 가격까지 dedup 키에 넣으면 이 두 행이 서로 다른 행으로 남아 뒤이은
        # MERGE INTO 단계에서 "타겟 1행에 소스 2행이 매칭"되는 MERGE_CARDINALITY_VIOLATION
        # 으로 파이프라인 전체가 실패한다(2025-10-17 재처리 중 실제로 재현/확인함:
        # 이편한세상청계센트럴포레/신동아/창신쌍용1). 이 자연키가 겹치는 행이 여럿이면
        # 거래가가 더 높은 쪽을 대표로 남긴다 - 어느 쪽이 최종 신고인지 구분할 타임스탬프가
        # API에 없어 결정적인 규칙이 필요했고, 이런 당일 동일 지번 가격 충돌 자체가 실제로는
        # 극히 드물다(3.5년 전체 데이터 중 3건).
        .withColumn(
            "_dedup_rn",
            F.row_number().over(
                Window.partitionBy(
                    "deal_date", "sgg_cd", "dong_cd", "apt_name",
                    "exclusive_area_m2", "floor", "mno", "sno",
                ).orderBy(F.desc("price_ten_thousand"))
            ),
        )
        .filter(F.col("_dedup_rn") == 1)
        .drop("_dedup_rn")
    ).cache()

    print(f"[INFO] [{label}] Silver 정제 완료 건수: {silver_df.count()}건")

    # -----------------------------------------------------------------------------
    # 5-5. Dimension(dim_apartment) MERGE INTO 방식 Upsert - 고유 키: sgg_cd, dong_cd, apt_name
    # -----------------------------------------------------------------------------
    dim_source_df = silver_df.select(
        "sgg_cd", "sgg_nm", "dong_cd", "dong_nm", "apt_name", "build_year", "mno", "sno"
    ).dropDuplicates(["sgg_cd", "dong_cd", "apt_name", "mno", "sno"]).withColumn(
        "apartment_id",
        F.sha2(F.concat_ws("|", *[
            F.coalesce(F.col(name), F.lit(""))
            for name in ("sgg_cd", "dong_cd", "apt_name", "mno", "sno")
        ]), 256),
    )

    dim_source_df.createOrReplaceTempView("dim_apartment_source")

    # Iceberg MERGE INTO: 키가 일치하면 UPDATE, 없으면 INSERT (삭제 후 재적재 방식 지양)
    # [스키마 드리프트 방어] "UPDATE SET *"/"INSERT *"는 Spark가 대상 테이블의 실제 컬럼
    # 목록을 기준으로 값을 자동 전개하는데, GCP 운영 환경의 lakehouse.dim_apartment
    # Iceberg 테이블이 (과거 실험/수동 작업 등으로) 이 스크립트의 CREATE TABLE DDL에는 없는
    # 여분의 컬럼(예: mno/sno)을 이미 물리적으로 갖고 있으면, source(dim_apartment_source,
    # 6개 컬럼만 보유)에는 그 이름의 컬럼이 없어 "UNRESOLVED_COLUMN" 분석 오류로 배치 전체가
    # 실패한다(실제로 GCP 운영 환경에서 `mno`를 찾을 수 없다는 오류로 재현됨). source/target의
    # 컬럼 구성이 100% 일치한다는 가정에 기대는 "*" 대신, 이 스크립트가 실제로 관리하는 6개
    # 컬럼만 명시적으로 지정한다 - target에 그 외 여분의 컬럼이 있어도 이 MERGE는 그 컬럼을
    # 건드리지 않고(NULL 유지) 항상 안전하게 동작한다.
    spark.sql("""
        MERGE INTO lakehouse.dim_apartment AS target
        USING dim_apartment_source AS source
        ON  target.sgg_cd = source.sgg_cd
        AND target.dong_cd = source.dong_cd
        AND target.apt_name = source.apt_name
        WHEN MATCHED THEN UPDATE SET
            target.sgg_nm = source.sgg_nm,
            target.dong_nm = source.dong_nm,
            target.build_year = source.build_year
        WHEN NOT MATCHED THEN INSERT (sgg_cd, sgg_nm, dong_cd, dong_nm, apt_name, build_year)
            VALUES (source.sgg_cd, source.sgg_nm, source.dong_cd, source.dong_nm, source.apt_name, source.build_year)
    """)

    print(f"[INFO] [{label}] dim_apartment MERGE INTO(Upsert) 완료")

    # -----------------------------------------------------------------------------
    # 5-6. Fact 데이터 생성: 소용량 dim_apartment 마스터를 Broadcast Join하여 Shuffle 최소화.
    #      바로 위에서 MERGE한 결과를 반영해야 하므로 dim_apartment를 여기서 다시 읽는다
    #      (다중 날짜 모드에서 이전 날짜가 새로 추가한 단지를 이번 날짜의 조인에서도
    #      곧바로 찾을 수 있어야 하기 때문 - 함수 진입 시점에 한 번만 읽어두면 안 된다).
    # -----------------------------------------------------------------------------
    # [MERGE_CARDINALITY_VIOLATION 방어 1] dim_apartment는 (sgg_cd, dong_cd, apt_name)
    # 키로 MERGE Upsert되므로 정상적으로는 키당 1행만 있어야 하지만, 이 MERGE 기반 Upsert가
    # 도입되기 전의 운영 데이터(과거 BULK 적재 등)에 이미 같은 키의 중복 행이 남아 있을 수도
    # 있다. 그런 중복이 있으면 아래 브로드캐스트 조인이 그 키 수만큼 fan-out(같은 거래가
    # 여러 행으로 뻥튀기)되어, 결과적으로 fact_df에 "완전히 같은 자연키를 가진 중복 행"이
    # 여러 개 생기고 5-6 MERGE INTO에서 MERGE_CARDINALITY_VIOLATION으로 이어진다. 조인 전에
    # 키 기준으로 한 번 더 dropDuplicates해 이 가능성을 원천 차단한다(정상 상황에서는 이미
    # 유일하므로 완전히 무해한 안전망일 뿐이다).
    dim_apartment_df = spark.table("lakehouse.dim_apartment").dropDuplicates(
        ["sgg_cd", "dong_cd", "apt_name"]
    )

    fact_df = (
        silver_df.alias("f")
        .join(
            broadcast(dim_apartment_df).alias("d"),
            on=[
                F.col("f.sgg_cd") == F.col("d.sgg_cd"),
                F.col("f.dong_cd") == F.col("d.dong_cd"),
                F.col("f.apt_name") == F.col("d.apt_name"),
                F.coalesce(F.col("f.mno"), F.lit("")) == F.coalesce(F.col("d.mno"), F.lit("")),
                F.coalesce(F.col("f.sno"), F.lit("")) == F.coalesce(F.col("d.sno"), F.lit("")),
            ],
            # 거래를 기준으로 보존한다. 마스터 미매칭 여부는 아래 품질 상태로 명시한다.
            how="left",
        )
        .select(
            "f.deal_date",
            "f.sgg_cd",
            "f.dong_cd",
            "f.apt_name",
            "f.price_ten_thousand",
            "f.exclusive_area_m2",
            "f.price_per_m2",
            "f.floor",
            "f.deal_type",
            "f.cancel_date",
            "f.agent_sgg_nm",
            "f.mno",
            "f.sno",
        )
        # [MERGE_CARDINALITY_VIOLATION 방어 2] 위 조인이 (이론상 불가능해야 하지만) 그래도
        # 자연키(자치구+법정동+단지명+지번+계약일+층+전용면적)당 2행 이상을 만들어냈다면,
        # 5-6 MERGE INTO의 ON 절과 똑같은 NULL-safe 키로 마지막에 한 번 더 중복을 제거한다
        # (mno/sno는 위에서 이미 NULL로 정규화됐으므로 raw 비교만으로 5-4와 동일한 결과가
        # 보장된다). 이 조인은 sgg_cd/dong_cd/apt_name 외에는 아무 값도 바꾸지 않으므로,
        # 자연키가 겹치는 행은 100% 동일한 내용일 것으로 기대되지만, 혹시라도 남는 동률은
        # 5-4와 동일하게 거래가가 더 높은 쪽을 대표로 남긴다.
        .withColumn(
            "_dedup_rn",
            F.row_number().over(
                Window.partitionBy(
                    "sgg_cd", "dong_cd", "apt_name", "mno", "sno",
                    "deal_date", "floor", "exclusive_area_m2",
                ).orderBy(F.desc("price_ten_thousand"))
            ),
        )
        .filter(F.col("_dedup_rn") == 1)
        .drop("_dedup_rn")
    )

    # [중복 적재 방지] 예전에는 이 자리에서 fact_df를 조건 없이 append했다. 이 계약일이
    # 뒤늦은 신고/정정 반영을 위해 "변경됨"으로 재감지될 때마다(매일 최근 90일을 다시
    # 스캔하는 fetch_real_estate_recent) 그날의 기존 Silver 행을 지우지 않은 채 그날
    # Bronze 전체를 다시 append해, 같은 실거래가 재실행 횟수만큼 fact_apt_transactions에
    # 중복 누적되는 문제가 있었다(실측: 활성 행 830,427건 중 665,643건이 완전 중복이고,
    # 같은 거래가 최대 8회까지 반복 적재된 사례도 확인됨 - 이 중복이 build_dong_pyeong_mart.py/
    # build_apt_recent_trade_mart.py의 거래건수·매매가 합계를 그대로 부풀렸다).
    # dim_apartment(5-5)와 동일하게 MERGE INTO로 전환해, "같은 거래"를 자연키(자치구+
    # 법정동+단지명+지번+계약일+층+전용면적)로 식별해 있으면 UPDATE(가격 정정 반영),
    # 없으면 INSERT만 하도록 바꾼다. 거래가(price_ten_thousand)는 자연키에서 의도적으로
    # 뺐다 - 같은 거래의 신고가가 나중에 정정되는 게 가장 흔한 "변경" 사례인데, 가격을
    # 키에 포함하면 정정을 "새 거래"로 오판해 옛 값이 지워지지 않고 계속 쌓인다(Gold
    # 마트들의 KEY_COLUMNS/RECORD_KEY_COLUMNS 설계와 동일한 이유 - apt_rtt_mart.py/
    # apt_mkt_trends_mart.py 참고). mno/sno는 비어있는 거래도 있어 coalesce로 NULL-safe
    # 비교한다(SQL에서 NULL은 그 자신과도 같다고 판정되지 않으므로, coalesce 없이 두면
    # mno/sno가 없는 거래가 매 실행마다 새 행으로 잘못 INSERT된다).
    # [파티션 프루닝] fact_apt_transactions는 deal_date로 파티셔닝돼 있지만, 위 ON 절의
    # "target.deal_date = source.deal_date"는 두 런타임 릴레이션 간의 조인 조건이라
    # Iceberg가 이것만으로는 target 파티션을 정적으로 좁히지 못하고(실측: 2026-06-12 하루치를
    # MERGE하는데 2023-12-29 ~ 2026-07-30까지 걸친 무관한 파티션 파일들을 함께 읽는 게
    # 실제로 확인됨), 매 반복(날짜)마다 테이블 전체(수백 개 파일)에 대해 새로 S3 커넥션을
    # 열게 된다. BULK 모드가 아닌 이상 이번에 처리 중인 계약일은 이미 알고 있으므로(Bronze가
    # day=파티션=계약일 기준으로 적재되어 source의 deal_date는 always 이 값과 같다 - 위
    # 5-6 도입부 주석 참고), 이를 리터럴 조건으로 명시해 target 스캔을 해당 날짜 파티션
    # 하나로 정적 프루닝한다. 날짜 수가 많아질수록(다중 날짜/데일리 배치) 반복마다 열리는
    # S3 커넥션 수가 누적되어, 실제로 이 프루닝이 빠진 상태에서는 86일 배치 중 16번째
    # 날짜(2026-06-12) 근처에서 MinIO가 누적된 커넥션 폭주를 감당하지 못해 Connection
    # refused로 전체 Spark 잡이 두 번 연속 죽는 장애가 있었다(2026-08-25).
    _target_date_filter = ""
    if not is_bulk:
        _deal_date_literal = f"{label[:4]}-{label[4:6]}-{label[6:8]}"
        _target_date_filter = f"AND target.deal_date = DATE'{_deal_date_literal}'\n        "

    # [스키마 드리프트 방어] dim_apartment MERGE와 동일한 이유로 "UPDATE SET *"/"INSERT *"
    # 대신 이 스크립트가 실제로 관리하는 컬럼을 명시적으로 나열한다 - fact_apt_transactions는
    # 이미 위 4번에서 mno/sno 컬럼 존재를 스스로 보장하므로 현재는 안전하지만, 앞으로 물리
    # 테이블에 이 스크립트가 모르는 컬럼이 추가되더라도(운영 환경 스키마 드리프트) 이 MERGE가
    # source에 없는 컬럼을 억지로 참조하다 실패하는 일이 없도록 dim_apartment와 동일한
    # 방어 스타일로 통일한다.
    fact_df.createOrReplaceTempView("fact_apt_transactions_source")
    spark.sql(f"""
        MERGE INTO lakehouse.fact_apt_transactions AS target
        USING fact_apt_transactions_source AS source
        ON  target.sgg_cd = source.sgg_cd
        AND target.dong_cd = source.dong_cd
        AND target.apt_name = source.apt_name
        AND coalesce(target.mno, '') = coalesce(source.mno, '')
        AND coalesce(target.sno, '') = coalesce(source.sno, '')
        AND target.deal_date = source.deal_date
        AND target.floor = source.floor
        AND target.exclusive_area_m2 = source.exclusive_area_m2
        {_target_date_filter}WHEN MATCHED THEN UPDATE SET
            target.price_ten_thousand = source.price_ten_thousand,
            target.price_per_m2 = source.price_per_m2,
            target.deal_type = source.deal_type,
            target.cancel_date = source.cancel_date,
            target.agent_sgg_nm = source.agent_sgg_nm
        WHEN NOT MATCHED THEN INSERT (
            deal_date, sgg_cd, dong_cd, apt_name, price_ten_thousand, exclusive_area_m2,
            price_per_m2, floor, deal_type, cancel_date, agent_sgg_nm, mno, sno
        ) VALUES (
            source.deal_date, source.sgg_cd, source.dong_cd, source.apt_name,
            source.price_ten_thousand, source.exclusive_area_m2, source.price_per_m2,
            source.floor, source.deal_type, source.cancel_date, source.agent_sgg_nm,
            source.mno, source.sno
        )
    """)

    print(f"[INFO] [{label}] fact_apt_transactions MERGE INTO(Upsert) 완료")

    silver_df.unpersist()


# =====================================================================================
# 6. 모드별로 처리 대상 Bronze 경로를 결정해 5번 함수를 호출한다.
# =====================================================================================
if BULK_MODE:
    _process_bronze_partition(BULK_BRONZE_PATH, is_bulk=True, label="BULK")
else:
    for _ctrt_day in target_dates:
        _process_bronze_partition(_bronze_path_for(_ctrt_day), is_bulk=False, label=_ctrt_day)


# =====================================================================================
# 7. 적재 결과 검증 출력 (각 테이블 상위 5건) - 모드/날짜 수와 무관하게 전체 처리가
#    끝난 뒤 한 번만 출력한다.
# =====================================================================================
print("\n===== [VALIDATION] lakehouse.dim_apartment 상위 5건 =====")
spark.table("lakehouse.dim_apartment").show(5, truncate=False)

print("\n===== [VALIDATION] lakehouse.fact_apt_transactions 상위 5건 =====")
spark.table("lakehouse.fact_apt_transactions").show(5, truncate=False)

_export_current_dim_apartment()
spark.stop()

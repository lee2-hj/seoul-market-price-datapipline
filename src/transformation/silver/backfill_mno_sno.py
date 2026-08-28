# -*- coding: utf-8 -*-
"""
1회성 백필: 이미 적재되어 있는 lakehouse.fact_apt_transactions 기존 행에
mno/sno(지번) 값만 채워 넣는 스크립트.

- Real_Estate_Transform.py의 Silver 변환 로직(필터/컬럼 매핑/캐스팅/dropDuplicates 키)을
  그대로 재사용해서 Bronze를 다시 정제하고, 기존 행과 동일한 키로 매칭되는 행에 대해서만
  mno/sno "두 컬럼만" UPDATE한다. 다른 컬럼은 절대 건드리지 않고, 새 행을 INSERT하지도
  않는다(순수 추가/보강 목적 - 충돌/중복 위험 없이 기존 데이터에 지번만 얹는다).
- Real_Estate_Transform.py 자체의 로직/스키마/파이프라인 동작은 변경하지 않는다
  (이 스크립트는 그 결과 위에 한 번 더 mno/sno만 보강하는 독립 실행 스크립트).

실행 방법:
  python src/transformation/silver/backfill_mno_sno.py
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =====================================================================================
# 1. 환경 변수 로드 (build_dong_pyeong_mart.py와 동일한 방식 - 로컬/Airflow 컨테이너 겸용)
# =====================================================================================
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_ENV_CANDIDATES = [
    _PROJECT_ROOT / "env" / ".env",
    Path("/opt/airflow/project/env/.env"),
]
for _env_path in _ENV_CANDIDATES:
    if _env_path.exists():
        load_dotenv(dotenv_path=_env_path, override=False)
        break

S3_END_POINT = os.getenv("S3_END_POINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
RAW_BUCKET = os.getenv("RAW")
LAKE_BUCKET = os.getenv("LAKE")

BRONZE_PATH = f"s3a://{RAW_BUCKET}/real_estate/year=*/month=*/day=*/*.parquet"
LAKE_WAREHOUSE = f"s3a://{LAKE_BUCKET}/"

# S3_END_POINT는 로컬(MinIO, 스킴 없이 "host:port")과 배포(HTTPS 스킴 포함,
# 예: "https://storage.googleapis.com") 양쪽 형식을 그대로 받는다. SSL 사용 여부는
# 이 스킴으로 판단해야 한다 - 하드코딩하면 로컬(HTTP 전용 MinIO)에서 SSL 핸드셰이크를
# 시도하다가 "Unsupported or unrecognized SSL message" 오류로 실패한다.
S3_USE_SSL = (S3_END_POINT or "").strip().lower().startswith("https://")
ENDPOINT_CLEAN = (S3_END_POINT or "").replace("https://", "").replace("http://", "")
_S3_SCHEME = "https" if S3_USE_SSL else "http"
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL") or f"{_S3_SCHEME}://{ENDPOINT_CLEAN}"


# =====================================================================================
# 2. SparkSession 생성 (Real_Estate_Transform.py와 동일한 Iceberg/S3A 설정)
# =====================================================================================
spark = (
    SparkSession.builder
    .appName("Real_Estate_Backfill_Mno_Sno")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
        "org.apache.hadoop:hadoop-aws:3.4.1",
    )
    # 8GB VM(GCP e2-standard-2) 메모리 안전 설정 - Real_Estate_Transform.py와 동일한 값.
    # 이 스크립트는 1회성 수동 백필용이라 다른 Gold/Silver 태스크와 동시에 돌 일이 거의
    # 없지만, 그래도 힙을 무제한으로 두지 않는다(환경변수로 오버라이드 가능).
    .config("spark.master", os.getenv("SPARK_MASTER", "local[2]"))
    .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
    .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "1g"))
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    .config("spark.sql.autoBroadcastJoinThreshold", os.getenv("SPARK_AUTO_BROADCAST_THRESHOLD", "5m"))
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
    .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_CLEAN)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if S3_USE_SSL else "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# =====================================================================================
# 3. 대상 테이블 mno/sno 컬럼 보장 (Real_Estate_Transform.py와 동일한 스키마 진화 로직)
# =====================================================================================
_existing_columns = set(spark.table("lakehouse.fact_apt_transactions").columns)
if "mno" not in _existing_columns or "sno" not in _existing_columns:
    print("[INFO] fact_apt_transactions에 mno/sno 컬럼이 없어 스키마 진화(ADD COLUMNS)로 추가합니다.")
    spark.sql("""
        ALTER TABLE lakehouse.fact_apt_transactions
        ADD COLUMNS (mno STRING, sno STRING)
    """)

before_null_cnt = spark.sql(
    "SELECT count(*) AS c FROM lakehouse.fact_apt_transactions WHERE mno IS NULL"
).collect()[0]["c"]
total_cnt = spark.table("lakehouse.fact_apt_transactions").count()
print(f"[INFO] 백필 시작 전: 전체 {total_cnt}건 중 mno가 비어있는 행 {before_null_cnt}건")


# =====================================================================================
# 4. Bronze 전체를 Real_Estate_Transform.py와 동일한 규칙으로 재정제 (mno/sno 포함)
#    - 필터/컬럼 매핑/캐스팅/dropDuplicates 키를 그대로 복제해, 기존 fact 행과
#      1:1로 매칭되는 소스를 만든다 (Real_Estate_Transform.py 자체는 건드리지 않음).
# =====================================================================================
bronze_df = spark.read.parquet(BRONZE_PATH)

source_df = (
    bronze_df
    .filter(F.col("BLDG_USG") == "아파트")
    .select(
        F.col("CGG_CD").cast("string").alias("sgg_cd"),
        F.col("STDG_CD").cast("string").alias("dong_cd"),
        F.col("BLDG_NM").cast("string").alias("apt_name"),
        F.to_date(F.col("CTRT_DAY").cast("string"), "yyyyMMdd").alias("deal_date"),
        F.coalesce(
            F.expr("try_cast(THING_AMT as decimal(15,2))"),
            F.lit(0.00)
        ).alias("price_ten_thousand"),
        F.expr("try_cast(ARCH_AREA as decimal(10,2))").alias("exclusive_area_m2"),
        F.expr("try_cast(FLR as int)").alias("floor"),
        F.col("MNO").cast("string").alias("mno"),
        F.col("SNO").cast("string").alias("sno"),
    )
    .filter(F.col("deal_date").isNotNull())
    .dropDuplicates([
        "deal_date", "sgg_cd", "dong_cd", "apt_name",
        "price_ten_thousand", "exclusive_area_m2", "floor",
    ])
    # 같은 키에 mno가 여러 개일 수는 이론상 없지만(dropDuplicates가 이미 걸렀음),
    # 방어적으로 mno가 있는 행만 소스로 남긴다(없는 값으로 UPDATE해봐야 의미 없음).
    .filter(F.col("mno").isNotNull() & (F.trim(F.col("mno")) != ""))
)

source_df.createOrReplaceTempView("mno_sno_backfill_source")


# =====================================================================================
# 5. MERGE INTO: 키가 일치하는 기존 행에 한해 mno/sno "두 컬럼만" UPDATE.
#    새 행 INSERT는 하지 않는다(순수 보강 목적 - 신규 적재는 기존 일별 파이프라인의 몫).
# =====================================================================================
spark.sql("""
    MERGE INTO lakehouse.fact_apt_transactions AS target
    USING mno_sno_backfill_source AS source
    ON  target.deal_date IS NOT DISTINCT FROM source.deal_date
    AND target.sgg_cd IS NOT DISTINCT FROM source.sgg_cd
    AND target.dong_cd IS NOT DISTINCT FROM source.dong_cd
    AND target.apt_name IS NOT DISTINCT FROM source.apt_name
    AND target.price_ten_thousand IS NOT DISTINCT FROM source.price_ten_thousand
    AND target.exclusive_area_m2 IS NOT DISTINCT FROM source.exclusive_area_m2
    AND target.floor IS NOT DISTINCT FROM source.floor
    WHEN MATCHED AND target.mno IS NULL THEN
        UPDATE SET target.mno = source.mno, target.sno = source.sno
""")


# =====================================================================================
# 6. 결과 검증 출력
# =====================================================================================
after_null_cnt = spark.sql(
    "SELECT count(*) AS c FROM lakehouse.fact_apt_transactions WHERE mno IS NULL"
).collect()[0]["c"]
after_total_cnt = spark.table("lakehouse.fact_apt_transactions").count()

print(f"[INFO] 백필 완료 후: 전체 {after_total_cnt}건 중 mno가 비어있는 행 {after_null_cnt}건")
print(f"[INFO] 신규로 mno/sno가 채워진 행: {before_null_cnt - after_null_cnt}건")
if after_total_cnt != total_cnt:
    print(
        f"[WARN] 전체 행 수가 백필 전후로 달라졌습니다({total_cnt} -> {after_total_cnt}). "
        "MERGE는 UPDATE만 수행했으므로 원래는 행 수가 절대 변하면 안 됩니다 - 확인 필요."
    )
else:
    print("[INFO] 전체 행 수 불변 확인 완료 (UPDATE만 수행됨, 행 추가/삭제 없음)")

print("\n===== [VALIDATION] mno/sno 채워진 행 상위 5건 =====")
spark.sql(
    "SELECT deal_date, sgg_cd, dong_cd, apt_name, mno, sno "
    "FROM lakehouse.fact_apt_transactions WHERE mno IS NOT NULL LIMIT 5"
).show(truncate=False)


# =====================================================================================
# 7. 스냅샷 정리: MERGE의 UPDATE는 Iceberg에서 copy-on-write로 동작해, 바뀐 파티션의 데이터
#    파일을 통째로 새로 쓰고 이전 파일은 "이전 스냅샷"으로만 남긴다. 이전 스냅샷을 만료시키지
#    않으면 mno/sno 컬럼이 없는 옛 파일이 S3에 그대로 남아있게 되는데, 이 프로젝트의 일부
#    다운스트림 스크립트(pipeline_apt_name.py 등)는 Iceberg 메타데이터를 거치지 않고
#    data/**/*.parquet를 DuckDB로 직접 글롭해서 읽기 때문에, 남아있는 옛 스키마 파일과
#    섞여 스키마 불일치 오류가 난다. 그래서 백필 직후 이 배치 안에서 바로 만료시킨다
#    (retain_last=1: 지금 막 만든 최신 스냅샷 하나만 남기고 이전 스냅샷은 전부 정리).
# =====================================================================================
print("[INFO] 이전 스냅샷 정리(expire_snapshots) 실행 중...")
spark.sql("""
    CALL lakehouse.system.expire_snapshots(
        table => 'lakehouse.fact_apt_transactions',
        older_than => now(),
        retain_last => 1
    )
""").show(truncate=False)
print("[INFO] 스냅샷 정리 완료 (mno/sno 없는 옛 데이터 파일 제거됨)")

spark.stop()

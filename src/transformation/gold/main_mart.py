# -*- coding: utf-8 -*-
"""
부동산 실거래가 Silver(Iceberg: dim_apartment, fact_apt_transactions)
-> Gold(데이터 마트, dm_main) 생성 스크립트
- 단독 실행 가능한 PySpark 배치 스크립트 (오케스트레이션 로직 없음, build_dong_pyeong_mart.py /
  build_apt_recent_trade_mart.py와 동일한 스타일)
- 적재 대상은 S3 Lake(MinIO)의 mart/ 경로뿐이다. MySQL/PostgreSQL 등 RDB에는 적재하지 않는다.
- Gold 레이어의 다른 마트들보다 먼저 도는 "최우선" 마트로, [자치구 x 법정동 x 단지 x 거래일자 x
  면적] 단위의 원자적(atomic) 집계를 제공한다(하위 마트들이 필요로 하는 좌표/지번 등 공통
  정제 데이터를 가장 먼저 만들어 둔다는 취지).

실행 방법:
  python src/transformation/gold/main_mart.py [BASE_DATE]
    - BASE_DATE(YYYY-MM-DD) 생략 시 오늘 날짜를 기준으로 최근 90일치를 집계한다.
    - 특정 과거 기준일로 다시 돌리고 싶으면 인자로 넘기면 된다(멱등적으로 그 base_date=
      경로만 덮어씀 - 아래 "8. 저장" 참고).
"""

import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from transformation.gold.adaptive_lookback import load_fact_with_adaptive_lookback
from utils.geocode_cache import load_geocode_cache, save_geocode_cache, split_cached_and_missing
from utils.spark_partition_upsert import upsert_spark_partition


# =====================================================================================
# 1. 집계 기준일(BASE_DATE) 및 조회 기간 결정
#    - sys.argv[1] == "YYYY-MM-DD": 해당 날짜를 기준일로 사용 (재실행/백필용)
#    - 인자 없음: 오늘 날짜를 기준일로 사용 (일별 배치용)
#    - 조회 기간은 BASE_DATE 포함 최근 LOOKBACK_DAYS일(기본 90일)
# =====================================================================================
LOOKBACK_DAYS = 90

if len(sys.argv) > 1:
    base_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
else:
    base_date = datetime.now().date()

start_date = base_date - timedelta(days=LOOKBACK_DAYS - 1)
BASE_DATE_STR = base_date.strftime("%Y-%m-%d")

print(
    f"[INFO] Gold 마트 dm_main 집계 시작: "
    f"base_date={BASE_DATE_STR}, 조회기간={start_date} ~ {base_date} ({LOOKBACK_DAYS}일)"
)


# =====================================================================================
# 2. 환경 변수 로드 (MinIO/S3 접속 정보, LAKE 버킷, 카카오맵 API 키)
#    - python-dotenv로 env/.env를 직접 로드한다 (로컬 실행: <project_root>/env/.env,
#      Airflow 컨테이너 실행: /opt/airflow/project/env/.env). override=False로 로드해,
#      호출하는 쪽(Airflow env_file 등)이 이미 채워둔 환경 변수는 덮어쓰지 않는다.
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
LAKE_BUCKET = os.getenv("LAKE")
KAKAO_MAP_REST_API_KEY = os.getenv("KAKAO_MAP_REST_API_KEY")

if not KAKAO_MAP_REST_API_KEY:
    print("[WARN] KAKAO_MAP_REST_API_KEY가 설정되지 않았습니다. 좌표는 전부 NULL로 채워집니다.")

LAKE_WAREHOUSE = f"s3a://{LAKE_BUCKET}/"

# S3_END_POINT는 로컬(MinIO, 스킴 없이 "host:port")과 배포(HTTPS 스킴 포함,
# 예: "https://storage.googleapis.com") 양쪽 형식을 그대로 받는다. SSL 사용 여부는
# 이 스킴으로 판단해야 한다 - 하드코딩하면 로컬(HTTP 전용 MinIO)에서 SSL 핸드셰이크를
# 시도하다가 "Unsupported or unrecognized SSL message" 오류로 실패한다.
S3_USE_SSL = (S3_END_POINT or "").strip().lower().startswith("https://")
ENDPOINT_CLEAN = (S3_END_POINT or "").replace("https://", "").replace("http://", "")
# Iceberg의 S3FileIO는 (Hadoop S3A와 달리) endpoint에 스킴이 붙은 URL을 요구한다.
S3_ENDPOINT_URL = f"{'https' if S3_USE_SSL else 'http'}://{ENDPOINT_CLEAN}"

# 저장 경로: {S3_END_POINT}/{LAKE}/mart/dm_main/base_date=YYYY-MM-DD
MART_PATH = f"s3a://{LAKE_BUCKET}/mart/dm_main/base_date={BASE_DATE_STR}"


# =====================================================================================
# 3. SparkSession 생성 (build_dong_pyeong_mart.py와 동일한 Iceberg/S3A 설정 - dim_apartment/
#    fact_apt_transactions가 이 카탈로그로 이미 적재돼 있으므로 그대로 맞춘다)
# =====================================================================================
spark = (
    SparkSession.builder
    .appName("Real_Estate_Gold_Main_Mart")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
        "org.apache.hadoop:hadoop-aws:3.4.1",
    )
    # --- 8GB VM(GCP e2-standard-2, 2 vCPU) 메모리 안전 설정 - Real_Estate_Transform.py와
    # 동일한 값. Airflow DAG가 Gold 태스크를 전부 순차 실행하도록 바꿔서(gold_mart_serial_pool)
    # 이 프로세스 혼자 여유 메모리(4.5~5GB)를 쓴다는 전제로, JVM 오버헤드까지 감안해 힙은
    # 보수적으로 3g로 잡는다(환경변수로 오버라이드 가능). ---
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
# 4. Silver 레이어 읽기 (원천 로딩 최적화) + 5. 1차 정제(데이터 품질 필터) + 적응형 조회기간
#    폴백 (dim_apartment에는 있지만 기본 90일 구간에는 거래가 없는 단지 보강)
#    - dim_apartment: 소용량 마스터 테이블이라 브로드캐스트 조인 대상으로 쓸 컬럼만 선별한다.
#    - fact_apt_transactions: load_fact_with_adaptive_lookback()이 우선 deal_date로
#      필터링하고 필요한 컬럼만 프로젝션한다(빠른 경로). 이 테이블은 PARTITIONED BY
#      (days(deal_date))로 만들어져 있어서(Real_Estate_Transform.py 참고), deal_date 조건이
#      Iceberg의 파티션 프루닝에 그대로 활용되어 최근 90일 day 파티션만 실제로 읽힌다.
#      dim_apartment에는 등록돼 있지만 이 90일 구간에 거래가 하나도 없는 단지("장기 미거래
#      단지")는, 그 단지에 한해서만 전체 이력에서 자신의 최신 거래일자를 찾아 그 날짜 기준
#      최근 90일을 추가로 보강한다(adaptive_lookback.py 모듈 docstring 참고) - 그래야
#      dim_apartment에 단지명은 있는데 프론트에서 조회할 데이터가 없는 문제가 발생하지 않는다.
#      mno/sno는 Silver 스키마 진화(ADD COLUMNS)로 이미 fact_apt_transactions에 들어 있으므로,
#      Bronze 원본을 별도로 다시 스캔할 필요가 없다.
#    - 거래취소건 제외 + 금액/면적 0 이하 제거(데이터 오염 방지)는
#      load_fact_with_adaptive_lookback() 내부에서 이미 적용되어 반환된다.
# =====================================================================================
dim_apartment_df = spark.table("lakehouse.dim_apartment").select(
    "sgg_cd", "sgg_nm", "dong_cd", "dong_nm", "apt_name"
)

fact_df = load_fact_with_adaptive_lookback(
    spark,
    dim_apartment_df=dim_apartment_df,
    fact_table="lakehouse.fact_apt_transactions",
    base_date=base_date,
    start_date=start_date,
    lookback_days=LOOKBACK_DAYS,
    extra_select_cols=("mno", "sno"),
)


# =====================================================================================
# 6. dim_apartment 브로드캐스트 조인 강제 적용 (소용량 마스터 테이블을 브로드캐스트해서
#    대용량 fact_apt_transactions와의 Shuffle을 없앤다 - broadcast() 힌트로 명시적으로
#    강제하므로 spark.sql.autoBroadcastJoinThreshold 설정값과 무관하게 항상 적용된다.)
#    조인 키: sgg_cd + dong_cd + apt_name (dim_apartment의 고유키와 동일)
#    + 건물명/동명 TRIM 공백 정제 (원천 데이터의 앞뒤 공백이 지오코딩 주소 조합과
#      그룹핑 키 양쪽에 그대로 섞여 들어가지 않도록 조인 직후 한 번만 정리한다).
# =====================================================================================
joined_df = (
    fact_df.alias("f")
    .join(
        broadcast(dim_apartment_df).alias("d"),
        on=[
            F.col("f.sgg_cd") == F.col("d.sgg_cd"),
            F.col("f.dong_cd") == F.col("d.dong_cd"),
            F.col("f.apt_name") == F.col("d.apt_name"),
        ],
        how="inner",
    )
    .select(
        F.col("f.sgg_cd").alias("sgg_cd"),
        F.col("d.sgg_nm").alias("sgg_nm"),
        F.col("f.dong_cd").alias("dong_cd"),
        F.trim(F.col("d.dong_nm")).alias("dong_nm"),
        F.trim(F.col("d.apt_name")).alias("apt_name"),
        F.col("f.price_ten_thousand").alias("price_ten_thousand"),
        F.col("f.exclusive_area_m2").alias("exclusive_area_m2"),
        F.col("f.deal_date").alias("deal_date"),
        F.col("f.mno").alias("mno"),
        F.col("f.sno").alias("sno"),
    )
).cache()  # 아래 7번(지오코딩용 distinct collect)과 8번(최종 집계)이 이 결과를 재사용하므로
           # 캐싱하지 않으면 원천 읽기+조인이 두 번 실행된다.


# =====================================================================================
# 7. 카카오맵 API 지오코딩 - 위/경도(LATITUDE, LONGITUDE) 부여
#    3단계 Fallback 파이프라인 (dim_apartment에는 지번이 없으므로 단지명 검색 위주):
#      1단계: 지번(JIBUN) + 정제 단지명(CLEAN_BLDG_NM) 키워드 검색 -> 주소/카테고리 검증
#      2단계: 지번 전용 주소 검색(address.json) -> 필지 좌표
#      3단계: 정제 단지명 키워드 검색 -> 주소/카테고리 검증
#      4단계: 법정동(CGG_NM+STDG_NM) 대표 좌표 (최종 Fallback, 정확도 낮음)
#    지번(mno/sno)은 fact_apt_transactions에 거래건마다 실려 있으므로, 단지(sgg_cd+dong_cd+
#    apt_name)별로 가장 흔하게 등장하는 (mno, sno) 조합을 대표 지번으로 뽑아 지오코딩
#    검색어로 쓴다(Bronze 원본을 다시 스캔하지 않고 이미 읽은 fact_df만으로 계산).
# =====================================================================================
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_KAKAO_KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
_KAKAO_ADDRESS_URL = "https://dapi.kakao.com/v2/local/search/address.json"
_geocode_session = requests.Session()
_geocode_retry = Retry(
    total=3, backoff_factor=1.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
_geocode_session.mount("https://", HTTPAdapter(max_retries=_geocode_retry))  # pyrefly: ignore[bad-argument-type]

# (sgg_nm, dong_nm, jibun, bldg_nm) 고유 키 기준 메모리 캐시 - 동일 단지 재요청을 100% 차단
# (요구사항 2번 "단지별 중복 호출 방지").
_geocode_cache: dict[tuple, tuple] = {}

_ROMAN_NUMERAL_MAP = {"Ⅰ": "1", "Ⅱ": "2", "Ⅲ": "3", "Ⅳ": "4", "Ⅴ": "5"}
_TRAILING_APT_RE = re.compile(r"아파트$")
_NOISE_CHARS_RE = re.compile(r"[-_,.~]")
_PAREN_RE = re.compile(r"\s*\([^)]*\)")
_MULTI_SPACE_RE = re.compile(r"\s+")


def _clean_bldg_nm(name: str | None) -> str:
    """단지명(BLDG_NM)을 카카오 검색어로 쓰기 좋게 정제 (괄호 제거, 로마자 변환,
    특수문자 공백화, 말미 '아파트' 접미사 제거, 공백 정리)."""
    if not name:
        return ""
    cleaned = name
    for roman, digit in _ROMAN_NUMERAL_MAP.items():
        cleaned = cleaned.replace(roman, digit)
    cleaned = _PAREN_RE.sub("", cleaned)
    cleaned = _NOISE_CHARS_RE.sub(" ", cleaned)
    cleaned = _MULTI_SPACE_RE.sub(" ", cleaned).strip()
    if cleaned != "아파트":
        stripped = _TRAILING_APT_RE.sub("", cleaned).strip()
        if stripped:  # 브랜드명 전체가 '...아파트'로만 구성된 경우는 원본을 유지
            cleaned = stripped
    return cleaned


def _format_jibun(mno, sno) -> str | None:
    """MNO(본번)/SNO(부번) 원본 문자열(예: "0022"/"0000")을 "22" 또는 "22-1" 형태로 변환.
    본번이 없거나 0이면(유효한 지번이 아니므로) None을 돌려준다."""
    try:
        mno_int = int(mno)
    except (TypeError, ValueError):
        return None
    if mno_int <= 0:
        return None
    try:
        sno_int = int(sno) if sno not in (None, "") else 0
    except (TypeError, ValueError):
        sno_int = 0
    return f"{mno_int}-{sno_int}" if sno_int > 0 else str(mno_int)


def _kakao_search(url: str, query: str) -> list[dict]:
    response = _geocode_session.get(
        url,
        headers={"Authorization": f"KakaoAK {KAKAO_MAP_REST_API_KEY}"},
        params={"query": query},
        timeout=5,
    )
    response.raise_for_status()
    return response.json().get("documents") or []


def _is_apartment_category(doc: dict) -> bool:
    return doc.get("category_group_code") == "PM9" or "아파트" in (doc.get("category_name") or "")


def _address_matches_dong(doc: dict, sgg_nm: str, dong_nm: str) -> bool:
    addr = (doc.get("address_name") or "") + (doc.get("road_address_name") or "")
    return sgg_nm in addr and dong_nm in addr


def _first_verified_match(docs: list[dict], sgg_nm: str, dong_nm: str) -> dict | None:
    """응답 목록 중 주소(구+동 포함)와 주거시설 카테고리를 모두 만족하는 최상단 결과."""
    for doc in docs:
        if _address_matches_dong(doc, sgg_nm, dong_nm) and _is_apartment_category(doc):
            return doc
    return None


def _geocode_apartment(sgg_nm: str, dong_nm: str, jibun: str | None, bldg_nm: str) -> tuple:
    """(latitude, longitude, is_exact_location)을 반환. 완전 실패 시 (None, None, False).
    예외는 여기서 전부 흡수해(Fallback 로직) 지오코딩 실패가 배치 전체를 죽이지 않도록 한다.
    is_exact_location은 1~3단계(지번/단지명 기반 정확 매칭)에서 찾으면 True, 4단계(법정동
    대표 좌표) 폴백이면 False다 - [2026-09-09] dong_pyeong_common.py와 좌표 캐시
    (geocode_cache, mart/_geocode_cache/)를 공유하게 되면서, 두 스크립트의 정확도 판정
    기준을 반드시 맞춰야 캐시를 통해 값이 섞여도 문제가 없다."""
    cache_key = (sgg_nm, dong_nm, jibun, bldg_nm)
    if cache_key in _geocode_cache:
        return _geocode_cache[cache_key]

    result = (None, None, False)
    if not KAKAO_MAP_REST_API_KEY:
        _geocode_cache[cache_key] = result
        return result

    clean_name = _clean_bldg_nm(bldg_nm)
    try:
        # 1단계: 지번 + 정제 단지명 키워드 검색
        if jibun and clean_name:
            doc = _first_verified_match(
                _kakao_search(_KAKAO_KEYWORD_URL, f"{sgg_nm} {dong_nm} {jibun} {clean_name}"),
                sgg_nm, dong_nm,
            )
            if doc:
                result = (float(doc["y"]), float(doc["x"]), True)

        # 2단계: 지번 전용 주소 검색 (필지 좌표)
        if result[0] is None and jibun:
            docs = _kakao_search(_KAKAO_ADDRESS_URL, f"{sgg_nm} {dong_nm} {jibun}")
            if docs:
                result = (float(docs[0]["y"]), float(docs[0]["x"]), True)

        # 3단계: 정제 단지명 키워드 검색 + 주소/카테고리 검증
        if result[0] is None and clean_name:
            doc = _first_verified_match(
                _kakao_search(_KAKAO_KEYWORD_URL, f"{sgg_nm} {dong_nm} {clean_name}"),
                sgg_nm, dong_nm,
            )
            if doc:
                result = (float(doc["y"]), float(doc["x"]), True)

        # 4단계: 법정동 대표 좌표 (최종 Fallback, 정확도 낮음)
        if result[0] is None:
            docs = _kakao_search(_KAKAO_KEYWORD_URL, f"{sgg_nm} {dong_nm}")
            if docs:
                result = (float(docs[0]["y"]), float(docs[0]["x"]), False)
    except Exception as e:
        print(f"[WARN] 지오코딩 실패 (sgg_nm={sgg_nm}, dong_nm={dong_nm}, jibun={jibun}, bldg_nm={bldg_nm}): {e}")

    _geocode_cache[cache_key] = result
    return result


# --- 7-1. 단지별 대표 지번(mno/sno) 계산: fact_df 안에서 가장 흔한 (mno, sno) 조합 채택 ---
jibun_mode_window = Window.partitionBy("sgg_cd", "dong_cd", "apt_name").orderBy(F.desc("cnt"))
jibun_df = (
    joined_df
    .filter(F.col("mno").isNotNull() & (F.trim(F.col("mno")) != ""))
    .groupBy("sgg_cd", "dong_cd", "apt_name", "mno", "sno").count()
    .withColumnRenamed("count", "cnt")
    .withColumn("rn", F.row_number().over(jibun_mode_window))
    .filter(F.col("rn") == 1)
    .select("sgg_cd", "dong_cd", "apt_name", "mno", "sno")
)
jibun_lookup = {
    (r["sgg_cd"], r["dong_cd"], r["apt_name"]): (r["mno"], r["sno"])
    for r in jibun_df.collect()
}

# --- 7-2. 이번 배치 대상 고유 단지 순회 + 지오코딩 실행 (distinct로 단지당 1회 호출만) ---
distinct_apt_rows = (
    joined_df.select("sgg_cd", "sgg_nm", "dong_cd", "dong_nm", "apt_name").distinct().collect()
)
print(f"[INFO] 지오코딩 대상 고유 단지 수: {len(distinct_apt_rows)}건")

# [2026-09-09 카카오맵 API 호출 절감] main_mart.py + dm_*.py 마트 4종이 서로 다른 프로세스로
# 독립 실행되며(SparkSession 미공유) 매번 각자 다시 지오코딩했다(하루에 최대 5번 중복 호출).
# 영구 캐시(geocode_cache, mart/_geocode_cache/)에 이미 있는 단지는 API를 호출하지 않고
# 캐시값을 그대로 쓰고, 캐시에 없는(신규) 단지만 실제로 지오코딩한다. 이 캐시는
# dong_pyeong_common.py(dm_*.py 4종)와 공유되므로, is_exact_location 판정 기준을 반드시
# 맞춰야 한다(_geocode_apartment 함수 docstring 참고).
_persistent_geocode_cache = load_geocode_cache(spark, LAKE_BUCKET)
_resolved_from_cache, _distinct_apt_rows_to_call = split_cached_and_missing(
    distinct_apt_rows, _persistent_geocode_cache
)
print(
    f"[INFO] 카카오맵 지오코딩 캐시 재사용 {len(_resolved_from_cache)}건 / "
    f"신규 호출 대상 {len(_distinct_apt_rows_to_call)}건"
)

geocode_rows = [
    (key[0], key[1], key[2], value[0], value[1])
    for key, value in _resolved_from_cache.items()
]
_newly_geocoded: dict[tuple, tuple] = {}
for row in _distinct_apt_rows_to_call:
    mno, sno = jibun_lookup.get((row["sgg_cd"], row["dong_cd"], row["apt_name"]), (None, None))
    jibun = _format_jibun(mno, sno)
    latitude, longitude, is_exact = _geocode_apartment(row["sgg_nm"], row["dong_nm"], jibun, row["apt_name"])
    geocode_rows.append((row["sgg_cd"], row["dong_cd"], row["apt_name"], latitude, longitude))
    if latitude is not None:
        _newly_geocoded[(row["sgg_cd"], row["dong_cd"], row["apt_name"])] = (latitude, longitude, is_exact)

print(f"[INFO] 지오코딩 완료 (신규 API 호출 대상 고유 캐시키 수: {len(_geocode_cache)}건)")

# 다음 실행을 위한 캐시 갱신: 기존 캐시 전체를 유지한 채(불변 정보이므로 이번 실행에 다시
# 등장하지 않은 단지도 지우지 않는다) 이번에 새로 찾은 좌표만 덧붙인다.
_updated_geocode_cache = dict(_persistent_geocode_cache)
_updated_geocode_cache.update(_newly_geocoded)
save_geocode_cache(spark, LAKE_BUCKET, _updated_geocode_cache)


def _sql_literal(value) -> str:
    """geocode_rows의 파이썬 값을 Spark SQL VALUES 절에 쓸 리터럴 문자열로 변환.
    작은따옴표는 ANSI SQL 표준처럼 ''로 두 번 쓰는 방식이 아니라 백슬래시(\\')로 이스케이프
    해야 한다 - Spark SQL 파서는 ''를 이스케이프로 인식하지 않고 그냥 통째로 삼켜버린다."""
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return f"{value!r}D"  # 'D' 접미사로 DoubleType 리터럴임을 명시 (기본은 DecimalType)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


geocode_schema = StructType([
    StructField("sgg_cd", StringType(), True),
    StructField("dong_cd", StringType(), True),
    StructField("apt_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

if geocode_rows:
    # spark.createDataFrame(python_list, schema)는 로컬 Windows 개발 환경에서 Python 워커
    # 콜백 접속 문제로 죽는 경우가 있어(build_apt_recent_trade_mart.py에서 확인된 이슈),
    # 대신 순수 Spark SQL VALUES 절로 만들면 JVM 안에서만 파싱/평가되어 파이썬 워커가
    # 전혀 필요 없다 (건수도 최대 수천 건 수준이라 SQL 문자열 크기는 문제없다).
    values_sql = ",\n".join(
        "({}, {}, {}, {}, {})".format(
            _sql_literal(sgg_cd), _sql_literal(dong_cd), _sql_literal(apt_name),
            _sql_literal(latitude), _sql_literal(longitude),
        )
        for sgg_cd, dong_cd, apt_name, latitude, longitude in geocode_rows
    )
    geocode_df = spark.sql(
        f"SELECT * FROM VALUES {values_sql} "
        "AS t(sgg_cd, dong_cd, apt_name, latitude, longitude)"
    )
    # VALUES 절의 한 컬럼이 NULL 리터럴로만 채워지면(예: 이번 배치의 모든 단지가 지오코딩에
    # 실패) Spark가 그 컬럼 타입을 DoubleType이 아니라 VOID(NullType)로 추론해버린다. VOID
    # 컬럼은 Parquet에 쓸 수 없으므로 geocode_schema로 명시 캐스팅해 항상 의도한 타입을 보장한다.
    geocode_df = geocode_df.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in geocode_schema.fields]
    )
else:
    geocode_df = spark.createDataFrame([], schema=geocode_schema)

# 좌표는 단지(sgg_cd+dong_cd+apt_name)당 하나뿐인 소용량 데이터라 여기서도 브로드캐스트 조인.
joined_df = joined_df.join(
    broadcast(geocode_df),
    on=["sgg_cd", "dong_cd", "apt_name"],
    how="left",
)


# =====================================================================================
# 8. 최종 집계: [자치구 x 법정동 x 단지 x 거래일자 x 면적] 단위로 거래량/금액 합계
#    - 공급평수 = (전용면적m2 * 1.3) / 3.30578, 거래건별 평당가 = 거래금액(만원) / 공급평수
#      (Silver의 price_per_m2는 "전용면적 기준 m2당가"라 이 마트가 요구하는 "공급면적 기준
#      평당가"와 산식이 달라서 재사용하지 않고 여기서 새로 계산한다 - build_dong_pyeong_mart.py
#      와 동일한 산식/상수를 사용).
#    -> MinIO S3 Lake mart/dm_main/ 경로에 Parquet으로 저장.
#    [멱등성/재실행] base_date=YYYY-MM-DD 전용 경로에 upsert_spark_partition()으로 쓴다 -
#    파티션이 없으면 새로 저장(INSERT), 있는데 내용이 완전히 같으면 재작성을 건너뛰고(SKIP),
#    다르면 키(cgg_cd+stdg_cd+bldg_nm+deal_date+area+mno+sno) 기준으로 병합(UPDATE)한다.
# =====================================================================================
SUPPLY_AREA_RATIO = 1.3  # 전용면적 -> 공급면적 환산 비율
PYEONG_M2 = 3.30578       # 1평 = 3.30578 m2

main_mart_df = (
    joined_df
    .withColumn(
        "pyeong_amt",
        F.col("price_ten_thousand")
        / ((F.col("exclusive_area_m2") * F.lit(SUPPLY_AREA_RATIO)) / F.lit(PYEONG_M2)),
    )
    .groupBy(
        "sgg_cd", "sgg_nm", "dong_cd", "dong_nm", "apt_name",
        "deal_date", "exclusive_area_m2", "mno", "sno", "latitude", "longitude",
    )
    .agg(
        F.count(F.lit(1)).cast(IntegerType()).alias("deal_cnt"),
        F.round(F.sum("price_ten_thousand")).cast(LongType()).alias("total_thing_amt"),
        F.round(F.sum("pyeong_amt")).cast(LongType()).alias("total_pyeong_amt"),
    )
    .select(
        F.lit(BASE_DATE_STR).alias("base_date"),
        F.col("sgg_cd").alias("cgg_cd"),
        F.col("sgg_nm").alias("cgg_nm"),
        F.col("dong_cd").alias("stdg_cd"),
        F.col("dong_nm").alias("stdg_nm"),
        F.col("apt_name").alias("bldg_nm"),
        "deal_date",
        F.col("exclusive_area_m2").alias("area"),
        "mno",
        "sno",
        "latitude",
        "longitude",
        "deal_cnt",
        "total_thing_amt",
        "total_pyeong_amt",
    )
)

# [2026-09-09 base_date 재실행 대응] 같은 base_date로 하루에 여러 번 다시 돌려도(수동 재실행
# 등) 매번 파티션 전체를 무조건 재작성하지 않고, 이전 저장 결과와 비교해 변경이 없으면
# 재작성을 건너뛰고(SKIP), 있으면 [자치구x법정동x단지x거래일자x면적x지번] 키 기준으로
# 병합(UPDATE)한다.
MAIN_MART_KEY_COLUMNS = ["cgg_cd", "stdg_cd", "bldg_nm", "deal_date", "area", "mno", "sno"]
upsert_spark_partition(
    spark, MART_PATH, main_mart_df, key_columns=MAIN_MART_KEY_COLUMNS, mart_label="dm_main"
)

spark.stop()
print("[INFO] Gold 마트 dm_main 저장 완료 (MinIO S3 Lake 전용, RDB 적재 없음)")

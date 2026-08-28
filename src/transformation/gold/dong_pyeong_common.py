# -*- coding: utf-8 -*-
"""
[동 x 평형대] 계열 Gold 마트 4종(dm_dong_pyeong_price_avg.py / dm_apt_price_avg.py /
dm_apt_pyeong_price.py / dm_apt_flr_price.py - 파일명은 각각 대응하는 MinIO S3 Lake
mart/ 폴더명과 동일하다)이 공유하는 준비 단계.

환경변수/SparkSession 기동 -> Silver(dim_apartment/fact_apt_transactions) 로딩 -> 1차
정제 -> dim_apartment 브로드캐스트 조인 -> 카카오맵 지오코딩(+ Bronze 지번 보조 조회) ->
평형대/층수 그룹 및 평당가 파생 컬럼까지 끝낸 공통 데이터프레임(common_df)을 만든다.

4개 dm_*.py 파일은 각각 완전히 독립된 배치 스크립트로 실행된다(공유 SparkSession 없음) -
Airflow에서 마트 4개를 동시에 띄우면 GCP 메모리 한도를 넘겨 OOM으로 죽을 수 있어서, 마트
하나가 끝나 프로세스가 완전히 종료되고 메모리를 반환한 뒤에야 다음 마트가 시작되도록
Airflow 태스크를 순차 체이닝했다(data_orchestration.py 참고). 그 대가로 Silver 읽기/조인과
카카오맵 지오코딩(외부 API 호출)은 마트마다 새로 반복된다 - 메모리 안전을 위해 의도적으로
받아들인 비용이다.
"""

import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOOKBACK_DAYS = 90
SUPPLY_AREA_RATIO = 1.3  # 전용면적 -> 공급면적 환산 비율
PYEONG_M2 = 3.30578       # 1평 = 3.30578 m2

# MinIO S3 Lake mart/ 밑의 실제 폴더명과 동일하다 - 각 이름에 대응하는 dm_*.py 파일이 하나씩 있다.
MART_NAMES = (
    "dm_dong_pyeong_price_avg",
    "dm_apt_price_avg",
    "dm_apt_pyeong_price",
    "dm_apt_flr_price",
)

# 마트 ②~④(단지 단위) 공통 groupBy 키. mno/sno(지번)는 latitude/longitude/is_exact_location과
# 마찬가지로 단지(apt_name)별로 고정된 값이라 groupBy 키에 그대로 포함시킨다(집계 대상이
# 아니라 단지 속성).
APT_GROUP_COLS = [
    "sgg_cd", "sgg_nm", "dong_cd", "dong_nm", "apt_name",
    "latitude", "longitude", "is_exact_location", "mno", "sno",
]


@dataclass
class GoldMartContext:
    spark: SparkSession
    common_df: DataFrame
    mart_paths: dict[str, str]
    base_date_str: str
    run_timestamp: datetime


def apt_select_cols(ctx: GoldMartContext) -> list:
    """마트 ②~④(단지 단위) 공통 select 컬럼 - base_date/구·동·단지명/좌표/지번/updated_at."""
    return [
        F.lit(ctx.base_date_str).alias("base_date"),
        F.col("sgg_cd").alias("cgg_cd"),
        F.col("sgg_nm").alias("cgg_nm"),
        F.col("dong_cd").alias("stdg_cd"),
        F.col("dong_nm").alias("stdg_nm"),
        F.col("apt_name").alias("bldg_nm"),
        "latitude",
        "longitude",
        "is_exact_location",
        "mno",
        "sno",
        F.lit(ctx.run_timestamp).alias("updated_at"),
    ]


def _load_env() -> None:
    """로컬 실행(<project_root>/env/.env)과 Airflow 컨테이너 실행(/opt/airflow/project/env/.env)
    양쪽 경로를 다 시도한다. override=False로 호출하는 쪽이 이미 채워둔 환경 변수는
    덮어쓰지 않고 빈 값만 보충한다."""
    project_root = Path(__file__).resolve().parents[3]
    env_candidates = [
        project_root / "env" / ".env",
        Path("/opt/airflow/project/env/.env"),
    ]
    for env_path in env_candidates:
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=False)
            break


def _create_spark_session(
    lake_bucket: str | None,
    s3_access_key: str | None,
    s3_secret_key: str | None,
    s3_end_point: str | None,
) -> SparkSession:
    lake_warehouse = f"s3a://{lake_bucket}/"

    # S3_END_POINT는 로컬(MinIO, 스킴 없이 "host:port")과 배포(HTTPS 스킴 포함,
    # 예: "https://storage.googleapis.com") 양쪽 형식을 그대로 받는다. SSL 사용 여부는
    # 이 스킴으로 판단해야 한다 - 하드코딩하면 로컬(HTTP 전용 MinIO)에서 SSL 핸드셰이크를
    # 시도하다가 "Unsupported or unrecognized SSL message" 오류로 실패한다.
    use_ssl = (s3_end_point or "").strip().lower().startswith("https://")
    endpoint_clean = (s3_end_point or "").replace("https://", "").replace("http://", "")
    s3_endpoint_url = f"{'https' if use_ssl else 'http'}://{endpoint_clean}"

    spark = (
        SparkSession.builder
        .appName("Real_Estate_Gold_Marts")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
            "org.apache.hadoop:hadoop-aws:3.4.1",
        )
        # --- 8GB VM(GCP e2-standard-2, 2 vCPU) 메모리 안전 설정 - Real_Estate_Transform.py와
        # 동일한 값. Airflow DAG가 [동 x 평형대] 마트 4종을 완전히 독립된 프로세스로 하나씩만
        # 순차 실행하도록 이미 나눠뒀으므로(build_dong_pyeong_mart.py 삭제 이력 참고), 이
        # 프로세스 혼자 여유 메모리(4.5~5GB)를 쓴다는 전제로 힙은 보수적으로 3g로 잡는다
        # (환경변수로 오버라이드 가능). ---
        .config("spark.master", os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "1g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.autoBroadcastJoinThreshold", os.getenv("SPARK_AUTO_BROADCAST_THRESHOLD", "5m"))
        .config("spark.sql.files.maxPartitionBytes", os.getenv("SPARK_MAX_PARTITION_BYTES", "64m"))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", lake_warehouse)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint_url)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", s3_access_key)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", s3_secret_key)
        .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_clean)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if use_ssl else "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =====================================================================================
# Bronze 원본에서 지번(MNO/SNO) 보조 조회 테이블 구축 - 지번은 Silver(fact_apt_transactions)
# 에는 없고 Bronze 원본에만 있는 필드라서, 이번 배치 조회기간에 해당하는 Bronze month
# 파티션만 선별적으로 읽어 (자치구+법정동+단지명)별로 가장 많이 등장한 (MNO, SNO) 조합을
# 대표 지번으로 뽑는다 (카카오맵 지오코딩 정확도를 높이는 용도). Silver/Iceberg 스키마는
# 건드리지 않는다.
# =====================================================================================
def _bronze_month_paths(raw_bucket: str, start: date, end: date) -> list[str]:
    months = []
    cur_year, cur_month = start.year, start.month
    while (cur_year, cur_month) <= (end.year, end.month):
        months.append((cur_year, cur_month))
        cur_year, cur_month = (cur_year + 1, 1) if cur_month == 12 else (cur_year, cur_month + 1)
    # 마지막 세그먼트까지 와일드카드(*.parquet)로 둔다 - Real_Estate_Transform.py의
    # BULK 모드가 쓰는 것과 같은 형태(day=*/*.parquet)다. day는 와일드카드인데 파일명만
    # 리터럴(real_estate_raw.parquet)로 고정하면 Spark의 FileStreamSink.hasMetadata
    # 체크가 그 리터럴 경로를 그대로 stat()하려다 S3A에서 FileNotFoundException을 던지는
    # 문제가 있었다(AnalysisException이 아니라서 except로도 못 잡고 배치가 죽었음).
    return [
        f"s3a://{raw_bucket}/real_estate/year={y:04d}/month={m:02d}/day=*/*.parquet"
        for y, m in months
    ]


def _build_jibun_lookup(
    spark: SparkSession,
    raw_bucket: str | None,
    start_date: date,
    base_date: date,
) -> dict[tuple, tuple]:
    """(sgg_cd, dong_cd, apt_name) -> (mno, sno) 대표 지번 조회 테이블을 만든다.
    raw_bucket이 없거나 조회기간에 해당하는 Bronze 파티션을 하나도 못 찾으면 빈 dict를
    돌려준다(호출자는 지번 없이 지오코딩 2~4단계로 폴백하면 된다 - best-effort)."""
    if not raw_bucket:
        print("[WARN] RAW 버킷 환경변수가 없어 지번(MNO/SNO) 보조 조회 없이 진행합니다 (지오코딩 1~2단계 생략).")
        return {}

    bronze_frames: list[DataFrame] = []
    for month_path in _bronze_month_paths(raw_bucket, start_date, base_date):
        try:
            bronze_frames.append(spark.read.parquet(month_path))
        except Exception as e:
            print(f"[WARN] Bronze 파티션을 건너뜁니다 ({month_path}): {type(e).__name__}: {e}")

    if not bronze_frames:
        print("[WARN] 조회기간에 해당하는 Bronze 원본 파티션을 찾지 못해 지번 보조 조회 없이 진행합니다.")
        return {}

    bronze_df = bronze_frames[0]
    for frame in bronze_frames[1:]:
        bronze_df = bronze_df.unionByName(frame)

    start_str, base_str = start_date.strftime("%Y%m%d"), base_date.strftime("%Y%m%d")
    bronze_jibun_df = (
        bronze_df
        .filter(
            (F.col("BLDG_USG") == "아파트")
            & (F.col("CTRT_DAY") >= F.lit(start_str)) & (F.col("CTRT_DAY") <= F.lit(base_str))
            & F.col("MNO").isNotNull() & (F.trim(F.col("MNO").cast("string")) != "")
        )
        .select(
            F.col("CGG_CD").cast("string").alias("sgg_cd"),
            F.col("STDG_CD").cast("string").alias("dong_cd"),
            F.trim(F.col("BLDG_NM").cast("string")).alias("apt_name"),
            F.trim(F.col("MNO").cast("string")).alias("mno"),
            F.trim(F.col("SNO").cast("string")).alias("sno"),
        )
    )
    # 단지별로 가장 흔한 (MNO, SNO) 조합을 대표 지번으로 채택 (동명이인 필지 오염 방지)
    jibun_mode_window = Window.partitionBy("sgg_cd", "dong_cd", "apt_name").orderBy(F.desc("cnt"))
    jibun_df = (
        bronze_jibun_df.groupBy("sgg_cd", "dong_cd", "apt_name", "mno", "sno").count()
        .withColumnRenamed("count", "cnt")
        .withColumn("rn", F.row_number().over(jibun_mode_window))
        .filter(F.col("rn") == 1)
        .select("sgg_cd", "dong_cd", "apt_name", "mno", "sno")
    )
    lookup = {
        (r["sgg_cd"], r["dong_cd"], r["apt_name"]): (r["mno"], r["sno"])
        for r in jibun_df.collect()
    }
    print(f"[INFO] 지번(MNO/SNO) 보조 조회 대상 단지 수: {len(lookup)}건 (Bronze {len(bronze_frames)}개 월 파티션)")
    return lookup


# =====================================================================================
# 카카오맵 API 지오코딩 - 위/경도(LATITUDE, LONGITUDE) + 정확도(IS_EXACT_LOCATION) 부여
#    4단계 Fallback 파이프라인:
#      1단계: 지번(JIBUN) + 정제 단지명(CLEAN_BLDG_NM) 키워드 검색 -> 주소/카테고리 검증
#      2단계: 지번 전용 주소 검색(address.json) -> 필지 좌표
#      3단계: 정제 단지명 키워드 검색 -> 주소/카테고리 검증
#      4단계: 법정동(CGG_NM+STDG_NM) 대표 좌표 (IS_EXACT_LOCATION=False)
#    1~3단계에서 좌표를 찾으면 IS_EXACT_LOCATION=True, 4단계로 떨어지면 False.
# =====================================================================================
_KAKAO_KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
_KAKAO_ADDRESS_URL = "https://dapi.kakao.com/v2/local/search/address.json"

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


def _geocode_apartments(
    distinct_apt_rows,
    jibun_lookup: dict[tuple, tuple],
    kakao_api_key: str | None,
) -> list[tuple]:
    """고유 단지 목록을 순회하며 지오코딩하고, (sgg_cd, dong_cd, apt_name, latitude,
    longitude, is_exact_location, mno, sno) 튜플 리스트를 돌려준다."""
    session = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))  # pyrefly: ignore[bad-argument-type]

    # (CGG_NM, STDG_NM, JIBUN, BLDG_NM) 고유 키 기준 메모리 캐시 - 동일 단지 재요청을 100% 차단.
    cache: dict[tuple, tuple] = {}

    def kakao_search(url: str, query: str) -> list[dict]:
        response = session.get(
            url,
            headers={"Authorization": f"KakaoAK {kakao_api_key}"},
            params={"query": query},
            timeout=5,
        )
        response.raise_for_status()
        return response.json().get("documents") or []

    def geocode_apartment(sgg_nm: str, dong_nm: str, jibun: str | None, bldg_nm: str) -> tuple:
        """(latitude, longitude, is_exact_location)을 반환. 완전 실패 시 (None, None, False)."""
        cache_key = (sgg_nm, dong_nm, jibun, bldg_nm)
        if cache_key in cache:
            return cache[cache_key]

        result = (None, None, False)
        if not kakao_api_key:
            cache[cache_key] = result
            return result

        clean_name = _clean_bldg_nm(bldg_nm)
        try:
            # 1단계: 지번 + 정제 단지명 키워드 검색
            if jibun and clean_name:
                doc = _first_verified_match(
                    kakao_search(_KAKAO_KEYWORD_URL, f"{sgg_nm} {dong_nm} {jibun} {clean_name}"),
                    sgg_nm, dong_nm,
                )
                if doc:
                    result = (float(doc["y"]), float(doc["x"]), True)

            # 2단계: 지번 전용 주소 검색 (필지 좌표)
            if result[0] is None and jibun:
                docs = kakao_search(_KAKAO_ADDRESS_URL, f"{sgg_nm} {dong_nm} {jibun}")
                if docs:
                    result = (float(docs[0]["y"]), float(docs[0]["x"]), True)

            # 3단계: 정제 단지명 키워드 검색 + 주소/카테고리 검증
            if result[0] is None and clean_name:
                doc = _first_verified_match(
                    kakao_search(_KAKAO_KEYWORD_URL, f"{sgg_nm} {dong_nm} {clean_name}"),
                    sgg_nm, dong_nm,
                )
                if doc:
                    result = (float(doc["y"]), float(doc["x"]), True)

            # 4단계: 법정동 대표 좌표 (최종 Fallback, 정확도 낮음)
            if result[0] is None:
                docs = kakao_search(_KAKAO_KEYWORD_URL, f"{sgg_nm} {dong_nm}")
                if docs:
                    result = (float(docs[0]["y"]), float(docs[0]["x"]), False)
        except Exception as e:
            print(f"[WARN] 지오코딩 실패 (sgg_nm={sgg_nm}, dong_nm={dong_nm}, jibun={jibun}, bldg_nm={bldg_nm}): {e}")

        cache[cache_key] = result
        return result

    rows = []
    exact_count = 0
    for row in distinct_apt_rows:
        mno, sno = jibun_lookup.get((row["sgg_cd"], row["dong_cd"], row["apt_name"]), (None, None))
        jibun = _format_jibun(mno, sno)
        latitude, longitude, is_exact = geocode_apartment(row["sgg_nm"], row["dong_nm"], jibun, row["apt_name"])
        exact_count += 1 if is_exact else 0
        # mno/sno는 위에서 jibun_lookup으로 조회해둔 값을 그대로 실어 나른다 - Bronze를
        # 다시 스캔하지 않고, 지오코딩에 쓰던 것과 동일한 대표 지번을 마트 출력에도 노출한다.
        rows.append((row["sgg_cd"], row["dong_cd"], row["apt_name"], latitude, longitude, is_exact, mno, sno))

    print(
        f"[INFO] 지오코딩 완료 (API 호출 대상 고유 캐시키 수: {len(cache)}건, "
        f"정확 매칭 IS_EXACT_LOCATION=True: {exact_count}/{len(distinct_apt_rows)}건)"
    )
    return rows


_GEOCODE_SCHEMA = StructType([
    StructField("sgg_cd", StringType(), True),
    StructField("dong_cd", StringType(), True),
    StructField("apt_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("is_exact_location", BooleanType(), True),
    StructField("mno", StringType(), True),
    StructField("sno", StringType(), True),
])


def _sql_literal(value) -> str:
    """geocode_rows의 파이썬 값을 Spark SQL VALUES 절에 쓸 리터럴 문자열로 변환.
    작은따옴표는 ANSI SQL 표준처럼 ''로 두 번 쓰는 방식이 아니라 백슬래시(\\')로 이스케이프
    해야 한다 - Spark SQL 파서는 ''를 이스케이프로 인식하지 않고 그냥 통째로 삼켜버려서,
    예를 들어 "역삼I'PARK"가 "역삼IPARK"로 아포스트로피가 사라진 채 저장되는 버그가 있었다
    (지오코딩 자체는 성공했는데 그 결과를 geocode_df로 만드는 이 단계에서 조인 키인
    apt_name이 원본과 달라져 버려 joined_df와 매칭이 안 되고 좌표가 전부 NULL로 빠졌다)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value!r}D"  # 'D' 접미사로 DoubleType 리터럴임을 명시 (기본은 DecimalType)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _geocode_rows_to_df(spark: SparkSession, geocode_rows: list[tuple]) -> DataFrame:
    if not geocode_rows:
        return spark.createDataFrame([], schema=_GEOCODE_SCHEMA)

    # spark.createDataFrame(python_list, schema)는 내부적으로 executor에 PySpark 워커
    # 프로세스를 띄워 pickle 역직렬화를 수행하는데, 로컬 Windows 개발 환경에서는 이 워커가
    # 제때 콜백 접속을 못해 "Python worker failed to connect back"으로 배치가 죽는 경우가
    # 있었다. 대신 순수 Spark SQL VALUES 절로 만들면 JVM 안에서만 파싱/평가되어 파이썬
    # 워커가 전혀 필요 없다 (건수도 최대 수천 건 수준이라 SQL 문자열 크기는 문제없다).
    values_sql = ",\n".join(
        "({}, {}, {}, {}, {}, {}, {}, {})".format(
            _sql_literal(sgg_cd), _sql_literal(dong_cd), _sql_literal(apt_name),
            _sql_literal(latitude), _sql_literal(longitude), _sql_literal(is_exact),
            _sql_literal(mno), _sql_literal(sno),
        )
        for sgg_cd, dong_cd, apt_name, latitude, longitude, is_exact, mno, sno in geocode_rows
    )
    geocode_df = spark.sql(
        f"SELECT * FROM VALUES {values_sql} "
        "AS t(sgg_cd, dong_cd, apt_name, latitude, longitude, is_exact_location, mno, sno)"
    )
    # VALUES 절의 한 컬럼이 NULL 리터럴로만 채워지면(예: 이번 배치의 모든 단지가 지오코딩에
    # 실패해 latitude/longitude가 전부 None) Spark가 그 컬럼 타입을 DoubleType이 아니라
    # VOID(NullType)로 추론해버린다. VOID 컬럼은 Parquet에 쓸 수 없어서(마트 ②~④는
    # latitude/longitude를 집계 없이 groupBy 키로 그대로 통과시키므로) 저장 시점에
    # UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE로 배치가 죽는다. _GEOCODE_SCHEMA로 명시
    # 캐스팅해 항상 의도한 타입을 보장한다.
    return geocode_df.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in _GEOCODE_SCHEMA.fields]
    )


def build_gold_mart_context(base_date: date) -> GoldMartContext:
    """base_date를 기준일로 최근 LOOKBACK_DAYS일 치 공통 정제 데이터프레임(common_df)과
    SparkSession, 마트별 저장 경로(MinIO S3 Lake mart/ 폴더별 경로)를 준비한다. 4개
    dm_*.py 마트 스크립트는 각자 독립 실행될 때마다 이 함수를 한 번씩 호출해 필요한 것을
    스스로 준비한다(SparkSession을 공유하지 않는다 - 모듈 docstring의 메모리 트레이드오프
    참고)."""
    start_date = base_date - timedelta(days=LOOKBACK_DAYS - 1)
    base_date_str = base_date.strftime("%Y-%m-%d")
    run_timestamp = datetime.now()

    print(
        f"[INFO] Gold 마트 4종 집계 시작: "
        f"base_date={base_date_str}, 조회기간={start_date} ~ {base_date} ({LOOKBACK_DAYS}일)"
    )

    _load_env()
    s3_end_point = os.getenv("S3_END_POINT")
    s3_access_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")
    lake_bucket = os.getenv("LAKE")
    raw_bucket = os.getenv("RAW")
    kakao_api_key = os.getenv("KAKAO_MAP_REST_API_KEY")

    if not kakao_api_key:
        print("[WARN] KAKAO_MAP_REST_API_KEY가 설정되지 않았습니다. 좌표는 전부 NULL로 채워집니다.")

    # 마트별 MinIO 저장 경로: {S3_END_POINT}/{LAKE}/mart/{마트명}/base_date=YYYY-MM-DD
    mart_paths = {
        name: f"s3a://{lake_bucket}/mart/{name}/base_date={base_date_str}"
        for name in MART_NAMES
    }

    spark = _create_spark_session(lake_bucket, s3_access_key, s3_secret_key, s3_end_point)

    # --- Silver 레이어 읽기 (원천 로딩 최적화) ---
    # dim_apartment: Iceberg 카탈로그로 읽어서(spark.table) 필요한 컬럼만 선별한 뒤
    # 브로드캐스트 조인에 사용한다.
    # fact_apt_transactions: deal_date 컬럼으로 필터링한다. 이 테이블은 PARTITIONED BY
    # (days(deal_date))로 만들어져 있어서(Real_Estate_Transform.py 참고) deal_date 조건이
    # Iceberg의 파티션 프루닝에 그대로 활용된다 - 최근 90일 day 파티션만 실제로 읽힌다.
    dim_apartment_df = spark.table("lakehouse.dim_apartment").select(
        "sgg_cd", "sgg_nm", "dong_cd", "dong_nm", "apt_name"
    )
    fact_df = (
        spark.table("lakehouse.fact_apt_transactions")
        .filter(
            (F.col("deal_date") >= F.lit(start_date)) & (F.col("deal_date") <= F.lit(base_date))
        )
        .select(
            "sgg_cd", "dong_cd", "apt_name",
            "price_ten_thousand", "exclusive_area_m2", "floor", "cancel_date", "deal_date",
        )
    )

    # --- 1차 정제 ---
    # 거래취소건 제외 + 금액/면적 0 이하 제거(데이터 오염이 평균/평당가 계산을 왜곡하지
    # 않도록). BLDG_USG(건물용도) == '아파트' 필터는 fact_apt_transactions가 이미 Silver
    # 레이어에서 걸러진 뒤 적재된 테이블이라 이 테이블에는 그 컬럼 자체가 없다.
    fact_df = fact_df.filter(
        (F.col("cancel_date").isNull() | (F.trim(F.col("cancel_date")) == ""))
        & (F.col("price_ten_thousand") > 0)
        & (F.col("exclusive_area_m2") > 0)
    )

    # --- dim_apartment 브로드캐스트 조인 (소용량 마스터 테이블을 브로드캐스트해서
    # Shuffle 최소화) + 건물명/동명 TRIM 공백 정제 ---
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
            F.col("f.floor").alias("floor"),
            F.col("f.deal_date").alias("deal_date"),
        )
    ).cache()  # 아래 지오코딩용 distinct collect와 공통 df 집계가 이 결과를 재사용하므로
               # 캐싱하지 않으면 원천 읽기+조인이 두 번 실행된다.

    # --- 카카오맵 API 지오코딩 (위/경도 + 정확도 부여) ---
    jibun_lookup = _build_jibun_lookup(spark, raw_bucket, start_date, base_date)

    distinct_apt_rows = (
        joined_df.select("sgg_cd", "sgg_nm", "dong_cd", "dong_nm", "apt_name").distinct().collect()
    )
    print(f"[INFO] 지오코딩 대상 고유 단지 수: {len(distinct_apt_rows)}건")

    geocode_rows = _geocode_apartments(distinct_apt_rows, jibun_lookup, kakao_api_key)
    geocode_df = _geocode_rows_to_df(spark, geocode_rows)

    joined_df = joined_df.join(
        broadcast(geocode_df),
        on=["sgg_cd", "dong_cd", "apt_name"],
        how="left",
    )

    # --- 평형대(PYEONG_GRP) / 층수 그룹(FLR_GRP) 분류 + 거래건별 평당가 산출 ---
    # 공급평수 = (전용면적m2 * 1.3) / 3.30578, 거래건별 평당가 = 거래금액(만원) / 공급평수
    # (Silver의 price_per_m2는 "전용면적 기준 m2당가"라 이 마트가 요구하는 "공급면적 기준
    # 평당가"와 산식이 달라서 재사용하지 않고 여기서 새로 계산한다.)
    common_df = (
        joined_df
        .withColumn(
            "pyeong_grp",
            F.when(F.col("exclusive_area_m2") < 50, F.lit("10"))
             .when(F.col("exclusive_area_m2") < 74, F.lit("20"))
             .when(F.col("exclusive_area_m2") < 100, F.lit("30"))
             .otherwise(F.lit("40+")),
        )
        .withColumn(
            "flr_grp",
            F.when(F.col("floor") <= 5, F.lit("LOW"))
             .when(F.col("floor") <= 15, F.lit("MID"))
             .otherwise(F.lit("HIGH")),
        )
        .withColumn(
            "supply_pyeong",
            (F.col("exclusive_area_m2") * F.lit(SUPPLY_AREA_RATIO)) / F.lit(PYEONG_M2),
        )
        .withColumn("pyeong_amt", F.col("price_ten_thousand") / F.col("supply_pyeong"))
    ).cache()

    common_count = common_df.count()
    print(f"[INFO] 공통 정제 데이터프레임 캐싱 완료: {common_count}건")

    return GoldMartContext(
        spark=spark,
        common_df=common_df,
        mart_paths=mart_paths,
        base_date_str=base_date_str,
        run_timestamp=run_timestamp,
    )

# -*- coding: utf-8 -*-
"""
아파트별 최근 90일 거래 지표 + 건축물 기본정보 Gold 데이터 마트(dm_apt_recent_trade) 생성 스크립트
- 단독 실행 가능한 PySpark 배치 스크립트 (오케스트레이션 로직 없음, build_dong_pyeong_mart.py와 동일한 스타일)
- 소스: dim_apartment / fact_apt_transactions (s3a://{LAKE}/ 하위 Iceberg 테이블)
- 외부 API: 공공데이터포털 국토교통부_건축HUB_건축물대장정보 서비스(getBrRecapTitleInfo)로
  세대수/사용승인일을 보강한다.
- 최종 결과는 S3 Lake(MinIO) mart/ 경로에 Parquet으로 저장한다.
  저장 경로: {S3_END_POINT}/{LAKE}/mart/dm_apt_recent_trade/base_date=YYYY-MM-DD
  (S3_END_POINT/LAKE는 env/.env의 값을 그대로 사용 - build_dong_pyeong_mart.py의
  MART_PATHS와 동일한 규칙).

실행 방법:
  python src/transformation/gold/build_apt_recent_trade_mart.py [BASE_DATE]
    - BASE_DATE(YYYY-MM-DD) 생략 시 오늘 날짜를 기준으로 최근 90일치를 집계한다.
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from transformation.gold.adaptive_lookback import load_fact_with_adaptive_lookback
from utils.building_info_cache import (
    load_building_info_cache,
    save_building_info_cache,
    split_cached_and_missing,
)
from utils.spark_partition_upsert import upsert_spark_partition

LOOKBACK_DAYS = 90
SUPPLY_AREA_RATIO = 1.3   # 전용면적 -> 공급면적 환산 비율
PYEONG_M2 = 3.30578        # 1평 = 3.30578 m2
BUILDING_API_URL = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrRecapTitleInfo"
API_MAX_WORKERS = 1 # 429(Too Many Requests) 방지를 위해 동시 호출 스레드 수 축소(기존 10 -> 1)
API_REQUEST_INTERVAL_SECONDS = 1.0  # 전체 스레드가 공유하는 API 호출 최소 간격(초). 429 방지용 스로틀링
API_MAX_RETRIES = 3  # 타임아웃/429/5xx 등 일시적 오류 발생 시 최대 재시도 횟수(최초 시도 제외)
API_RETRY_BACKOFF_BASE_SECONDS = 1.0  # 재시도 대기 시간(초). 시도마다 지수적으로 증가(1s, 2s, 4s ...)
API_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
API_CONNECT_TIMEOUT_SECONDS = 5  # TCP 연결 자체는 원래도 빠르게 실패/성공하므로 기존 값 유지
API_READ_TIMEOUT_SECONDS = 20  # 건축HUB API 응답이 5초를 넘기는 경우가 많아 재시도 소진 전에
# 정상 응답을 받을 여유를 둔다(클라우드 환경에서 "Read timed out. (read timeout=5)"로 재시도
# 3회를 전부 소진하고 실패하는 사례가 반복돼 상향).
DAILY_TRAFFIC_LIMIT = 10000  # 공공데이터포털 건축HUB API 일일 최대 호출 가능 건수
# User-Agent 미지정 시 서버가 봇/스크립트 요청으로 간주해 503을 돌려주는 경우가 있어 명시한다.
API_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


class DailyTrafficExceededError(Exception):
    """공공데이터포털 일일 트래픽 한도(DAILY_TRAFFIC_LIMIT)를 초과했을 때 발생시키는 예외."""


class ApiCallLimiter:
    """여러 스레드에서 동시에 호출해도 안전하게 API 호출 건수를 세는 카운터."""

    def __init__(self, max_calls: int):
        self.max_calls = max_calls
        self.count = 0
        self._lock = threading.Lock()

    def increment(self) -> None:
        with self._lock:
            self.count += 1
            if self.count > self.max_calls:
                raise DailyTrafficExceededError(
                    f"공공데이터포털 일일 트래픽 한도({self.max_calls}건) 초과"
                )


api_call_limiter = ApiCallLimiter(DAILY_TRAFFIC_LIMIT)


class ApiRateLimiter:
    """여러 스레드가 동시에 호출해도 API 호출 간 최소 간격(min_interval)을 지키게 하는 스로틀러."""

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last_call_time = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            remaining = self.min_interval - (now - self._last_call_time)
            if remaining > 0:
                time.sleep(remaining)
            self._last_call_time = time.monotonic()


api_rate_limiter = ApiRateLimiter(API_REQUEST_INTERVAL_SECONDS)

BUILDING_API_SCHEMA = StructType([
    StructField("sgg_cd", StringType(), True),
    StructField("dong_cd", StringType(), True),
    StructField("apt_name", StringType(), True),
    StructField("household_count", IntegerType(), True),
    StructField("use_approval_date", StringType(), True),  # yyyyMMdd 원본 문자열
])


# =====================================================================================
# 1. 환경 변수 로드
# =====================================================================================
def load_config() -> dict:
    """env/.env를 로드하고 이 스크립트가 필요로 하는 환경변수를 dict로 모아 반환한다."""
    project_root = Path(__file__).resolve().parents[3]
    env_path = project_root / "env" / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    config = {
        "s3_endpoint": os.getenv("S3_END_POINT"),
        "s3_access_key": os.getenv("S3_ACCESS_KEY"),
        "s3_secret_key": os.getenv("S3_SECRET_KEY"),
        "lake_bucket": os.getenv("LAKE"),
        "data_go_kr_key": os.getenv("DATA_GO_KR_KEY"),
    }
    missing = [key for key, value in config.items() if not value]
    if missing:
        print(f"[WARN] 비어있는 환경변수: {missing}")
    return config


# =====================================================================================
# 2. SparkSession 생성 (Iceberg 카탈로그 + Hadoop S3A/MinIO 연동)
#    dim_apartment/fact_apt_transactions는 lakehouse(hadoop 타입) 카탈로그의 워런하우스
#    루트가 s3a://{LAKE}/ 이므로, 두 테이블의 실제 데이터는 s3a://{LAKE}/dim_apartment,
#    s3a://{LAKE}/fact_apt_transactions 하위에 저장된다.
# =====================================================================================
def create_spark_session(config: dict) -> SparkSession:
    lake_warehouse = f"s3a://{config['lake_bucket']}/"

    # s3_endpoint는 로컬(MinIO, 스킴 없이 "host:port")과 배포(HTTPS 스킴 포함,
    # 예: "https://storage.googleapis.com") 양쪽 형식을 그대로 받는다. SSL 사용 여부는
    # 이 스킴으로 판단해야 한다 - 하드코딩하면 로컬(HTTP 전용 MinIO)에서 SSL 핸드셰이크를
    # 시도하다가 "Unsupported or unrecognized SSL message" 오류로 실패한다.
    raw_endpoint = config["s3_endpoint"] or ""
    use_ssl = raw_endpoint.strip().lower().startswith("https://")
    endpoint_clean = raw_endpoint.replace("https://", "").replace("http://", "")
    s3_endpoint_url = f"{'https' if use_ssl else 'http'}://{endpoint_clean}"

    spark = (
        SparkSession.builder
        .appName("Gold_Apt_Recent_Trade_Mart")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
            "org.apache.hadoop:hadoop-aws:3.4.1",
        )
        # --- 8GB VM(GCP e2-standard-2, 2 vCPU) 메모리 안전 설정 - Real_Estate_Transform.py와
        # 동일한 값. Airflow DAG가 Gold 태스크를 전부 순차 실행하도록 바꿔서
        # (gold_mart_serial_pool) 이 프로세스 혼자 여유 메모리(4.5~5GB)를 쓴다는 전제로,
        # JVM 오버헤드까지 감안해 힙은 보수적으로 3g로 잡는다(환경변수로 오버라이드 가능). ---
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
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", config["s3_access_key"])
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", config["s3_secret_key"])
        .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_clean)
        .config("spark.hadoop.fs.s3a.access.key", config["s3_access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", config["s3_secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true" if use_ssl else "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =====================================================================================
# 3. dim_apartment 로드 (Broadcast Join에 쓸 컬럼만 선별)
#    [2026-09-10] mno/sno/apartment_id는 이 스크립트 CREATE TABLE DDL(목표 스키마)에는
#    적혀 있지만, apartment_key_v2 컷오버(Real_Estate_Transform.py CUTOVER_APARTMENT_KEY_V2)
#    이전까지는 실제 운영 lakehouse.dim_apartment Iceberg 테이블에 물리적으로 존재하지
#    않는다(dim_apartment MERGE INTO도 이 3컬럼은 채운 적이 없다 - 5-5 MERGE INTO 참고).
#    여기서 select하면 UNRESOLVED_COLUMN.WITH_SUGGESTION AnalysisException으로 즉시 실패한다
#    (2026-09-09 GCP 운영 환경 실제 재현). 컷오버 전까지는 dim_apartment의 실제 키(sgg_cd,
#    dong_cd, apt_name)에 해당하는 컬럼만 읽는다.
# =====================================================================================
def load_dim_apartment(spark: SparkSession) -> DataFrame:
    return spark.table("lakehouse.dim_apartment").select(
        "sgg_cd", "sgg_nm", "dong_cd", "dong_nm", "apt_name", "build_year"
    )


# =====================================================================================
# 4. Step 1: fact_apt_transactions 최근 90일 필터링(+ 적응형 조회기간 폴백) + 아파트 단위
#    1차 집계
#    - load_fact_with_adaptive_lookback()이 deal_date 필터로 Iceberg의 days(deal_date)
#      파티션 프루닝을 그대로 활용해 최근 90일 파티션만 읽는다(빠른 경로). dim_apartment에는
#      있지만 이 90일 구간에 거래가 없는 단지("장기 미거래 단지")는, 그 단지에 한해서만 전체
#      이력에서 자신의 최신 거래일자를 찾아 그 날짜 기준 최근 90일을 추가로 보강한다
#      (adaptive_lookback.py 참고) - dim_apartment에 단지명은 있는데 프론트에서 조회할
#      데이터가 없는 문제를 막기 위함이다. 거래취소건 제외 + 금액/면적 0 이하 제거는 이
#      안에서 이미 적용된다.
#    - 아파트 식별 키(sgg_cd+dong_cd+apt_name) 기준으로 매매가 총합/평단가 총합/
#      최근 매매가(max_by)/거래량을 여기서 한 번에 집계해, 이후 단계는 소용량
#      집계 결과만 다루게 만든다(셔플 최소화).
#    - 면적(평) 지표 total_pyeong/latest_trade_pyeong도 같은 집계에서 함께 뽑는다 -
#      total_price_per_pyeong과 동일하게 공급면적 기준 평(supply_pyeong, SUPPLY_AREA_RATIO
#      적용)을 쓴다. latest_trade_pyeong은 latest_trade_amount와 동일하게 max_by(..., "deal_date")
#      로 뽑아, "가장 최근 거래 1건"의 금액과 면적이 서로 같은 거래를 가리키도록 맞춘다.
#    - mno/sno(지번)는 API 조회 파라미터로 쓸 값이라, 아파트당 대표값 1개만
#      (first-non-null) 함께 뽑아둔다.
# =====================================================================================
def aggregate_recent_trades(
    spark: SparkSession, dim_df: DataFrame, base_date: date, start_date: date
) -> DataFrame:
    fact_df = (
        load_fact_with_adaptive_lookback(
            spark,
            dim_apartment_df=dim_df,
            fact_table="lakehouse.fact_apt_transactions",
            base_date=base_date,
            start_date=start_date,
            lookback_days=LOOKBACK_DAYS,
            extra_select_cols=("mno", "sno"),
        )
        .withColumn(
            "supply_pyeong",
            (F.col("exclusive_area_m2") * F.lit(SUPPLY_AREA_RATIO)) / F.lit(PYEONG_M2),
        )
        .withColumn("price_per_pyeong", F.col("price_ten_thousand") / F.col("supply_pyeong"))
    )

    return fact_df.groupBy("sgg_cd", "dong_cd", "apt_name", "mno", "sno").agg(
        F.round(F.sum("price_ten_thousand")).cast(LongType()).alias("total_trade_amount"),
        F.round(F.sum("price_per_pyeong")).cast(LongType()).alias("total_price_per_pyeong"),
        F.round(F.sum("supply_pyeong"), 2).cast(DoubleType()).alias("total_pyeong"),
        F.round(F.max_by("price_ten_thousand", "deal_date")).cast(LongType()).alias("latest_trade_amount"),
        F.round(F.max_by("supply_pyeong", "deal_date"), 2).cast(DoubleType()).alias("latest_trade_pyeong"),
        F.count(F.lit(1)).cast(IntegerType()).alias("trade_count"),
    )


# =====================================================================================
# 5. Step 2: API 호출 최적화
#    5-1. 1차 집계 결과에서 고유 아파트 식별 키만 Driver로 collect
#    5-2. Driver에서 ThreadPoolExecutor로 건축HUB API를 병렬 호출해 세대수/사용승인일 수집
#    5-3. 수집 결과를 작은 Spark DataFrame으로 변환 (Broadcast Join용)
# =====================================================================================
def collect_apartment_keys(agg_df: DataFrame) -> list[Row]:
    """1차 집계된 고유 아파트 식별 키(+지번) 목록만 Driver로 가져온다."""
    return agg_df.select("sgg_cd", "dong_cd", "apt_name", "mno", "sno").collect()


def _format_jibun_param(value) -> str:
    """MNO/SNO 원본 문자열을 건축HUB API가 요구하는 4자리 zero-padded 지번 코드로 변환."""
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = 0
    return f"{number:04d}"


def _normalize_use_approval_date(value) -> str | None:
    """건축HUB API의 useAprDay가 빈 문자열/공백만 있는 경우 None으로 정규화한다.
    Spark 4.0 ANSI 모드에서 F.to_date(..., "yyyyMMdd")가 공백 문자열을 NULL로
    변환하지 않고 CANNOT_PARSE_TIMESTAMP 예외를 던지는 문제를 여기서 막는다."""
    if value is None:
        return None
    stripped = str(value).strip()
    return stripped or None


def fetch_building_info(row: Row, api_key: str) -> dict:
    """
    건축HUB 건축물대장정보 서비스(getBrRecapTitleInfo)를 호출해 세대수/사용승인일을 조회한다.
    실패하거나 결과가 없으면 두 값 모두 None으로 채운 dict를 반환한다(배치 전체는 계속 진행).
    """
    result: dict[str, object] = {
        "sgg_cd": row["sgg_cd"],
        "dong_cd": row["dong_cd"],
        "apt_name": row["apt_name"],
        "household_count": None,
        "use_approval_date": None,
    }

    if not row["mno"]:
        return result

    params = {
        "serviceKey": api_key,
        "sigunguCd": row["sgg_cd"],
        "bjdongCd": row["dong_cd"],
        "platGbCd": "0",
        "bun": _format_jibun_param(row["mno"]),
        "ji": _format_jibun_param(row["sno"]),
        "numOfRows": "1",
        "pageNo": "1",
        "_type": "json",
    }

    # 타임아웃/429/5xx 같은 일시적 오류는 지수 백오프(1s, 2s, 4s ...)로 최대
    # API_MAX_RETRIES회까지 재시도한다. 재시도를 다 소진해도 실패하면 두 값 모두
    # None으로 채운 result를 반환해 배치 전체는 계속 진행된다.
    last_error = None
    for attempt in range(API_MAX_RETRIES + 1):
        # 일일 트래픽 한도를 초과하면 실제 API 호출 전에 DailyTrafficExceededError를 발생시킨다.
        api_call_limiter.increment()
        # 스레드 간 호출 간격을 강제해 429(Too Many Requests)를 방지한다.
        api_rate_limiter.wait()

        try:
            response = requests.get(
                BUILDING_API_URL,
                params=params,
                headers=API_REQUEST_HEADERS,
                timeout=(API_CONNECT_TIMEOUT_SECONDS, API_READ_TIMEOUT_SECONDS),
            )
            if response.status_code in API_RETRYABLE_STATUS_CODES:
                raise requests.exceptions.HTTPError(
                    f"retryable status code {response.status_code}", response=response
                )
            response.raise_for_status()
            # [2026-09-10] 공공데이터포털 API는 정상 200 응답이면서도 (서비스키 미등록/일시
            # 오류/해당 지번 데이터 없음 등의 이유로) "response"나 "body"가 키는 있되 값이
            # null인 응답을 종종 돌려준다("response": null, "body": null 등). 이전에는
            # .get("response", {})의 default {}가 "키가 아예 없을 때"만 적용되고 "값이
            # None으로 명시된 경우"에는 적용되지 않아, 그다음 .get("body", ...) 호출이
            # AttributeError('NoneType' object has no attribute 'get')를 던졌다. 이
            # AttributeError는 requests.exceptions.RequestException이 아니라서 아래 except에
            # 잡히지 않고 그대로 스레드 밖으로 전파되어 fetch_building_info_parallel()의
            # future.result()에서 재발생, main() 전체를 크래시시켰다(이 함수 docstring이
            # 약속한 "실패하면 None으로 채운 dict를 반환하고 배치는 계속 진행"이 지켜지지
            # 않는 버그). "or {}"로 값이 None이어도 항상 dict로 정규화해 이 경로 자체를
            # 없앤다.
            response_body = (response.json().get("response") or {}).get("body") or {}
            items = response_body.get("items") if isinstance(response_body, dict) else None
            item = (items or {}).get("item") if isinstance(items, dict) else None
            if isinstance(item, list):
                item = item[0] if item else None

            if item:
                result["household_count"] = item.get("hhldCnt")
                result["use_approval_date"] = _normalize_use_approval_date(item.get("useAprDay"))
            return result
        except (requests.exceptions.RequestException, ValueError, AttributeError, TypeError, KeyError) as e:
            # [2026-09-10] 위 정규화로도 못 막을 수 있는 그 밖의 예상 밖 응답 형태(JSON 파싱
            # 실패, "response"/"body"가 dict가 아닌 다른 타입 등)까지 방어선으로 함께 잡는다 -
            # 이 함수가 애초에 약속한 "실패 시 None으로 채운 결과 반환, 배치는 계속 진행"과
            # 동일하게 재시도 후 폴백 처리되도록 한다(네트워크 오류와 동일하게 취급).
            last_error = e
            if attempt < API_MAX_RETRIES:
                time.sleep(API_RETRY_BACKOFF_BASE_SECONDS * (2 ** attempt))
                continue
            print(
                f"[WARN] 건축물대장 API 호출 실패 (최대 재시도 {API_MAX_RETRIES}회 초과) "
                f"(sgg_cd={row['sgg_cd']}, dong_cd={row['dong_cd']}, apt_name={row['apt_name']}): {last_error}"
            )

    return result


def fetch_building_info_parallel(apartment_rows: list[Row], api_key: str) -> list[dict]:
    """Driver에서 ThreadPoolExecutor로 건축HUB API를 병렬 호출한다."""
    if not api_key:
        print("[WARN] DATA_GO_KR_KEY가 없어 세대수/사용승인일 조회를 건너뜁니다.")
        return [
            {
                "sgg_cd": row["sgg_cd"], "dong_cd": row["dong_cd"], "apt_name": row["apt_name"],
                "household_count": None, "use_approval_date": None,
            }
            for row in apartment_rows
        ]

    results = []
    with ThreadPoolExecutor(max_workers=API_MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_building_info, row, api_key) for row in apartment_rows]
        try:
            for future in as_completed(futures):
                results.append(future.result())
        except DailyTrafficExceededError:
            # 아직 시작하지 않은 나머지 호출은 취소하고, 상위(main)로 예외를 그대로 전파해
            # 프로그램을 종료시킨다.
            for pending_future in futures:
                pending_future.cancel()
            raise

    print(f"[INFO] 건축HUB API 조회 완료: {len(results)}건")
    return results


def _sql_literal(value) -> str:
    """API 조회 결과의 파이썬 값을 Spark SQL VALUES 절에 쓸 리터럴 문자열로 변환."""
    if value is None:
        return "NULL"
    if isinstance(value, int):
        return str(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def build_api_dataframe(spark: SparkSession, api_results: list[dict]) -> DataFrame:
    """
    API 조회 결과(list[dict])를 작은 Spark DataFrame으로 변환한다(Broadcast Join용).
    로컬 Windows 환경에서는 spark.createDataFrame(list, schema)가 파이썬 워커 콜백
    문제로 실패하는 경우가 있어(build_dong_pyeong_mart.py와 동일한 이유), JVM 안에서만
    파싱/평가되는 Spark SQL VALUES 절로 만든다.
    """
    if not api_results:
        # 빈 리스트일 때도 spark.createDataFrame([], schema=...)는 로컬 Windows 환경에서
        # 위와 동일한 파이썬 워커 콜백 문제를 일으키므로, 값 없이(WHERE 1=0) SQL만으로
        # 스키마가 확정된 빈 DataFrame을 만든다.
        empty_values_sql = _sql_literal("") + ", " + _sql_literal("") + ", " + _sql_literal("") \
            + ", " + _sql_literal(0) + ", " + _sql_literal("")
        df = spark.sql(
            f"SELECT * FROM VALUES ({empty_values_sql}) "
            "AS t(sgg_cd, dong_cd, apt_name, household_count, use_approval_date) WHERE 1=0"
        )
        return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in BUILDING_API_SCHEMA.fields])

    values_sql = ",\n".join(
        "({}, {}, {}, {}, {})".format(
            _sql_literal(row["sgg_cd"]),
            _sql_literal(row["dong_cd"]),
            _sql_literal(row["apt_name"]),
            _sql_literal(int(row["household_count"]) if row["household_count"] not in (None, "") else None),
            _sql_literal(row["use_approval_date"] or None),
        )
        for row in api_results
    )
    df = spark.sql(
        f"SELECT * FROM VALUES {values_sql} "
        "AS t(sgg_cd, dong_cd, apt_name, household_count, use_approval_date)"
    )
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in BUILDING_API_SCHEMA.fields])


# =====================================================================================
# 6. Step 3: Broadcast Join으로 최종 Gold 마트 완성
#    - 1차 집계 결과(agg_df, Executor에 파티셔닝돼 있음)에 dim_apartment/api_building_df
#      (둘 다 소용량) 를 broadcast()로 붙여 셔플 없이 조인한다.
# =====================================================================================
def build_gold_mart(agg_df: DataFrame, dim_df: DataFrame, api_df: DataFrame) -> DataFrame:
    joined_df = (
        agg_df.alias("f")
        # [2026-09-10] dim_apartment는 mno/sno 컬럼이 없고(위 load_dim_apartment 주석 참고)
        # 실제 고유키는 (sgg_cd, dong_cd, apt_name) 3개뿐이므로 그 키로만 조인한다.
        .join(
            broadcast(dim_df).alias("d"),
            on=[
                F.col("f.sgg_cd") == F.col("d.sgg_cd"),
                F.col("f.dong_cd") == F.col("d.dong_cd"),
                F.col("f.apt_name") == F.col("d.apt_name"),
            ],
            how="inner",
        )
        .join(
            broadcast(api_df).alias("a"),
            on=[
                F.col("f.sgg_cd") == F.col("a.sgg_cd"),
                F.col("f.dong_cd") == F.col("a.dong_cd"),
                F.col("f.apt_name") == F.col("a.apt_name"),
            ],
            how="left",
        )
    )

    # 건축HUB API의 useAprDay가 항상 완전한 yyyyMMdd(8자리)로 오지는 않는다 - 실제로
    # '199810'/'200107'처럼 일(dd)이 빠진 6자리 값, 드물게는 연도(yyyy) 4자리만 오는 값도
    # 섞여 들어온다. 예전에는 F.to_date(..., "yyyyMMdd")로 통일 파싱했는데, ANSI 모드에서
    # 이런 값을 만나면 NULL로 바꾸지 않고 CANNOT_PARSE_TIMESTAMP 예외를 던져 배치 전체가
    # 죽었다(write 단계에서 로우를 실제로 평가할 때 터짐). 이제는 원본 문자열 길이별로
    # F.try_to_timestamp(...)(파싱 실패 시 예외 대신 NULL)로 유효성을 검증한 뒤, 확보한
    # 정밀도만큼만 "yyyy-MM-dd" / "yyyy-MM" / "yyyy"로 포맷한다 - 있는 정보(연/월)까지
    # 통째로 버리지 않고 최대한 살려서 저장한다. 그래서 이 컬럼은 DateType이 아니라
    # (정밀도가 섞여 있으므로) 가변 포맷 문자열(StringType)로 저장한다.
    _raw_use_approval = F.trim(F.col("a.use_approval_date"))
    _len_use_approval = F.length(_raw_use_approval)
    _parsed_day = F.try_to_timestamp(_raw_use_approval, F.lit("yyyyMMdd"))
    _parsed_month = F.try_to_timestamp(_raw_use_approval, F.lit("yyyyMM"))
    _parsed_year = F.try_to_timestamp(_raw_use_approval, F.lit("yyyy"))

    use_approval_date_col = (
        F.when((_len_use_approval == 8) & _parsed_day.isNotNull(), F.date_format(_parsed_day, "yyyy-MM-dd"))
        .when((_len_use_approval == 6) & _parsed_month.isNotNull(), F.date_format(_parsed_month, "yyyy-MM"))
        .when((_len_use_approval == 4) & _parsed_year.isNotNull(), F.date_format(_parsed_year, "yyyy"))
        .otherwise(F.lit(None).cast(StringType()))
    )
    use_approval_year_col = (
        F.when((_len_use_approval == 8) & _parsed_day.isNotNull(), F.year(_parsed_day))
        .when((_len_use_approval == 6) & _parsed_month.isNotNull(), F.year(_parsed_month))
        .when((_len_use_approval == 4) & _parsed_year.isNotNull(), F.year(_parsed_year))
        .otherwise(F.lit(None).cast(IntegerType()))
    )

    return joined_df.select(
        # 아파트 식별자/이름/위치 정보 (dim_apartment 기준)
        # [2026-09-10] dim_apartment.apartment_id는 실제로 존재/적재된 적이 없어(위 join 주석
        # 참고) 참조할 수 없다. 이 결과가 upsert_spark_partition(key_columns=["apt_id"])의
        # 병합 키로 쓰이므로, agg_df의 자연키(sgg_cd/dong_cd/apt_name/mno/sno - Step 1
        # groupBy와 동일 granularity)를 Real_Estate_Transform.py의 dim_apartment_source
        # apartment_id와 동일한 sha2 공식으로 해시해 대체한다. 같은 입력이면 항상 같은
        # apt_id가 나오므로(결정적 함수) 재실행/재집계해도 멱등성이 그대로 유지된다.
        F.sha2(
            F.concat_ws(
                "|",
                *[
                    F.coalesce(F.col(f"f.{name}"), F.lit(""))
                    for name in ("sgg_cd", "dong_cd", "apt_name", "mno", "sno")
                ],
            ),
            256,
        ).alias("apt_id"),
        F.col("d.apt_name").alias("apt_name"),
        F.col("d.sgg_cd").alias("sgg_cd"),
        F.col("d.sgg_nm").alias("sgg_nm"),
        F.col("d.dong_cd").alias("dong_cd"),
        F.col("d.dong_nm").alias("dong_nm"),
        F.col("d.build_year").cast(IntegerType()).alias("build_year"),
        F.col("f.mno").alias("mno"),
        F.col("f.sno").alias("sno"),
        # 최근 90일 거래 지표
        F.col("f.total_trade_amount").alias("total_trade_amount"),
        F.col("f.total_price_per_pyeong").alias("total_price_per_pyeong"),
        F.col("f.total_pyeong").alias("total_pyeong"),
        F.col("f.latest_trade_amount").alias("latest_trade_amount"),
        F.col("f.latest_trade_pyeong").alias("latest_trade_pyeong"),
        F.col("f.trade_count").alias("trade_count"),
        # 건축HUB API 연계 정보
        F.col("a.household_count").alias("household_count"),
        use_approval_date_col.alias("use_approval_date"),
        use_approval_year_col.alias("use_approval_year"),
    )


# =====================================================================================
# 7. 실행 진입점
# =====================================================================================
def main():
    if len(sys.argv) > 1:
        base_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        base_date = datetime.now().date()
    start_date = base_date - timedelta(days=LOOKBACK_DAYS - 1)

    print(
        f"[INFO] 아파트 Gold 마트(dm_apt_recent_trade) 생성 시작: "
        f"base_date={base_date}, 조회기간={start_date} ~ {base_date} ({LOOKBACK_DAYS}일)"
    )

    config = load_config()
    spark = create_spark_session(config)

    dim_df = load_dim_apartment(spark)
    agg_df = aggregate_recent_trades(spark, dim_df, base_date, start_date).cache()

    apartment_rows = collect_apartment_keys(agg_df)
    print(f"[INFO] 최근 {LOOKBACK_DAYS}일 거래가 있는 고유 아파트 수: {len(apartment_rows)}건")

    # [2026-09-09 건축HUB API 호출 절감] 세대수/사용승인일은 건물 준공 시점에 정해지는
    # 사실상 불변 정보라, 이전에 이미 조회에 성공한 단지는 영구 캐시(building_info_cache)에서
    # 바로 가져오고 API를 다시 호출하지 않는다. 캐시에 없는(신규) 단지나 이전에 실패했던
    # 단지만 실제로 API를 호출한다 - 일일 호출 한도를 매번 전체 단지 재조회에 낭비하지
    # 않기 위함이다.
    cached_building_info = load_building_info_cache(spark, config["lake_bucket"])
    resolved_from_cache, apartment_rows_to_call = split_cached_and_missing(
        apartment_rows, cached_building_info
    )
    print(
        f"[INFO] 건축HUB API 캐시 재사용 {len(resolved_from_cache)}건 / "
        f"신규 호출 대상 {len(apartment_rows_to_call)}건"
    )

    try:
        api_results = fetch_building_info_parallel(apartment_rows_to_call, config["data_go_kr_key"])
    except DailyTrafficExceededError:
        print("[ERROR] 공공데이터포털에서 일일트래픽이 초과되어 종료합니다")
        # 진행 중이던 캐시/집계 데이터를 모두 지우고, 저장(write) 없이 종료한다.
        agg_df.unpersist()
        spark.stop()
        sys.exit(1)

    # 캐시로 즉시 해결된 결과 + 이번에 새로 API로 조회한 결과를 합쳐 최종 api_results를 만든다.
    cached_results = [
        {
            "sgg_cd": key[0], "dong_cd": key[1], "apt_name": key[2],
            "household_count": value[0], "use_approval_date": value[1],
        }
        for key, value in resolved_from_cache.items()
    ]
    api_results = cached_results + api_results

    # 다음 실행을 위한 캐시 갱신: 기존 캐시 전체를 유지한 채(이번 실행에 다시 등장하지 않은
    # 단지의 정보도 여전히 불변 정보이므로 지우지 않는다) 새로 성공한 결과만 덧붙인다.
    updated_cache = dict(cached_building_info)
    for result in api_results:
        if result["household_count"] is not None or result["use_approval_date"] is not None:
            key = (result["sgg_cd"], result["dong_cd"], result["apt_name"])
            updated_cache[key] = (result["household_count"], result["use_approval_date"])
    save_building_info_cache(spark, config["lake_bucket"], updated_cache)

    api_df = build_api_dataframe(spark, api_results)

    gold_df = build_gold_mart(agg_df, dim_df, api_df).cache()

    # -----------------------------------------------------------------------------
    # S3 Lake(MinIO) mart/ 경로에 최종 결과를 저장한다.
    # 저장 경로: {S3_END_POINT}/{LAKE}/mart/dm_apt_recent_trade/base_date=YYYY-MM-DD
    # - S3_END_POINT는 create_spark_session()에서 이미 s3a 커넥터 endpoint로 등록돼 있으므로,
    #   실제 URI 문자열 자체는 LAKE 버킷 기준 s3a://{LAKE}/... 형태로 구성한다
    #   (build_dong_pyeong_mart.py의 MART_PATHS와 동일한 규칙).
    # - [2026-09-09 base_date 재실행 대응] 같은 base_date로 하루에 여러 번 다시 돌려도(수동
    #   재실행 등) upsert_spark_partition()이 매번 무조건 전체 재작성하지 않고, 이전 저장
    #   결과와 비교해 변경이 없으면 재작성을 건너뛰고(SKIP), 있으면 apt_id 기준으로
    #   병합(UPDATE)한다. 파티션 자체가 없으면 그대로 새로 저장한다(INSERT).
    # -----------------------------------------------------------------------------
    mart_path = f"s3a://{config['lake_bucket']}/mart/dm_apt_recent_trade/base_date={base_date}"
    upsert_spark_partition(
        spark, mart_path, gold_df, key_columns=["apt_id"], mart_label="dm_apt_recent_trade"
    )

    print("\n===== [SCHEMA] dm_apt_recent_trade =====")
    gold_df.printSchema()

    print("\n===== [SAMPLE] dm_apt_recent_trade 상위 20건 =====")
    gold_df.show(20, truncate=False)

    total_count = gold_df.count()
    print(f"\n===== [COUNT] dm_apt_recent_trade 총 레코드 수: {total_count}건 =====")

    spark.stop()


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""
공공데이터포털 국토교통부 건축HUB_건축물대장정보(getBrRecapTitleInfo) 조회 결과 공유 캐시 -
build_apt_recent_trade_mart.py 전용.

[문제] 세대수(household_count)/사용승인일(use_approval_date)은 건물 준공 시점에 정해지는
사실상 불변 정보인데도, 이 스크립트는 매일 "최근 90일 거래가 있는 고유 아파트 전체"를
다시 API로 조회한다. 건축HUB API는 일일 호출 한도(DAILY_TRAFFIC_LIMIT)가 있어, 매일 같은
아파트를 반복 조회하면 정작 신규 아파트를 조회하기 전에 한도가 소진될 수 있다.

[해결] (sgg_cd, dong_cd, apt_name) 단지 식별 키 기준으로 이미 조회에 성공한 결과를 S3
Lake(MinIO) mart/_building_info_cache/data.parquet에 영구 저장해두고, 다음 실행부터는
캐시에 있는 단지는 API를 호출하지 않고 캐시값을 그대로 쓴다. 조회에 실패했거나(household_
count/use_approval_date가 둘 다 None) 아직 한 번도 조회하지 못한 단지만 API를 호출한다 -
일시적 실패나 신규 아파트도 다음 실행에서 자연스럽게 재시도된다.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

BUILDING_INFO_CACHE_SCHEMA = StructType([
    StructField("sgg_cd", StringType(), True),
    StructField("dong_cd", StringType(), True),
    StructField("apt_name", StringType(), True),
    StructField("household_count", IntegerType(), True),
    StructField("use_approval_date", StringType(), True),
])


def building_info_cache_path(lake_bucket: str) -> str:
    return f"s3a://{lake_bucket}/mart/_building_info_cache/data.parquet"


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def load_building_info_cache(
    spark: SparkSession, lake_bucket: str
) -> dict[tuple[str, str, str], tuple]:
    """(sgg_cd, dong_cd, apt_name) -> (household_count, use_approval_date) 캐시를 읽는다.
    캐시 파일이 아직 없으면(첫 실행) 빈 dict를 반환한다."""
    path = building_info_cache_path(lake_bucket)
    try:
        df = spark.read.parquet(path)
    except Exception:
        return {}

    cache: dict[tuple[str, str, str], tuple] = {}
    for row in df.collect():
        cache[(row["sgg_cd"], row["dong_cd"], row["apt_name"])] = (
            row["household_count"], row["use_approval_date"],
        )
    return cache


def save_building_info_cache(
    spark: SparkSession, lake_bucket: str, cache: dict[tuple[str, str, str], tuple]
) -> None:
    """캐시 전체를 통째로 덮어쓴다. cache가 비어 있으면 아무 것도 하지 않는다."""
    if not cache:
        return
    path = building_info_cache_path(lake_bucket)
    values_sql = ",\n".join(
        "({}, {}, {}, {}, {})".format(
            _sql_literal(key[0]), _sql_literal(key[1]), _sql_literal(key[2]),
            _sql_literal(value[0]), _sql_literal(value[1]),
        )
        for key, value in cache.items()
    )
    df = spark.sql(
        f"SELECT * FROM VALUES {values_sql} "
        "AS t(sgg_cd, dong_cd, apt_name, household_count, use_approval_date)"
    )
    df = df.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in BUILDING_INFO_CACHE_SCHEMA.fields]
    )
    df.write.mode("overwrite").parquet(path)


def split_cached_and_missing(
    apartment_rows: list,
    cache: dict[tuple[str, str, str], tuple],
) -> tuple[dict[tuple[str, str, str], tuple], list]:
    """apartment_rows(sgg_cd/dong_cd/apt_name을 가진 Row 목록)를 캐시에 이미 성공 결과가
    있는 것과 없는 것으로 나눈다. household_count/use_approval_date가 둘 다 None으로 저장된
    캐시 항목은 "조회 실패"로 간주해 재시도 대상(missing)에 포함시킨다."""
    resolved: dict[tuple[str, str, str], tuple] = {}
    missing: list = []
    for row in apartment_rows:
        key = (row["sgg_cd"], row["dong_cd"], row["apt_name"])
        cached = cache.get(key)
        if cached is not None and (cached[0] is not None or cached[1] is not None):
            resolved[key] = cached
        else:
            missing.append(row)
    return resolved, missing

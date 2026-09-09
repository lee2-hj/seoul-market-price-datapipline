# -*- coding: utf-8 -*-
"""
카카오맵 지오코딩 결과 공유 캐시 - main_mart.py와 [동 x 평형대] 계열 마트 4종(공통 준비
로직: dong_pyeong_common.py)이 함께 쓴다.

[문제] 아파트 좌표(latitude/longitude)는 사실상 불변 정보인데도, 이 5개 스크립트는 각자
독립된 프로세스로 실행되며(SparkSession을 공유하지 않음) 매번 자기 프로세스 안에서만
유효한 인메모리 캐시(_geocode_cache)만 두고 있었다. 그 결과 같은 아파트가 하루에 최대
5번(main_mart + dm_*.py 4종) 카카오맵 API로 다시 지오코딩되고, DAG를 재실행하면 또 처음부터
전부 다시 호출된다.

[해결] (sgg_cd, dong_cd, apt_name) 단지 식별 키 기준으로 이미 좌표를 성공적으로 찾은
결과를 S3 Lake(MinIO) mart/_geocode_cache/data.parquet에 영구 저장해두고, 다음 호출부터는
이 캐시에 있는 단지는 API를 호출하지 않고 캐시값을 그대로 쓴다. 지오코딩에 실패한
단지(latitude가 None)는 캐시에 남기지 않아 - 다음 실행에서 다시 시도된다(일시적 실패를
영구히 못 찾는 것으로 확정짓지 않기 위함).

[주의] 이 캐시는 "성공한 결과만" 담으므로, 새로 지어진 단지가 카카오맵 검색에 아직 안
잡히는 경우 등은 계속 재시도된다 - 의도된 동작이다.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

GEOCODE_CACHE_SCHEMA = StructType([
    StructField("sgg_cd", StringType(), True),
    StructField("dong_cd", StringType(), True),
    StructField("apt_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    # main_mart.py처럼 정확도 구분이 없는 호출부는 None으로 저장/무시하면 된다.
    StructField("is_exact_location", BooleanType(), True),
])


def geocode_cache_path(lake_bucket: str) -> str:
    return f"s3a://{lake_bucket}/mart/_geocode_cache/data.parquet"


def _sql_literal(value) -> str:
    """캐시 dict 값을 Spark SQL VALUES 절 리터럴로 변환한다. spark.createDataFrame(list,
    schema)는 로컬 Windows 환경에서 Python 워커 콜백 문제로 실패하는 경우가 있어(다른
    Gold 마트 스크립트들과 동일한 이유), 순수 SQL VALUES 절로 우회한다."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value!r}D"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def load_geocode_cache(spark: SparkSession, lake_bucket: str) -> dict[tuple[str, str, str], tuple]:
    """(sgg_cd, dong_cd, apt_name) -> (latitude, longitude, is_exact_location) 캐시를 읽는다.
    캐시 파일이 아직 없으면(첫 실행) 빈 dict를 반환한다."""
    path = geocode_cache_path(lake_bucket)
    try:
        df = spark.read.parquet(path)
    except Exception:
        return {}

    cache: dict[tuple[str, str, str], tuple] = {}
    for row in df.collect():
        cache[(row["sgg_cd"], row["dong_cd"], row["apt_name"])] = (
            row["latitude"], row["longitude"], row["is_exact_location"],
        )
    return cache


def save_geocode_cache(
    spark: SparkSession, lake_bucket: str, cache: dict[tuple[str, str, str], tuple]
) -> None:
    """캐시 전체를 통째로 덮어쓴다(이번 실행 기준 알고 있는 전체 좌표). cache가 비어 있으면
    아무 것도 하지 않는다(캐시 파일을 지우지 않음 - 읽기 실패와 "의도적으로 빈 캐시"를
    구분할 필요가 없어 굳이 지울 이유가 없다)."""
    if not cache:
        return
    path = geocode_cache_path(lake_bucket)
    values_sql = ",\n".join(
        "({}, {}, {}, {}, {}, {})".format(
            _sql_literal(key[0]), _sql_literal(key[1]), _sql_literal(key[2]),
            _sql_literal(value[0]), _sql_literal(value[1]),
            _sql_literal(value[2] if len(value) > 2 else None),
        )
        for key, value in cache.items()
    )
    df = spark.sql(
        f"SELECT * FROM VALUES {values_sql} "
        "AS t(sgg_cd, dong_cd, apt_name, latitude, longitude, is_exact_location)"
    )
    df = df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in GEOCODE_CACHE_SCHEMA.fields])
    df.write.mode("overwrite").parquet(path)


def split_cached_and_missing(
    distinct_apt_rows: list,
    cache: dict[tuple[str, str, str], tuple],
) -> tuple[dict[tuple[str, str, str], tuple], list]:
    """distinct_apt_rows(sgg_cd/dong_cd/apt_name을 가진 Row 목록)를 캐시에 이미 있는 것과
    없는 것으로 나눈다. 반환값: (이번에 캐시로 즉시 해결된 결과 dict, API 호출이 필요한
    Row 목록)."""
    resolved: dict[tuple[str, str, str], tuple] = {}
    missing: list = []
    for row in distinct_apt_rows:
        key = (row["sgg_cd"], row["dong_cd"], row["apt_name"])
        if key in cache:
            resolved[key] = cache[key]
        else:
            missing.append(row)
    return resolved, missing

"""Persistent Kakao geocoding cache backed by an Iceberg dimension table."""

from pyspark.sql import DataFrame, SparkSession


TABLE_NAME = "lakehouse.dim_apartment_location"


def ensure_location_table(spark: SparkSession) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            location_id STRING,
            sgg_cd STRING,
            dong_cd STRING,
            apt_name STRING,
            mno STRING,
            sno STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            is_exact_location BOOLEAN,
            geocode_status STRING,
            geocode_provider STRING,
            updated_at TIMESTAMP
        ) USING iceberg
    """)


def load_success_cache(spark: SparkSession) -> dict[tuple, tuple]:
    """Return only reusable successes; failed rows must be retried on a later run."""
    ensure_location_table(spark)
    rows = spark.table(TABLE_NAME).filter(
        "latitude IS NOT NULL AND longitude IS NOT NULL"
    ).select(
        "sgg_cd", "dong_cd", "apt_name", "mno", "sno",
        "latitude", "longitude", "is_exact_location",
    ).collect()
    return {
        (r["sgg_cd"], r["dong_cd"], r["apt_name"], r["mno"], r["sno"]): (
            r["latitude"], r["longitude"], bool(r["is_exact_location"])
        )
        for r in rows
    }


def upsert_locations(spark: SparkSession, locations: DataFrame) -> None:
    """Upsert one physical apartment location per raw jibun-aware natural key."""
    ensure_location_table(spark)
    source = locations.selectExpr(
        "sha2(concat_ws('|', coalesce(sgg_cd, ''), coalesce(dong_cd, ''), "
        "coalesce(apt_name, ''), coalesce(mno, ''), coalesce(sno, '')), 256) AS location_id",
        "sgg_cd", "dong_cd", "apt_name", "mno", "sno",
        "latitude", "longitude", "is_exact_location",
        "CASE WHEN latitude IS NULL OR longitude IS NULL THEN 'FAILED' "
        "WHEN is_exact_location THEN 'EXACT' ELSE 'APPROXIMATE' END AS geocode_status",
        "'KAKAO' AS geocode_provider",
        "current_timestamp() AS updated_at",
    )
    source.createOrReplaceTempView("location_updates")
    spark.sql(f"""
        MERGE INTO {TABLE_NAME} target
        USING location_updates source
        ON target.sgg_cd = source.sgg_cd
        AND target.dong_cd = source.dong_cd
        AND target.apt_name = source.apt_name
        AND coalesce(target.mno, '') = coalesce(source.mno, '')
        AND coalesce(target.sno, '') = coalesce(source.sno, '')
        WHEN MATCHED THEN UPDATE SET
            target.location_id = source.location_id,
            target.latitude = source.latitude,
            target.longitude = source.longitude,
            target.is_exact_location = source.is_exact_location,
            target.geocode_status = source.geocode_status,
            target.geocode_provider = source.geocode_provider,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)


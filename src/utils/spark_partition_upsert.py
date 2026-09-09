# -*- coding: utf-8 -*-
"""
Gold 마트 공통 유틸(PySpark 기반 마트 전용: main_mart.py / dm_dong_pyeong_price_avg.py /
dm_apt_price_avg.py / dm_apt_pyeong_price.py / dm_apt_flr_price.py /
build_apt_recent_trade_mart.py):
base_date 파티션 하나를 무조건 write.mode("overwrite")로 통째로 재작성하는 대신, DuckDB+
Polars 계열 마트(apt_rtt_mart.py/apt_mkt_trends_mart.py)의 upsert_partition()과 동일한
취지로 Insert/Update/Skip을 판별해 처리한다.

[동기] 이 마트들의 base_date는 "스크립트 실행일"이라 원칙적으로 하루에 한 번만 새 파티션이
생기지만, Airflow 수동 재실행/재시도로 같은 base_date를 하루에 여러 번 다시 돌리는 경우가
드물지 않다. 이때 매번 무조건 전체 재작성을 하면 새로 계산된 값이 이전 실행과 완전히
같아도 불필요하게 파일을 다시 쓰게 된다(SKIP 불가) - 대용량 마트일수록 이 IO 비용이 크다.

[구현 방식 - DuckDB 버전과의 차이] Polars의 row fingerprint(정렬 후 hash_rows)는 전체 행을
드라이버로 모으지 않고도 사실상 동일한 효과를 내지만, Spark DataFrame에서 같은 방식을 쓰려면
전체 collect가 필요해 비싸다. 대신 이 모듈은 "행 수 + 행별 해시의 합(order-independent
checksum)"으로 동일성을 근사 판정한다 - 극히 드문 해시 충돌 가능성이 있지만(다른 행들이
바뀌었는데 합이 우연히 같아지는 경우), 실용적으로는 충분히 안전하고 driver로 전체 행을
모으지 않고 Spark 집계(action) 한 번으로 끝난다.

[주의 - 같은 경로를 읽고 다시 쓰는 문제] UPDATE 케이스는 mart_path에서 읽은 old_df를 그
mart_path에 다시 write.mode("overwrite")한다. old_df를 캐시(materialize)해두지 않으면
Spark의 지연 평가 특성상 write 시점에 old_df의 원본 파일을 다시 읽으려다(그 사이 write가
이미 경로를 지우기 시작했을 수 있어) 실패하거나 잘못된 결과가 나올 수 있다. 그래서 old_df를
읽자마자 곧바로 .cache() + count()로 강제 materialize한다.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _fingerprint(df: DataFrame, row_count: int | None = None) -> tuple[int, int]:
    """(행 수, 행별 해시의 합)을 계산한다. 컬럼 순서가 달라도(같은 컬럼 집합이면) 같은
    결과가 나오도록 컬럼명을 정렬한 뒤 해시를 계산하고, 행 순서와 무관하게 같은 값이
    나오도록 합(교환법칙 성립)으로 합친다."""
    cols = sorted(df.columns)
    row_hash = F.hash(*[F.col(c) for c in cols]).cast("bigint")
    agg = df.select(F.sum(row_hash).alias("checksum")).collect()[0]
    checksum = agg["checksum"] or 0
    count = row_count if row_count is not None else df.count()
    return count, checksum


def upsert_spark_partition(
    spark: SparkSession,
    mart_path: str,
    new_df: DataFrame,
    key_columns: list[str],
    mart_label: str = "",
) -> str:
    """base_date 파티션 하나(mart_path)를 Insert/Update/Skip 중 하나로 처리하고 그 결과를
    문자열로 반환한다.
      - INSERT: 해당 경로에 기존 파티션이 없거나(최초 실행) 비어 있음 -> new_df 그대로 저장.
      - SKIP:   기존 파티션과 new_df의 내용(행 수 + 체크섬)이 완전히 같음 -> 재작성 생략.
      - UPDATE: key_columns 기준으로 기존 행 중 new_df에 없는 키만 보존(anti-join)하고
                new_df를 그대로 union해 저장한다 - 같은 키는 new_df 값으로 교체(Upsert),
                new_df에만 있는 키는 추가(Insert), 기존에만 있던 키는 보존.

    key_columns는 이 마트가 "레코드 하나"를 식별하는 비즈니스 키 컬럼들이어야 한다(예:
    dm_apt_price_avg는 [cgg_cd, stdg_cd, bldg_nm]).
    """
    try:
        old_df = spark.read.parquet(mart_path).cache()
        old_row_count = old_df.count()
    except Exception:
        new_df.write.mode("overwrite").parquet(mart_path)
        print(f"[INSERT] {mart_label}: 신규 파티션 생성 ({new_df.count()}건) - {mart_path}")
        return "insert"

    if old_row_count == 0:
        # 빈 파티션(과거 실패한 실행 등)도 없는 것과 동일하게 INSERT로 취급한다.
        new_df.write.mode("overwrite").parquet(mart_path)
        old_df.unpersist()
        print(f"[INSERT] {mart_label}: 빈 기존 파티션을 신규 결과로 교체 ({new_df.count()}건) - {mart_path}")
        return "insert"

    old_count, old_checksum = _fingerprint(old_df, row_count=old_row_count)
    new_count, new_checksum = _fingerprint(new_df)

    if old_count == new_count and old_checksum == new_checksum:
        old_df.unpersist()
        print(f"[SKIP]   {mart_label}: 변경 없음 ({old_count}건, 파티션 재작성 생략) - {mart_path}")
        return "skip"

    merged_df = old_df.join(new_df, on=key_columns, how="left_anti").unionByName(
        new_df, allowMissingColumns=True
    )
    merged_df.write.mode("overwrite").parquet(mart_path)
    merged_count = merged_df.count()
    old_df.unpersist()
    print(
        f"[UPDATE] {mart_label}: 기존 {old_count}건 + 신규계산 {new_count}건 -> "
        f"병합 후 {merged_count}건 (같은 키는 최신값으로 교체) - {mart_path}"
    )
    return "update"

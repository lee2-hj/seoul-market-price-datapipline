# -*- coding: utf-8 -*-
"""
Gold 마트 공통 유틸(PySpark 마트 전용): dim_apartment에는 등록돼 있지만(단지명 존재) 기본
조회기간(오늘 기준 최근 LOOKBACK_DAYS일)에는 거래가 하나도 없는 "장기 미거래 단지"를 위한
적응형(adaptive) 조회기간 폴백 로직.

[문제 상황]
main_mart.py / dong_pyeong_common.py / build_apt_recent_trade_mart.py는 지금까지
fact_apt_transactions를 "오늘 기준 최근 90일"이라는 하나의 고정 구간으로만 조회했다
(Iceberg의 days(deal_date) 파티션 프루닝을 최대한 활용하기 위해서다). 그런데
dim_apartment(Silver)에는 등록돼 있지만 그 90일 구간 안에는 거래가 아예 없는 단지(오래
전에만 거래된 저빈도 단지)는 fact와의 조인 결과 자체가 없어 어떤 Gold 마트에도 등장하지
못한다. 프론트엔드는 dim_apartment 기준으로 단지명을 노출/검색하므로, 이 경우 "단지명은
있는데 조회하면 데이터가 없는" 문제가 발생한다.

[해결 방식 - 2단계, 예외 케이스에만 추가 비용을 지불한다]
  1) 기존과 동일하게 "오늘 기준 최근 LOOKBACK_DAYS일" 고정 구간을 그대로 먼저 읽는다
     (빠른 경로 - Iceberg 파티션 프루닝을 100% 활용한다. 대다수 단지는 이 경로만으로
     충분하다).
  2) 그 빠른 경로에 전혀 나타나지 않는 dim_apartment 단지("미거래 단지")만 추려서, 그
     단지들에 한해서만 fact_apt_transactions 전체 이력을 훑어(컬럼 프루닝 + 단지 키
     브로드캐스트 세미조인으로 최대한 좁혀서) 그 단지 스스로의 "가장 최근 거래일자"를
     찾고, 그 날짜를 기준으로 각자 자신만의 최근 LOOKBACK_DAYS일 구간을 채워 넣는다.
  대다수(정상적으로 거래 중인) 단지는 추가 비용이 전혀 없고, 미거래 단지(전체 중 소수일
  것으로 기대)에 대해서만 전체 이력 스캔이라는 무거운 연산을 치른다 - "모든 실행마다 항상
  전체 이력을 스캔"하는 설계보다 OOM 위험이 훨씬 낮다(GCP e2-standard-2, 2 vCPU/8GB 기준).

[메모리 안전]
  - 전체 이력 스캔은 반드시 미거래 단지 키로 브로드캐스트 세미조인해 최대한 이른 시점에
    걸러낸 뒤에만 groupBy/캐싱한다(전체 이력 자체를 통째로 캐싱하지 않는다).
  - select()로 필요한 컬럼만 프로젝션한다(컬럼 프루닝 - Parquet/Iceberg 열 스캔 비용 절감).
  - 폴백 결과 데이터프레임은 "미거래 단지 수 x LOOKBACK_DAYS일" 규모로, 원래 빠른 경로
    데이터보다 훨씬 작을 것으로 기대된다.
  - missing_keys.count()/전체 이력 스캔은 "미거래 단지가 있을 때만" 실행된다(없으면
    fast_df를 그대로 반환하고 추가 비용 0).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# dim_apartment <-> fact_apt_transactions 두 테이블이 공통으로 가진, 단지를 특정하는 조인 키.
APT_KEY_COLS = ["sgg_cd", "dong_cd", "apt_name"]

# 모든 Gold 마트가 공통으로 쓰는 데이터 품질 필터: 거래취소건 제외 + 금액/면적 0 이하 제거.
# main_mart.py/dong_pyeong_common.py/build_apt_recent_trade_mart.py가 각자 인라인으로
# 적용하던 조건과 동일하다 - 이 모듈이 반환하는 데이터프레임은 이미 이 필터가 적용된
# 상태이므로, 호출부에서 다시 적용할 필요가 없다.
def apply_quality_filter(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("cancel_date").isNull() | (F.trim(F.col("cancel_date")) == ""))
        & (F.col("price_ten_thousand") > 0)
        & (F.col("exclusive_area_m2") > 0)
    )


def load_fact_with_adaptive_lookback(
    spark: SparkSession,
    dim_apartment_df: DataFrame,
    fact_table: str,
    base_date,
    start_date,
    lookback_days: int,
    extra_select_cols: tuple = (),
) -> DataFrame:
    """
    fact_table(예: "lakehouse.fact_apt_transactions")에서 [start_date, base_date] 고정
    구간(빠른 경로, Iceberg 파티션 프루닝)을 읽고, dim_apartment_df에는 있지만 이 구간에
    거래가 하나도 없는 단지에 한해 전체 이력에서 자신의 최신 거래일자를 찾아 그 날짜 기준
    최근 lookback_days일 데이터를 추가로 채워 합친 데이터프레임을 반환한다.

    dim_apartment_df: 최소 sgg_cd/dong_cd/apt_name 컬럼을 포함해야 한다.
    반환 스키마: ["sgg_cd", "dong_cd", "apt_name", "price_ten_thousand",
    "exclusive_area_m2", "deal_date", "cancel_date", *extra_select_cols] - 데이터 품질
    필터(apply_quality_filter)가 이미 적용된 상태다.
    """
    base_select_cols = [
        "sgg_cd", "dong_cd", "apt_name",
        "price_ten_thousand", "exclusive_area_m2", "deal_date", "cancel_date",
    ]
    select_cols = base_select_cols + [c for c in extra_select_cols if c not in base_select_cols]

    fast_df = apply_quality_filter(
        spark.table(fact_table)
        .filter((F.col("deal_date") >= F.lit(start_date)) & (F.col("deal_date") <= F.lit(base_date)))
        .select(*select_cols)
    )

    dim_keys = dim_apartment_df.select(*APT_KEY_COLS).distinct()
    fast_keys = fast_df.select(*APT_KEY_COLS).distinct()

    # dim_apartment에는 있지만 빠른 경로(최근 lookback_days일)에는 거래가 없는 단지 목록.
    # dim_keys/fast_keys 둘 다 소용량(수만 건 이하)이라 브로드캐스트 조인으로 충분히 가볍다.
    missing_keys = dim_keys.join(broadcast(fast_keys), on=APT_KEY_COLS, how="left_anti")
    missing_count = missing_keys.count()

    if missing_count == 0:
        print(
            f"[INFO] 적응형 조회기간 폴백 대상 없음: dim_apartment의 모든 단지가 기본 "
            f"{lookback_days}일 구간 안에 거래 데이터를 가지고 있습니다."
        )
        return fast_df

    print(
        f"[INFO] 기본 {lookback_days}일 구간에 거래가 없는 단지 {missing_count}건 발견 - "
        f"전체 이력에서 단지별 최신 거래일자를 찾아 그 날짜 기준 최근 {lookback_days}일을 "
        f"추가로 보강합니다(적응형 조회기간 폴백)."
    )

    missing_keys_bc = broadcast(missing_keys)

    # 전체 이력 중 "미거래 단지"에 해당하는 행만 남긴다 - 브로드캐스트 세미조인으로 최대한
    # 이른 시점에 걸러내, 이후 단계(groupBy/조인)가 전체 이력이 아니라 이 작은 부분집합만
    # 다루게 만든다(전체 이력 자체를 cache하지 않음 - OOM 안전). base_date 이후 미래 데이터는
    # 애초에 대상이 아니므로 상한만 유지한다.
    stale_history_df = (
        apply_quality_filter(
            spark.table(fact_table)
            .filter(F.col("deal_date") <= F.lit(base_date))
            .select(*select_cols)
        )
        .join(missing_keys_bc, on=APT_KEY_COLS, how="inner")
    ).cache()

    latest_per_apt = (
        stale_history_df.groupBy(*APT_KEY_COLS)
        .agg(F.max("deal_date").alias("latest_deal_date"))
        .withColumn("fallback_start_date", F.date_sub(F.col("latest_deal_date"), lookback_days - 1))
    )

    fallback_df = (
        stale_history_df.alias("s")
        .join(broadcast(latest_per_apt).alias("w"), on=APT_KEY_COLS, how="inner")
        .filter(
            (F.col("s.deal_date") >= F.col("w.fallback_start_date"))
            & (F.col("s.deal_date") <= F.col("w.latest_deal_date"))
        )
        .select(*[F.col(f"s.{c}") for c in select_cols])
    )

    combined_df = fast_df.unionByName(fallback_df)
    print(f"[INFO] 적응형 조회기간 폴백 보강 완료 (대상 단지 {missing_count}건).")
    return combined_df

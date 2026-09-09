# -*- coding: utf-8 -*-
"""Gold 마트 ① DM_DONG_PYEONG_PRICE_AVG - MinIO S3 Lake mart/dm_dong_pyeong_price_avg/
폴더에 대응하는 단독 실행 가능한 PySpark 배치 스크립트. [동] 그룹(자치구+법정동) 최근
90일 평균가를 집계해 저장한다.
좌표는 동 안의 단지들을 평균한 대표 좌표를 부여한다(동 단위 지도 핀에 사용).
IS_EXACT_LOCATION은 그 평균에 들어간 단지 좌표가 전부 정확 매칭이었을 때만 True로 두는
보수적 집계(bool_and) - 하나라도 4단계 법정동 폴백이 섞이면 대표 좌표의 정밀도를 보장할
수 없다고 보기 때문이다.

Silver 읽기/조인/카카오맵 지오코딩 등 공통 준비 로직은 dong_pyeong_common.py에 있다.
다른 dm_*.py 마트 스크립트와 SparkSession을 공유하지 않는다 - GCP 메모리 제약으로
Airflow에서 마트를 하나씩 순차 실행(이전 마트가 완전히 끝나 메모리를 반환한 뒤 다음
마트 시작)해야 해서, 각 스크립트가 완전히 독립적으로 뜨고 죽는다.

실행 방법:
  python src/transformation/gold/dm_dong_pyeong_price_avg.py [BASE_DATE]
    - BASE_DATE(YYYY-MM-DD) 생략 시 오늘 날짜를 기준으로 최근 90일치를 집계한다.
"""

import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType

from transformation.gold.dong_pyeong_common import GoldMartContext, build_gold_mart_context
from utils.spark_partition_upsert import upsert_spark_partition

MART_NAME = "dm_dong_pyeong_price_avg"
# [자치구+법정동] 그룹 하나를 식별하는 비즈니스 키 (base_date 재실행 시 upsert 기준).
KEY_COLUMNS = ["cgg_cd", "stdg_cd"]


def build(ctx: GoldMartContext) -> DataFrame:
    return (
        ctx.common_df.groupBy("sgg_cd", "sgg_nm", "dong_cd", "dong_nm")
        .agg(
            F.count(F.lit(1)).cast(IntegerType()).alias("deal_cnt"),
            F.round(F.avg("latitude"), 6).alias("latitude"),
            F.round(F.avg("longitude"), 6).alias("longitude"),
            F.bool_and("is_exact_location").alias("is_exact_location"),
            # 동별 매매가격 총합 (만원) - avg_thing_amt(평균)를 대체하는 합계 컬럼
            F.round(F.sum("price_ten_thousand")).cast(LongType()).alias("total_thing_amt"),
            # 동별 평당가 총합 (만원/평) - avg_pyeong_amt(평균)를 대체하는 합계 컬럼
            F.round(F.sum("pyeong_amt")).cast(LongType()).alias("total_pyeong_amt"),
        )
        .select(
            F.lit(ctx.base_date_str).alias("base_date"),
            F.col("sgg_cd").alias("cgg_cd"),
            F.col("sgg_nm").alias("cgg_nm"),
            F.col("dong_cd").alias("stdg_cd"),
            F.col("dong_nm").alias("stdg_nm"),
            "deal_cnt",
            "latitude",
            "longitude",
            "is_exact_location",
            "total_thing_amt",
            "total_pyeong_amt",
        )
    )


def run(ctx: GoldMartContext) -> None:
    mart_path = ctx.mart_paths[MART_NAME]
    # base_date 재실행(수동 재시도 등) 시 무조건 전체 재작성하지 않고, 변경 없으면 스킵,
    # 있으면 KEY_COLUMNS 기준으로 병합한다.
    upsert_spark_partition(ctx.spark, mart_path, build(ctx), key_columns=KEY_COLUMNS, mart_label=f"① {MART_NAME}")


if len(sys.argv) > 1:
    _base_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
else:
    _base_date = datetime.now().date()

_ctx = build_gold_mart_context(_base_date)
run(_ctx)
_ctx.spark.stop()
print(f"[INFO] {MART_NAME} 저장 완료 (MinIO S3 Lake 전용, RDB 적재 없음)")

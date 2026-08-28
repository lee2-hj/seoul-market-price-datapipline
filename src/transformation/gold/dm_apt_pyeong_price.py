# -*- coding: utf-8 -*-
"""Gold 마트 ③ DM_APT_PYEONG_PRICE - MinIO S3 Lake mart/dm_apt_pyeong_price/ 폴더에
대응하는 단독 실행 가능한 PySpark 배치 스크립트. [단지 x 평형별] 최근 90일 평균가를
집계해 저장한다.

Silver 읽기/조인/카카오맵 지오코딩 등 공통 준비 로직은 dong_pyeong_common.py에 있다.
다른 dm_*.py 마트 스크립트와 SparkSession을 공유하지 않는다 - GCP 메모리 제약으로
Airflow에서 마트를 하나씩 순차 실행(이전 마트가 완전히 끝나 메모리를 반환한 뒤 다음
마트 시작)해야 해서, 각 스크립트가 완전히 독립적으로 뜨고 죽는다.

실행 방법:
  python src/transformation/gold/dm_apt_pyeong_price.py [BASE_DATE]
    - BASE_DATE(YYYY-MM-DD) 생략 시 오늘 날짜를 기준으로 최근 90일치를 집계한다.
"""

import sys
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType

from transformation.gold.dong_pyeong_common import (
    APT_GROUP_COLS,
    GoldMartContext,
    apt_select_cols,
    build_gold_mart_context,
)

MART_NAME = "dm_apt_pyeong_price"


def build(ctx: GoldMartContext) -> DataFrame:
    return (
        ctx.common_df.groupBy(*APT_GROUP_COLS, "pyeong_grp")
        .agg(
            F.count(F.lit(1)).cast(IntegerType()).alias("deal_cnt"),
            F.round(F.sum("price_ten_thousand")).cast(LongType()).alias("total_thing_amt"),
            F.round(F.sum("pyeong_amt")).cast(LongType()).alias("total_pyeong_amt"),
            # 최근 거래가/평당가/계약일자/거래 평수 (마트 ②와 동일한 max_by(값, deal_date) 패턴)
            F.round(F.max_by("price_ten_thousand", "deal_date")).cast(LongType()).alias("recent_thing_amt"),
            F.round(F.max_by("pyeong_amt", "deal_date")).cast(LongType()).alias("recent_pyeong_amt"),
            F.max("deal_date").alias("recent_deal_date"),
            F.round(F.max_by("supply_pyeong", "deal_date"), 2).alias("recent_supply_pyeong"),
        )
        .select(
            *apt_select_cols(ctx), "pyeong_grp",
            "deal_cnt", "total_thing_amt", "total_pyeong_amt",
            "recent_thing_amt", "recent_pyeong_amt", "recent_deal_date", "recent_supply_pyeong",
        )
    )


def run(ctx: GoldMartContext) -> None:
    mart_path = ctx.mart_paths[MART_NAME]
    build(ctx).write.mode("overwrite").parquet(mart_path)
    print(f"[INFO] ③ {MART_NAME} 저장 완료: {mart_path}")


if len(sys.argv) > 1:
    _base_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
else:
    _base_date = datetime.now().date()

_ctx = build_gold_mart_context(_base_date)
run(_ctx)
_ctx.spark.stop()
print(f"[INFO] {MART_NAME} 저장 완료 (MinIO S3 Lake 전용, RDB 적재 없음)")

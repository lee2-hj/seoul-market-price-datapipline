# -*- coding: utf-8 -*-
"""
Gold 마트 공통 유틸(DuckDB+Polars 기반 건별 마트 전용: apt_rtt_mart.py / apt_mkt_trends_mart.py):
dim_apartment에는 등록돼 있지만 기본 조회기간(오늘 기준 최근 LOOKBACK_DAYS일)에는 거래가
하나도 없는 "장기 미거래 단지"를 위한 적응형(adaptive) 조회기간 폴백.

[문제/해결 방식 개요 - PySpark 계열(adaptive_lookback.py)과 동일한 문제의식]
apt_rtt_mart.py/apt_mkt_trends_mart.py는 Iceberg 카탈로그가 아니라 원본 parquet을 "하루(day)
단위"로 직접 글롭해 day 파티션마다 DuckDB 쿼리 -> Polars 변환 -> upsert_partition()을
반복하는 구조다(2026-08-30 SIGABRT 장애 대응 - 90일 전체를 한 번에 읽으면 메모리 폭증이
재현된 이력이 있어, 하루 단위로 쪼개 메모리를 좁게 유지한다).

이 구조에서는 기본 90일 구간을 하루씩 훑고 난 뒤, "그 90일 동안 단 한 번도 등장하지 않은
dim_apartment 단지"만 추려서 시작일 하루 전(start_date - 1일)부터 계속 하루씩 더 과거로
거슬러 올라가며 훑는다. 매 날짜에서:
  - 아직 최신 거래일자를 못 찾은("탐색 중") 단지는 그 날짜에 거래가 있는지 확인하고, 있으면
    그 날짜가 곧 그 단지의 "최신 거래일자"가 된다(그보다 최근 날짜들은 이미 다 훑었는데
    없었기 때문에 성립한다).
  - 이미 최신 거래일자를 찾은("수집 중") 단지는, 그 날짜가 자신의 최근 LOOKBACK_DAYS일
    구간(latest_deal_date - LOOKBACK_DAYS + 1 ~ latest_deal_date) 안에 있는 동안 계속
    데이터를 걷어간다(구간을 벗어나면 관심 대상에서 제외한다).
"탐색 중" + "수집 중" 단지가 모두 사라지거나(전부 처리 완료) 안전 상한
(ADAPTIVE_LOOKBACK_MAX_EXTRA_DAYS)에 도달하면 멈춘다.

[메모리 안전]
매 날짜 조회는 "이번 날짜에 아직 관심 있는 단지 목록"만으로 좁혀서 실행한다(dim_apartment_bc
전체가 아니라 그 날짜에 필요한 소수 단지만 세미조인) - 정상 90일 구간 조회보다 훨씬 좁은
폭의 단지만 다루게 되어 메모리 사용량이 작다. 대다수 실행에서는 미거래 단지 자체가 없어
이 폴백이 아예 실행되지 않는다(추가 비용 0).
"""

import os
from datetime import date, timedelta

import duckdb
import polars as pl

# 안전 상한: 미거래 단지 폴백을 위해 기본 LOOKBACK_DAYS일 구간보다 더 과거로 거슬러 올라갈
# 수 있는 최대 추가 일수. 무한정 과거로 스캔하며 하루씩 S3 파티션을 여는 것을 막기 위한
# 방어선이다(환경변수로 오버라이드 가능 - 기본값 1095일 = 약 3년).
MAX_EXTRA_LOOKBACK_DAYS = int(os.getenv("ADAPTIVE_LOOKBACK_MAX_EXTRA_DAYS", "1095"))

INTERESTED_APTS_TABLE = "_adaptive_interested_apts"


def load_full_dim_apartment_keys(con: duckdb.DuckDBPyConnection) -> set:
    """dim_apartment_bc(load_dim_apartment_broadcast()가 이미 만들어둔 TEMP TABLE)에서
    (sgg_cd, dong_cd, apt_name) 전체 고유 단지 키 집합을 읽어온다."""
    rows = con.execute(
        "SELECT DISTINCT sgg_cd, dong_cd, apt_name FROM dim_apartment_bc"
    ).fetchall()
    return {(r[0], r[1], r[2]) for r in rows}


def _register_interested_apts(con: duckdb.DuckDBPyConnection, keys: set) -> None:
    """이번 날짜 조회에서 "아직 관심 있는" 단지 키 집합을 TEMP TABLE로 등록한다. 매 반복마다
    이 함수로 통째로 교체(CREATE OR REPLACE)한다 - keys는 폴백 대상(dim_apartment 전체 중
    극소수)이라 매번 다시 만들어도 비용이 미미하다."""
    df = pl.DataFrame(
        {
            "sgg_cd": [k[0] for k in keys],
            "dong_cd": [k[1] for k in keys],
            "apt_name": [k[2] for k in keys],
        },
        schema={"sgg_cd": pl.Utf8, "dong_cd": pl.Utf8, "apt_name": pl.Utf8},
    )
    con.register("_adaptive_keys_src", df.to_arrow())
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {INTERESTED_APTS_TABLE} AS SELECT * FROM _adaptive_keys_src"
    )
    con.unregister("_adaptive_keys_src")


def run_adaptive_backward_fallback(
    con: duckdb.DuckDBPyConnection,
    lake_bucket: str,
    start_date: date,
    lookback_days: int,
    seen_apt_keys: set,
    mart_name: str,
    fetch_day_fn,
    shape_fn,
    upsert_fn,
) -> dict:
    """
    dim_apartment 전체 단지 중 기본 LOOKBACK_DAYS일 구간(seen_apt_keys)에 전혀 등장하지 않은
    단지를 찾아, start_date 하루 전부터 과거로 거슬러 올라가며 각 단지 자신의 최신 거래일자
    기준 최근 lookback_days일 데이터를 추가로 채운다.

    fetch_day_fn(con, lake_bucket, day_str, filter_table) -> pl.DataFrame
        해당 마트의 fetch_joined_day()와 동일한 시그니처에 filter_table 키워드 인자만
        추가된 버전이어야 한다(INTERESTED_APTS_TABLE과 세미조인해 결과를 좁힌다).
    shape_fn(pl.DataFrame) -> pl.DataFrame
        해당 마트의 shape_*_columns() 그대로.
    upsert_fn(con, lake_bucket, day_str, mart_day_df) -> str
        해당 마트의 upsert_partition() 그대로(base_date/record_key 등 저장 스키마에 안
        맞는 컬럼은 호출부에서 이미 drop한 뒤 이 함수에 넘겨야 한다 - 기존 메인 루프와
        동일한 규약).

    반환값: {"status_counts": {...}, "total_row_count": int, "processed_day_count": int,
             "extra_days_scanned": int, "unresolved_count": int}
    """
    full_keys = load_full_dim_apartment_keys(con)
    missing_keys = full_keys - seen_apt_keys

    if not missing_keys:
        print(
            f"[INFO] {mart_name}: 적응형 조회기간 폴백 대상 없음 (dim_apartment 전 단지가 "
            f"기본 {lookback_days}일 구간 안에 거래 데이터를 가지고 있습니다)."
        )
        return {
            "status_counts": {"insert": 0, "update": 0, "skip": 0},
            "total_row_count": 0,
            "processed_day_count": 0,
            "extra_days_scanned": 0,
            "unresolved_count": 0,
        }

    print(
        f"[INFO] {mart_name}: 기본 {lookback_days}일 구간에 거래가 없는 단지 "
        f"{len(missing_keys)}건 발견 - 과거로 거슬러 올라가며 단지별 최신 거래일자 기준 "
        f"최근 {lookback_days}일을 보강합니다(안전 상한: 추가 최대 {MAX_EXTRA_LOOKBACK_DAYS}일)."
    )

    searching: set = set(missing_keys)   # 아직 최신 거래일을 못 찾은 단지
    collecting: dict = {}                 # 찾은 단지 -> latest_deal_date(수집 기준일)
    status_counts = {"insert": 0, "update": 0, "skip": 0}
    total_row_count = 0
    processed_day_count = 0
    extra_days_scanned = 0

    day = start_date - timedelta(days=1)

    while (searching or collecting) and extra_days_scanned < MAX_EXTRA_LOOKBACK_DAYS:
        interested = searching | set(collecting.keys())
        _register_interested_apts(con, interested)

        raw_day_df = fetch_day_fn(con, lake_bucket, day.strftime("%Y-%m-%d"), filter_table=INTERESTED_APTS_TABLE)

        if raw_day_df.height > 0:
            day_keys = set(
                zip(
                    raw_day_df["sgg_cd"].to_list(),
                    raw_day_df["dong_cd"].to_list(),
                    raw_day_df["apt_name"].to_list(),
                )
            )
            # 탐색 중이던 단지가 오늘 발견되면: 오늘이 바로 그 단지의 최신 거래일자다(더
            # 최근 날짜는 이미 다 훑었는데 없었으므로).
            newly_found = day_keys & searching
            for key in newly_found:
                collecting[key] = day
            searching -= newly_found

            mart_day_df = shape_fn(raw_day_df)
            status = upsert_fn(con, lake_bucket, day.strftime("%Y-%m-%d"), mart_day_df)
            status_counts[status] += 1
            total_row_count += mart_day_df.height
            processed_day_count += 1

        # 수집 구간(latest_deal_date 기준 최근 lookback_days일)을 벗어난 단지는 더 이상
        # 관심 대상이 아니므로 제거한다 - 오늘(day)이 구간 시작일에 도달했으면 이 단지는
        # 오늘 처리를 끝으로 완료된 것이라 그다음 날(day-1)부터는 제외해야 한다.
        finished = [
            key for key, latest in collecting.items()
            if day <= latest - timedelta(days=lookback_days - 1)
        ]
        for key in finished:
            del collecting[key]

        day -= timedelta(days=1)
        extra_days_scanned += 1

    if searching:
        print(
            f"[WARN] {mart_name}: 안전 상한({MAX_EXTRA_LOOKBACK_DAYS}일) 도달 - 여전히 "
            f"최신 거래일자를 찾지 못한 단지 {len(searching)}건은 이번 실행에서 보강되지 "
            f"않았습니다(다음 실행 때 다시 시도됩니다)."
        )

    print(
        f"[INFO] {mart_name}: 적응형 조회기간 폴백 완료 - 추가로 거슬러 올라간 일수 "
        f"{extra_days_scanned}일, 대상 단지 {len(missing_keys)}건 중 "
        f"{len(missing_keys) - len(searching)}건 최신 거래일자 확인, 거래 존재 파티션 "
        f"{processed_day_count}개 처리 (Insert {status_counts['insert']} / "
        f"Update {status_counts['update']} / Skip {status_counts['skip']})"
    )

    return {
        "status_counts": status_counts,
        "total_row_count": total_row_count,
        "processed_day_count": processed_day_count,
        "extra_days_scanned": extra_days_scanned,
        "unresolved_count": len(searching),
    }

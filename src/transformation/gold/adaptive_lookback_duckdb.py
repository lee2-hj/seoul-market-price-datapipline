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

import gc
import os
from datetime import date, datetime, timedelta

import duckdb
import polars as pl

# 안전 상한: 미거래 단지 폴백을 위해 기본 LOOKBACK_DAYS일 구간보다 더 과거로 거슬러 올라갈
# 수 있는 최대 추가 일수. 무한정 과거로 스캔하며 하루씩 S3 파티션을 여는 것을 막기 위한
# 방어선이다(환경변수로 오버라이드 가능 - 기본값 1095일 = 약 3년).
MAX_EXTRA_LOOKBACK_DAYS = int(os.getenv("ADAPTIVE_LOOKBACK_MAX_EXTRA_DAYS", "1095"))

INTERESTED_APTS_TABLE = "_adaptive_interested_apts"

# [2026-09-09 재실행 비용 절감] 장기 미거래 단지의 "최신 거래일자를 못 찾음(searching)"
# 탐색 결과를 다음 실행에서도 재사용하기 위한 캐시 저장 위치. 이 캐시가 없으면 Airflow를
# 다시 돌릴 때마다 이미 지난 실행에서 찾아둔 단지까지 매번 최대 MAX_EXTRA_LOOKBACK_DAYS일을
# 처음부터 다시 거슬러 올라가며 재탐색하게 된다(단지별로 한 번만 비싼 탐색을 하고, 이후에는
# 캐시된 날짜 기준 lookback_days 구간만 직접 조회하도록 아래 run_adaptive_backward_fallback()이
# 이 캐시를 활용한다). mart_name별로 따로 저장해 apt_rtt_mart/apt_mkt_trends_mart가 서로
# 캐시를 덮어쓰지 않게 한다.
DORMANT_STATE_DIR = "_dormant_apt_state"
DORMANT_STATE_FILE = "data.parquet"


def _dormant_state_path(lake_bucket: str, mart_name: str) -> str:
    return f"s3://{lake_bucket}/mart/{mart_name}/{DORMANT_STATE_DIR}/{DORMANT_STATE_FILE}"


def _load_dormant_state(
    con: duckdb.DuckDBPyConnection, lake_bucket: str, mart_name: str
) -> dict[tuple[str, str, str], date]:
    """이전 실행에서 저장해둔 "단지 키 -> 최신 거래일자" 캐시를 읽는다. 캐시 파일이 아직
    없으면(첫 실행 등) 빈 dict를 반환한다 - 이 경우 전부 기존처럼 처음부터 탐색한다."""
    path = _dormant_state_path(lake_bucket, mart_name)
    try:
        rows = con.execute(
            f"SELECT sgg_cd, dong_cd, apt_name, latest_deal_date "
            f"FROM read_parquet('{path}', hive_partitioning=false)"
        ).fetchall()
    except Exception:
        return {}

    state: dict[tuple[str, str, str], date] = {}
    for sgg_cd, dong_cd, apt_name, latest_deal_date_str in rows:
        try:
            state[(sgg_cd, dong_cd, apt_name)] = datetime.strptime(
                latest_deal_date_str, "%Y-%m-%d"
            ).date()
        except (TypeError, ValueError):
            # 손상되었거나 형식이 다른 행 하나 때문에 캐시 전체를 못 쓰게 되진 않도록 건너뛴다.
            continue
    return state


def _save_dormant_state(
    con: duckdb.DuckDBPyConnection,
    lake_bucket: str,
    mart_name: str,
    state: dict[tuple[str, str, str], date],
) -> None:
    """이번 실행 기준 "여전히 미거래 상태인 단지 -> 최신 거래일자" 캐시를 통째로 다시 쓴다
    (이번 실행에서 다시 거래가 확인된 단지는 호출부에서 이미 state 밖으로 빠져 있어, 여기서
    자연스럽게 캐시에서도 사라진다). state가 비어 있으면(폴백 대상 자체가 없는 정상 케이스)
    빈 파일을 쓰는 대신 그대로 둔다."""
    if not state:
        return
    path = _dormant_state_path(lake_bucket, mart_name)
    df = pl.DataFrame(
        {
            "sgg_cd": [k[0] for k in state],
            "dong_cd": [k[1] for k in state],
            "apt_name": [k[2] for k in state],
            "latest_deal_date": [v.strftime("%Y-%m-%d") for v in state.values()],
        },
        schema={
            "sgg_cd": pl.Utf8, "dong_cd": pl.Utf8, "apt_name": pl.Utf8,
            "latest_deal_date": pl.Utf8,
        },
    )
    con.register("_dormant_state_write", df)
    con.execute(f"""
        COPY (SELECT * FROM _dormant_state_write)
        TO '{path}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    con.unregister("_dormant_state_write")


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

    status_counts = {"insert": 0, "update": 0, "skip": 0}
    total_row_count = 0
    processed_day_count = 0
    extra_days_scanned = 0

    # [2026-09-09 재실행 비용 절감] 이전 실행에서 이미 "이 단지의 최신 거래일자는 언제다"를
    # 찾아둔 캐시(_load_dormant_state)를 확인한다. 이번에도 여전히 미거래 상태인 단지가
    # 캐시에 있으면 탐색(searching) 없이 그 날짜 기준 lookback_days 구간만 곧바로 조회한다
    # (Phase A). 캐시에 없는(이번에 처음 미거래로 확인된) 단지만 기존 방식대로 하루씩
    # 거슬러 올라가며 탐색한다(Phase B) - 매 실행마다 전체 미거래 단지를 최대
    # MAX_EXTRA_LOOKBACK_DAYS일씩 재탐색하던 비용을 단지당 1회로 줄이기 위함이다.
    cached_state = _load_dormant_state(con, lake_bucket, mart_name)
    resolved_keys = {key: cached_state[key] for key in missing_keys if key in cached_state}
    new_missing_keys = missing_keys - resolved_keys.keys()

    print(
        f"[INFO] {mart_name}: 기본 {lookback_days}일 구간에 거래가 없는 단지 "
        f"{len(missing_keys)}건 발견 (캐시로 즉시 재조회 {len(resolved_keys)}건 / "
        f"신규 탐색 대상 {len(new_missing_keys)}건, 안전 상한: 추가 최대 "
        f"{MAX_EXTRA_LOOKBACK_DAYS}일)."
    )

    # -------------------------------------------------------------------
    # Phase A: 캐시에 이미 최신 거래일자가 있는 단지 - 탐색 없이 그 날짜 기준
    # lookback_days 구간(day_to_keys)만 날짜별로 묶어 직접 조회한다.
    # -------------------------------------------------------------------
    if resolved_keys:
        day_to_keys: dict[str, set] = {}
        for key, latest in resolved_keys.items():
            window_start = latest - timedelta(days=lookback_days - 1)
            d = window_start
            while d <= latest:
                day_to_keys.setdefault(d.strftime("%Y-%m-%d"), set()).add(key)
                d += timedelta(days=1)

        for day_str in sorted(day_to_keys.keys()):
            _register_interested_apts(con, day_to_keys[day_str])
            raw_day_df = fetch_day_fn(con, lake_bucket, day_str, filter_table=INTERESTED_APTS_TABLE)
            if raw_day_df.height > 0:
                mart_day_df = shape_fn(raw_day_df)
                status = upsert_fn(con, lake_bucket, day_str, mart_day_df)
                status_counts[status] += 1
                total_row_count += mart_day_df.height
                processed_day_count += 1

        print(
            f"[INFO] {mart_name}: 캐시 기반 재조회 완료 - 단지 {len(resolved_keys)}건, "
            f"조회한 날짜 {len(day_to_keys)}일(탐색 과정 생략)"
        )

    # -------------------------------------------------------------------
    # Phase B: 캐시에 없는(신규) 미거래 단지 - 기존과 동일하게 하루씩 거슬러 올라가며
    # 탐색(searching)과 수집(collecting)을 병행한다. discovered_dates는 발견 즉시(collecting에
    # 넣는 시점) 기록해두어, 안전 상한에 걸려 수집이 끝까지 못 끝나더라도 "찾아낸 날짜"만큼은
    # 캐시에 남겨 다음 실행이 이어받을 수 있게 한다.
    # -------------------------------------------------------------------
    searching: set = set(new_missing_keys)   # 아직 최신 거래일을 못 찾은 단지
    collecting: dict = {}                     # 찾은 단지 -> latest_deal_date(수집 기준일)
    discovered_dates: dict = {}               # 이번 실행에서 새로 찾아낸 단지 -> 최신 거래일자

    day = start_date - timedelta(days=1)

    # [2026-09-10 OOM 대응] dormant_state 캐시가 비어있어(첫 실행 등) missing_keys 전체가
    # new_missing_keys로 넘어오면, 이 while 루프가 안전 상한(MAX_EXTRA_LOOKBACK_DAYS, 기본
    # 1095일)까지 하루 단위로 계속 돌 수 있다. 그런데 그 대부분의 날짜는 거래가 아예 없어서
    # (raw_day_df.height == 0) searching/collecting 집합이 전혀 안 바뀌는데도, 예전 코드는
    # 매 반복마다 무조건 _register_interested_apts()로 새 Arrow 테이블을 등록 ->
    # CREATE OR REPLACE TEMP TABLE -> 등록 해제를 반복했다. 이 재등록 자체는 논리적으로
    # 무해하지만(내용이 같으면 결과도 같음), 이력이 없는 오래된 구간이 수백~1000일 넘게
    # 이어지는 콜드스타트에서는 이 반복 횟수만큼 DuckDB/Arrow 쪽 임시 객체가 쌓여
    # "OutOfMemoryException: ArrowBuffer: failed to allocate ... bytes"로 죽는 게 실제로
    # 재현됐다(2026-09-10, 3,806개 단지가 전부 캐시 미스라 처음부터 끝까지 탐색해야 했던
    # apt_mkt_trends_mart.py 실행). interested 집합이 실제로 바뀐 경우에만 재등록하도록
    # 바꿔 이 불필요한 반복 재생성을 없앤다 - 조회 결과(찾아내는 단지/날짜)는 이전과
    # 완전히 동일하다.
    _previous_interested: frozenset | None = None

    while (searching or collecting) and extra_days_scanned < MAX_EXTRA_LOOKBACK_DAYS:
        interested = searching | set(collecting.keys())
        _interested_frozen = frozenset(interested)
        if _interested_frozen != _previous_interested:
            _register_interested_apts(con, interested)
            _previous_interested = _interested_frozen

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
                discovered_dates[key] = day
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

        # [2026-09-10 OutOfMemoryException(ArrowBuffer) 대응 - 위 재등록 생략/insertion
        # order 끄기 조치로도 해결 안 됨] 실측 결과 fact_apt_transactions_current는
        # 2023-01-29부터 데이터가 있어(약 1,020개 날짜 파티션 중 대부분에 실제 거래 존재),
        # 콜드스타트 폴백(캐시가 비어 3,806개 단지 전부 탐색 필요)은 대부분의 반복에서
        # IOException으로 빠르게 건너뛰어지는 게 아니라 실제로 read_parquet+조인+
        # Arrow(.pl()) 변환을 매번 수행한다. 매 반복이 만드는 DuckDB/Arrow/Polars 결과
        # 객체가 단순 참조 카운트만으로는 바로 회수되지 않고(C 확장 객체 간 순환 참조는
        # 파이썬의 세대별 가비지 컬렉터가 나중에야 청소함) 반복 수백 회가 넘도록 계속
        # 쌓이면서 DuckDB 메모리 추적기가 결국 OOM으로 죽는 것이 실제로 재현됐다. 일정
        # 주기로 강제 가비지 컬렉션을 돌려 이 지연된 회수를 앞당긴다 - 탐색/폴백 로직이나
        # 결과에는 전혀 영향이 없다.
        if extra_days_scanned % 30 == 0:
            gc.collect()

    if searching:
        print(
            f"[WARN] {mart_name}: 안전 상한({MAX_EXTRA_LOOKBACK_DAYS}일) 도달 - 여전히 "
            f"최신 거래일자를 찾지 못한 단지 {len(searching)}건은 이번 실행에서 보강되지 "
            f"않았습니다(다음 실행 때 다시 시도됩니다)."
        )

    print(
        f"[INFO] {mart_name}: 적응형 조회기간 폴백 완료 - 신규 탐색으로 거슬러 올라간 일수 "
        f"{extra_days_scanned}일, 신규 대상 단지 {len(new_missing_keys)}건 중 "
        f"{len(discovered_dates)}건 최신 거래일자 확인, 거래 존재 파티션 "
        f"{processed_day_count}개 처리 (Insert {status_counts['insert']} / "
        f"Update {status_counts['update']} / Skip {status_counts['skip']})"
    )

    # 다음 실행을 위한 캐시 갱신: 이번 실행 기준 여전히 미거래 상태인 단지(resolved_keys +
    # 이번에 새로 찾은 discovered_dates)만 남긴다 - 이번에 다시 거래가 생겨 missing_keys에서
    # 빠진 단지는 seen_apt_keys가 다음 실행에도 계속 잡아줄 것이므로 캐시에 남겨둘 필요가
    # 없어 자연히 제외된다(위에서 이미 missing_keys 기준으로만 resolved_keys를 구성했음).
    updated_state = dict(resolved_keys)
    updated_state.update(discovered_dates)
    _save_dormant_state(con, lake_bucket, mart_name, updated_state)

    return {
        "status_counts": status_counts,
        "total_row_count": total_row_count,
        "processed_day_count": processed_day_count,
        "extra_days_scanned": extra_days_scanned,
        "unresolved_count": len(searching),
    }

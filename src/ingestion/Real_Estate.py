# 서울시 부동산(아파트) 실거래가 원본 정보 적재

import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timedelta

import polars as pl
from duckdb import DuckDBPyConnection

from utils.raw_pipeline import create_session_with_retry, ensure_connection

# 서울 열린데이터광장 부동산 실거래가 정보 API (tbLnOpendataRtmsV)
# 공공데이터포털 API와 달리 쿼리스트링이 아니라 URL 경로 세그먼트로
# 인증키/조회구간/필터를 전달하는 방식이다:
#   {BASE_URL}/{인증키}/{TYPE}/{SERVICE}/{START_INDEX}/{END_INDEX}/{필터...}
BASE_URL = "http://openapi.seoul.go.kr:8088"
REQUEST_TYPE = "json"
SERVICE_NAME = "tbLnOpendataRtmsV"
OPERATION_NAME = "tbLnOpendataRtmsV"

# 한 번에 조회 가능한 최대 건수. 이 값 단위로 START_INDEX~END_INDEX 구간을 옮겨가며 전체를 수집한다.
# (TODO: 실제 인증키로 호출해서 서울 열린데이터광장 공식 가이드 기준 최대 허용 건수가 맞는지 재확인)
PAGE_SIZE = 1000

# API 자체가 돌려주는 논리적 오류(RESULT.CODE != INFO-000)에 대한 짧은 추가 재시도 횟수.
# 네트워크 레벨(타임아웃/5xx 등) 재시도는 create_session_with_retry의 세션이 이미 처리한다.
MAX_PAGE_ATTEMPTS = 3

# 호출 하나마다 최소 이만큼은 쉬어서 요청 빈도를 낮춘다. 짧은 시간에 요청이 몰리면
# 서버 쪽에서 DoS 공격으로 오인해 나중에 차단될 수 있어서 둔 안전장치다.
REQUEST_INTERVAL_SECONDS = 0.5

# [2026-09-09 API 호출 수 절감] fetch_real_estate_recent가 매일 lookback_days(기본 90일)
# 전체를 하루 단위로 다시 호출하면, 공공데이터포털류 API의 일일 호출 건수 제한을 매일
# 똑같이 소모하게 된다. 최근 RECENT_DAILY_CHECK_DAYS일은 신고 지연/정정이 가장 잦은
# 구간이라 매일 그대로 재확인하고, 그보다 오래된 날짜는 완전히 스킵하는 대신
# STALE_RECHECK_INTERVAL_DAYS일에 한 번씩만 주기적으로 재확인한다(offset 기준 모듈로 -
# 별도 상태 저장 없이도 각 오래된 날짜가 결국 주기적으로 다시 확인되게 하는 가장 단순한
# 방식). "완전히 스킵"이 아니라 "빈도만 낮추는" 방식이라 뒤늦은 정정도 결국 반영된다.
RECENT_DAILY_CHECK_DAYS = int(os.getenv("REAL_ESTATE_RECENT_DAILY_CHECK_DAYS", "14"))
STALE_RECHECK_INTERVAL_DAYS = int(os.getenv("REAL_ESTATE_STALE_RECHECK_INTERVAL_DAYS", "7"))

# ---------------------------------------------------------------------------
# 서울 열린데이터광장 API는 선택 필터를 쿼리스트링이 아니라 경로 세그먼트 "위치"로 받는다.
# 즉 뒤쪽 필터(BLDG_USG 등)를 쓰려면 그 앞 순서의 필터 자리까지 전부 채워야 한다 - 이때
# 빈 자리는 빈 문자열이 아니라 공백 문자(" ", URL 인코딩 시 %20) 하나로 채워야 한다
# (실제 호출 URL 예시로 CTRT_DAY 자리(SNO 다음, BLDG_NM 이전)까지 확인됨:
#  .../1/5/2026/%20/%20/%20/%20/%20/%20/%20/%20/20260811/%20/
#  → RCPT_YR=2026, CGG_CD~SNO는 %20(공백)으로 스킵, 그 다음 자리가 CTRT_DAY=20260811).
# BLDG_NM~OPBIZ_RESTAGNT_SGG_NM 구간(CTRT_DAY 이후)은 아직 공식 문서로 재검증되지 않았다.
# TODO: BLDG_NM 이후 순서가 실제 API 문서상의 필터 순서와 정확히 일치하는지 재검증 필요.
# ---------------------------------------------------------------------------
FILTER_PARAM_ORDER = [
    "RCPT_YR", "CGG_CD", "CGG_NM", "STDG_CD", "STDG_NM",
    "LOTNO_SE", "LOTNO_SE_NM", "MNO", "SNO", "CTRT_DAY",
    "BLDG_NM", "THING_AMT", "ARCH_AREA", "LAND_AREA", "FLR",
    "RGHT_SE", "RTRCN_DAY", "ARCH_YR", "BLDG_USG", "DCLR_SE",
    "OPBIZ_RESTAGNT_SGG_NM",
]

# 항상 걸어야 하는 기본 필터. 서울 전체를 가져와야 해서 CGG_CD(자치구코드)는 비워두고,
# BLDG_USG(건물용도)만 "아파트"로 고정한다. CTRT_DAY(계약일자)는 호출 시점의 ctrt_day_from/to에
# 따라 fetch_real_estate가 동적으로 이 필터에 추가한다(단일 날짜로 정확히 일치할 때만 - 이
# API는 위치 기반 파라미터라 범위 조회를 지원하지 않고 정확히 하나의 값만 받을 수 있다).
BASE_FILTERS = {"BLDG_USG": "아파트"}

logger = logging.getLogger(__name__)


def _default_raw_path() -> str:
    """upsert_real_estate가 raw_path를 안 넘겼을 때 읽을 기본 경로.
    fetch_real_estate가 각 row를 계약일(CTRT_DAY) 기준 연/월/일 폴더에 나눠서 저장하기 때문에,
    여기서는 그 파티션 전체를 한 번에 읽을 수 있는 glob 패턴을 돌려준다
    (DuckDB의 read_parquet는 glob 패턴으로 여러 파일을 한 번에 읽을 수 있다)."""
    return f"s3://{os.environ.get('RAW')}/real_estate/year=*/month=*/day=*/real_estate_raw.parquet"


def _raw_path_for_ctrt_day(ctrt_day: str) -> str:
    """CTRT_DAY(계약일, YYYYMMDD 문자열) 기준 Hive 스타일 연/월/일 파티션 경로를 만든다.
    같은 계약일 폴더는 다음 실행 때 최신 전체 데이터로 통째로 덮어써진다 - 그래서 뒤늦게
    신고된 과거 계약 건도 재실행 한 번이면 자연스럽게 반영된다(별도 재처리 로직이 필요 없음)."""
    year, month, day = ctrt_day[:4], ctrt_day[4:6], ctrt_day[6:8]
    return (
        f"s3://{os.environ.get('RAW')}/real_estate/"
        f"year={year}/month={month}/day={day}/real_estate_raw.parquet"
    )


def _fingerprint_rows(rows: list[dict]) -> str:
    """행 목록의 내용 기준 지문(fingerprint)을 계산한다. API가 뒤늦게 신고/정정 건을 반영하는
    경우가 많아 같은 계약일을 여러 번 다시 조회하게 되는데, 매번 그대로 덮어쓰면 실제로는
    바뀐 게 없는 날짜까지 불필요하게 다시 쓰고 후속 처리를 다시 태우게 된다. 행 순서/딕셔너리
    키 순서가 달라져도 내용이 같으면 같은 지문이 나오도록, 각 행을 키 정렬 JSON 문자열로 만든
    뒤 그 문자열들을 다시 정렬해서 합치고 해시한다."""
    normalized = sorted(
        json.dumps(
            {str(k): ("" if v is None else str(v)) for k, v in row.items()},
            sort_keys=True, ensure_ascii=False,
        )
        for row in rows
    )
    return hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()


def _read_existing_day_rows(con: DuckDBPyConnection, ctrt_day: str) -> list[dict] | None:
    """이미 저장된 해당 계약일 parquet이 있으면 읽어서 row 목록(list[dict])으로 반환하고,
    아직 한 번도 저장된 적 없는 날짜면(오늘 새로 등장한 계약일 등) None을 반환한다."""
    path = _raw_path_for_ctrt_day(ctrt_day)
    try:
        existing_df = con.execute(f"SELECT * FROM read_parquet('{path}')").pl()
    except Exception:
        # 해당 계약일 폴더가 아직 없는 경우(S3 404 등) - "기존 데이터 없음"으로 간주한다.
        return None
    return existing_df.to_dicts()


def _group_rows_by_ctrt_day(rows: list[dict]) -> dict[str, list[dict]]:
    """row 목록을 CTRT_DAY(계약일) 값 기준으로 묶는다.
    CTRT_DAY가 없는 row는 어느 날짜 폴더에 넣을지 판단할 기준이 없어서 건너뛰고 경고를 남긴다."""
    grouped: dict[str, list[dict]] = {}
    skipped = 0
    for row in rows:
        ctrt_day = row.get("CTRT_DAY")
        if not ctrt_day:
            skipped += 1
            continue
        grouped.setdefault(ctrt_day, []).append(row)

    if skipped:
        logger.warning("CTRT_DAY 값이 없는 row %d건은 날짜 폴더에 못 넣어서 건너뛰었습니다.", skipped)

    return grouped


def _filter_ctrt_days(
    grouped: dict[str, list[dict]],
    ctrt_day_from: str | None,
    ctrt_day_to: str | None,
) -> dict[str, list[dict]]:
    """ctrt_day_from~ctrt_day_to(둘 다 YYYYMMDD 문자열, 포함) 범위로 CTRT_DAY 그룹을 제한한다.
    둘 다 None이면 전체를 그대로 반환한다 (매일 도는 파이프라인은 늦게 들어온 신고 건도
    반영해야 하니 범위를 안 걸고 항상 전체를 갱신한다 - 초기 백필처럼 특정 기간만 필요할 때만
    이 필터를 쓴다)."""
    if ctrt_day_from is None and ctrt_day_to is None:
        return grouped
    return {
        ctrt_day: rows
        for ctrt_day, rows in grouped.items()
        if (ctrt_day_from is None or ctrt_day >= ctrt_day_from)
        and (ctrt_day_to is None or ctrt_day <= ctrt_day_to)
    }


def _build_filter_segments(filters: dict[str, str]) -> list[str]:
    """FILTER_PARAM_ORDER 순서대로, 값이 채워진 마지막 필터 자리까지 경로 세그먼트를 만든다.
    중간에 값이 없는 자리는 공백 문자(" ") 하나로 채운다 - 빈 문자열("")로 채우면 URL에 "//"
    (빈 경로 세그먼트)가 생겨 API가 뒤쪽 자리 정렬을 제대로 못 읽는 것으로 확인됐다
    (실제 호출 URL 예시의 스킵 자리가 전부 %20인 것과 일치)."""
    if not filters:
        return []
    last_index = max(FILTER_PARAM_ORDER.index(key) for key in filters)
    return [filters.get(name, " ") for name in FILTER_PARAM_ORDER[: last_index + 1]]


def _build_page_url(service_key: str, start_index: int, end_index: int, filters: dict[str, str]) -> str:
    """한 페이지(START_INDEX~END_INDEX) 조회용 요청 URL을 조립한다.
    requests의 params= 를 거치지 않고 경로 세그먼트를 직접 이어붙여 완성된 URL로 보낸다."""
    segments = [
        BASE_URL, service_key, REQUEST_TYPE, SERVICE_NAME,
        str(start_index), str(end_index),
    ]
    segments.extend(_build_filter_segments(filters))
    return "/".join(segments)


def _parse_real_estate_json(response_text: str, operation_name: str) -> tuple[int, list[dict]]:
    """서울 열린데이터광장 응답(JSON)을 파싱한다.
    공공데이터포털의 {"response": {"resultCode": ..., "item": [...]}} 구조와 달리, 최상위가
    SERVICE_NAME(tbLnOpendataRtmsV) 키로 감싸져 있고 그 안에 RESULT.CODE/MESSAGE + row 배열이
    들어있는 구조라 parse_items_xml을 그대로 쓸 수 없다. 그래서 이 API 전용 파서를 따로 둔다.

    다만 "조회된 데이터가 없음"(CODE=INFO-200)이거나 다른 논리적 오류인 경우에는 API가
    SERVICE_NAME 래퍼 없이 {"RESULT": {...}}를 최상위에 바로 내려준다(정상 응답만 래퍼가
    있음). 이 경우 body를 SERVICE_NAME 키로 찾으면 없어서 빈 dict가 되고 CODE/MESSAGE가
    둘 다 None으로 보여 실제로는 "0건"인 정상 상황을 에러로 오인하게 된다."""
    payload = json.loads(response_text)
    body = payload.get(SERVICE_NAME)

    if body is None:
        # SERVICE_NAME 래퍼가 없는 응답: 최상위 RESULT를 직접 본다.
        # [2026-09-10] .get("RESULT", {})의 default {}는 "RESULT" 키가 아예 없을 때만
        # 적용되고, 공공데이터포털이 종종 내려주는 "RESULT": null 형태의 응답에는 적용되지
        # 않아 None이 그대로 반환된다 - 그러면 바로 다음 줄 result.get("CODE")가
        # AttributeError('NoneType' object has no attribute 'get')를 던진다. 이 AttributeError는
        # 아래에서 기대하는 RuntimeError가 아니라서 _fetch_page의 논리적 오류 재시도 루프도
        # 못 잡고, 특히 probe 요청(fetch_real_estate 최초 1건 조회)에서 발생하면 아무 보호
        # 장치 없이 task_fetch_real_estate 전체를 그대로 크래시시킨다. "or {}"로 None이어도
        # 항상 dict로 정규화해, 의도한 대로 RuntimeError(원인 메시지 포함)로 안전하게
        # 종료되게 한다.
        result = payload.get("RESULT") or {}
        result_code = result.get("CODE")
        if result_code == "INFO-200":
            # "해당하는 데이터가 없습니다" - 에러가 아니라 정상적인 0건 응답이다.
            return 0, []
        result_msg = result.get("MESSAGE")
        raise RuntimeError(f"{operation_name} 실패: CODE={result_code}, MESSAGE={result_msg}")

    # 위와 동일한 이유("RESULT": null 응답 방어)로 body 쪽도 or {}로 정규화한다.
    result = body.get("RESULT") or {}
    result_code = result.get("CODE")
    if result_code != "INFO-000":
        result_msg = result.get("MESSAGE")
        raise RuntimeError(f"{operation_name} 실패: CODE={result_code}, MESSAGE={result_msg}")

    total_count = int(body.get("list_total_count", 0))
    rows = body.get("row", [])  # 이미 dict 리스트라 XML처럼 태그를 따로 순회할 필요가 없다.

    return total_count, rows


def _fetch_page(
    service_key: str, start_index: int, end_index: int, filters: dict[str, str]
) -> tuple[int, list[dict]]:
    """한 페이지(START_INDEX~END_INDEX)를 조회해서 (list_total_count, 이번 페이지 row 목록)을 반환한다.
    HTTP 레벨(타임아웃/5xx) 재시도는 세션의 Retry 어댑터가 담당하고, 여기서는 API가 정상 200과
    함께 논리적 오류(RESULT.CODE)를 돌려주는 경우에만 짧게 추가로 재시도한다."""
    url = _build_page_url(service_key, start_index, end_index, filters)
    session = create_session_with_retry()

    last_error: Exception | None = None
    for attempt in range(1, MAX_PAGE_ATTEMPTS + 1):
        response = session.get(url, timeout=30)
        # 성공/실패 여부와 상관없이 호출 하나마다 최소 이만큼은 쉬어서 요청 빈도를 낮춘다.
        time.sleep(REQUEST_INTERVAL_SECONDS)
        response.raise_for_status()
        try:
            return _parse_real_estate_json(response.text, OPERATION_NAME)
        except RuntimeError as exc:
            last_error = exc
            logger.warning(
                "페이지(%s~%s) 조회 실패, 재시도 %s/%s: %s",
                start_index, end_index, attempt, MAX_PAGE_ATTEMPTS, exc,
            )
            time.sleep(1.0 * attempt)

    raise RuntimeError(
        f"{OPERATION_NAME} 페이지({start_index}~{end_index}) 조회 실패 (재시도 {MAX_PAGE_ATTEMPTS}회 소진)"
    ) from last_error


def _build_page_ranges(total_count: int, already_covered: int) -> list[tuple[int, int]]:
    """already_covered(먼저 probe로 받아둔 건수) 다음 인덱스부터 total_count까지를 PAGE_SIZE
    단위로 잘라 (START_INDEX, END_INDEX) 목록을 만든다. 마지막 구간은 PAGE_SIZE로 딱 안
    떨어져도 min()으로 total_count를 넘지 않게 자른다."""
    ranges: list[tuple[int, int]] = []
    start = already_covered + 1
    while start <= total_count:
        end = min(start + PAGE_SIZE - 1, total_count)
        ranges.append((start, end))
        start = end + 1
    return ranges


# ---------------------------------------------------------------------------
# fetch_real_estate: START_INDEX/END_INDEX를 옮겨가며 전체 페이지 조회 -> JSON 파싱
#                     -> CTRT_DAY(계약일)별 연/월/일 파티션에 parquet 저장
# ---------------------------------------------------------------------------

# 아파트 매매 실거래가는 goodId 같은 단일 파라미터 없이도 전체 조회가 되지만, 한 번에 최대
# PAGE_SIZE건까지만 내려주기 때문에 list_total_count를 기준으로 끝까지 반복 조회해야 한다.
def fetch_real_estate(
    con: DuckDBPyConnection | None,
    service_key: str | None = None,
    ctrt_day_from: str | None = None,
    ctrt_day_to: str | None = None,
) -> str:
    # con이 None이거나 S3 설정이 안 돼 있어도 여기서 항상 MinIO 연결을 보장한다.
    con = ensure_connection(con)
    service_key = service_key or os.environ.get("KEY")
    # os.environ.get("KEY")도 str | None이라 narrowing이 안 되면 _fetch_page(service_key: str)에
    # None이 넘어갈 수 있다고 타입체커가 판단한다. 여기서 명시적으로 None을 걸러 타입을 좁힌다.
    if service_key is None:
        raise RuntimeError("KEY 환경변수가 설정되지 않았습니다 (env/.env의 KEY 확인 필요).")

    # CTRT_DAY(계약일자)는 API가 위치 기반 파라미터라 범위 조회를 지원하지 않고 정확히 하나의
    # 값만 받을 수 있다. ctrt_day_from/to가 둘 다 주어지고 서로 같을 때(데일리 배치가 항상
    # 이렇게 호출한다)만 서버 필터로 걸고, 그 외(범위/백필 등)는 기존처럼 전체를 받아와
    # _filter_ctrt_days로 사후 필터링한다.
    filters = dict(BASE_FILTERS)
    if ctrt_day_from is not None and ctrt_day_from == ctrt_day_to:
        filters["CTRT_DAY"] = ctrt_day_from

    # 1) 가벼운 probe 요청(1건)으로 총 건수(list_total_count)를 먼저 파악한다.
    #    이 요청으로 받은 row는 버리지 않고 그대로 최종 결과에 포함시켜서, 같은 구간을 다시 요청하지 않는다.
    total_count, probe_rows = _fetch_page(service_key, 1, 1, filters)
    all_rows: list[dict] = list(probe_rows)
    logger.info(
        "%s 전체 건수(list_total_count): %s (probe 요청으로 %s건 선확보)",
        OPERATION_NAME, total_count, len(probe_rows),
    )

    if total_count <= 0:
        # 수집할 데이터가 아예 없는 것도 정상적인 케이스라, 에러로 죽이지 않고 경고 로그만 남긴다.
        logger.warning("%s: list_total_count=%s, 수집할 데이터가 없습니다.", OPERATION_NAME, total_count)

    # 2) probe에서 이미 받은 건수 다음부터, PAGE_SIZE 단위로 나머지 구간을 계산한다.
    page_ranges = _build_page_ranges(total_count, already_covered=len(probe_rows))
    total_pages = len(page_ranges)
    failed_ranges: list[tuple[int, int]] = []

    for page_no, (start_index, end_index) in enumerate(page_ranges, start=1):
        logger.info(
            "%s: %s/%s 페이지 요청 중 (%s~%s)",
            OPERATION_NAME, page_no, total_pages, start_index, end_index,
        )
        try:
            _, rows = _fetch_page(service_key, start_index, end_index, filters)
        except Exception as exc:  # noqa: BLE001 - 페이지 하나의 실패로 전체 수집이 죽지 않도록 의도적으로 넓게 잡는다.
            # _fetch_page가 자체 재시도(MAX_PAGE_ATTEMPTS)까지 다 소진하고도 실패한 경우.
            # 이 페이지 하나 때문에 전체 수집을 중단하지 않고, 실패 구간만 기록한 뒤 계속 진행한다.
            logger.error(
                "%s: %s/%s 페이지(%s~%s) 최종 실패, 이 구간은 건너뜁니다: %s",
                OPERATION_NAME, page_no, total_pages, start_index, end_index, exc,
            )
            failed_ranges.append((start_index, end_index))
            continue

        # 마지막 페이지 등에서 실제 반환된 row 수가 요청 범위보다 적게 오는 것도 정상 케이스라 그대로 누적한다.
        all_rows.extend(rows)

    collected_count = len(all_rows)
    if failed_ranges:
        missing_count = sum(end - start + 1 for start, end in failed_ranges)
        logger.error(
            "%s: 총 %s건 중 %s건 수집, %s건 누락 (실패 구간: %s)",
            OPERATION_NAME, total_count, collected_count, missing_count, failed_ranges,
        )
    else:
        logger.info("%s: 총 %s건 중 %s건 수집 완료", OPERATION_NAME, total_count, collected_count)

    # 페이지마다 개별 DataFrame을 만들어 pl.concat 하지 않고, dict 리스트로 모아뒀다가 한 번에 합친다
    # (더 단순하고, 페이지 간 스키마가 미묘하게 달라져도 문제가 덜하다).
    # 저장 전 확인용: 상위 5개 데이터를 로그로 출력 (Airflow 등에서 태스크 로그로 확인 가능)
    logger.info("부동산 실거래가 상위 5개 미리보기:\n%s", pl.DataFrame(all_rows).head(5))

    # 3) row를 계약일(CTRT_DAY) 기준으로 묶고, 필요하면(백필처럼) 지정한 기간으로만 좁힌 뒤
    #    각 날짜 폴더에 저장한다. S3(MinIO)는 진짜 디렉터리 개념이 없어 폴더를 미리 만들
    #    필요가 없다 - COPY TO는 그 경로를 그대로 오브젝트 키로 PUT할 뿐이라 연/월/일
    #    "폴더"는 저절로 생긴다. 같은 계약일 폴더는 매번 최신 전체 데이터로 통째로
    #    덮어써지므로, 뒤늦게 신고된 과거 계약 건도 재실행 한 번이면 자연스럽게 반영된다.
    grouped = _group_rows_by_ctrt_day(all_rows)
    grouped = _filter_ctrt_days(grouped, ctrt_day_from, ctrt_day_to)

    # API가 실거래 신고를 뒤늦게 반영하는 경우가 많아(계약일이 지난 뒤에도 새 신고/정정이
    # 들어옴), 같은 계약일을 여러 번 다시 조회하게 된다. 매번 무조건 덮어쓰지 않고, 기존
    # 저장분과 지문(_fingerprint_rows)이 같으면 건너뛰어 불필요한 재적재/후속 처리를 막는다.
    changed_ctrt_days: list[str] = []
    skipped_ctrt_days: list[str] = []
    for ctrt_day, rows in grouped.items():
        existing_rows = _read_existing_day_rows(con, ctrt_day)
        if existing_rows is not None and _fingerprint_rows(existing_rows) == _fingerprint_rows(rows):
            skipped_ctrt_days.append(ctrt_day)
            continue
        path = _raw_path_for_ctrt_day(ctrt_day)
        df = pl.DataFrame(rows)
        con.execute(f"COPY (SELECT * FROM df) TO '{path}' (FORMAT PARQUET)")
        changed_ctrt_days.append(ctrt_day)

    if skipped_ctrt_days:
        logger.info(
            "%s: 기존 저장분과 동일해 건너뛴 계약일 %d건: %s",
            OPERATION_NAME, len(skipped_ctrt_days), skipped_ctrt_days,
        )
    logger.info(
        "%s: CTRT_DAY 기준 %d개 날짜 폴더 신규 저장/갱신 완료: %s",
        OPERATION_NAME, len(changed_ctrt_days), changed_ctrt_days,
    )

    return {
        "raw_path": _default_raw_path(),
        "checked_ctrt_days": sorted(grouped.keys()),
        "changed_ctrt_days": sorted(changed_ctrt_days),
    }


# ---------------------------------------------------------------------------
# fetch_real_estate_recent: 오늘부터 lookback_days일 전까지 계약일(CTRT_DAY)을 하루씩
# 다시 확인한다. 서울 열린데이터광장 API가 실거래 신고를 뒤늦게 반영하는 경우가 많아서(신고
# 기한 특성상 계약일이 지난 뒤에도 새 신고/정정이 계속 들어옴), "어제" 하루만 매일 조회하는
# 기존 방식으로는 그런 지연 반영분을 영영 놓친다. 그래서 매일 최근 lookback_days일 전체를
# 하루 단위(API가 CTRT_DAY 범위 조회를 지원하지 않아 이 방식만 서버 필터를 탈 수 있다)로
# 다시 조회하되, fetch_real_estate 내부의 지문 비교로 실제 변경이 없는 날짜는 그대로 건너뛴다.
# ---------------------------------------------------------------------------
def fetch_real_estate_recent(
    con: DuckDBPyConnection | None,
    service_key: str | None = None,
    lookback_days: int = 90,
    as_of_date: date | None = None,
) -> dict:
    """as_of_date(포함)부터 lookback_days일 전까지 계약일별로 fetch_real_estate를 반복
    호출한다. as_of_date를 생략하면 오늘 날짜를 기준으로 쓴다(매일 도는 DAG의 기본 동작).
    백필처럼 과거 특정 날짜를 기준일 삼아 "그 날짜 기준 최근 90일 재확인"을 그대로 재현하고
    싶을 때는(run_real_estate_backfill.py 참고) as_of_date를 명시적으로 넘기면 된다.
    반환값의 changed_ctrt_days는 이번 실행에서 실제로 내용이 달라져 다시 저장된 계약일
    (YYYYMMDD) 목록이다 - 이 값을 후속 단계(Silver 변환/upsert)에 그대로 넘기면 실제로
    바뀐 날짜만 다시 반영하면 된다."""
    con = ensure_connection(con)
    base_date = as_of_date or datetime.now().date()

    all_changed: list[str] = []
    all_checked: list[str] = []
    skipped_count = 0
    for offset in range(lookback_days):
        # 최근 RECENT_DAILY_CHECK_DAYS일은 매일 재확인하고, 그보다 오래된 날짜는
        # STALE_RECHECK_INTERVAL_DAYS일에 한 번만 API를 호출한다(위 상수 설명 참고) - 공공
        # API 일일 호출 수를 아끼기 위함이며, 완전히 건너뛰는 게 아니라 주기를 늘리는
        # 것뿐이라 뒤늦은 정정도 결국 반영된다.
        if offset >= RECENT_DAILY_CHECK_DAYS and offset % STALE_RECHECK_INTERVAL_DAYS != 0:
            skipped_count += 1
            continue
        ctrt_day = (base_date - timedelta(days=offset)).strftime("%Y%m%d")
        result = fetch_real_estate(con, service_key, ctrt_day_from=ctrt_day, ctrt_day_to=ctrt_day)
        all_checked.extend(result["checked_ctrt_days"])
        all_changed.extend(result["changed_ctrt_days"])

    if skipped_count:
        logger.info(
            "%s: 오래된 계약일(최근 %d일 이후) 중 %d일은 이번 실행에서 호출 생략 "
            "(%d일마다 한 번만 재확인).",
            OPERATION_NAME, RECENT_DAILY_CHECK_DAYS, skipped_count, STALE_RECHECK_INTERVAL_DAYS,
        )

    logger.info(
        "%s: %s 기준 최근 %d일(계약일 %d건 확인) 중 %d건 변경: %s",
        OPERATION_NAME, base_date, lookback_days, len(all_checked), len(all_changed), sorted(all_changed),
    )

    return {
        "raw_path": _default_raw_path(),
        "checked_ctrt_days": sorted(all_checked),
        "changed_ctrt_days": sorted(all_changed),
    }


# ---------------------------------------------------------------------------
# upsert_real_estate: parquet -> 실거래 데이터를 테이블에 적재 (계약일 단위 전체 교체)
# ---------------------------------------------------------------------------

# TODO: 명확한 단일/복합 기본키가 아직 확인되지 않았다. 자치구코드+법정동코드+지번+건물명+계약일+층
# 조합이 유력한 후보로 보이지만, 실제 키를 받아 데이터를 확인하기 전까지는 확신할 수 없다.
# 대신 계약일(CTRT_DAY)을 원자적 교체 단위로 삼는다 - Bronze 원본 저장(_raw_path_for_ctrt_day)이
# 이미 "같은 계약일 폴더는 매번 최신 전체 데이터로 통째로 덮어쓴다"는 규칙이므로, 이 테이블도
# 같은 규칙을 그대로 따른다: 바뀐 계약일이 있으면 그 날짜의 기존 행을 전부 지우고 새로 받은
# 행 전체로 다시 채운다(DELETE 후 INSERT). 그러면 새로 추가된 신고 건은 자연히 새 행으로
# 들어오고, 이미 있던 신고 건의 정정(가격/면적 등 수정)도 옛 값이 안 남고 통째로 최신값으로
# 교체된다 - 행 단위 매칭 키가 없어도 "그 날짜 전체"를 단위로 삼으면 정확하게 갱신할 수 있다.
def _create_table_if_not_exists(con: DuckDBPyConnection, table_name: str, columns: list[str]) -> None:
    """최초 실행 시에만 테이블 생성. 단일 기본키가 없어서 PRIMARY KEY 없이 모든 컬럼 + created_at만 둔다."""
    columns_ddl = ", ".join(f'"{c}" VARCHAR' for c in columns)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_ddl},
            created_at TIMESTAMP
        )
    """)


def _replace_ctrt_day(con: DuckDBPyConnection, table_name: str, ctrt_day: str) -> None:
    """해당 계약일(CTRT_DAY)의 기존 행을 전부 지우고, Bronze 원본(그 날짜 parquet)의 최신
    전체 행으로 다시 채운다. DELETE와 INSERT를 하나의 트랜잭션으로 묶어, 둘 사이에 프로세스가
    죽어도 그 날짜 데이터가 통째로 사라진 채 남는 일이 없게 한다."""
    path = _raw_path_for_ctrt_day(ctrt_day)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f'DELETE FROM {table_name} WHERE "CTRT_DAY" = ?', [ctrt_day])
        con.execute(f"""
            INSERT INTO {table_name}
            SELECT raw.*, now() AS created_at
            FROM read_parquet('{path}') AS raw
        """)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


# raw parquet을 읽어 테이블에 적재 (raw 추출과 완전히 분리)
def upsert_real_estate(
    con: DuckDBPyConnection | None,
    table_name: str,
    changed_ctrt_days: list[str],
) -> str:
    """changed_ctrt_days(fetch_real_estate/fetch_real_estate_recent가 돌려준, 실제로 내용이
    바뀐 계약일 목록)에 대해서만 그 날짜의 기존 행을 최신 Bronze 원본으로 통째로 교체한다.
    변경된 계약일이 하나도 없으면(오늘 새로 신고/정정된 건이 없는 정상적인 경우) 아무 것도
    하지 않고 그대로 반환한다."""
    con = ensure_connection(con)

    if not changed_ctrt_days:
        logger.info("변경된 계약일이 없어 upsert를 건너뜁니다.")
        return table_name

    first_path = _raw_path_for_ctrt_day(changed_ctrt_days[0])
    columns = con.sql(f"SELECT * FROM read_parquet('{first_path}')").columns
    _create_table_if_not_exists(con, table_name, columns)

    for ctrt_day in changed_ctrt_days:
        _replace_ctrt_day(con, table_name, ctrt_day)

    logger.info("계약일 %d건 upsert(교체) 완료: %s", len(changed_ctrt_days), changed_ctrt_days)
    return table_name

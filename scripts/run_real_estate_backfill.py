# 부동산(아파트) 실거래가 초기 백필(backfill) 스크립트.
#
# Real_Estate.py(fetch_real_estate_recent/upsert_real_estate)는 Airflow DAG(data_orchestration)가
# 매일 자동으로 돌리는 파이프라인인데, 이 스크립트로 특정 기간을 미리/추가로 다시 채워 넣을 수
# 있다. DAG가 매일 "오늘부터 최근 LOOKBACK_DAYS일 전까지"를 기준일(as_of_date) 삼아 재확인하는
# 것과 똑같은 로직을, 이 스크립트는 입력받은 시작일~종료일 구간의 "날짜 하나하나를 각각 기준일
# 삼아" 반복 실행한다 - 즉 각 기준일마다 그 날짜로부터 최근 LOOKBACK_DAYS일치를 다시 확인하고,
# 실제로 내용이 바뀐 계약일만 모아 마지막에 한 번에 테이블에 반영한다(아래 main() 참고).
# 예) 시작일~종료일이 2026-06-01~2026-06-03이면 2026-06-01/06-02/06-03 세 날짜를 각각 기준일
# 삼아 그 날짜 기준 최근 90일을 재확인한다(기간이 겹치는 날짜는 fetch_real_estate 내부의
# 지문 비교로 변경분이 없으면 자동으로 건너뛰므로 중복 재저장은 일어나지 않는다).
#
# [참고] 서울 열린데이터광장 API는 CTRT_DAY(계약일)를 fromDate~toDate 범위로 조회하는 걸
# 지원하지 않는다 - 위치 기반 경로 세그먼트 파라미터라 정확히 하나의 값만 필터로 걸 수 있다.
# 그래서 fetch_real_estate_recent도 내부적으로 계약일 하루씩 반복 호출하는 방식으로 돼 있다
# (Real_Estate.py 참고).
#
# 실행 방법 (프로젝트 루트에서, 가상환경 활성화 후):
#   python scripts/run_real_estate_backfill.py
#   python scripts/run_real_estate_backfill.py <시작일> <종료일>   (범위 직접 지정)
#     - 시작일/종료일은 YYYYMMDD(예: 20260701) 또는 YYYY-MM-DD(예: 2026-07-01) 둘 다 인식해서
#       자동으로 YYYYMMDD로 맞춘다. 그 외 형식은 날짜 오류로 로그를 남기고 즉시 종료한다.
# run_real_estate_backfill.bat을 쓰면 실행 시 시작일/종료일을 직접 입력받아 위 인자로 넘겨준다
# (입력 없이 그냥 Enter만 치면 인자 없이 실행되어 아래 기본값(최근 BACKFILL_DEFAULT_DAYS일~어제)이 쓰인다).

# import calendar  # [기존 소스] 주석 처리된 _months_ago()가 쓰던 import - 삭제하지 않음
import logging
import sys
from datetime import date, datetime, timedelta

from ingestion.Real_Estate import fetch_real_estate_recent, upsert_real_estate

# DuckDB 적재 대상 테이블명 (data_orchestration.py DAG와 동일하게 맞춘다)
REAL_ESTATE_TABLE_NAME = "real_estate"

# 기준일마다 다시 확인할 기간(오늘부터/기준일부터 며칠 전까지) - data_orchestration.py DAG의
# REAL_ESTATE_FETCH_LOOKBACK_DAYS와 동일하게 맞춘다.
LOOKBACK_DAYS = 90

# [기존 소스 - 인자 없이 실행할 때의 기본 기간 용도] (주석 처리, 삭제하지 않음)
# 최초 도입 당시 과거 이력을 통째로 채우는 "초기 백필"용으로 3개월을 기본값으로 뒀던 상수.
# 지금은 날짜를 직접 입력해 원하는 범위로 언제든 백필할 수 있어(run_real_estate_backfill.bat
# 프롬프트 또는 CLI 인자), 인자 없이 실행하는 기본 동작은 BACKFILL_DEFAULT_DAYS(최근 1주일)로
# 대체했다. 몇 개월치를 한 번에 채워야 하면 이 상수 대신 시작일/종료일을 직접 입력하면 된다.
# BACKFILL_MONTHS = 3

# 백필 대상 기간(인자 없이 실행했을 때 기본값): 오늘 기준 "어제"까지, 그로부터 며칠 전까지 채울지
BACKFILL_DEFAULT_DAYS = 7

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# [기존 소스 - 위 BACKFILL_MONTHS 기본값 계산 용도] (주석 처리, 삭제하지 않음)
# def _months_ago(base: date, months: int) -> date:
#     """base로부터 months개월 전 날짜를 계산한다. 말일 기준으로 계산해서 대상 달에 없는
#     일자가 나오면(예: 5/31 -> 4월) 그 달의 마지막 날로 맞춘다."""
#     month = base.month - months
#     year = base.year
#     while month <= 0:
#         month += 12
#         year -= 1
#     day = min(base.day, calendar.monthrange(year, month)[1])
#     return date(year, month, day)


# CTRT_DAY 자체는 YYYYMMDD(구분자 없음)만 쓰지만, 사람이 입력할 때는 YYYY-MM-DD가 더 자연스러워서
# 두 형식 다 인식하고 자동으로 YYYYMMDD로 맞춘다. 여기 없는 형식은 전부 날짜 오류로 처리한다.
_SUPPORTED_DATE_FORMATS = ("%Y%m%d", "%Y-%m-%d")


def _parse_ctrt_day_arg(value: str, label: str) -> date:
    """명령행으로 받은 날짜 문자열을 date로 변환한다. YYYYMMDD(예: 20260701), YYYY-MM-DD
    (예: 2026-07-01) 둘 다 인식한다. 그 외 형식은 조용히 넘어가지 않고 날짜 오류를 로그로
    남긴 뒤 그대로 종료한다(호출한 .bat도 errorlevel 1로 같이 종료됨)."""
    value = value.strip()
    for fmt in _SUPPORTED_DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    logger.error(
        "%s 값(%s)이 날짜 형식과 맞지 않습니다 (YYYYMMDD 또는 YYYY-MM-DD 형식만 지원). 날짜 오류로 종료합니다.",
        label, value,
    )
    raise SystemExit(1)


def main() -> None:
    # 시작일/종료일을 명령행 인자로 직접 받으면(run_real_estate_backfill.bat 실행 시 입력한 값)
    # 그 범위를 그대로 쓰고, 인자가 없으면 기본값(최근 BACKFILL_DEFAULT_DAYS일 ~ 어제)을 쓴다.
    if len(sys.argv) >= 3:
        start_date = _parse_ctrt_day_arg(sys.argv[1], "시작일(fromDate)")
        end_date = _parse_ctrt_day_arg(sys.argv[2], "종료일(toDate)")
        if start_date > end_date:
            logger.error("시작일(%s)이 종료일(%s)보다 늦습니다. 날짜 오류로 종료합니다.", start_date, end_date)
            raise SystemExit(1)
    else:
        end_date = date.today() - timedelta(days=1)  # 어제까지
        # [기존 소스 - 최근 BACKFILL_MONTHS개월치 기본값 용도] (주석 처리, 삭제하지 않음)
        # start_date = _months_ago(end_date, BACKFILL_MONTHS) + timedelta(days=1)
        start_date = end_date - timedelta(days=BACKFILL_DEFAULT_DAYS - 1)  # 최근 1주일(어제 포함 7일)

    logger.info(
        "부동산 실거래가 백필 시작: 기준일 %s ~ %s (기준일마다 최근 %d일 재확인)",
        start_date, end_date, LOOKBACK_DAYS,
    )

    # 입력받은 시작일~종료일 구간의 날짜 하나하나를 각각 기준일(as_of_date) 삼아, 그 기준일
    # 기준 최근 LOOKBACK_DAYS일을 fetch_real_estate_recent로 재확인한다 - data_orchestration.py
    # DAG가 매일 "오늘"을 기준일로 삼아 하는 것과 동일한 로직을 과거의 여러 기준일에 대해
    # 반복 재현하는 것이다. 각 기준일 실행은 fetch_real_estate_recent 내부에서 계약일별로
    # 기존 저장분과 지문을 비교해 실제로 안 바뀐 날짜는 그대로 건너뛰므로, 기준일 구간이 겹쳐도
    # (예: 90일보다 좁은 간격의 여러 기준일) 변경 없는 계약일이 중복 재저장되지는 않는다.
    changed_ctrt_days: set[str] = set()
    as_of = start_date
    while as_of <= end_date:
        logger.info("기준일=%s 최근 %d일 재확인 중...", as_of, LOOKBACK_DAYS)
        result = fetch_real_estate_recent(None, lookback_days=LOOKBACK_DAYS, as_of_date=as_of)
        changed_ctrt_days.update(result["changed_ctrt_days"])
        as_of += timedelta(days=1)

    if not changed_ctrt_days:
        logger.info(
            "기준일 %s ~ %s 범위에 변경된 데이터가 없습니다. upsert할 대상이 없어 종료합니다.",
            start_date, end_date,
        )
        return

    sorted_changed_days = sorted(changed_ctrt_days)
    table_name = upsert_real_estate(None, REAL_ESTATE_TABLE_NAME, sorted_changed_days)
    logger.info("부동산 실거래가 upsert 완료: table=%s, 반영 계약일=%d건", table_name, len(sorted_changed_days))
    logger.info(
        "백필 종료. 기준일 %s ~ %s 범위 반영 완료 (실제 변경 계약일 %d건: %s).",
        start_date, end_date, len(sorted_changed_days), sorted_changed_days,
    )


if __name__ == "__main__":
    main()

# 서울시 부동산(아파트) 실거래가 원본(raw) 수집 및 Silver/Gold 레이어 정제 오케스트레이션 DAG
#
# 매일 0시에 실행되며, 계약일(CTRT_DAY) 기준 "오늘부터 최근 REAL_ESTATE_FETCH_LOOKBACK_DAYS일
# 전까지"를 하루씩 다시 조회해 그 연/월/일(year=/month=/day=) 파티션에 parquet으로 저장한다
# (서울 열린데이터광장 API가 실거래 신고를 뒤늦게 반영하는 경우가 많아서, "어제" 하루만
# 보면 계약일이 며칠~몇 달 지난 뒤에 들어오는 신고/정정 건을 놓치기 때문이다). 각 날짜는
# fetch_real_estate_recent 내부에서 기존 저장분과 내용을 비교해 실제로 바뀐 날짜만 다시
# 저장하고, 그 변경된 계약일(changed_ctrt_days) 목록을 DuckDB 테이블 적재(해당 날짜 전체
# 교체)와 PySpark 정제 스크립트(Real_Estate_Transform.py) 양쪽에 그대로 넘긴다 -
# Real_Estate_Transform.py는 콤마로 이어붙인 계약일 목록을 받아 SparkSession 1회로 그
# 날짜들만 반복 처리해 Iceberg Silver 레이어까지 적재한다(변경된 계약일이 없으면 건너뜀).
# 이어서 Silver 적재가 끝난 시점의 데이터를 기준으로 Gold 레이어 데이터 마트를 빌드해 S3 Lake
# mart/ 경로에 적재한다: main_mart.py -> apt_summary_mart(build_apt_recent_trade_mart.py) ->
# [동 x 평형대] 계열 4종(dm_dong_pyeong_price_avg.py -> dm_apt_price_avg.py ->
# dm_apt_pyeong_price.py -> dm_apt_flr_price.py) -> apt_name Elasticsearch 색인
# (pipeline_apt_name.py) -> apt_rtt_mart.py -> apt_mkt_trends_mart.py까지 전부 하나의
# 순차 체인이다.
#
# ===========================================================================================
# [메모리 안전 설계 - GCP e2-standard-2(2 vCPU, 8GB RAM) 단일 VM]
# Airflow 상주 프로세스(Scheduler/Webserver/Worker/MetaDB)가 이미 2.5~3GB를 점유하고 있어,
# Gold 마트 서브프로세스(PySpark JVM 또는 DuckDB) 하나가 쓸 수 있는 실제 여유 메모리는
# 4.5~5GB 수준이다. 이 예산 안에서 100% 완주하려면 "동시에 무거운 프로세스가 2개 이상 뜨는
# 상황"을 구조적으로 없애야 한다. 세 겹으로 방어한다:
#
#   1) 단일 순차 체인(이 파일의 핵심 변경) - 예전에는 main_mart 완료 후 PySpark 체인
#      (apt_summary -> dong_pyeong 4종 -> es 색인)과 DuckDB 체인(apt_rtt -> apt_mkt_trends)이
#      두 갈래로 갈라져 독립적으로(=동시에) 실행됐다. 이제 두 체인을 리턴값으로 이어붙여
#      Silver 변환부터 apt_mkt_trends_mart까지 전부 하나의 선형 체인으로 만들었다 - 어떤
#      두 Gold 태스크도 서로의 완료를 기다리지 않고 동시에 "실행 가능" 상태가 되는 경우가
#      구조적으로 없다.
#   2) Airflow Pool(gold_mart_serial_pool, slots=1) - 1)번이 이 DAG *안에서*의 병렬을
#      막는다면, Pool은 CeleryExecutor 워커 자체의 동시성(worker_concurrency, 기본값까지
#      최대 16)이나 향후 코드 수정으로 체인이 다시 갈라지는 회귀를 막는 인프라 레벨 안전망이다.
#      Silver 변환 + 무거운 Gold 서브프로세스 태스크 전부가 이 Pool의 slot 1개를 공유하므로,
#      Airflow 스케줄러/실행기 설정과 무관하게 물리적으로 한 번에 하나만 실행된다.
#      최초 배포 시 한 번만 아래 명령으로 Pool을 만들어야 한다(멱등적 - 이미 있으면 갱신만 함):
#        docker compose exec airflow-scheduler airflow pools set \
#          gold_mart_serial_pool 1 "Serialize all Spark/DuckDB Gold mart subprocess jobs on the 8GB VM"
#      (docker-compose.yaml에 AIRFLOW__CELERY__WORKER_CONCURRENCY=2도 추가해 워커 프로세스
#      자체의 동시 실행 개수도 vCPU 수에 맞춰 낮춰뒀다 - 이 DAG 밖의 태스크까지 포함한
#      2차 방어선.)
#   3) 서브프로세스 격리 + 스크립트별 메모리 상한(아래 _run_streaming_subprocess와 각
#      PySpark/DuckDB 스크립트의 SparkSession/SET 설정) - 프로세스가 하나씩만 뜨더라도, 그
#      하나가 무한정 메모리를 쓰면 여전히 OOM이 난다. spark.driver.memory=3g,
#      DUCKDB_MEMORY_LIMIT=3GB 등으로 "동시에 1개"에 "그 1개도 상한선 안에서" 조건을 더한다.
# ===========================================================================================

import logging
import os
import signal
import subprocess
import threading
import tomllib
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv

# 실행 경로(.env 위치, 각 Silver/Gold 스크립트 경로)는 컨테이너 내부 절대경로라 소스에
# 그대로 박아두면 마운트 구조가 바뀔 때마다 이 DAG 코드를 고쳐야 한다. 그래서 실제 경로값은
# airflow/config/paths.toml(docker-compose의 기존 config 볼륨으로 /opt/airflow/config/
# paths.toml에 자동 마운트됨)로 분리했다. 설정 파일 자체의 위치 하나는 모든 설정 로더가
# 공통으로 갖는 부트스트랩 상수라 코드에 남을 수밖에 없는데, 그마저도 하드코딩 대신 환경변수
# (AIRFLOW_PATHS_CONFIG)로 오버라이드 가능한 기본값으로 둔다.
_PATHS_CONFIG_FILE = os.environ.get("AIRFLOW_PATHS_CONFIG", "/opt/airflow/config/paths.toml")
with open(_PATHS_CONFIG_FILE, "rb") as _paths_file:
    _PATHS = tomllib.load(_paths_file)

# 프로젝트 통합 환경 변수(env/.env) 로드. 경로는 위 paths.toml의 [env].dotenv_path에서 가져온다.
load_dotenv(dotenv_path=_PATHS["env"]["dotenv_path"])

from ingestion.Real_Estate import fetch_real_estate_recent, upsert_real_estate
from transformation.gold.pipeline_apt_name import run_apt_name_es_pipeline

# DuckDB 적재 대상 테이블명 (필요하면 이 부분만 바꾸면 된다)
REAL_ESTATE_TABLE_NAME = "real_estate"

# 매일 계약일(CTRT_DAY)을 다시 확인할 기간 - 오늘부터 이 값만큼 전까지. 서울 열린데이터광장
# API의 뒤늦은 신고/정정 반영을 감안한 값이며, Gold 마트들의 LOOKBACK_DAYS(최근 90일 집계)와
# 맞춰 90으로 둔다(그보다 오래된 계약일이 뒤늦게 바뀌어도 어차피 마트 집계 범위 밖이다).
REAL_ESTATE_FETCH_LOOKBACK_DAYS = 90

# Silver 변환 + 무거운 Gold 서브프로세스 태스크 전부가 공유하는 slot=1 Pool 이름. 이 DAG
# 코드는 Pool을 "만들지" 않는다(운영 DB에 쓰는 작업이라 DAG 파싱 시점에 하면 안 됨) -
# 위 헤더 주석의 `airflow pools set` 명령을 배포 시 한 번 실행해야 한다. Pool이 아직 없으면
# 해당 태스크들은 실패하지 않고 "이 pool을 찾을 수 없음" 상태로 대기만 하니, 배포 후 Airflow
# UI(Admin > Pools)에서 pool이 보이는지 꼭 확인할 것.
GOLD_MART_POOL = "gold_mart_serial_pool"

logger = logging.getLogger(__name__)

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="data_orchestration",
    description="서울 아파트 매매 실거래가 원본 수집·적재 및 Silver/Gold 레이어 정제",
    schedule="0 0 * * *",  # 매일 0시
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,  # 동시 실행 방지: 두 실행이 겹치면 같은 Iceberg 테이블에 동시 커밋을 시도해 충돌한다
    default_args=default_args,
    tags=["seoul-open-data", "raw", "silver", "gold", "mart", "real-estate", "apt-name"],
)
def data_orchestration():
    # -----------------------------------------------------------------
    # subprocess 스트리밍 실행 헬퍼 (task_transform_silver_real_estate와 모든 Gold 마트
    # 태스크가 공용으로 씀)
    # - 기존에는 subprocess.run(capture_output=True)를 써서, PySpark 스크립트가 몇 분~몇십 분
    #   걸리는 동안 Airflow 태스크 로그에 아무것도 안 찍히다가 끝나고서야 stdout/stderr가
    #   한 번에 몰아서 출력됐다(진행 상황을 실시간으로 볼 수 없고, 오래 멈춘 것처럼 보임).
    # - Popen + PIPE로 바꾸고 stdout/stderr를 한 줄씩 즉시 logger로 흘려보낸다. 두 파이프를
    #   각각 별도 스레드에서 동시에 읽는 이유: 한쪽만(예: stdout만) 순회하며 읽으면, 자식
    #   프로세스가 stderr에 출력을 많이 쌓아 OS 파이프 버퍼가 가득 찼을 때 자식은 그 쓰기에서
    #   블로킹되고 부모는 stdout 읽기에서 블로킹되어 서로 교착 상태에 빠질 수 있다.
    #
    # [메모리 회수 보강 - 8GB VM 대응]
    #   - MALLOC_ARENA_MAX=1: glibc(Linux)의 malloc은 기본적으로 스레드마다 별도 arena를
    #     최대 코어수x8개까지 만드는데, JVM(Spark)/DuckDB처럼 스레드를 많이 쓰는 프로세스는
    #     각 arena에 free된 메모리를 들고 있다가 필요할 때만 재사용하고 OS에는 안 돌려주는
    #     경우가 많다 - 실제 사용량보다 RSS(실 점유 메모리)가 훨씬 부풀어 보이는 흔한 원인.
    #     arena를 1개로 고정해 이 부풀림을 없앤다(glibc 전용 값이라 다른 libc/Windows에서는
    #     그냥 무시되는 환경변수라 안전하다 - 이 DAG는 Airflow 컨테이너(Linux)에서만 돈다).
    #   - start_new_session=True: 자식을 새 프로세스 그룹(세션)으로 분리한다. PySpark
    #     드라이버 스크립트는 자신이 JVM(py4j 게이트웨이)을 별도 자식 프로세스로 더 띄우는데,
    #     Airflow가 이 태스크를 강제 종료(SIGTERM)할 때 파이썬 드라이버만 죽고 그 JVM
    #     손자 프로세스가 고아로 남아 메모리를 계속 붙든 채 실행되는 사고를 막으려면, 신호를
    #     프로세스 "그룹" 전체에 보낼 수 있어야 한다(os.killpg) - 그러려면 먼저 별도 그룹으로
    #     분리해둬야 한다.
    #   - SIGTERM 핸들러: Airflow가 이 태스크를 죽일 때(수동 kill, 재시작, 타임아웃 등)
    #     보내는 SIGTERM을 가로채 os.killpg로 자식 프로세스 그룹 전체를 즉시 SIGKILL한다.
    #     아무 것도 안 하면 파이썬 프로세스(부모)만 종료되고 이미 fork된 JVM/DuckDB 자식은
    #     알아서 안 죽을 수 있다 - 그 상태로 남으면 8GB 중 몇 GB를 계속 점유한 채 다음 태스크가
    #     시작되어 버려 이 파일 전체의 메모리 예산 설계가 무의미해진다.
    #   - memory_limit_mb(선택): 지정하면 자식 프로세스에 RLIMIT_AS(가상 메모리 총량) 하드
    #     캡을 건다. 커널 OOM killer는 시스템 전체에서 "누구를 죽일지" 예측하기 어렵게
    #     고르므로(이 VM에서는 최악의 경우 Airflow scheduler/webserver 자신이 희생양이 될 수
    #     있다), 그 전에 이 자식 프로세스 자신의 malloc이 즉시 실패(MemoryError)하게 만들어
    #     사고 범위를 이 자식 하나로 좁힌다. [주의] JVM(PySpark) 프로세스에는 쓰지 않는다 -
    #     JVM은 실제 사용량(RSS)보다 훨씬 큰 가상 주소 공간(Metaspace/코드캐시/스레드 스택
    #     예약분 등)을 미리 잡아두는 경우가 흔해서, RLIMIT_AS를 걸면 실제로는 메모리가 남아
    #     있는데도 JVM 자체가 기동 단계에서 실패할 수 있다. DuckDB/Polars처럼 순수 C/Python
    #     프로세스에서 "설정한 memory_limit보다 여유 있는 절대 상한선"으로만 방어적으로 쓴다.
    def _run_streaming_subprocess(
        cmd: list[str],
        log_label: str,
        extra_env: dict[str, str] | None = None,
        memory_limit_mb: int | None = None,
    ) -> None:
        env = os.environ.copy()
        # PYTHONUNBUFFERED=1: 자식 프로세스의 stdout이 TTY가 아니라 파이프에 연결되면 파이썬은
        # stdout을 라인 버퍼링이 아니라 완전 버퍼링(보통 4~8KB)한다 - bufsize=1은 "부모가 파이프를
        # 읽는 방식"만 라인 단위로 만들 뿐, "자식이 언제 쓰는지"는 자식 프로세스 자체의 버퍼링
        # 정책을 따른다. 그래서 자식이 OOM 등으로 SIGKILL당해 갑자기 죽으면, 그 전에 이미 실행된
        # print() 출력들이 버퍼에 쌓인 채 한 번도 flush되지 못하고 통째로 사라져 Airflow 로그에
        # 아무 진행 상황도 안 남는다(apt_rtt_mart.py/apt_mkt_trends_mart.py가 SIGKILL로 죽었을 때
        # 실제로 첫 print() 한 줄조차 로그에 없었던 원인). PYTHONUNBUFFERED=1로 자식의 stdout을
        # 강제로 언버퍼링해, 죽기 직전까지의 진행 상황이 항상 로그에 남도록 한다.
        env["PYTHONUNBUFFERED"] = "1"
        env["MALLOC_ARENA_MAX"] = "1"
        if extra_env:
            env.update(extra_env)

        def _apply_child_limits() -> None:
            # fork() 직후 exec() 이전, 자식 프로세스 안에서만 실행된다.
            os.setsid()
            if memory_limit_mb is not None:
                import resource

                limit_bytes = memory_limit_mb * 1024 * 1024
                resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
            preexec_fn=_apply_child_limits if memory_limit_mb is not None else os.setsid,
        )

        def _stream(pipe, log_func) -> None:
            for line in iter(pipe.readline, ""):
                log_func(f"[{log_label}] {line.rstrip()}")
            pipe.close()

        stdout_thread = threading.Thread(target=_stream, args=(process.stdout, logger.info))
        stderr_thread = threading.Thread(target=_stream, args=(process.stderr, logger.error))
        stdout_thread.start()
        stderr_thread.start()

        def _kill_process_group(signum, frame) -> None:
            logger.error(f"[{log_label}] SIGTERM 수신 - 자식 프로세스 그룹(pid={process.pid})을 강제 종료합니다.")
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            raise SystemExit(143)

        # Airflow가 이 태스크를 죽일 때(수동 kill/재시작/타임아웃) 보내는 SIGTERM을 가로채,
        # 파이썬 드라이버만 죽고 JVM/DuckDB 자식이 고아로 남는 사고를 막는다. Celery
        # prefork 워커는 태스크를 별도 OS 프로세스(그 프로세스의 메인 스레드)에서 실행하므로
        # signal.signal 등록이 정상 동작하지만, 혹시 메인 스레드가 아닌 실행 컨텍스트(예:
        # 로컬 단위 테스트)에서 호출되면 ValueError가 나므로 방어적으로 무시한다 - 그 경우
        # 기본 SIGTERM 동작(파이썬 프로세스 종료)으로 폴백될 뿐, 정상 실행 경로에는 영향이
        # 없다.
        try:
            previous_handler = signal.signal(signal.SIGTERM, _kill_process_group)
        except ValueError:
            previous_handler = None

        try:
            stdout_thread.join()
            stderr_thread.join()
            returncode = process.wait()
        finally:
            if previous_handler is not None:
                signal.signal(signal.SIGTERM, previous_handler)

        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd)

    # -----------------------------------------------------------------
    # RealEstate(부동산 실거래가): fetch -> upsert -> Silver 변환
    # -----------------------------------------------------------------
    @task
    def task_fetch_real_estate() -> list[str]:
        """부동산 실거래가를 계약일(CTRT_DAY) 기준 오늘부터 REAL_ESTATE_FETCH_LOOKBACK_DAYS일
        전까지 하루씩 다시 조회해 연/월/일 파티션에 parquet으로 저장한다. 서울 열린데이터광장
        API가 실거래 신고를 뒤늦게 반영하는 경우가 많아서(계약일이 지난 뒤에도 새 신고/정정이
        들어옴), "어제"만 보는 대신 매일 최근 구간 전체를 다시 훑는다 - 다만 fetch_real_estate
        내부에서 기존 저장분과 지문을 비교해 실제로 안 바뀐 날짜는 그대로 건너뛰므로, 매번
        90일치를 통째로 다시 쓰지는 않는다. 반환값은 이번 실행에서 실제로 내용이 달라진
        계약일(YYYYMMDD) 목록이며, 다음 task(upsert)가 그 날짜만 테이블에 반영한다.
        con=None을 넘기면 fetch_real_estate_recent 내부에서 connect.py를 통해
        MinIO(S3) 설정이 된 DuckDB 연결을 알아서 만들어 쓴다.
        [Pool 미적용] 이 태스크는 요청 수십 개를 순회하는 API 호출 위주라 메모리 사용량이
        작아 gold_mart_serial_pool에 넣지 않았다 - 아래 Silver 변환/Gold 태스크들과 시간대가
        겹쳐도 무방하다."""
        result = fetch_real_estate_recent(None, lookback_days=REAL_ESTATE_FETCH_LOOKBACK_DAYS)
        logger.info(
            "부동산 실거래가 원본 확인 완료: 최근 %d일 중 %d건 변경 - %s",
            REAL_ESTATE_FETCH_LOOKBACK_DAYS, len(result["changed_ctrt_days"]), result["changed_ctrt_days"],
        )
        return result["changed_ctrt_days"]

    @task
    def task_upsert_real_estate(changed_ctrt_days: list[str]) -> str:
        """변경된 계약일(changed_ctrt_days)만 DuckDB 테이블에 반영한다 - 단일 PK가 없어서,
        해당 계약일의 기존 행을 전부 지우고 최신 Bronze 원본으로 통째로 교체한다(새로 추가된
        신고 건과 기존 신고 건의 정정 모두 이 방식으로 정확히 반영된다).
        [Pool 미적용] 대상 테이블이 작아 메모리 사용량이 미미하다 - fetch와 마찬가지로
        gold_mart_serial_pool 밖에 둔다."""
        table_name = upsert_real_estate(None, REAL_ESTATE_TABLE_NAME, changed_ctrt_days)
        logger.info("부동산 실거래가 upsert 완료: table=%s, 반영 계약일=%d건", table_name, len(changed_ctrt_days))
        return table_name

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_transform_silver_real_estate(changed_ctrt_days: list[str]) -> None:
        """
        Bronze 원천 수집 완료 후, PySpark 정제 스크립트(Real_Estate_Transform.py)를 실행하여
        Iceberg Silver 레이어로 가공 적재한다. task_fetch_real_estate가 돌려준
        changed_ctrt_days(이번 실행에서 실제로 내용이 바뀐 계약일 YYYYMMDD 목록)를 콤마로
        이어붙여 그대로 인자로 넘긴다 - Real_Estate_Transform.py가 콤마로 구분된 날짜 목록을
        받으면 SparkSession을 1회만 기동해 그 날짜들만 반복 처리한다(날짜마다 스크립트를
        다시 띄우지 않는다). 변경된 계약일이 하나도 없으면(오늘 새로 신고/정정된 건이 없는
        정상적인 경우) 스크립트를 아예 실행하지 않고 건너뛴다.
        [Pool 적용] 이 태스크도 PySpark JVM을 띄우는 무거운 작업(특히 변경 계약일이 많은
        날은 BULK에 준하는 부하)이라 gold_mart_serial_pool에 포함시켰다 - 뒤이은 Gold
        태스크들과 절대 겹치지 않게 한다.
        """
        import sys

        if not changed_ctrt_days:
            logger.info("변경된 계약일이 없어 Silver 변환을 건너뜁니다.")
            return

        script_path = _PATHS["scripts"]["real_estate_transform"]
        ctrt_days_arg = ",".join(changed_ctrt_days)

        logger.info(f"Silver 변환 스크립트 실행 시작 (대상 계약일 {len(changed_ctrt_days)}건: {ctrt_days_arg})")
        _run_streaming_subprocess(
            [sys.executable, script_path, ctrt_days_arg],
            log_label="Silver 변환",
        )
        logger.info("Silver 변환 스크립트 실행 완료")

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_build_gold_main_mart(dummy_input: None = None) -> None:
        """
        Silver 레이어 적재 완료 후, Gold 레이어의 최우선(1순위) 태스크로 [자치구 x 법정동 x
        단지 x 거래일자 x 면적] 단위 원자적 집계 Gold 마트(dm_main)를 빌드하여
        S3 Lake(warehouse/mart/dm_main/) 경로에 적재한다(main_mart.py, MySQL 등 별도 DB에는
        적재하지 않음). 카카오맵 지오코딩으로 단지별 좌표(latitude/longitude)를 부여하고
        지번(mno/sno)까지 포함하는, 이후 Gold 마트들의 공통 정제 데이터 성격의 마트다.
        dummy_input은 실제로 쓰이지 않고, task_transform_silver_real_estate의 리턴값(None)을
        그대로 받아서 TaskFlow API가 두 태스크 사이의 의존성(Silver 적재 완료 후 실행)을
        자동으로 잡아주도록 하는 용도다.
        """
        import sys

        today_dash = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
        script_path = _PATHS["scripts"]["main_mart"]

        logger.info(f"Gold 마트 생성 스크립트 실행 시작 (base_date={today_dash})")
        _run_streaming_subprocess(
            [sys.executable, script_path, today_dash],
            log_label="main_mart",
        )
        logger.info("Gold 마트 생성 스크립트 실행 완료")

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_build_gold_apt_summary_mart(dummy_input: None = None) -> None:
        """
        main_mart Gold 마트 빌드 완료 후, [아파트별] 최근 90일 거래 지표 + 건축물대장 기본정보
        (세대수/사용승인일) Gold 마트를 빌드하여 S3 Lake(warehouse/mart/) 경로에 적재한다
        (build_apt_recent_trade_mart.py, MySQL 등 별도 DB에는 적재하지 않음).
        dummy_input은 실제로 쓰이지 않고, task_build_gold_main_mart의 리턴값(None)을
        그대로 받아서 TaskFlow API가 두 태스크 사이의 의존성(main_mart 적재 완료 후 실행)을
        자동으로 잡아주도록 하는 용도다.
        """
        import sys

        today_dash = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
        script_path = _PATHS["scripts"]["apt_summary_mart"]

        logger.info(f"Gold 마트 생성 스크립트 실행 시작 (base_date={today_dash})")
        _run_streaming_subprocess(
            [sys.executable, script_path, today_dash],
            log_label="apt_recent_trade_mart",
        )
        logger.info("Gold 마트 생성 스크립트 실행 완료")

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_build_gold_dm_dong_pyeong_price_avg(dummy_input: None = None) -> None:
        """
        apt_summary Gold 마트 빌드 완료 후, [동 x 평형대] 계열 Gold 마트 4종 중 첫 번째
        ([동] 그룹 최근 90일 평균가, dm_dong_pyeong_price_avg)를 빌드하여
        S3 Lake(warehouse/mart/dm_dong_pyeong_price_avg/) 경로에 적재한다(MySQL 등 별도
        DB에는 적재하지 않음).
        dummy_input은 실제로 쓰이지 않고, task_build_gold_apt_summary_mart의 리턴값(None)을
        그대로 받아서 TaskFlow API가 두 태스크 사이의 의존성(apt_summary Gold 마트 적재 완료
        후 순차 실행)을 자동으로 잡아주도록 하는 용도다.
        """
        import sys

        today_dash = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
        script_path = _PATHS["scripts"]["dm_dong_pyeong_price_avg_mart"]

        logger.info(f"Gold 마트 생성 스크립트 실행 시작 (base_date={today_dash})")
        _run_streaming_subprocess(
            [sys.executable, script_path, today_dash],
            log_label="dm_dong_pyeong_price_avg",
        )
        logger.info("Gold 마트 생성 스크립트 실행 완료")

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_build_gold_dm_apt_price_avg(dummy_input: None = None) -> None:
        """
        dm_dong_pyeong_price_avg 빌드 완료 후, [동 x 평형대] 계열 Gold 마트 4종 중 두 번째
        ([단지] 전체 최근 90일 평균가, dm_apt_price_avg)를 빌드하여
        S3 Lake(warehouse/mart/dm_apt_price_avg/) 경로에 적재한다.
        dummy_input은 task_build_gold_dm_dong_pyeong_price_avg의 리턴값(None)을 그대로
        받아서 순차 실행을 강제하는 용도다.
        """
        import sys

        today_dash = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
        script_path = _PATHS["scripts"]["dm_apt_price_avg_mart"]

        logger.info(f"Gold 마트 생성 스크립트 실행 시작 (base_date={today_dash})")
        _run_streaming_subprocess(
            [sys.executable, script_path, today_dash],
            log_label="dm_apt_price_avg",
        )
        logger.info("Gold 마트 생성 스크립트 실행 완료")

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_build_gold_dm_apt_pyeong_price(dummy_input: None = None) -> None:
        """
        dm_apt_price_avg 빌드 완료 후, [동 x 평형대] 계열 Gold 마트 4종 중 세 번째
        ([단지 x 평형별] 최근 90일 평균가, dm_apt_pyeong_price)를 빌드하여
        S3 Lake(warehouse/mart/dm_apt_pyeong_price/) 경로에 적재한다.
        dummy_input은 task_build_gold_dm_apt_price_avg의 리턴값(None)을 그대로 받아서
        순차 실행을 강제하는 용도다.
        """
        import sys

        today_dash = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
        script_path = _PATHS["scripts"]["dm_apt_pyeong_price_mart"]

        logger.info(f"Gold 마트 생성 스크립트 실행 시작 (base_date={today_dash})")
        _run_streaming_subprocess(
            [sys.executable, script_path, today_dash],
            log_label="dm_apt_pyeong_price",
        )
        logger.info("Gold 마트 생성 스크립트 실행 완료")

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_build_gold_dm_apt_flr_price(dummy_input: None = None) -> None:
        """
        dm_apt_pyeong_price 빌드 완료 후, [동 x 평형대] 계열 Gold 마트 4종 중 마지막
        ([단지 x 층수별] 최근 90일 평균가, dm_apt_flr_price)을 빌드하여
        S3 Lake(warehouse/mart/dm_apt_flr_price/) 경로에 적재한다.
        dummy_input은 task_build_gold_dm_apt_pyeong_price의 리턴값(None)을 그대로 받아서
        순차 실행을 강제하는 용도다.
        """
        import sys

        today_dash = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
        script_path = _PATHS["scripts"]["dm_apt_flr_price_mart"]

        logger.info(f"Gold 마트 생성 스크립트 실행 시작 (base_date={today_dash})")
        _run_streaming_subprocess(
            [sys.executable, script_path, today_dash],
            log_label="dm_apt_flr_price",
        )
        logger.info("Gold 마트 생성 스크립트 실행 완료")

    @task
    def task_index_apt_name_es(dummy_input: None = None) -> dict:
        """
        [동 x 평형대] 계열 Gold 마트 4종 적재 완료 후, 아파트 단지명 검색 인덱스
        (Elasticsearch apt_name)를 색인한다 (src/transformation/gold/pipeline_apt_name.py).
        이 DAG의 다른 태스크와 달리 subprocess로 별도 스크립트를 띄우지 않고,
        run_apt_name_es_pipeline()을 같은 워커 프로세스 안에서 직접 호출한다(반환값이
        자동으로 XCom에 실려 색인 건수를 Airflow UI에서 바로 확인할 수 있다).
        dim_apartment에 자치구/법정동 명칭이 이미 들어 있어 MySQL 마스터 테이블에 의존하지
        않으므로, region_master.py 선행 실행은 필요 없다.
        dummy_input은 task_build_gold_dm_apt_flr_price의 리턴값(None)을 그대로 받아서
        TaskFlow API가 두 태스크 사이의 의존성([동 x 평형대] 계열 Gold 마트 4종 적재 완료
        후 실행)을 자동으로 잡아주도록 하는 용도다.
        [Pool 미적용] 별도 프로세스를 띄우지 않고 DuckDB(pandas) 기반 벌크 색인만 수행해
        PySpark/DuckDB 마트들보다 메모리 사용량이 작다 - 다만 이 DAG의 유일한 선형 체인
        안에 있어 어차피 다른 태스크와 동시에 실행될 일은 없다."""
        result = run_apt_name_es_pipeline()
        logger.info(f"apt_name Elasticsearch 색인 파이프라인 완료: {result}")
        return result

    # DuckDB 스크립트(apt_rtt_mart.py/apt_mkt_trends_mart.py)에 주입할 메모리 설정. 두
    # 스크립트 모두 os.getenv("DUCKDB_MEMORY_LIMIT", "1GB")/os.getenv("DUCKDB_THREADS", "2")로
    # 이미 환경변수를 읽도록 되어 있어(apt_rtt_mart.py 참고), 코드를 고치지 않고 여기서
    # 값만 주입한다. 이제는 Gold 마트가 완전히 순차 실행되므로(위 헤더 주석의 1)+2)번),
    # DuckDB 프로세스 하나가 이 VM의 여유 메모리(4.5~5GB) 대부분을 안전하게 쓸 수 있어
    # 기존 기본값 1GB보다 넉넉한 3GB로 올린다.
    _DUCKDB_ENV = {"DUCKDB_MEMORY_LIMIT": "3GB", "DUCKDB_THREADS": "2"}
    # RLIMIT_AS 하드 캡: memory_limit(3GB)의 1.5배 정도 여유를 둬서, DuckDB 자체 계정에
    # 안 잡히는 부수적 할당(Arrow/Polars 변환 버퍼 등)까지 감안하면서도 폭주 시 이 VM의
    # 전체 예산(4.5~5GB)을 넘기 전에 이 프로세스 혼자 먼저 실패하게 만든다.
    _DUCKDB_MEMORY_LIMIT_MB = 4608  # 3GB * 1.5

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_build_gold_apt_rtt_mart(dummy_input: dict | None = None) -> None:
        """
        apt_name Elasticsearch 색인 완료 후, 아파트 거래동향(RTT) Gold 마트
        (apt_rtt_mart.py)를 빌드하여 S3 Lake(warehouse/mart/apt_rtt/) 경로에 적재한다.
        PySpark가 아니라 Polars+DuckDB로 동작하는 독립 스크립트이지만, dim_apartment/
        fact_apt_transactions만 읽으면 되고 dong_pyeong 계열 마트나 색인 결과 자체에는
        의존하지 않는다 - 다만 8GB VM에서는 이 태스크도 다른 Gold 태스크와 절대 동시에
        뜨면 안 되므로, 이제는 앞선 모든 Gold 태스크가 끝난 뒤(단일 순차 체인) 시작한다.
        dummy_input은 실제로 쓰이지 않고, task_index_apt_name_es의 리턴값(색인 결과 dict)을
        그대로 받아서 순차 실행을 강제하는 용도다.
        """
        import sys

        today_dash = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
        script_path = _PATHS["scripts"]["apt_rtt_mart"]

        logger.info(f"Gold 마트 생성 스크립트 실행 시작 (as_of_date={today_dash})")
        _run_streaming_subprocess(
            [sys.executable, script_path, today_dash],
            log_label="apt_rtt_mart",
            extra_env=_DUCKDB_ENV,
            memory_limit_mb=_DUCKDB_MEMORY_LIMIT_MB,
        )
        logger.info("Gold 마트 생성 스크립트 실행 완료")

    @task(pool=GOLD_MART_POOL, pool_slots=1)
    def task_build_gold_apt_mkt_trends_mart(dummy_input: None = None) -> None:
        """
        apt_rtt Gold 마트 빌드 완료 후, 아파트 시장 동향(apt_mkt_trends) Gold 마트
        (apt_mkt_trends_mart.py)를 빌드하여 S3 Lake(warehouse/mart/apt_mkt_trends/) 경로에
        적재한다. apt_rtt_mart.py와 동일하게 PySpark가 아니라 Polars+DuckDB로 동작하는
        독립 스크립트이고, dim_apartment/fact_apt_transactions만 읽는다. 이 DAG의 마지막
        태스크다.
        dummy_input은 task_build_gold_apt_rtt_mart의 리턴값(None)을 그대로 받아서 순차
        실행을 강제하는 용도다.
        """
        import sys

        today_dash = pendulum.now("Asia/Seoul").format("YYYY-MM-DD")
        script_path = _PATHS["scripts"]["apt_mkt_trends_mart"]

        logger.info(f"Gold 마트 생성 스크립트 실행 시작 (as_of_date={today_dash})")
        _run_streaming_subprocess(
            [sys.executable, script_path, today_dash],
            log_label="apt_mkt_trends_mart",
            extra_env=_DUCKDB_ENV,
            memory_limit_mb=_DUCKDB_MEMORY_LIMIT_MB,
        )
        logger.info("Gold 마트 생성 스크립트 실행 완료")

    # -----------------------------------------------------------------
    # Task 흐름 - fetch가 찾아낸 changed_ctrt_days(실제로 바뀐 계약일 목록)를 upsert와 Silver
    # 변환 양쪽에 그대로 넘긴다(둘 다 fetch 완료에만 의존 - Silver 변환은 S3 Bronze 원본을
    # 직접 읽지 upsert가 채우는 DuckDB 테이블을 읽지 않으므로, upsert 완료를 기다릴 필요가
    # 없다. upsert는 가벼운 작업이라 Pool 밖에 있고, Silver 변환과 시간대가 겹쳐도 무방하다).
    #
    # Silver 변환부터는 전부 하나의 선형 체인이다(예전의 두 갈래 fork를 없앴다):
    #   Silver 변환 -> main_mart -> apt_summary -> dm_dong_pyeong_price_avg -> dm_apt_price_avg
    #   -> dm_apt_pyeong_price -> dm_apt_flr_price -> apt_name Elasticsearch 색인
    #   -> apt_rtt_mart -> apt_mkt_trends_mart
    # 각 화살표는 앞 태스크의 리턴값(대부분 None)을 다음 태스크의 dummy_input으로 그대로
    # 넘기는 TaskFlow 의존성이다. 이 체인에 들어있는 서브프로세스 태스크들은 전부
    # gold_mart_serial_pool(slot=1)도 공유하므로, 의존성 그래프상으로도 물리적으로도 한
    # 번에 하나만 실행된다.
    # -----------------------------------------------------------------
    changed_ctrt_days = task_fetch_real_estate()
    task_upsert_real_estate(changed_ctrt_days)
    silver_result = task_transform_silver_real_estate(changed_ctrt_days)
    main_mart_result = task_build_gold_main_mart(silver_result)
    apt_summary_mart_result = task_build_gold_apt_summary_mart(main_mart_result)
    dm_dong_pyeong_price_avg_result = task_build_gold_dm_dong_pyeong_price_avg(apt_summary_mart_result)
    dm_apt_price_avg_result = task_build_gold_dm_apt_price_avg(dm_dong_pyeong_price_avg_result)
    dm_apt_pyeong_price_result = task_build_gold_dm_apt_pyeong_price(dm_apt_price_avg_result)
    dm_apt_flr_price_result = task_build_gold_dm_apt_flr_price(dm_apt_pyeong_price_result)
    apt_name_es_result = task_index_apt_name_es(dm_apt_flr_price_result)
    apt_rtt_mart_result = task_build_gold_apt_rtt_mart(apt_name_es_result)
    task_build_gold_apt_mkt_trends_mart(apt_rtt_mart_result)


data_orchestration()

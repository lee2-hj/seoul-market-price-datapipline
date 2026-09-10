# -*- coding: utf-8 -*-
"""
서울시 아파트 시장 동향(MKT_TRENDS: Market Trends) Gold 데이터 마트 생성 스크립트
- 단독 실행 가능한 배치 스크립트. PySpark 없이 Polars + DuckDB만으로 동작한다
  (apt_rtt_mart.py와 동일한 이유 - 건별(트랜잭션 레벨) 결과라 셔플이 큰 분산 처리 없이도
  DuckDB의 벡터화 엔진 + Polars의 인메모리 컬럼 연산만으로 충분하다).
- 소스: dim_apartment / fact_apt_transactions (S3 Lake(MinIO)에 Iceberg로 적재된 데이터 파일을
  DuckDB httpfs로 직접 조회 - apt_rtt_mart.py/pipeline_apt_name.py와 동일한 방식).
  경로: {S3_END_POINT}/{LAKE}/dim_apartment , {S3_END_POINT}/{LAKE}/fact_apt_transactions
- 산출: 최근 90일 아파트 매매 거래를 자치구/법정동/지번(mno·sno) 단위로 정리한 건별
  (자치구코드/자치구명/법정동코드/법정동명/아파트명/mno/sno/계약일자/층수/거래건수/거래금액/평/평단가)
  데이터를 S3 Lake(MinIO) mart/apt_mkt_trends/base_date=YYYY-MM-DD 경로에
  Parquet(Snappy 압축)로 저장한다.

[아파트 특정 조인 키(Composite Join Key)에 대한 설계 노트]
요구사항이 명시한 "아파트를 특정하는" 조인 키는 (자치구코드, 법정동코드, mno, sno) 4개
컬럼이다. 다만 dim_apartment의 실제 스키마(Real_Estate_Transform.py 참고)에는
sgg_cd/sgg_nm/dong_cd/dong_nm/apt_name/build_year만 있고 mno/sno 컬럼 자체가 없다 -
지번(mno/sno)은 fact_apt_transactions에만 존재한다. 그래서:
  - dim_apartment <-> fact_apt_transactions의 물리적 Broadcast Join은 두 테이블이 공통으로
    가진 (sgg_cd, dong_cd, apt_name)으로 수행해 자치구명/법정동명을 보강한다(다른 Gold
    마트들과 동일한 방식). apt_name은 원래 조인 키 용도로만 쓰였으나, 이후 요구사항에 따라
    최종 마트 컬럼에도 단지명 표시용으로 함께 내보낸다(아파트를 특정하는 비즈니스 키 자체는
    여전히 mno/sno이고, apt_name은 그 옆에 붙는 조회용 부가 컬럼일 뿐이다).
  - 요구사항이 말하는 "아파트 특정 조인 키" (sgg_cd, dong_cd, mno, sno)는 이 마트가 하나의
    아파트(필지)를 식별하는 비즈니스 키로 채택되어, 6번 Upsert 로직의 레코드 고유 식별 키
    (RECORD_KEY_COLUMNS)의 기반이 된다(거기에 계약일자/층수/전용면적을 더해 개별 거래를
    구분한다 - apt_name 대신 mno/sno로 아파트를 특정하는 것이 이 마트의 요구사항이므로).

[파티션/Upsert 설계 - base_date의 의미]
apt_rtt_mart.py와 동일하게, base_date는 "스크립트를 실행한 날짜"가 아니라 "각 거래 행의
실제 계약일자(deal_date)"다. 재실행할 때마다 오늘 기준 최근 90일을 다시 훑으므로(공공
API의 뒤늦은 신고/정정 반영), 같은 계약일자 파티션을 매번 다시 만나 그 파티션 단위로
신규/변경/무변경을 판별할 수 있어야 하기 때문이다.

실행 방법 (단독 CLI, 프로젝트 루트에서 가상환경 활성화 후):
  python src/transformation/gold/apt_mkt_trends_mart.py [AS_OF_DATE]
    - AS_OF_DATE(YYYY-MM-DD) 생략 시 오늘 날짜를 기준으로 최근 90일치를 재확인한다.
    - 특정 과거 기준일로 다시 돌리고 싶으면 인자로 넘기면 된다. 몇 번을 다시 돌려도 실제로
      내용이 바뀐 base_date 파티션만 다시 쓰인다(멱등적).

준비물: env/.env (또는 OS 환경변수)에 S3_END_POINT / S3_ACCESS_KEY / S3_SECRET_KEY / LAKE.

[제약] 이 스크립트는 코드만 작성된 상태이며, 실제로 실행해 MinIO에 데이터를 적재하거나
Airflow DAG에 태스크로 연동하는 작업은 별도로 진행되지 않았다.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import polars as pl
from dotenv import load_dotenv

from transformation.gold.adaptive_lookback_duckdb import run_adaptive_backward_fallback

# =====================================================================================
# 1. 상수
# =====================================================================================
LOOKBACK_DAYS = 90          # 계약일자(deal_date) 기준 최근 N일
PYEONG_M2 = 3.30578          # 1평 = 3.30578 m2 (전용면적 기준 평수 환산 - 공급면적 배율 미적용)

# dim_apartment는 파티션이 없어 data/ 밑에 바로 parquet이 있고, fact_apt_transactions는
# Iceberg 테이블 생성 시 PARTITIONED BY (days(deal_date))로 만들어져 있어(Real_Estate_Transform.py
# 참고) data/deal_date_day=YYYY-MM-DD/*.parquet 형태로 중첩되어 있다.
DIM_APARTMENT_GLOB = "dim_apartment_current/*.parquet"

# [2026-08-30 SIGABRT 장애 대응 - apt_rtt_mart.py와 동일 패턴] 90일 전체를 재귀 글롭(**)으로
# 한 번에 조회하던 이전 방식은 raw_df/mart_df/partition_by() 복사본이 동시에 메모리에 떠 있어,
# 데이터 규모가 커지면(apt_rtt_mart.py에서 1,296만 행 기준 실제 재현됨) Airflow가 이 프로세스에
# 걸어둔 RLIMIT_AS(가상메모리 하드캡)를 Rust 기반 Polars 할당자가 들이받고
# `memory allocation of N bytes failed` + SIGABRT로 죽는다. apt_rtt_mart.py를 먼저 하루 단위
# 조회로 고치면서 이 스크립트도 fact_apt_transactions 규모/조인 패턴이 완전히 동일해 같은
# 장애가 재현될 것으로 보고 선제적으로 동일하게 고친다 - main()이 90일을 순회하며 하루치씩
# 이 글롭으로 그날 파티션 디렉터리 하나만 직접 지정해 조회한다(재귀 글롭 1회로 90개 디렉터리를
# 매번 다시 나열하지 않아도 되는 부수 효과도 apt_rtt_mart.py와 동일).
FACT_APT_TRANSACTIONS_DAY_GLOB = "fact_apt_transactions_current/deal_date_day={day}/*.parquet"

# 최종 마트 이름/저장 경로: {S3_END_POINT}/{LAKE}/mart/apt_mkt_trends/base_date=YYYY-MM-DD/data.parquet
# 파일명을 고정값(PARTITION_FILE_NAME)으로 둬서, 같은 base_date를 몇 번을 다시 써도(Update)
# 그 경로의 파일 하나만 교체될 뿐 파일이 계속 쌓이지 않는다(멱등적 덮어쓰기).
MART_NAME = "apt_mkt_trends"
PARTITION_FILE_NAME = "data.parquet"

# 아파트 특정 조인 키(Composite Join Key, 요구사항 4번) - 자치구코드/법정동코드/mno/sno
# 조합으로 "특정 아파트(필지)"를 식별한다. dim_apartment에는 mno/sno가 없어 물리적 조인은
# apt_name으로 수행하지만(아래 load_dim_apartment_broadcast/fetch_joined_day 참고), 이 마트가
# 다루는 "아파트 단위"의 비즈니스 키는 이 4개 컬럼이다.
# 컬럼명은 fetch_joined_day()가 돌려주는 원본 이름(sgg_cd/dong_cd)이 아니라
# shape_mkt_trends_columns()가 최종 마트 스키마로 리네이밍한 뒤의 이름(cgg_cd/stdg_cd)을
# 쓴다 - _with_record_key()가 그 리네이밍 이후의 데이터프레임에 적용되기 때문이다.
APT_IDENTITY_COLUMNS = ["cgg_cd", "stdg_cd", "mno", "sno"]

# 레코드(개별 거래) 고유 식별 키(Unique Key). 같은 아파트(APT_IDENTITY_COLUMNS)라도 계약일자/
# 층/평(면적)이 다르면 별개의 거래이므로 세 컬럼을 더해 Upsert/Dedup 기준으로 삼는다.
# 면적 구분에는 원본 exclusive_area_m2가 아니라 파생 컬럼인 pyeong을 쓴다 - exclusive_area_m2는
# 최종 마트 스키마에 없어(요구사항 4번 필수 컬럼 목록에 원본 전용면적이 없음) 파티션을
# S3에서 다시 읽어온 old_df에는 애초에 존재하지 않는 컬럼이라, 그걸 키에 넣으면 재실행 시
# (기존 파티션과 비교하는 시점에) ColumnNotFoundError로 깨진다. pyeong은 항상 저장 스키마에
# 남아있는 컬럼이라 이 문제가 없다.
# [중요] 거래금액(trade_amount)은 의도적으로 키에서 뺐다 - "기존 데이터 변경"의 가장 흔한
# 사례가 가격 정정(같은 거래의 신고가가 나중에 수정되는 경우)인데, 거래금액을 키에 포함시키면
# 가격이 바뀔 때마다 "같은 거래의 최신값"이 아니라 "전혀 다른 새 거래"로 오판되어 옛 레코드가
# 지워지지 않고 계속 누적된다(apt_rtt_mart.py에서 동일하게 재현/확인한 문제).
RECORD_KEY_COLUMNS = APT_IDENTITY_COLUMNS + ["deal_date", "floor", "pyeong"]


# =====================================================================================
# 2. 환경 변수 로드 (.env 또는 OS 환경변수) + MinIO(S3) DuckDB 연결 설정
#    - 로컬 실행(<project_root>/env/.env)과 Airflow 컨테이너 실행(/opt/airflow/project/env/.env)
#      양쪽 경로를 다 시도한다 (apt_rtt_mart.py/build_dong_pyeong_mart.py와 동일한 패턴).
#    - override=False: 호출하는 쪽(Airflow env_file 등)이 이미 채워둔 환경변수는 덮어쓰지 않고
#      빈 값만 보충한다.
# =====================================================================================
def load_config() -> dict:
    project_root = Path(__file__).resolve().parents[3]
    env_candidates = [
        project_root / "env" / ".env",
        Path("/opt/airflow/project/env/.env"),
    ]
    for env_path in env_candidates:
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=False)
            break

    config = {
        "s3_endpoint": os.getenv("S3_END_POINT"),
        "s3_access_key": os.getenv("S3_ACCESS_KEY"),
        "s3_secret_key": os.getenv("S3_SECRET_KEY"),
        "lake_bucket": os.getenv("LAKE"),
    }
    missing = [key for key, value in config.items() if not value]
    if missing:
        print(f"[WARN] 비어있는 환경변수: {missing}")
    return config


def get_duckdb_connection(config: dict) -> duckdb.DuckDBPyConnection:
    """MinIO(S3) 접속 설정이 적용된 DuckDB 연결을 만든다.
    로컬(MinIO, 스킴 없이 "host:port", 평문 HTTP, path-style)과 배포(HTTPS 스킴 포함,
    virtual-hosted-style)를 s3_endpoint의 스킴으로 자동 판별한다 (apt_rtt_mart.py/
    src/utils/connect.py의 configure_minio와 동일한 판별 방식 - 이 스크립트는 PySpark 없이
    순수 DuckDB만 쓰는 독립 스크립트라 여기서도 동일하게 다시 설정한다).

    [SIGKILL(OOM) 대응] apt_rtt_mart.py와 동일한 이유로 memory_limit을 명시한다 - DuckDB는
    memory_limit 미지정 시 컨테이너 cgroup 한도가 아니라 호스트(Docker Desktop VM)의 전체
    물리 메모리 기준으로 기본 한도를 잡아, 실제 여유 메모리보다 많은 메모리를 쓰려다 커널 OOM
    killer에 SIGKILL당할 수 있다(Airflow에서 apt_rtt_mart.py와 같은 실행에 동일 증상으로 재현됨).
    memory_limit + temp_directory로 한도 초과 시 예외 대신 디스크 스필을 강제한다."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # 기본값 3GB: GCP e2-standard-2(2 vCPU, 8GB RAM) VM 기준 - Airflow 상주 프로세스가
    # 이미 2.5~3GB를 쓰고 있어 여유 메모리는 4.5~5GB뿐이지만, Airflow DAG가 Gold 마트를
    # 전부 순차 실행하도록 바뀌어(gold_mart_serial_pool) 이 프로세스가 뜰 때는 다른 무거운
    # 프로세스가 동시에 돌지 않는다는 전제로 예전 기본값(1GB)보다 넉넉하게 잡았다.
    memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "3GB")
    threads = os.getenv("DUCKDB_THREADS", "2")
    temp_directory = os.getenv("DUCKDB_TEMP_DIRECTORY", "/tmp/duckdb_spill_apt_mkt_trends_mart")

    raw_endpoint = config["s3_endpoint"] or ""
    use_ssl = raw_endpoint.strip().lower().startswith("https://")
    endpoint_clean = raw_endpoint.replace("https://", "").replace("http://", "")
    url_style = "vhost" if use_ssl else "path"

    con.execute(f"""
        SET s3_endpoint='{endpoint_clean}';
        SET s3_access_key_id='{config["s3_access_key"]}';
        SET s3_secret_access_key='{config["s3_secret_key"]}';
        SET s3_use_ssl={'true' if use_ssl else 'false'};
        SET s3_url_style='{url_style}';
        SET memory_limit='{memory_limit}';
        SET threads={threads};
        SET temp_directory='{temp_directory}';
    """)
    # [2026-09-10 OutOfMemoryException(ArrowBuffer) 대응] 적응형 조회기간 폴백
    # (run_adaptive_backward_fallback)이 dormant_state 캐시 없이 콜드스타트로 도는 경우
    # (이번 재현: 3,806개 단지가 전부 캐시 미스라 안전 상한 1,095일까지 하루씩 거슬러
    # 올라가며 반복 조회해야 했음), interested 집합 재등록을 건너뛰는 최적화(위 호출부
    # adaptive_lookback_duckdb.py 수정)를 적용해도 여전히 "ArrowBuffer: failed to
    # allocate ... bytes"로 죽었다. DuckDB는 기본적으로 멀티스레드 파이프라인 실행 결과의
    # 삽입 순서(insertion order)를 보존하기 위해 중간 결과를 재정렬 가능하도록 메모리에
    # 붙들고 있는데, 이 스크립트처럼 짧은 조회를 수백~1,000번 넘게 반복하며 매번
    # Arrow(.pl())로 결과를 뽑아가는 패턴에서는 이 순서 보존 버퍼가 반복 호출마다 계속
    # 쌓여 메모리를 붙든다(에러 메시지가 스스로 제안하는 "Disabling insertion-order
    # preservation" 조치와 정확히 일치하는 증상). 이 스크립트/폴백 로직 어디에서도 DuckDB
    # 결과의 행 순서에 의존하지 않으므로(각 날짜 결과는 그대로 집계/조인/저장될 뿐 순서를
    # 쓰지 않음) 순서 보존을 꺼서 중간 버퍼를 더 적극적으로 반환하게 한다.
    con.execute("SET preserve_insertion_order=false;")
    return con


# =====================================================================================
# 3. dim_apartment 로드 - Broadcast Join(In-Memory Map Join)용 빌드 사이드 구체화
#    dim_apartment는 자치구/법정동/단지명 차원 테이블로 건수가 작다(수만 건 수준, fact_apt_
#    transactions 대비 훨씬 작음). DuckDB TEMP TABLE로 통째로 메모리에 구체화(materialize)해
#    두면, 이후 fact와의 조인에서 이 작은 테이블이 해시 조인의 build side가 되어 large-table
#    쪽(fact)을 셔플/재파티셔닝하지 않고 스트리밍하며 그대로 probe할 수 있다 - PySpark의
#    broadcast(dim_df)와 동일한 효과를 단일 프로세스 DuckDB에서 얻는 방식이다.
#    apt_name은 dim_apartment와 fact_apt_transactions가 공유하는 유일한 단지 식별 컬럼이라
#    조인에 반드시 필요하며(모듈 docstring의 설계 노트 참고), 최종 마트 출력 컬럼에도 단지명
#    표시용으로 함께 포함한다(아파트를 특정하는 비즈니스 키는 여전히 mno/sno).
# =====================================================================================
def load_dim_apartment_broadcast(con: duckdb.DuckDBPyConnection, lake_bucket: str) -> None:
    s3_path = f"s3://{lake_bucket}/{DIM_APARTMENT_GLOB}"
    print(f"[INFO] dim_apartment 브로드캐스트 테이블 구체화: {s3_path}")
    # [2026-09-10] mno/sno는 apartment_key_v2 컷오버 전까지 dim_apartment_current 스냅샷에
    # 물리적으로 존재하지 않는다(Real_Estate_Transform.py의 _export_current_dim_apartment()
    # 참고 - 컷오버 전 lakehouse.dim_apartment 자체에 이 컬럼이 없다). 조인 키도 apt_name
    # 기준(모듈 상단 주석 참고)이고 최종 출력 mno/sno는 fact 쪽 값만 쓰므로, 이 브로드캐스트
    # 테이블에서는 실제로 존재하는 컬럼만 선택한다.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_apartment_bc AS
        SELECT DISTINCT sgg_cd, sgg_nm, dong_cd, dong_nm, apt_name
        FROM read_parquet('{s3_path}')
    """)
    # fetchone()은 정적으로 tuple | None으로 추론되어(COUNT(*)가 항상 한 행을 반환한다는 사실을
    # 타입 체커는 모름) [0] 인덱싱에 경고가 뜬다. None 방어 처리로 안전하게 언패킹한다.
    count_row = con.execute("SELECT COUNT(*) FROM dim_apartment_bc").fetchone()
    row_count = count_row[0] if count_row is not None else 0
    print(f"[INFO] dim_apartment 브로드캐스트 테이블 행 수: {row_count}건")


# =====================================================================================
# 4. fact_apt_transactions 조회 - Predicate/Projection Pushdown + Broadcast Join
#    - [2026-08-30 SIGABRT 장애 대응] 하루(day() 파티션 디렉터리 하나)만 조회한다 - main()이
#      90일을 순회하며 하루치씩 호출한다(모듈 상단 FACT_APT_TRANSACTIONS_DAY_GLOB 주석 참고).
#      디렉터리 자체를 하루 단위로 직접 지정하므로, 재귀 글롭(**)으로 90개 디렉터리를 매번
#      다시 나열하지 않아도 된다.
#    - Projection Pushdown: 서브쿼리에서 실제로 쓰는 컬럼만 SELECT해, 파케이 파일에서 그
#      컬럼들만 읽어오게 한다(price_per_m2/deal_type/agent_sgg_nm 등은 애초에 읽지 않음).
#    - mno IS NOT NULL 필터: mno/sno가 이 마트의 아파트 특정 조인 키(APT_IDENTITY_COLUMNS)의
#      핵심이므로, 지번 정보가 없어 아파트를 특정할 수 없는 거래는 애초에 대상에서 제외한다.
#    - Broadcast Join: 3번에서 구체화해둔 dim_apartment_bc(작은 build side)에 fact(필터링/
#      프로젝션이 끝난 큰 쪽)를 (sgg_cd, dong_cd, apt_name) 기준 INNER JOIN한다.
#    - union_by_name=true: Iceberg 메타데이터 없이 원본 parquet을 직접 글롭하는 방식이라,
#      과거에 스키마가 바뀐 적 있는 옛 파일과 컬럼 구성이 다를 수 있음을 관대하게 처리한다.
#    - 90일 구간 전체를 한 번의 쿼리로 가져온다(파티션마다 재조회하지 않음) - 이후 5번에서
#      Polars가 base_date(=deal_date)별로 인메모리 분할해 파티션 단위 Upsert에 넘긴다.
# =====================================================================================
def fetch_joined_day(
    con: duckdb.DuckDBPyConnection,
    lake_bucket: str,
    day_str: str,
    filter_table: str | None = None,
) -> pl.DataFrame:
    """filter_table: 지정하면 dim_apartment_bc 조인 결과를 그 TEMP TABLE(sgg_cd/dong_cd/
    apt_name)과 추가로 세미조인해 좁힌다 - adaptive_lookback_duckdb.py의 적응형 조회기간
    폴백이 "이번 날짜에 아직 관심 있는 소수 단지"만 조회할 때 사용한다(기본 호출은 None이라
    기존 동작과 완전히 동일하다)."""
    fact_s3_path = f"s3://{lake_bucket}/{FACT_APT_TRANSACTIONS_DAY_GLOB.format(day=day_str)}"
    extra_join = (
        f"INNER JOIN {filter_table} AS m "
        "ON f.sgg_cd = m.sgg_cd AND f.dong_cd = m.dong_cd AND f.apt_name = m.apt_name"
        if filter_table else ""
    )
    query = f"""
        SELECT
            f.sgg_cd  AS sgg_cd,
            d.sgg_nm  AS sgg_nm,
            f.dong_cd AS dong_cd,
            d.dong_nm AS dong_nm,
            f.apt_name AS apt_name,
            f.mno AS mno,
            f.sno AS sno,
            f.deal_date AS deal_date,
            f.floor AS floor,
            f.price_ten_thousand AS price_ten_thousand,
            f.exclusive_area_m2 AS exclusive_area_m2
        FROM (
            SELECT
                sgg_cd, dong_cd, apt_name, mno, sno,
                deal_date, floor, price_ten_thousand, exclusive_area_m2
            FROM read_parquet('{fact_s3_path}', union_by_name=true)
            WHERE (cancel_date IS NULL OR TRIM(cancel_date) = '')
              AND price_ten_thousand > 0
              AND exclusive_area_m2 > 0
              AND mno IS NOT NULL AND TRIM(mno) <> ''
        ) AS f
        LEFT JOIN dim_apartment_bc AS d
            ON f.sgg_cd = d.sgg_cd
           AND f.dong_cd = d.dong_cd
           AND f.apt_name = d.apt_name
        {extra_join}
    """
    try:
        con.execute(f"COPY ({query}) TO '/tmp/_duck_day.parquet' (FORMAT PARQUET)")
        return pl.read_parquet('/tmp/_duck_day.parquet')
    except duckdb.IOException:
        # 해당 날짜에 거래가 아예 없어 파티션 디렉터리 자체가 없는 경우(S3 404 계열) - 빈
        # 결과(0건)로 간주하고 넘어간다(read_existing_partition()과 동일한 예외 처리 패턴).
        return pl.DataFrame(schema={
            "sgg_cd": pl.Utf8, "sgg_nm": pl.Utf8, "dong_cd": pl.Utf8, "dong_nm": pl.Utf8,
            "apt_name": pl.Utf8, "mno": pl.Utf8, "sno": pl.Utf8, "deal_date": pl.Date,
            "floor": pl.Int64, "price_ten_thousand": pl.Int64, "exclusive_area_m2": pl.Float64,
        })


# =====================================================================================
# 5. Polars 변환 - 평(전용면적 기준) 환산 + 평단가 + 거래건수(건별 트랜잭션 플래그) +
#    base_date(=계약일자) 부여
#    DuckDB에서는 조인/필터링까지만 하고, 마트 스키마를 완성하는 컬럼 파생은 Polars의
#    표현식으로 처리한다(요구사항의 "Polars 및 DuckDB" 병행 활용 - DuckDB는 S3/조인,
#    Polars는 인메모리 컬럼 연산 및 파티션 단위 Upsert 담당).
#    base_date는 스크립트 실행일이 아니라 각 행의 실제 계약일자(deal_date)를 그대로 쓴다 -
#    이래야 재실행 시 같은 파티션을 다시 만나 Upsert 판별이 가능하다(모듈 docstring 참고).
# =====================================================================================
def shape_mkt_trends_columns(raw_df: pl.DataFrame) -> pl.DataFrame:
    lf = raw_df.lazy()
    return (
        lf.select(
            pl.col("sgg_cd").alias("cgg_cd"),
            pl.col("sgg_nm").alias("cgg_nm"),
            pl.col("dong_cd").alias("stdg_cd"),
            pl.col("dong_nm").alias("stdg_nm"),
            pl.col("apt_name"),
            pl.col("mno"),
            pl.col("sno"),
            pl.col("deal_date"),
            pl.col("floor"),
            pl.col("price_ten_thousand").alias("trade_amount"),
            # 평(면적): 전용면적(m2) 기준 평수 환산 (전용면적 / 3.30578) - 요구사항대로 공급면적
            # 배율(1.3)은 적용하지 않는다.
            (pl.col("exclusive_area_m2") / PYEONG_M2).round(2).alias("pyeong"),
            # 거래건수: 집계(GROUP BY) 대신 건별(row-level) 트랜잭션 플래그로 반영한다 - 이
            # 마트는 거래 1건 = 1행이므로, 이 컬럼은 항상 1이고 후속 집계(SUM(trade_count))로
            # 원하는 단위(자치구/법정동/일자 등)의 거래건수를 자유롭게 낼 수 있게 해주는 용도다.
            pl.lit(1).cast(pl.Int32).alias("trade_count"),
            # Hive 스타일 파티션 컬럼(=계약일자 문자열). upsert_partition()이 이 값 기준으로
            # base_date=YYYY-MM-DD 경로를 결정한다.
            pl.col("deal_date").cast(pl.Utf8).alias("base_date"),
        )
        # 평단가: 거래금액(만원) / 평(면적). 평(면적)은 exclusive_area_m2 > 0 필터가 이미
        # DuckDB 조회 단계(4번)에서 걸려 있어 0으로 나뉠 일이 없다.
        .with_columns(
            (pl.col("trade_amount") / pl.col("pyeong")).round(2).alias("pyeong_amt")
        )
        .collect()
    )


def drop_technical_columns(df: pl.DataFrame) -> pl.DataFrame:
    """파티셔닝/레코드 키 구성에만 쓰고 최종 저장 스키마에는 남기지 않는 컬럼(base_date,
    record_key)을 제거한다. 존재하는 컬럼만 골라 지워, 이미 drop된 뒤 다시 불려도 안전하다."""
    drop_cols = [c for c in ("base_date", "record_key") if c in df.columns]
    return df.drop(drop_cols)


# =====================================================================================
# 6. 파티션/데이터 단위 Upsert(Insert + Update) 및 중복 방지 로직
#    - 고유 식별 키: RECORD_KEY_COLUMNS(1번 상수 참고) 조합을 "|" 구분자로 이어 record_key
#      문자열 컬럼을 만든다. 이 컬럼은 비교/병합에만 쓰고 최종 저장 스키마에는 남기지 않는다.
#    - upsert_partition()이 base_date(=계약일자) 파티션 하나를 아래 3가지로 분기 처리한다:
#        1) INSERT: 해당 base_date 파티션이 아직 없음 -> 새로 수집한 데이터를 그대로 새 파티션
#           으로 기록.
#        2) UPDATE(Merge): 파티션은 있지만 내용이 다름 -> 기존 데이터(old_df)와 새로 수집한
#           데이터(new_df)를 record_key 기준으로 병합한다. Polars concat() + unique(keep='last')
#           로 "새 키는 추가, 기존 키는 최신값(new_df)으로 교체"를 한 번에 수행하고, 몇 건이
#           순수 신규 키인지는 join(how='anti')로 별도 집계해 로그로 남긴다.
#        3) SKIP: 파티션도 있고 내용도 완전히 동일함 -> old_df/new_df를 record_key 기준 정렬 후
#           행 해시(hash_rows)로 비교해 정확히 같으면 파티션 파일을 다시 쓰지 않고(IO 생략)
#           그대로 반환한다.
# =====================================================================================
def _with_record_key(df: pl.DataFrame) -> pl.DataFrame:
    """RECORD_KEY_COLUMNS 값들을 이어붙인 record_key 컬럼을 추가한다. Null은 빈 문자열로
    취급해(sno가 없는 거래도 있음) 두 값이 둘 다 Null일 때 키가 어긋나지 않게 한다."""
    return df.with_columns(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("") for c in RECORD_KEY_COLUMNS],
            separator="|",
        ).alias("record_key")
    )


def _row_fingerprint(df: pl.DataFrame) -> list:
    """record_key 기준으로 정렬한 뒤 행 해시(hash_rows)를 뽑는다. 두 데이터프레임의 이
    지문이 완전히 같으면(리스트 길이/값 전부 일치) 행 순서와 무관하게 내용이 100% 동일하다는
    뜻이다 - apt_rtt_mart.py의 _row_fingerprint와 동일한 목적/구현."""
    return df.sort("record_key").hash_rows(seed=0).to_list()


def _partition_path(lake_bucket: str, day_str: str) -> str:
    return f"s3://{lake_bucket}/mart/{MART_NAME}/base_date={day_str}/{PARTITION_FILE_NAME}"


def read_existing_partition(
    con: duckdb.DuckDBPyConnection, lake_bucket: str, day_str: str
) -> pl.DataFrame | None:
    """이미 저장된 해당 base_date 파티션이 있으면 읽어서 반환하고, 아직 한 번도 저장된 적
    없는 날짜면(S3 404 등) None을 반환한다.
    hive_partitioning=false: 경로 자체가 "base_date=YYYY-MM-DD" 형태라, DuckDB가 이 리터럴
    단일 파일 경로에서도 Hive 파티셔닝을 자동 감지해 파일에는 없는 base_date 컬럼을 결과에
    끼워 넣을 수 있다(apt_rtt_mart.py에서 실제로 겪은 문제 - write_partition_file()이 파일에는
    이미 base_date를 뺀 스키마로 저장하므로, 읽을 때 이 컬럼이 되살아나면 새로 수집한 데이터와
    컬럼 수가 안 맞아 병합/지문 비교가 깨진다). 명시적으로 꺼서 저장 스키마 그대로 읽는다."""
    path = _partition_path(lake_bucket, day_str)
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{path}', hive_partitioning=false)"
        ).pl()
    except Exception:
        return None


def write_partition_file(
    con: duckdb.DuckDBPyConnection, lake_bucket: str, day_str: str, df: pl.DataFrame
) -> None:
    """base_date 파티션 하나를 정해진 파일명(PARTITION_FILE_NAME)으로 통째로 (재)기록한다.
    PARTITION_BY 없이 정확한 파일 경로에 직접 COPY TO 하므로, 이 파티션 하나만 갱신되고
    다른 base_date 파티션들은 전혀 건드리지 않는다(요구사항의 "변경이 발생한 base_date
    파티션만 선별적으로 재작성"). FORMAT PARQUET + COMPRESSION SNAPPY로 저장 포맷 요구사항을
    충족한다."""
    path = _partition_path(lake_bucket, day_str)
    con.register("apt_mkt_trends_partition_write", df)
    con.execute(f"""
        COPY (SELECT * FROM apt_mkt_trends_partition_write)
        TO '{path}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    con.unregister("apt_mkt_trends_partition_write")


def upsert_partition(
    con: duckdb.DuckDBPyConnection,
    lake_bucket: str,
    day_str: str,
    new_day_df: pl.DataFrame,
) -> str:
    """base_date=day_str 파티션 하나를 Insert/Update/Skip 중 하나로 처리하고, 어느 경우였는지
    문자열로 반환한다."""
    # 이번에 새로 수집된 데이터 자체 안에서부터 record_key 기준 중복을 제거한다(소스에서 같은
    # 거래가 중복으로 뽑혀올 가능성 방지 - 마지막 값을 최신으로 간주).
    new_day_df = _with_record_key(new_day_df).unique(subset=["record_key"], keep="last")

    old_df = read_existing_partition(con, lake_bucket, day_str)

    if old_df is None:
        # 1) INSERT: 해당 base_date 파티션 자체가 없던 경우 - 그대로 새 파티션으로 기록.
        write_partition_file(
            con, lake_bucket, day_str, drop_technical_columns(new_day_df)
        )
        print(f"[INSERT] base_date={day_str}: 신규 파티션 생성 ({new_day_df.height}건)")
        return "insert"

    old_df = _with_record_key(old_df)

    # 3) SKIP: 파티션은 있지만 새로 수집한 데이터와 완전히 동일함 -> 파티션 파일을 다시 쓰지
    #    않고 그대로 반환한다 (요구사항: "해당 파티션 저장 IO 작업을 건너뜀").
    if _row_fingerprint(new_day_df) == _row_fingerprint(old_df):
        print(f"[SKIP]   base_date={day_str}: 변경 없음 ({old_df.height}건, 파티션 재작성 생략)")
        return "skip"

    # 2) UPDATE(Merge): record_key 기준으로 old_df와 new_day_df를 합친 뒤 unique(keep='last')로
    #    합친다 - concat에서 new_day_df가 old_df보다 뒤에 오므로, 같은 record_key가 양쪽에 다
    #    있으면 keep='last'가 new_day_df 쪽 값을 최신값으로 채택한다(=Update), old_df에만 있던
    #    키는 그대로 남고(보존), new_day_df에만 있던 키는 자연히 추가된다(=Insert). 순수 신규
    #    키 건수는 join(how='anti')로 별도 집계해 로그에 남긴다.
    new_only_count = new_day_df.join(old_df, on="record_key", how="anti").height
    # 현재 Silver 스냅샷이 해당 날짜의 권위 있는 전체 집합이므로 삭제/취소된 과거 키를 보존하지 않는다.
    merged = new_day_df
    write_partition_file(con, lake_bucket, day_str, drop_technical_columns(merged))
    print(
        f"[UPDATE] base_date={day_str}: 기존 {old_df.height}건 + 신규수집 {new_day_df.height}건 "
        f"-> 병합 후 {merged.height}건 (신규 키 {new_only_count}건, 동일 키는 최신값으로 교체)"
    )
    return "update"


# =====================================================================================
# 7. 실행 진입점
# =====================================================================================
def main() -> None:
    if len(sys.argv) > 1:
        as_of_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        as_of_date = datetime.now().date()
    start_date = as_of_date - timedelta(days=LOOKBACK_DAYS - 1)

    print(
        f"[INFO] 아파트 시장 동향 Gold 마트({MART_NAME}) 생성 시작: "
        f"as_of_date={as_of_date}, 조회기간={start_date} ~ {as_of_date} ({LOOKBACK_DAYS}일)"
    )

    config = load_config()
    con = get_duckdb_connection(config)
    lake_bucket = config["lake_bucket"]

    load_dim_apartment_broadcast(con, lake_bucket)

    # [2026-08-30 SIGABRT 장애 대응 - apt_rtt_mart.py와 동일 패턴] 90일치를 한 번에 Polars로
    # 적재하지 않고, 하루(base_date) 단위로 조회 -> 파생컬럼 -> upsert까지 그 자리에서 끝내고
    # 다음 날짜로 넘어간다. 매 반복마다 두 변수를 새로 대입하므로 이전 날짜의 데이터는 파이썬
    # GC 대상이 되어 다음 날짜로 넘어가기 전에 회수된다. 스키마/샘플 요약은 마지막으로 처리한
    # 날짜의 결과를 대표값으로 출력한다(요약용 정보일 뿐 저장 로직과는 무관 - 스키마는 모든
    # 날짜가 동일하다).
    status_counts = {"insert": 0, "update": 0, "skip": 0}
    total_row_count = 0
    processed_day_count = 0
    last_mart_day_df: pl.DataFrame | None = None
    # 기본 90일 구간에서 실제로 거래가 확인된 단지 키 집합 - 적응형 조회기간 폴백(아래)이
    # "dim_apartment에는 있지만 이 90일 안에는 거래가 없는 단지"를 판별하는 데 쓴다.
    seen_apt_keys: set = set()

    day = start_date
    while day <= as_of_date:
        day_str = day.strftime("%Y-%m-%d")
        raw_day_df = fetch_joined_day(con, lake_bucket, day_str)

        if raw_day_df.height == 0:
            day += timedelta(days=1)
            continue

        seen_apt_keys.update(
            zip(raw_day_df["sgg_cd"].to_list(), raw_day_df["dong_cd"].to_list(), raw_day_df["apt_name"].to_list())
        )

        mart_day_df = shape_mkt_trends_columns(raw_day_df)
        status = upsert_partition(
            con, lake_bucket, day_str, drop_technical_columns(mart_day_df)
        )

        status_counts[status] += 1
        total_row_count += mart_day_df.height
        processed_day_count += 1
        last_mart_day_df = mart_day_df

        day += timedelta(days=1)

    print(
        f"\n[INFO] {MART_NAME} 파티션 Upsert 완료: 조회 기간 내 거래 존재 파티션 {processed_day_count}개 처리 "
        f"(Insert {status_counts['insert']} / Update {status_counts['update']} / Skip {status_counts['skip']})"
    )

    # -----------------------------------------------------------------------------
    # 적응형 조회기간 폴백: dim_apartment에는 있지만 위 기본 90일 구간에는 거래가 전혀
    # 없던 단지에 한해, 과거로 거슬러 올라가며 자신의 최신 거래일자 기준 최근 90일을
    # 추가로 채운다(adaptive_lookback_duckdb.py 모듈 docstring 참고). 대다수 실행에서는
    # 이런 단지가 없어 즉시 반환되며 추가 비용이 없다.
    #
    # [2026-09-09 FastAPI count:0 대응] run_adaptive_backward_fallback()이 찾아낸 데이터를
    # 예전처럼 각 단지의 과거 deal_date 파티션(base_date=day_str)에 바로 upsert하면, FastAPI는
    # MAX(base_date)(=as_of_date) 최신 파티션만 조회하므로 장기 미거래 단지가 그 파티션에는
    # 전혀 반영되지 않아 count:0으로 보인다. 그래서 upsert_fn 자리에 실제 저장 대신 수집만
    # 하는 collect_fallback_upsert를 넘기고, 폴백이 끝난 뒤 모인 데이터를 한꺼번에
    # as_of_date_str(최신 파티션) 하나로 upsert한다 - upsert_partition()이 그 파티션에 이미
    # 있는 당일 거래 데이터와 자동으로 record_key 기준 병합해주므로 별도 처리가 필요 없다.
    # -----------------------------------------------------------------------------
    fallback_rows: list[pl.DataFrame] = []

    def collect_fallback_upsert(
        c: duckdb.DuckDBPyConnection, lb: str, d: str, df: pl.DataFrame
    ) -> str:
        """실제 파티션 저장 대신, 폴백으로 수집된 하루치 데이터를 fallback_rows에만 모은다
        (과거 deal_date 파티션에는 쓰지 않음). 반환값 "skip"은 status_counts 카운트 자리를
        채우기 위한 임시값일 뿐 실제 처리 결과와 무관하다 - 실제 Insert/Update 여부는 아래
        최종 as_of_date_str 파티션 upsert 시점에 결정되며, 그 결과는 fallback_rows 건수
        기반으로 별도 로그에 남긴다."""
        fallback_rows.append(drop_technical_columns(df))
        return "skip"

    # [2026-09-10 OutOfMemoryException(ArrowBuffer) 대응] 위에서 이미 여러 조치(불필요한
    # 임시테이블 재등록 생략, insertion order 보존 끄기, 주기적 gc.collect() - 전부
    # adaptive_lookback_duckdb.py)를 적용했는데도 콜드스타트 폴백(dormant_state 캐시가
    # 비어 수천 개 단지 전부를 최대 1,095일씩 하루 단위로 탐색해야 하는 경우)에서
    # "ArrowBuffer: failed to allocate ... bytes" OOM이 계속 재현됐다. 폴백은 위 90일
    # 메인 루프(87개 파티션 처리)보다 훨씬 무거운 반복을 이어가는데, 같은 DuckDB
    # 커넥션을 계속 재사용하다 보니 메인 루프가 이미 써버린(그리고 온전히 반환되지
    # 않았을 수 있는) 메모리 상태 위에서 폴백이 시작된다. 폴백 직전에 커넥션을 통째로
    # 닫고 새로 열어(dim_apartment_bc도 새 커넥션에 다시 구체화 - 7,165건뿐이라 비용
    # 무시할 수준) 메인 루프가 남긴 상태와 완전히 무관한 깨끗한 메모리에서 폴백을
    # 시작하도록 한다. 이후 코드는 재할당된 con을 그대로 이어서 쓴다.
    con.close()
    con = get_duckdb_connection(config)
    load_dim_apartment_broadcast(con, lake_bucket)

    fallback_result = run_adaptive_backward_fallback(
        con,
        lake_bucket,
        start_date=start_date,
        lookback_days=LOOKBACK_DAYS,
        seen_apt_keys=seen_apt_keys,
        mart_name=MART_NAME,
        fetch_day_fn=fetch_joined_day,
        shape_fn=shape_mkt_trends_columns,
        upsert_fn=collect_fallback_upsert,
    )
    status_counts = {
        key: status_counts[key] + fallback_result["status_counts"][key] for key in status_counts
    }
    total_row_count += fallback_result["total_row_count"]
    processed_day_count += fallback_result["processed_day_count"]

    # 폴백으로 모인 데이터(있다면)를 최신 파티션(as_of_date_str) 하나로 합쳐 upsert한다.
    # upsert_partition()은 그 파티션에 이미 당일 거래 데이터가 있어도 record_key 기준으로
    # 자동 병합하므로 기존 데이터를 덮어쓰지 않는다.
    as_of_date_str = as_of_date.strftime("%Y-%m-%d")
    if fallback_rows:
        combined_fallback_df = pl.concat(fallback_rows, how="vertical")
        fallback_status = upsert_partition(con, lake_bucket, as_of_date_str, combined_fallback_df)
        print(
            f"\n[INFO] {MART_NAME}: 장기 미거래 단지 폴백 데이터 {combined_fallback_df.height}건을 "
            f"최신 파티션(base_date={as_of_date_str})에 병합 upsert 완료 ({fallback_status})"
        )
    else:
        print(f"\n[INFO] {MART_NAME}: 폴백 대상 없음 - 최신 파티션({as_of_date_str}) 병합 생략")

    if last_mart_day_df is not None:
        final_schema_df = drop_technical_columns(last_mart_day_df)
        print(f"\n===== [SCHEMA] {MART_NAME} =====")
        print(final_schema_df.schema)

        print(f"\n===== [SAMPLE] {MART_NAME} 마지막 처리 파티션({day_str}) 상위 20건 =====")
        with pl.Config(tbl_cols=-1, tbl_rows=20):
            print(final_schema_df.head(20))

    print(f"\n===== [COUNT] {MART_NAME} 이번 실행 조회 총 레코드 수: {total_row_count}건 =====")

    con.close()


if __name__ == "__main__":
    main()

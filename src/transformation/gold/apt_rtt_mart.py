# -*- coding: utf-8 -*-
"""
서울시 아파트 거래동향(RTT: Real-estate Trade Trend) Gold 데이터 마트 생성 스크립트
- 단독 실행 가능한 배치 스크립트. PySpark 없이 Polars + DuckDB만으로 동작한다
  (build_dong_pyeong_mart.py/build_apt_recent_trade_mart.py는 PySpark 기반이지만, 이 마트는
  건별(트랜잭션 레벨) 결과라 셔플이 큰 분산 처리 없이도 DuckDB의 벡터화 엔진 + Polars의
  인메모리 컬럼 연산만으로 충분하다).
- 소스: dim_apartment / fact_apt_transactions (S3 Lake(MinIO)에 Iceberg로 적재된 데이터 파일을
  DuckDB httpfs로 직접 조회 - pipeline_apt_name.py/region_master.py와 동일한 방식).
- 산출: 최근 90일 아파트 매매 거래 건별(자치구/법정동/거래일자/층수/거래가/평/거래건수) 데이터를
  S3 Lake(MinIO) mart/RTT/base_date=YYYY-MM-DD 경로에 Parquet(Snappy 압축)로 저장한다.

[파티션/Upsert 설계 - base_date의 의미]
이 마트는 실행할 때마다 "오늘"을 기준으로 최근 90일 구간을 다시 훑는다(공공 API의 뒤늦은
신고/정정 반영 - Real_Estate.py의 fetch_real_estate_recent와 동일한 문제의식). 즉 어제 실행과
오늘 실행이 겹치는 89일 구간을 매번 다시 스캔하게 되므로, base_date를 "스크립트를 실행한
날짜"가 아니라 "각 거래 행의 실제 거래일자(deal_date)"로 정의한다 - 그래야 재실행 때마다
같은 날짜의 파티션을 다시 만나 그 파티션 단위로 신규/변경/무변경을 판별할 수 있다. 90일 조회
기간 안의 날짜마다 별도 파티션(base_date=YYYY-MM-DD)으로 나뉘며, 파티션마다 독립적으로
Insert/Update/Skip을 판단한다(아래 upsert_partition() 참고).

실행 방법 (단독 CLI, 프로젝트 루트에서 가상환경 활성화 후):
  python src/transformation/gold/apt_rtt_mart.py [AS_OF_DATE]
    - AS_OF_DATE(YYYY-MM-DD) 생략 시 오늘 날짜를 기준으로 최근 90일치를 재확인한다.
    - 특정 과거 기준일로 다시 돌리고 싶으면 인자로 넘기면 된다. 어떤 기준일로 몇 번을
      다시 돌려도, 실제로 내용이 바뀐 base_date 파티션만 다시 쓰인다(멱등적).

준비물: env/.env 에 S3_END_POINT / S3_ACCESS_KEY / S3_SECRET_KEY / LAKE.

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

# =====================================================================================
# 1. 상수
# =====================================================================================
LOOKBACK_DAYS = 90          # 거래일자(deal_date) 기준 최근 N일
PYEONG_M2 = 3.30578          # 1평 = 3.30578 m2 (전용면적 기준 평수 환산 - 공급면적 1.3배율 미적용)

# dim_apartment는 파티션이 없어 data/ 밑에 바로 parquet이 있고, fact_apt_transactions는
# Iceberg 테이블 생성 시 PARTITIONED BY (days(deal_date))로 만들어져 있어(Real_Estate_Transform.py
# 참고) data/deal_date_day=YYYY-MM-DD/*.parquet 형태로 중첩되어 있다 - 재귀 글롭(**)이 필요하다
# (pipeline_apt_name.py의 DIM_APARTMENT_GLOB/FACT_APT_TRANSACTIONS_GLOB와 동일한 규칙).
DIM_APARTMENT_GLOB = "dim_apartment/data/*.parquet"
FACT_APT_TRANSACTIONS_GLOB = "fact_apt_transactions/data/**/*.parquet"

# 최종 마트 이름/저장 경로: {S3_END_POINT}/{LAKE}/mart/RTT/base_date=YYYY-MM-DD/data.parquet
# 파일명을 고정값(PARTITION_FILE_NAME)으로 둬서, 같은 base_date를 몇 번을 다시 써도(Update)
# 그 경로의 파일 하나만 교체될 뿐 파일이 계속 쌓이지 않는다(멱등적 덮어쓰기).
MART_NAME = "RTT"
PARTITION_FILE_NAME = "data.parquet"

# 레코드 고유 식별 키(Unique Key). fact_apt_transactions에는 접수번호 같은 단일 PK가 없어서
# (Real_Estate.py의 원본 실거래가 테이블과 동일한 사정), "거래를 유일하게 특정 짓는" 컬럼
# 조합을 키로 쓴다: 단지(자치구+법정동+단지명) + 지번(mno/sno, 동일 단지 내 동/호 구분에
# 근접) + 거래일자 + 층 + 전용면적. 이 조합이 같은 두 행은 "같은 거래"로 간주해 Upsert/Dedup의
# 기준으로 삼는다 (아래 _with_record_key 참고).
# [중요] 거래가(trade_amount)는 의도적으로 키에서 뺐다 - 요구사항이 예로 든 "거래정보 변경"의
# 가장 흔한 사례가 바로 가격 정정(같은 거래의 신고가가 나중에 수정되는 경우)인데, 거래가를
# 키에 포함시키면 가격이 달라질 때마다 "같은 거래의 최신값"이 아니라 "전혀 다른 새 거래"로
# 오판되어 옛 레코드가 지워지지 않고 계속 누적된다(실제로 로컬 테스트에서 이 문제를 재현해
# 확인했다). 나머지 키 컬럼(단지+지번+거래일자+층+전용면적)이 같으면 같은 거래로 보고, 그
# 안에서 거래가가 바뀐 경우를 "그 거래의 최신 신고가로 Update"하는 것이 이 마트의 의도다.
KEY_COLUMNS = [
    "sgg_cd", "dong_cd", "apt_name", "mno", "sno",
    "deal_date", "floor", "exclusive_area_m2",
]


# =====================================================================================
# 2. 환경 변수 로드 (.env 또는 OS 환경변수) + MinIO(S3) DuckDB 연결 설정
#    - 로컬 실행(<project_root>/env/.env)과 Airflow 컨테이너 실행(/opt/airflow/project/env/.env)
#      양쪽 경로를 다 시도한다 (build_dong_pyeong_mart.py/pipeline_apt_name.py와 동일한 패턴).
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
    virtual-hosted-style)를 s3_endpoint의 스킴으로 자동 판별한다 (src/utils/connect.py의
    configure_minio와 동일한 판별 방식 - 이 스크립트는 PySpark 없이 순수 DuckDB만 쓰는
    독립 스크립트라 여기서도 동일하게 다시 설정한다).

    [SIGKILL(OOM) 대응] memory_limit을 지정하지 않으면 DuckDB는 컨테이너의 cgroup 메모리
    한도가 아니라 호스트(Docker Desktop VM)의 전체 물리 메모리를 기준으로 기본 한도(약 80%)를
    잡는다 - 컨테이너 자체에 별도 메모리 제한이 없더라도, 여러 컨테이너가 같은 Docker Desktop
    VM의 메모리를 나눠 쓰는 환경에서는 DuckDB가 실제 여유 메모리보다 훨씬 많은 메모리를 쓰려다
    커널 OOM killer에 의해 SIGKILL로 죽을 수 있다(실제로 Airflow에서 이 증상이 재현됨 -
    'died with <Signals.SIGKILL: 9>', 프로세스 stdout은 한 줄도 못 남기고 즉사).
    memory_limit + temp_directory를 명시해, 그 한도에 가까워지면 예외 대신 디스크로 스필하도록
    강제한다. 값은 실행 환경(Airflow 컨테이너 vs 로컬)에 맞게 env로 오버라이드 가능하게 둔다."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # 기본값 3GB: GCP e2-standard-2(2 vCPU, 8GB RAM) VM 기준 - Airflow 상주 프로세스가
    # 이미 2.5~3GB를 쓰고 있어 여유 메모리는 4.5~5GB뿐이지만, Airflow DAG가 Gold 마트를
    # 전부 순차 실행하도록 바뀌어(gold_mart_serial_pool) 이 프로세스가 뜰 때는 다른 무거운
    # 프로세스가 동시에 돌지 않는다는 전제로 예전 기본값(1GB)보다 넉넉하게 잡았다.
    memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "3GB")
    threads = os.getenv("DUCKDB_THREADS", "2")
    temp_directory = os.getenv("DUCKDB_TEMP_DIRECTORY", "/tmp/duckdb_spill_apt_rtt_mart")

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
    return con


# =====================================================================================
# 3. dim_apartment 로드 - Broadcast Join(In-Memory Map Join)용 빌드 사이드 구체화
#    dim_apartment는 자치구/법정동/단지명 차원 테이블로 건수가 작다(수만 건 수준, fact_apt_
#    transactions 대비 훨씬 작음). DuckDB TEMP TABLE로 통째로 메모리에 구체화(materialize)해
#    두면, 이후 fact와의 조인에서 이 작은 테이블이 해시 조인의 build side가 되어 large-table
#    쪽(fact)을 셔플/재파티셔닝하지 않고 스트리밍하며 그대로 probe할 수 있다 - PySpark의
#    broadcast(dim_df)와 동일한 효과를 단일 프로세스 DuckDB에서 얻는 방식이다.
# =====================================================================================
def load_dim_apartment_broadcast(con: duckdb.DuckDBPyConnection, lake_bucket: str) -> None:
    s3_path = f"s3://{lake_bucket}/{DIM_APARTMENT_GLOB}"
    print(f"[INFO] dim_apartment 브로드캐스트 테이블 구체화: {s3_path}")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dim_apartment_bc AS
        SELECT sgg_cd, sgg_nm, dong_cd, dong_nm, apt_name
        FROM read_parquet('{s3_path}')
    """)
    # fetchone()은 정적으로 tuple | None으로 추론되어(COUNT(*)가 항상 한 행을 반환한다는 사실을
    # 타입 체커는 모름) [0] 인덱싱에 경고가 뜬다. None 방어 처리로 안전하게 언패킹한다.
    count_row = con.execute("SELECT COUNT(*) FROM dim_apartment_bc").fetchone()
    row_count = count_row[0] if count_row is not None else 0
    print(f"[INFO] dim_apartment 브로드캐스트 테이블 행 수: {row_count}건")


# =====================================================================================
# 4. fact_apt_transactions 조회 - Predicate/Projection Pushdown + Broadcast Join
#    - Predicate Pushdown: WHERE 절의 deal_date_day 조건이 fact_apt_transactions의 실제 물리
#      파티션 디렉터리(data/deal_date_day=YYYY-MM-DD/)와 일치하므로, DuckDB가 파티션 값 자체로
#      디렉터리 단위 스킵(파티션 프루닝)을 수행한다. deal_date(파티션 컬럼과 별개의 일반
#      컬럼)만으로 걸렀다면 파일을 일단 열어 row-group 통계로만 걸러지므로, 굳이 파티션
#      컬럼(deal_date_day)을 기준으로 필터링해 디렉터리 단위로 먼저 잘라낸다.
#    - Projection Pushdown: 서브쿼리에서 실제로 쓰는 컬럼만 SELECT해, 파케이 파일에서 그
#      컬럼들만 읽어오게 한다(price_per_m2/deal_type/agent_sgg_nm 등은 애초에 읽지 않음).
#      mno/sno는 레코드 고유 키(KEY_COLUMNS) 구성에 필요해 함께 선택한다.
#    - Broadcast Join: 위 3번에서 구체화해둔 dim_apartment_bc(작은 build side)에 fact(필터링/
#      프로젝션이 끝난 큰 쪽)를 INNER JOIN한다 - 조인 키(sgg_cd, dong_cd, apt_name) 기준.
#    - union_by_name=true: Iceberg 메타데이터 없이 원본 parquet을 직접 글롭하는 방식이라,
#      과거에 스키마가 바뀐 적 있는 옛 파일과 컬럼 구성이 다를 수 있음을 관대하게 처리한다
#      (pipeline_apt_name.py의 fact_apt_transactions 조회와 동일한 이유).
#    - 90일 구간 전체를 한 번의 쿼리로 가져온다(파티션마다 재조회하지 않음) - 이후 5번에서
#      Polars가 base_date(=deal_date)별로 인메모리 분할해 파티션 단위 Upsert에 넘긴다.
# =====================================================================================
def fetch_joined_raw(
    con: duckdb.DuckDBPyConnection,
    lake_bucket: str,
    as_of_date,
    start_date,
) -> pl.DataFrame:
    fact_s3_path = f"s3://{lake_bucket}/{FACT_APT_TRANSACTIONS_GLOB}"
    query = f"""
        SELECT
            d.sgg_cd  AS sgg_cd,
            d.sgg_nm  AS sgg_nm,
            d.dong_cd AS dong_cd,
            d.dong_nm AS dong_nm,
            d.apt_name AS apt_name,
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
            WHERE deal_date_day >= DATE '{start_date}'
              AND deal_date_day <= DATE '{as_of_date}'
              AND (cancel_date IS NULL OR TRIM(cancel_date) = '')
              AND price_ten_thousand > 0
              AND exclusive_area_m2 > 0
        ) AS f
        INNER JOIN dim_apartment_bc AS d
            ON f.sgg_cd = d.sgg_cd
           AND f.dong_cd = d.dong_cd
           AND f.apt_name = d.apt_name
    """
    print(f"[INFO] fact_apt_transactions 조회(필터+브로드캐스트 조인): {fact_s3_path}")
    return con.execute(query).pl()


# =====================================================================================
# 5. Polars 변환 - 평(전용면적 기준) 환산 + 거래건수(건별 트랜잭션 플래그) + base_date(=거래일자) 부여
#    DuckDB에서는 조인/필터링까지만 하고, 마트 스키마를 완성하는 컬럼 파생은 Polars의
#    표현식으로 처리한다(요구사항의 "Polars 및 DuckDB" 병행 활용 - DuckDB는 S3/조인,
#    Polars는 인메모리 컬럼 연산 및 파티션 단위 Upsert 담당).
#    base_date는 스크립트 실행일이 아니라 각 행의 실제 거래일자(deal_date)를 그대로 쓴다 -
#    이래야 재실행 시 같은 파티션을 다시 만나 Upsert 판별이 가능하다(모듈 docstring 참고).
# =====================================================================================
def shape_rtt_columns(raw_df: pl.DataFrame) -> pl.DataFrame:
    lf = raw_df.lazy()
    return (
        lf.select(
            pl.col("sgg_cd"),
            pl.col("sgg_nm"),
            pl.col("dong_cd"),
            pl.col("dong_nm"),
            pl.col("apt_name"),
            pl.col("mno"),
            pl.col("sno"),
            pl.col("deal_date"),
            pl.col("floor"),
            pl.col("price_ten_thousand").alias("trade_amount"),
            # 평(면적): 전용면적(m2) 기준 평수 환산 (전용면적 / 3.30578) - 다른 마트의
            # "공급면적(전용 * 1.3) 기준 평형" 계산과 달리, 이 마트는 요구사항대로 전용면적을
            # 그대로 평으로 환산한다(1.3 공급 배율 미적용).
            (pl.col("exclusive_area_m2") / PYEONG_M2).round(2).alias("pyeong"),
            # exclusive_area_m2 원본값도 레코드 키(KEY_COLUMNS) 구성에 그대로 필요해 함께 둔다.
            pl.col("exclusive_area_m2"),
            # 거래건수: 집계(GROUP BY) 대신 건별(row-level) 트랜잭션 플래그로 반영한다 - 이
            # 마트는 거래 1건 = 1행이므로, 이 컬럼은 항상 1이고 후속 집계(SUM(trade_count))로
            # 원하는 단위(자치구/동/일자 등)의 거래건수를 자유롭게 낼 수 있게 해주는 용도다.
            pl.lit(1).cast(pl.Int32).alias("trade_count"),
            # Hive 스타일 파티션 컬럼(=거래일자 문자열). upsert_partition()이 이 값 기준으로
            # base_date=YYYY-MM-DD 경로를 결정한다.
            pl.col("deal_date").cast(pl.Utf8).alias("base_date"),
        )
        .collect()
    )


# =====================================================================================
# 6. 파티션/데이터 단위 Upsert(Insert + Update) 및 중복 방지 로직
#    - 고유 식별 키: KEY_COLUMNS(1번 상수 참고) 조합을 "|" 구분자로 이어 record_key 문자열
#      컬럼을 만든다. 이 컬럼은 비교/병합에만 쓰고 최종 저장 스키마에는 남기지 않는다.
#    - upsert_partition()이 base_date(=거래일자) 파티션 하나를 아래 3가지로 분기 처리한다:
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
    """KEY_COLUMNS 값들을 이어붙인 record_key 컬럼을 추가한다. Null은 빈 문자열로 취급해
    (mno/sno가 없는 거래도 있음) 두 값이 둘 다 Null일 때 키가 어긋나지 않게 한다."""
    return df.with_columns(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("") for c in KEY_COLUMNS],
            separator="|",
        ).alias("record_key")
    )


def _row_fingerprint(df: pl.DataFrame) -> list:
    """record_key 기준으로 정렬한 뒤 행 해시(hash_rows)를 뽑는다. 두 데이터프레임의 이
    지문이 완전히 같으면(리스트 길이/값 전부 일치) 행 순서와 무관하게 내용이 100% 동일하다는
    뜻이다 - Real_Estate.py의 _fingerprint_rows와 동일한 목적을, Polars 벡터화 연산으로
    구현한 버전이다."""
    return df.sort("record_key").hash_rows(seed=0).to_list()


def _partition_path(lake_bucket: str, day_str: str) -> str:
    return f"s3://{lake_bucket}/mart/{MART_NAME}/base_date={day_str}/{PARTITION_FILE_NAME}"


def read_existing_partition(
    con: duckdb.DuckDBPyConnection, lake_bucket: str, day_str: str
) -> pl.DataFrame | None:
    """이미 저장된 해당 base_date 파티션이 있으면 읽어서 반환하고, 아직 한 번도 저장된 적
    없는 날짜면(S3 404 등) None을 반환한다 (Real_Estate.py의 _read_existing_day_rows와
    동일한 패턴).
    hive_partitioning=false: 경로 자체가 "base_date=YYYY-MM-DD" 형태라, DuckDB가 이 리터럴
    단일 파일 경로에서도 Hive 파티셔닝을 자동 감지해 파일에는 없는 base_date 컬럼을 결과에
    끼워 넣는다(실제로 겪은 문제 - write_partition_file()이 파일에는 이미 base_date를 뺀
    스키마로 저장하므로, 읽을 때 이 컬럼이 되살아나면 새로 수집한 데이터(new_day_df, base_date
    없음)와 컬럼 수가 안 맞아 병합/지문 비교가 깨진다). 명시적으로 꺼서 저장 스키마 그대로
    읽는다."""
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
    파티션만 선별적으로 재작성")."""
    path = _partition_path(lake_bucket, day_str)
    con.register("rtt_partition_write", df)
    con.execute(f"""
        COPY (SELECT * FROM rtt_partition_write)
        TO '{path}'
        (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    con.unregister("rtt_partition_write")


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
        write_partition_file(con, lake_bucket, day_str, new_day_df.drop("record_key"))
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
    merged = pl.concat([old_df, new_day_df]).unique(subset=["record_key"], keep="last")
    write_partition_file(con, lake_bucket, day_str, merged.drop("record_key"))
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
        f"[INFO] 아파트 거래동향 Gold 마트({MART_NAME}) 생성 시작: "
        f"as_of_date={as_of_date}, 조회기간={start_date} ~ {as_of_date} ({LOOKBACK_DAYS}일)"
    )

    config = load_config()
    con = get_duckdb_connection(config)
    lake_bucket = config["lake_bucket"]

    load_dim_apartment_broadcast(con, lake_bucket)
    raw_df = fetch_joined_raw(con, lake_bucket, as_of_date, start_date)
    print(f"[INFO] 브로드캐스트 조인 결과 행 수: {raw_df.height}건")

    mart_df = shape_rtt_columns(raw_df)

    # base_date(=거래일자)별로 나눠 파티션 단위 Upsert를 수행한다. 90일 조회 기간 안에서도
    # 실제로 거래가 있는 날짜만 그룹이 생기므로, 이번 조회에 거래가 0건인 날짜는 아예 건드리지
    # 않는다 - 기존 파티션이 있어도 삭제하지 않고 그대로 둔다(안전한 보수적 동작. 취소 처리된
    # 거래는 이미 4번의 cancel_date 필터에서 걸러졌으므로 새 결과에 없는 게 정상일 수 있다).
    day_groups = mart_df.partition_by("base_date", as_dict=True)

    status_counts = {"insert": 0, "update": 0, "skip": 0}
    for (day_str,), day_df in sorted(day_groups.items()):
        status = upsert_partition(con, lake_bucket, day_str, day_df.drop("base_date"))
        status_counts[status] += 1

    print(
        f"\n[INFO] {MART_NAME} 파티션 Upsert 완료: 조회 기간 내 거래 존재 파티션 {len(day_groups)}개 처리 "
        f"(Insert {status_counts['insert']} / Update {status_counts['update']} / Skip {status_counts['skip']})"
    )

    print(f"\n===== [SCHEMA] {MART_NAME} =====")
    print(mart_df.schema)

    print(f"\n===== [SAMPLE] {MART_NAME} 상위 20건 =====")
    with pl.Config(tbl_cols=-1, tbl_rows=20):
        print(mart_df.head(20))

    print(f"\n===== [COUNT] {MART_NAME} 이번 실행 조회 총 레코드 수: {mart_df.height}건 =====")

    con.close()


if __name__ == "__main__":
    main()

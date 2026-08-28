# -*- coding: utf-8 -*-
"""
Gold: 아파트 단지명 검색 인덱스(Elasticsearch apt_name) 적재 파이프라인
- 원천: S3 Lake(MinIO)의 lakehouse.dim_apartment(자치구/법정동/단지명 차원, sgg_nm/dong_nm
  텍스트 컬럼까지 이미 포함) + lakehouse.fact_apt_transactions(단지별 실거래, 최신 계약일자
  산출용) Iceberg 데이터 파일
- 산출: Elasticsearch 인덱스 apt_name (자치구 x 법정동 x 단지명 문서, 단지별 최신 실거래
  계약일자 포함) - 단지명 검색/자동완성 전용 용도이며, 가격 조회는 여전히 S3 Lake mart/
  경로의 Parquet(dm_apt_pyeong_price/dm_apt_flr_price, 여기에도 apt_name 컬럼이 이미 있음)를
  그대로 쓴다. 즉 이 인덱스는 RDBMS를 거치지 않고 dim_apartment -> Elasticsearch로 직결한다.

[변경 이력] 이전에는 이 파이프라인이 MySQL tb_apt_name(자치구/법정동 FK + 단지명)에
upsert했으나, 검색(부분일치/자동완성)에는 RDBMS보다 Elasticsearch가 적합하고 dim_apartment에
sgg_nm/dong_nm이 이미 있어 MySQL 마스터 테이블(tb_sgg_master/tb_dong_master) 조인이 애초에
필요 없었으므로, MySQL 경로를 걷어내고 Elasticsearch 직접 색인으로 교체했다.

실행 방법 (단독 CLI, 프로젝트 루트에서 가상환경 활성화 후):
  python src/transformation/gold/pipeline_apt_name.py

Airflow 연동:
  PythonOperator 또는 @task로 run_apt_name_es_pipeline()을 호출한다.
  (파일 하단 "Airflow 태스크 스니펫" 주석 참고)

준비물: env/.env 에 S3_END_POINT / S3_ACCESS_KEY / S3_SECRET_KEY / LAKE, ES_HOST
        (Elasticsearch 접속 정보, 예: 100.98.111.49:9200).
"""

import hashlib
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from utils.connect import get_duckdb_connect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =====================================================================================
# 1. 환경 변수 로드 (build_dong_pyeong_mart.py / pipeline_apt_name.py 기존 패턴과 동일:
#    로컬 실행과 Airflow 컨테이너 실행 양쪽의 env/.env 경로를 모두 시도) - override=False로
#    호출하는 쪽(Airflow env_file 등)이 이미 채워둔 환경 변수는 덮어쓰지 않고 빈 값만 보충한다.
# =====================================================================================
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_ENV_CANDIDATES = [
    _PROJECT_ROOT / "env" / ".env",
    Path("/opt/airflow/project/env/.env"),
]
for _env_path in _ENV_CANDIDATES:
    if _env_path.exists():
        load_dotenv(dotenv_path=_env_path, override=False)
        break


# =====================================================================================
# 2. S3 Lake 원천 경로 (Iceberg 데이터 파일을 DuckDB httpfs로 직접 조회 - scripts/
#    region_master.py와 동일한 방식. dim_apartment는 파티션이 없어 data/ 밑에 바로
#    parquet이 있고, fact_apt_transactions는 PARTITIONED BY (days(deal_date))라서
#    data/deal_date_day=.../*.parquet 형태로 중첩되어 있어 재귀 글롭(**)이 필요하다.)
# =====================================================================================
DIM_APARTMENT_GLOB = "dim_apartment/data/*.parquet"
FACT_APT_TRANSACTIONS_GLOB = "fact_apt_transactions/data/**/*.parquet"

ES_INDEX_NAME = "apt_name"
CHUNK_SIZE = 2000


# =====================================================================================
# 3. Elasticsearch 인덱스 매핑
#    - apt_name: 기본 analyzer(오타/부분일치 색인용 edge_ngram)로 색인하고, search_analyzer는
#      edge_ngram 없이 표준 토크나이즈만 적용해 "검색어가 입력될 때마다 검색어 자체를 잘게
#      쪼개는" edge_ngram 특유의 과매칭을 막는다(색인 시에만 ngram 확장, 검색 시에는 그대로).
#      정확 일치/정렬용으로 apt_name.keyword 서브필드도 함께 둔다.
#    - 한국어 형태소 분석(nori)은 Elasticsearch 서버에 analysis-nori 플러그인이 설치되어
#      있어야 해서(docker-compose의 기본 elasticsearch 이미지에는 없음) 여기서는 플러그인
#      없이도 동작하는 edge_ngram 기반 부분일치로 구성했다. 추후 nori 플러그인을 설치하면
#      apt_name analyzer만 nori 기반으로 교체하면 된다.
# =====================================================================================
_INDEX_BODY = {
    "settings": {
        "analysis": {
            "filter": {
                "apt_name_edge_ngram": {
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20,
                },
            },
            "analyzer": {
                "apt_name_index_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "apt_name_edge_ngram"],
                },
                "apt_name_search_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"],
                },
            },
        },
    },
    "mappings": {
        "properties": {
            "apt_name": {
                "type": "text",
                "analyzer": "apt_name_index_analyzer",
                "search_analyzer": "apt_name_search_analyzer",
                "fields": {
                    "keyword": {"type": "keyword"},
                },
            },
            "sgg_cd": {"type": "keyword"},
            "sgg_nm": {"type": "keyword"},
            "dong_cd": {"type": "keyword"},
            "dong_nm": {"type": "keyword"},
            "last_deal_date": {"type": "date"},
            # 지번(본번/부번) - 검색에는 쓰지 않고 단순 저장/조회용 필드라 apt_name과 달리
            # edge_ngram 분석기를 태우지 않는다 (keyword: 검색 대상 아님, 정확 값만 보관).
            "mno": {"type": "keyword"},
            "sno": {"type": "keyword"},
        },
    },
}

# mno/sno 필드를 나중에(이 필드 추가 이전에) 인덱스가 이미 만들어져 있던 환경에도 적용하기
# 위한 부분 매핑. Elasticsearch는 기존 필드의 매핑을 재색인 없이 "변경"할 수는 없지만, 기존에
# 없던 새 필드를 "추가"하는 것은 언제든 안전하게 허용된다 - 그래서 _ensure_index가 인덱스
# 생성을 건너뛴 경우(이미 존재)에도 이 매핑만 별도로 put_mapping한다.
_MNO_SNO_MAPPING = {
    "mno": {"type": "keyword"},
    "sno": {"type": "keyword"},
}


def _resolve_es_client() -> Elasticsearch:
    es_host = os.environ.get("ES_HOST")
    if not es_host:
        raise RuntimeError("ES_HOST가 설정되어 있지 않습니다. env/.env를 확인하세요.")
    url = es_host if "://" in es_host else f"http://{es_host}"
    return Elasticsearch(url)


def _ensure_index(client: Elasticsearch) -> None:
    """apt_name 인덱스가 없으면 매핑과 함께 생성한다 (있으면 생성은 스킵 -
    매핑은 최초 생성 시에만 통째로 적용되고, 이미 있는 인덱스의 기존 필드 매핑을 바꾸려면
    재색인이 필요하기 때문에 여기서 임의로 덮어쓰지 않는다). 다만 mno/sno처럼 기존에 없던
    "새 필드"를 추가하는 것은 재색인 없이도 안전하므로, 인덱스가 이미 있던 환경이라도
    put_mapping으로 그 필드만 보강한다."""
    if client.indices.exists(index=ES_INDEX_NAME):
        logger.info("Elasticsearch 인덱스 '%s' 이미 존재 - 생성 스킵", ES_INDEX_NAME)
        client.indices.put_mapping(index=ES_INDEX_NAME, properties=_MNO_SNO_MAPPING)
        logger.info("Elasticsearch 인덱스 '%s'에 mno/sno 필드 매핑 보강 완료", ES_INDEX_NAME)
        return
    client.indices.create(index=ES_INDEX_NAME, body=_INDEX_BODY)
    logger.info("Elasticsearch 인덱스 '%s' 생성 완료", ES_INDEX_NAME)


# =====================================================================================
# 4. S3 Lake 원천 읽기 (DuckDB httpfs) - dim_apartment(단지 차원, sgg_nm/dong_nm 포함) +
#    fact_apt_transactions(단지별 최신 실거래 계약일자 집계)
# =====================================================================================
def _read_dim_apartment(lake_bucket: str) -> pd.DataFrame:
    """dim_apartment에서 (sgg_cd, sgg_nm, dong_cd, dong_nm, apt_name) 고유 조합을 읽어온다.
    dim_apartment 자체에 자치구/법정동 명칭이 이미 들어 있어(build_dong_pyeong_mart.py의
    _APT_SELECT_COLS와 동일 컬럼), MySQL 마스터 테이블 조인 없이 바로 검색 문서를 만들 수
    있다."""
    con = get_duckdb_connect()
    s3_path = f"s3://{lake_bucket}/{DIM_APARTMENT_GLOB}"
    logger.info("dim_apartment 원천 조회: %s", s3_path)
    df = con.execute(
        "SELECT DISTINCT sgg_cd, sgg_nm, dong_cd, dong_nm, apt_name "
        f"FROM read_parquet('{s3_path}')"
    ).df()
    con.close()
    if df.empty:
        raise RuntimeError(f"dim_apartment에서 읽어온 데이터가 없습니다: {s3_path}")
    logger.info("dim_apartment 고유 단지 수: %d건", len(df))
    return df


def _read_last_deal_dates(lake_bucket: str) -> pd.DataFrame:
    """fact_apt_transactions에서 (sgg_cd, dong_cd, apt_name) 그룹별 최신 deal_date를
    집계한다. 거래취소건(cancel_date가 채워진 행)은 최신일자 산출에서 제외한다
    (build_dong_pyeong_mart.py의 취소건 필터와 동일한 기준)."""
    con = get_duckdb_connect()
    s3_path = f"s3://{lake_bucket}/{FACT_APT_TRANSACTIONS_GLOB}"
    logger.info("fact_apt_transactions 원천 조회(최신 계약일자 집계): %s", s3_path)
    # union_by_name=true: Iceberg의 ADD COLUMNS(mno/sno)는 메타데이터만 바뀌고 이미 있던
    # 데이터 파일은 다시 쓰지 않으므로, 그 컬럼이 실제로 바뀐(rewrite된) 적 없는 옛 파일은
    # 물리적으로 mno/sno 컬럼이 없다. Iceberg 메타데이터 없이 원본 parquet을 직접 글롭하는
    # 이 함수는 그 스키마 차이를 union_by_name으로 관대하게 처리해야 한다(없는 컬럼은 NULL).
    df = con.execute(
        f"""
        SELECT sgg_cd, dong_cd, apt_name, MAX(deal_date) AS last_deal_date
        FROM read_parquet('{s3_path}', union_by_name=true)
        WHERE cancel_date IS NULL OR TRIM(cancel_date) = ''
        GROUP BY sgg_cd, dong_cd, apt_name
        """
    ).df()
    con.close()
    logger.info("fact_apt_transactions 최신 계약일자 집계 대상 단지 수: %d건", len(df))
    return df


def _read_representative_mno_sno(lake_bucket: str) -> pd.DataFrame:
    """fact_apt_transactions에서 (sgg_cd, dong_cd, apt_name) 그룹별로 가장 흔하게 등장한
    (mno, sno) 조합을 대표 지번으로 뽑는다 (build_dong_pyeong_mart.py의 jibun_lookup과 동일한
    최빈값 선정 방식 - 검색(apt_name)에는 관여하지 않고 단순 저장용 부가 필드다)."""
    con = get_duckdb_connect()
    s3_path = f"s3://{lake_bucket}/{FACT_APT_TRANSACTIONS_GLOB}"
    logger.info("fact_apt_transactions 원천 조회(대표 mno/sno 집계): %s", s3_path)
    df = con.execute(
        f"""
        WITH counted AS (
            SELECT sgg_cd, dong_cd, apt_name, mno, sno, COUNT(*) AS cnt
            FROM read_parquet('{s3_path}', union_by_name=true)
            WHERE mno IS NOT NULL AND TRIM(mno) != ''
            GROUP BY sgg_cd, dong_cd, apt_name, mno, sno
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY sgg_cd, dong_cd, apt_name ORDER BY cnt DESC
            ) AS rn
            FROM counted
        )
        SELECT sgg_cd, dong_cd, apt_name, mno, sno
        FROM ranked
        WHERE rn = 1
        """
    ).df()
    con.close()
    logger.info("대표 mno/sno 집계 대상 단지 수: %d건", len(df))
    return df


# =====================================================================================
# 5. Elasticsearch 문서 _id: (sgg_cd, dong_cd, apt_name) 조합의 md5 해시로 결정론적으로
#    만든다. 매번 전체 재계산해서 같은 _id로 index 액션(전체 덮어쓰기)을 실행하는 멱등적
#    (idempotent) 배치라, 문서가 이미 있으면 그대로 갱신되고 새 조합이면 새로 생긴다
#    (MySQL의 INSERT ... ON DUPLICATE KEY UPDATE와 동등한 upsert 효과).
# =====================================================================================
def _make_doc_id(sgg_cd: str, dong_cd: str, apt_name: str) -> str:
    key = f"{sgg_cd}|{dong_cd}|{apt_name}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def _build_actions(df: pd.DataFrame):
    # itertuples()의 컬럼 속성은 pandas 타입 스텁상 여러 스칼라 타입의 유니온으로 잡혀
    # .isoformat() 같은 datetime 전용 메서드 호출에 타입체커가 걸리므로, to_dict(records)로
    # 얻은 값을 pd.Timestamp(...)로 명시 변환해 항상 Timestamp 타입에서 호출한다.
    for record in df.to_dict(orient="records"):
        raw_last_deal_date = record["last_deal_date"]
        last_deal_date = (
            None if pd.isna(raw_last_deal_date) else pd.Timestamp(raw_last_deal_date).isoformat()
        )
        raw_mno, raw_sno = record.get("mno"), record.get("sno")
        yield {
            "_index": ES_INDEX_NAME,
            "_id": _make_doc_id(record["sgg_cd"], record["dong_cd"], record["apt_name"]),
            "_source": {
                "sgg_cd": record["sgg_cd"],
                "sgg_nm": record["sgg_nm"],
                "dong_cd": record["dong_cd"],
                "dong_nm": record["dong_nm"],
                "apt_name": record["apt_name"],
                "last_deal_date": last_deal_date,
                # 검색(apt_name)에는 관여하지 않는 부가 정보 - keyword로만 저장한다.
                "mno": None if pd.isna(raw_mno) else raw_mno,
                "sno": None if pd.isna(raw_sno) else raw_sno,
            },
        }


def _bulk_index(client: Elasticsearch, df: pd.DataFrame) -> int:
    if df.empty:
        logger.info("색인할 apt_name 데이터가 없습니다.")
        return 0
    success_count, errors = bulk(
        client, _build_actions(df), chunk_size=CHUNK_SIZE, raise_on_error=True
    )
    if errors:
        logger.warning("Elasticsearch bulk 색인 중 일부 에러 발생: %s", errors)
    logger.info("Elasticsearch bulk 색인 완료: %d건", success_count)
    return success_count


# =====================================================================================
# 6. 메인 진입점 (Airflow PythonOperator/@task가 직접 임포트해서 호출)
# =====================================================================================
def run_apt_name_es_pipeline() -> dict:
    """S3 Lake의 dim_apartment/fact_apt_transactions를 읽어 Elasticsearch apt_name
    인덱스에 색인한다. 매번 전체 재계산하는 멱등적(idempotent) 배치라 몇 번을 다시 돌려도
    안전하다."""
    lake_bucket = os.environ.get("LAKE")
    if not lake_bucket:
        raise RuntimeError("LAKE 버킷 환경변수가 설정되어 있지 않습니다.")

    client = _resolve_es_client()
    _ensure_index(client)

    dim_df = _read_dim_apartment(lake_bucket)
    fact_df = _read_last_deal_dates(lake_bucket)
    mno_sno_df = _read_representative_mno_sno(lake_bucket)

    merged_df = (
        dim_df
        .merge(fact_df, on=["sgg_cd", "dong_cd", "apt_name"], how="left")
        .merge(mno_sno_df, on=["sgg_cd", "dong_cd", "apt_name"], how="left")
    )

    indexed_count = _bulk_index(client, merged_df)

    result = {
        "dim_apartment_count": len(dim_df),
        "matched_last_deal_count": len(fact_df),
        "matched_mno_sno_count": len(mno_sno_df),
        "indexed_count": indexed_count,
    }
    logger.info("apt_name Elasticsearch 색인 파이프라인 완료: %s", result)
    return result


if __name__ == "__main__":
    run_apt_name_es_pipeline()


# =====================================================================================
# Airflow 태스크 스니펫 (기존 DAG 파일에 추가할 때 참고용 - 이 파일에서는 실행되지 않음)
#
# --- PythonOperator (권장 - 별도 프로세스 기동 오버헤드 없이 같은 워커에서 실행,
#     반환값이 자동으로 XCom에 실린다) ---
#
#   from airflow.operators.python import PythonOperator
#   from transformation.gold.pipeline_apt_name import run_apt_name_es_pipeline
#
#   task_index_apt_name_es = PythonOperator(
#       task_id="index_apt_name_es",
#       python_callable=run_apt_name_es_pipeline,
#   )
#
#   # 기존 TaskFlow(@dag/@task) 스타일 DAG(data_orchestration.py)라면 @task로도 동일하게
#   # 감쌀 수 있다:
#   #
#   #   @task
#   #   def task_index_apt_name_es(dummy_input=None) -> dict:
#   #       return run_apt_name_es_pipeline()
#
# 이 태스크는 dim_apartment/fact_apt_transactions Silver 적재(task_transform_silver_
# real_estate) 이후에 실행되도록 의존성을 걸면 된다 (MySQL 마스터 테이블에 더 이상
# 의존하지 않으므로 region_master.py 선행 실행은 필요 없다):
#
#   silver_result = task_transform_silver_real_estate(table_name)
#   task_index_apt_name_es(silver_result)
# =====================================================================================

# 공공데이터포털 원본(raw) 수집 파이프라인 공통 함수
# ProductInfo_raw.py, StoreInfo_raw.py 등 개별 API 수집 모듈에서 공통으로 사용한다.

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xml.etree.ElementTree as ET
from duckdb import DuckDBPyConnection

# config.paths를 import하면 그 안에서 env/.env를 자동으로 로드한다 (KEY, RAW 등).
# raw_pipeline을 쓰는 모든 ingestion 모듈이 이 import 하나로 .env를 보장받는다.
from config import paths as _paths  # noqa: F401

# DuckDB <-> MinIO(S3) 연결 설정은 connect.py 한 곳에서만 관리한다.
from utils.connect import configure_minio, get_duckdb_connect


def ensure_connection(con: DuckDBPyConnection | None) -> DuckDBPyConnection:
    """
    S3(MinIO)에 접근하는 모든 DuckDB 작업 앞에서 호출하는 공통 함수.
      - con이 None이면 connect.py로 새 연결을 만들고 S3 설정까지 적용해서 반환한다.
      - con이 이미 있으면 "이미 설정돼 있겠지"라고 가정하지 않고, 그 위에 S3 설정을 한 번 더
        재적용한다 (idempotent라 여러 번 실행해도 안전). 호출자가 S3 설정이 안 된 con을
        넘기는 실수를 여기서 막기 위함이다.
    """
    if con is None:
        return get_duckdb_connect()
    return configure_minio(con)


def default_raw_path(sub_dir: str, file_name: str) -> str:
    """RAW 버킷 기준 고정 저장 경로 생성: s3://{RAW}/{sub_dir}/{file_name}"""
    return f"s3://{os.environ.get('RAW')}/{sub_dir}/{file_name}"


def create_session_with_retry() -> requests.Session:
    """API 호출이 실패해도 1초 -> 2초 -> 4초 간격으로 최대 3번 재시도하는 세션"""
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retry))  # pyrefly: ignore[bad-argument-type]
    session.mount("https://", HTTPAdapter(max_retries=retry))  # pyrefly: ignore[bad-argument-type]
    return session


def download_xml(url: str, params: dict) -> str:
    """지정한 URL을 재시도 세션으로 호출해서 XML 응답을 문자열로 반환.
    requests의 params= 가 서비스키를 재인코딩하지 않도록, 쿼리스트링을 직접 조립해서 완성된 URL로 요청한다."""
    query = "&".join(f"{key}={value}" for key, value in params.items())
    full_url = f"{url}?{query}"
    session = create_session_with_retry()
    response = session.get(full_url, timeout=30)
    response.raise_for_status()
    return response.text


def parse_items_xml(xml_text: str, operation_name: str) -> list[dict]:
    """공공데이터포털 공통 응답(XML)의 item 태그들을 dict 리스트로 변환 (필드 가공 없이 태그명 = 컬럼명)"""
    root = ET.fromstring(xml_text)  # 루트 태그: <response>

    result_code = root.findtext("resultCode")
    if result_code != "00":
        result_msg = root.findtext("resultMsg")
        raise RuntimeError(f"{operation_name} 실패: resultCode={result_code}, resultMsg={result_msg}")

    rows = []
    for item in root.findall(".//item"):
        row = {}
        for field in item:
            row[field.tag] = field.text
        rows.append(row)
    return rows


def get_raw_columns(
    con: DuckDBPyConnection | None,
    raw_path: str,
    pk_col: str,
) -> list[str]:
    """raw parquet의 컬럼 목록을 가져오고, 기본키 컬럼이 있는지 확인. con은 ensure_connection()을 통해 항상 S3 설정이 적용된 상태로 사용한다."""
    con = ensure_connection(con)
    columns = con.sql(f"SELECT * FROM read_parquet('{raw_path}')").columns
    if pk_col not in columns:
        raise RuntimeError(f"raw 데이터에 기본키 컬럼 '{pk_col}'이 없습니다: {columns}")
    return columns


def create_table_if_not_exists(
    con: DuckDBPyConnection | None,
    table_name: str,
    columns: list[str],
    pk_col: str,
) -> None:
    """최초 실행 시에만 테이블 생성 (모든 컬럼 VARCHAR + created_at/updated_at + pk_col 기본키). con은 ensure_connection()을 통해 항상 S3 설정이 적용된 상태로 사용한다."""
    con = ensure_connection(con)
    columns_ddl = ", ".join(f'"{c}" VARCHAR' for c in columns)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_ddl},
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY ("{pk_col}")
        )
    """)


def upsert_from_parquet(
    con: DuckDBPyConnection | None,
    table_name: str,
    raw_path: str,
    other_cols: list[str],
    pk_col: str,
) -> None:
    """
    pk_col 기준으로 upsert 실행. con은 ensure_connection()을 통해 항상 S3 설정이 적용된 상태로 사용한다.
      - 신규 pk_col          -> INSERT, created_at/updated_at 모두 now()
      - 기존 pk_col + 값 변경  -> UPDATE, updated_at만 now()로 갱신 (created_at은 그대로 유지)
      - 기존 pk_col + 값 동일  -> WHERE 조건에서 걸러져서 아무 것도 하지 않음
    """
    con = ensure_connection(con)
    set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in other_cols)
    changed_clause = " OR ".join(
        f'{table_name}."{c}" IS DISTINCT FROM EXCLUDED."{c}"' for c in other_cols
    )

    con.execute(f"""
        INSERT INTO {table_name}
        SELECT *, now() AS created_at, now() AS updated_at
        FROM read_parquet('{raw_path}')
        ON CONFLICT ("{pk_col}") DO UPDATE SET
            {set_clause},
            "updated_at" = now()
        WHERE {changed_clause}
    """)

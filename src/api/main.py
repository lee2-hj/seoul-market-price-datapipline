# -*- coding: utf-8 -*-
"""아파트 Gold 마트(dm_apt_pyeong_price / dm_apt_flr_price) 최신 파티션 조회 API.

MinIO(S3 호환) LAKE 버킷의 mart/{마트명}/base_date=YYYY-MM-DD/ 파티션 중 가장 최신
날짜를 자동으로 찾아 그 안의 Parquet 파일을 읽고, 자치구/법정동으로 필터링해 그대로
반환한다. build_dong_pyeong_mart.py가 Spark로 저장한 마트라 파티션 하나가 여러 개의
part-*.parquet 파일로 나뉘어 있을 수 있어, 그 파일들을 모두 읽어 합친다.

실행 방법 (프로젝트 루트에서, 가상환경 활성화 후):
    uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
"""

import json
import re
from enum import Enum
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query
from pydantic_settings import BaseSettings, SettingsConfigDict

import boto3

# =====================================================================================
# 1. 환경 변수 로드 (env/.env - 이 프로젝트의 공통 환경변수 파일 위치)
# =====================================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / "env" / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")

    S3_END_POINT: str
    LAKE: str
    S3_ACCESS_KEY: str
    S3_SECRET_KEY: str


settings = Settings()


def get_s3_client():
    """MinIO(S3 호환) 접속용 boto3 클라이언트. 프로젝트의 다른 S3A/Iceberg 설정과
    동일하게 path-style 주소 방식을 쓴다. S3_END_POINT는 로컬(MinIO, 스킴 없이
    "host:port")과 배포(HTTPS 스킴 포함, 예: "https://storage.googleapis.com") 양쪽
    형식을 그대로 받으므로, 엔드포인트에 붙일 스킴은 그 안에 이미 https://가 있는지로
    판단한다 - 하드코딩하면 배포 시 "http://https://..." 같은 깨진 URL이 만들어진다."""
    raw_endpoint = settings.S3_END_POINT.strip()
    use_ssl = raw_endpoint.lower().startswith("https://")
    endpoint_clean = raw_endpoint.replace("https://", "").replace("http://", "")
    return boto3.client(
        "s3",
        endpoint_url=f"{'https' if use_ssl else 'http'}://{endpoint_clean}",
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


# =====================================================================================
# 2. query_type -> 마트 prefix 매핑
#    (LAKE 버킷 안의 실제 저장 경로: mart/{마트명}/base_date=YYYY-MM-DD/,
#     build_dong_pyeong_mart.py의 MART_PATHS와 동일한 규칙)
# =====================================================================================
class QueryType(str, Enum):
    PYEONG = "pyeong"
    FLOOR = "floor"


MART_PREFIX_BY_TYPE: dict[QueryType, str] = {
    QueryType.PYEONG: "mart/dm_apt_pyeong_price/",
    QueryType.FLOOR: "mart/dm_apt_flr_price/",
}

_BASE_DATE_RE = re.compile(r"^base_date=(\d{4}-\d{2}-\d{2})/$")


# =====================================================================================
# 3. S3 경로 탐색 (최신 base_date 파티션 자동 식별) + 데이터 로드
# =====================================================================================
def find_latest_partition_prefix(s3_client, bucket: str, mart_prefix: str) -> str:
    """mart_prefix(예: mart/dm_apt_pyeong_price/) 아래의 base_date=YYYY-MM-DD/
    "폴더" 목록을 Delimiter="/"로 훑어서, 날짜 문자열이 가장 큰(=최신) 파티션의
    전체 prefix를 돌려준다."""
    paginator = s3_client.get_paginator("list_objects_v2")
    latest_date: str | None = None

    for page in paginator.paginate(Bucket=bucket, Prefix=mart_prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            prefix = common_prefix["Prefix"]
            suffix = prefix[len(mart_prefix):]
            match = _BASE_DATE_RE.match(suffix)
            if not match:
                continue
            date_str = match.group(1)
            if latest_date is None or date_str > latest_date:
                latest_date = date_str

    if latest_date is None:
        raise HTTPException(
            status_code=404,
            detail=f"'{mart_prefix}' 아래에 base_date 파티션이 존재하지 않습니다.",
        )

    return f"{mart_prefix}base_date={latest_date}/"


def read_partition_records(s3_client, bucket: str, partition_prefix: str) -> list[dict]:
    """partition_prefix 안의 모든 *.parquet 파일(Spark가 여러 part 파일로 나눠 저장했을
    수 있음)을 읽어 하나로 합친 뒤, 가공 없이 레코드(dict) 리스트로 변환한다."""
    paginator = s3_client.get_paginator("list_objects_v2")
    parquet_keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=partition_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                parquet_keys.append(key)

    if not parquet_keys:
        raise HTTPException(
            status_code=404,
            detail=f"파티션에 Parquet 파일이 없습니다: {partition_prefix}",
        )

    tables = []
    for key in parquet_keys:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        tables.append(pq.read_table(BytesIO(body)))

    combined_df = pa.concat_tables(tables).to_pandas()

    # numpy/Timestamp 스칼라 타입이 그대로 남아 있으면 JSON 직렬화 시 에러가 날 수 있어,
    # pandas의 to_json(날짜는 ISO 포맷)을 거쳐 순수 파이썬 타입으로 변환한다.
    return json.loads(combined_df.to_json(orient="records", date_format="iso"))


# =====================================================================================
# 4. FastAPI 앱 + 엔드포인트
# =====================================================================================
app = FastAPI(title="Apartment Mart API")


@app.get("/api/apartments/compare")
def compare_apartments(
    cgg_nm: str = Query(..., description="자치구명 (예: 강남구)"),
    stdg_nm: str = Query(..., description="법정동명 (예: 역삼동)"),
    query_type: QueryType = Query(..., description="조회 타입 (pyeong: 평형별, floor: 층수별)"),
):
    bucket = settings.LAKE
    mart_prefix = MART_PREFIX_BY_TYPE[query_type]

    try:
        s3_client = get_s3_client()
        partition_prefix = find_latest_partition_prefix(s3_client, bucket, mart_prefix)
        records = read_partition_records(s3_client, bucket, partition_prefix)
    except HTTPException:
        raise
    except (BotoCoreError, ClientError) as exc:
        raise HTTPException(
            status_code=500, detail=f"S3(MinIO) 조회 중 오류가 발생했습니다: {exc}"
        ) from exc

    filtered = [
        record for record in records
        if record.get("cgg_nm") == cgg_nm and record.get("stdg_nm") == stdg_nm
    ]

    if not filtered:
        raise HTTPException(
            status_code=404,
            detail=(
                f"조건에 맞는 데이터가 없습니다 "
                f"(cgg_nm={cgg_nm}, stdg_nm={stdg_nm}, query_type={query_type.value})"
            ),
        )

    return filtered

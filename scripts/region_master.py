# 자치구(sgg) 및 행정동(dong) 기준 정보(위경도 포함)를 MySQL에 upsert하는 스크립트.
#
# 소스는 사람이 손으로 관리하는 CSV가 아니라, Real_Estate_Transform.py가 이미 Iceberg
# Silver 레이어에 적재해 둔 dim_apartment 테이블(MinIO LAKE 버킷)이다. 거기서 실제로
# 존재하는 sgg_cd/sgg_nm, dong_cd/dong_nm 조합을 뽑아 카카오 API로 좌표를 구하고
# MySQL에 upsert한다.
#
# dim_apartment는 원천 데이터가 쌓일수록 커버되는 자치구/행정동이 늘어날 수 있으므로,
# 이 스크립트는 여러 번 다시 실행해도 안전(idempotent)하도록 설계했다:
#   - 이미 좌표를 구해둔 sgg_cd/dong_cd는 카카오 API를 다시 부르지 않고 캐시를 재사용한다.
#   - MySQL에는 INSERT ... ON DUPLICATE KEY UPDATE로 적재해서, 값이 실제로 달라진 행만
#     UPDATE되고 updated_at이 갱신된다 (완전히 같은 값이면 MySQL이 자체적으로 무갱신 처리
#     -> updated_at도 그대로 유지됨. 실제 MySQL로 검증된 동작).
#
# [중요] dim_apartment의 dong_cd(원천 STDG_CD)는 자치구 내부에서만 유일한 5자리 코드다
# (실제 데이터로 확인: dong_cd=10100 하나가 22개 자치구의 서로 다른 동을 가리킴). 그대로
# UNIQUE 키로 쓰면 다른 자치구의 동끼리 충돌하므로, sgg_cd(5자리)+dong_cd(5자리)를 이어붙인
# 표준 10자리 법정동코드를 만들어 tb_dong_master.dong_cd에 저장한다.
#
# 실행 방법 (프로젝트 루트에서, 가상환경 활성화 후):
#   python scripts/region_master.py
#
# 준비물: env/.env 에 S3_END_POINT / S3_ACCESS_KEY / S3_SECRET_KEY / LAKE,
#         DATABASE_URL, KAKAO_MAP_REST_API_KEY (카카오맵/로컬 API가 활성화된 앱의 키)

import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKeyConstraint,
    MetaData,
    Numeric,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.sql import func

from utils.connect import get_duckdb_connect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------------
# 경로 설정 (스크립트 위치 기준으로 프로젝트 루트를 계산해, 실행 위치와 무관하게 동작)
# -----------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / "env" / ".env"

EXTERNAL_DIR = PROJECT_ROOT / "data" / "external"
SGG_COORDS_CSV_PATH = EXTERNAL_DIR / "tb_sgg_master_with_coords.csv"
DONG_COORDS_CSV_PATH = EXTERNAL_DIR / "tb_dong_master_with_coords.csv"

# LAKE 버킷 기준 상대 경로 (dim_apartment는 날짜 파티션이 없는 단일 Iceberg 테이블이라
# 데이터 파일이 dim_apartment/data/ 밑에 바로 있다)
DIM_APARTMENT_RELATIVE_GLOB = "dim_apartment/data/*.parquet"

KAKAO_GEOCODE_URL = "https://dapi.kakao.com/v2/local/search/address.json"
KAKAO_REQUEST_INTERVAL_SEC = 0.1  # 카카오 API 호출 제한 대비 최소한의 간격


# -----------------------------------------------------------------------------------
# MySQL DDL (SQLAlchemy Core): tb_user(my_gu, my_dong)가 sgg_cd/dong_cd를 FK로 참조할
# 수 있도록 두 컬럼에 UNIQUE 제약을 명시하고, tb_dong_master.sgg_id -> tb_sgg_master.id
# 참조에 이름 있는 FK 제약(fk_tb_dong_tb_sgg, ON DELETE CASCADE)을 건다.
# updated_at은 "ON UPDATE CURRENT_TIMESTAMP"까지 명시해야 값이 실제로 바뀔 때만 MySQL이
# 자동으로 갱신해준다 (SQLAlchemy Core에 전용 API가 없어 server_default에 원시 SQL 사용).
# 별도의 CREATE INDEX 구문은 두지 않는다 (UNIQUE/PK/FK로만 제약).
# -----------------------------------------------------------------------------------
metadata = MetaData()

tb_sgg_master = Table(
    "tb_sgg_master",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("sgg_cd", String(10), nullable=False, unique=True),
    Column("sgg_nm", String(50), nullable=False),
    Column("center_lat", Numeric(10, 7), nullable=False),
    Column("center_lng", Numeric(10, 7), nullable=False),
    Column("created_at", DateTime, server_default=func.current_timestamp()),
    Column("updated_at", DateTime, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")),
    mysql_engine="InnoDB",
)

tb_dong_master = Table(
    "tb_dong_master",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("sgg_id", BigInteger, nullable=False),
    Column("sgg_cd", String(10), nullable=False),
    Column("dong_cd", String(10), nullable=False, unique=True),
    Column("dong_nm", String(50), nullable=False),
    Column("center_lat", Numeric(10, 7), nullable=False),
    Column("center_lng", Numeric(10, 7), nullable=False),
    Column("created_at", DateTime, server_default=func.current_timestamp()),
    Column("updated_at", DateTime, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")),
    ForeignKeyConstraint(
        ["sgg_id"], ["tb_sgg_master.id"],
        name="fk_tb_dong_tb_sgg", ondelete="CASCADE",
    ),
    mysql_engine="InnoDB",
)


def _ensure_updated_at_column(engine, table_name: str) -> None:
    """create_all()은 이미 존재하는 테이블을 건드리지 않으므로, 예전 스키마(updated_at
    없음)로 이미 만들어져 있는 테이블에는 여기서 컬럼을 추가해준다."""
    with engine.begin() as conn:
        column_exists = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema = DATABASE() AND table_name = :t AND column_name = 'updated_at'"
            ),
            {"t": table_name},
        ).scalar()
        if not column_exists:
            logger.info("%s에 updated_at 컬럼이 없어 추가합니다.", table_name)
            conn.execute(text(
                f"ALTER TABLE {table_name} ADD COLUMN updated_at DATETIME "
                "DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
            ))


class GeocodeNotFoundError(Exception):
    """카카오 지오코딩 결과가 없는 경우 (주소 자체가 없거나 원천 데이터의 sgg/dong
    조합이 잘못된 경우). API/네트워크/인증 문제와 달리 이 행 하나만의 문제이므로
    호출부에서 건너뛰고 계속 진행할 수 있도록 별도 예외로 구분한다."""


def geocode_address(query: str, api_key: str) -> tuple[float, float]:
    """카카오 로컬 API(주소 검색)로 query를 지오코딩해 (center_lat, center_lng)를 반환한다."""
    response = requests.get(
        KAKAO_GEOCODE_URL,
        headers={"Authorization": f"KakaoAK {api_key}"},
        params={"query": query},
        timeout=10,
    )
    if not response.ok:
        # raise_for_status()는 상태 코드만 보여주고 카카오가 돌려주는 실제 에러 사유
        # (errorType/message, 예: 서비스 미활성화, IP 제한 등)는 숨겨버려서 원인 파악이
        # 안 되므로, 응답 본문을 그대로 포함해서 예외를 던진다. 이런 실패는 이후 모든
        # 호출에도 똑같이 나는 시스템적 문제라 즉시 중단한다(GeocodeNotFoundError와 구분).
        raise RuntimeError(
            f"카카오 지오코딩 API 호출 실패: HTTP {response.status_code}, "
            f"query='{query}', response={response.text}"
        )
    documents = response.json().get("documents", [])
    if not documents:
        raise GeocodeNotFoundError(f"카카오 지오코딩 결과가 없습니다: '{query}'")

    # x = 경도(longitude) -> center_lng, y = 위도(latitude) -> center_lat
    doc = documents[0]
    center_lat = float(doc["y"])
    center_lng = float(doc["x"])
    return center_lat, center_lng


def _geocode_dataframe(df: pd.DataFrame, build_query, api_key: str) -> pd.DataFrame:
    """df의 각 행마다 build_query(row)로 만든 질의를 지오코딩해 center_lat/center_lng
    컬럼을 추가한 새 DataFrame을 반환한다. 결과가 없는 행(GeocodeNotFoundError)은
    원천 데이터 오류일 가능성이 높으므로 경고만 남기고 결과에서 제외한다(전체를
    막지 않음). API 호출 자체의 실패(인증/네트워크 등)는 그대로 전파해 즉시 중단한다."""
    matched_rows = []
    lats: list[float] = []
    lngs: list[float] = []
    not_found: list[dict] = []
    for _, row in df.iterrows():
        query = build_query(row)
        try:
            lat, lng = geocode_address(query, api_key)
        except GeocodeNotFoundError:
            not_found.append(row.to_dict())
            logger.warning("지오코딩 결과 없음, 이 행은 제외합니다: query='%s', row=%s", query, row.to_dict())
            time.sleep(KAKAO_REQUEST_INTERVAL_SEC)
            continue
        matched_rows.append(row)
        lats.append(lat)
        lngs.append(lng)
        logger.info("지오코딩 완료: '%s' -> (%.7f, %.7f)", query, lat, lng)
        time.sleep(KAKAO_REQUEST_INTERVAL_SEC)

    if not_found:
        logger.warning("지오코딩 결과가 없어 이번 배치에서 제외된 행: %d건", len(not_found))

    result = pd.DataFrame(matched_rows, columns=df.columns).reset_index(drop=True)
    result["center_lat"] = lats
    result["center_lng"] = lngs
    return result


def _drop_incomplete_rows(df: pd.DataFrame, required_cols: list[str], label: str) -> pd.DataFrame:
    """required_cols 중 하나라도 비어 있는(null 또는 빈 문자열) 행을 제외한다.
    dim_apartment는 원천 데이터 품질 이슈(예: sgg_cd만 있고 sgg_nm이 빈 값인 행)가
    섞여 들어올 수 있어, 이런 행 때문에 전체 지오코딩이 멈추지 않도록 걸러내고 경고만 남긴다."""
    is_complete = pd.Series(True, index=df.index)
    for col in required_cols:
        is_complete &= df[col].notna() & (df[col].astype(str).str.strip() != "")

    incomplete = df[~is_complete]
    if not incomplete.empty:
        logger.warning(
            "%s: %s 중 비어 있는 값이 있는 행 %d건을 제외합니다: %s",
            label, required_cols, len(incomplete), incomplete.to_dict(orient="records"),
        )
    return df[is_complete].reset_index(drop=True)


def _read_dim_apartment(lake_bucket: str) -> pd.DataFrame:
    """MinIO Silver 레이어(LAKE 버킷)의 dim_apartment에서 실제로 존재하는
    sgg_cd/sgg_nm/dong_cd/dong_nm 조합을 읽어온다 (duckdb httpfs로 parquet 직접 조회)."""
    con = get_duckdb_connect()
    s3_path = f"s3://{lake_bucket}/{DIM_APARTMENT_RELATIVE_GLOB}"
    logger.info("dim_apartment 원천 조회: %s", s3_path)
    df = con.execute(
        f"SELECT DISTINCT sgg_cd, sgg_nm, dong_cd, dong_nm FROM read_parquet('{s3_path}')"
    ).df()
    con.close()
    if df.empty:
        raise RuntimeError(f"dim_apartment에서 읽어온 데이터가 없습니다: {s3_path}")
    logger.info("dim_apartment에서 자치구/행정동 조합 %d건 발견", len(df))
    return df


def _geocode_incremental(
    source_df: pd.DataFrame, key_col: str, cache_path: Path, build_query, api_key: str
) -> pd.DataFrame:
    """source_df(key_col + 최신 이름 컬럼들)에 좌표를 채워 반환한다.
    cache_path에 이미 좌표가 있는 key는 카카오 API를 다시 부르지 않고 재사용하고,
    처음 보는 key만 새로 지오코딩해서 캐시에 추가 저장한다 (이름은 항상 source_df의
    최신 값을 쓰고, 좌표만 캐시에서 가져오거나 새로 구한다)."""
    if cache_path.exists():
        cache_df = pd.read_csv(cache_path, dtype={key_col: str})
    else:
        cache_df = pd.DataFrame(columns=[key_col, "center_lat", "center_lng"])

    known_keys = set(cache_df[key_col])
    new_rows = source_df[~source_df[key_col].isin(known_keys)].drop_duplicates(subset=[key_col])

    if not new_rows.empty:
        logger.info("신규 지오코딩 대상 %d건: %s", len(new_rows), sorted(new_rows[key_col]))
        geocoded_new = _geocode_dataframe(new_rows, build_query=build_query, api_key=api_key)
        cache_df = pd.concat(
            [cache_df, geocoded_new[[key_col, "center_lat", "center_lng"]]],
            ignore_index=True,
        )
        cache_df.to_csv(cache_path, index=False)
        logger.info("좌표 캐시 갱신 완료: %s (누적 %d건)", cache_path, len(cache_df))
    else:
        logger.info("신규 지오코딩 대상 없음(전부 캐시 재사용, 카카오 API 호출 없음): %s", cache_path)

    merged = source_df.merge(cache_df[[key_col, "center_lat", "center_lng"]], on=key_col, how="left")
    still_missing = merged[merged["center_lat"].isna()]
    if not still_missing.empty:
        # 지오코딩 결과가 끝내 없는 경우(카카오에 없는 주소 조합 등)는 원천 데이터
        # 오류일 가능성이 높다. 이 행들만 제외하고 나머지는 정상 진행한다.
        logger.warning(
            "좌표를 구하지 못해 이번 실행에서 제외되는 %s: %d건 %s",
            key_col, len(still_missing), sorted(still_missing[key_col].unique()),
        )
        merged = merged[merged["center_lat"].notna()].reset_index(drop=True)
    return merged


def _upsert_dataframe(engine, table: Table, df: pd.DataFrame, update_cols: list[str]) -> None:
    """INSERT ... ON DUPLICATE KEY UPDATE로 upsert한다. 값이 기존과 완전히 같은 행은
    MySQL이 자체적으로 무갱신 처리하므로 updated_at도 그대로 유지된다."""
    if df.empty:
        logger.info("upsert할 데이터가 없습니다: %s", table.name)
        return

    records = df.to_dict(orient="records")
    stmt = mysql_insert(table).values(records)
    update_map = {col: stmt.inserted[col] for col in update_cols}
    stmt = stmt.on_duplicate_key_update(**update_map)

    with engine.begin() as conn:
        result = conn.execute(stmt)
    logger.info(
        "%s upsert 완료: %d건 제출 (MySQL 보고 rowcount=%d; 신규는 매치당 1, 실제 값이 "
        "바뀐 행만 매치당 2, 값이 완전히 같으면 0으로 집계됨)",
        table.name, len(records), result.rowcount,
    )


def main() -> None:
    load_dotenv(dotenv_path=ENV_PATH)
    database_url = os.environ.get("DATABASE_URL")
    kakao_api_key = os.environ.get("KAKAO_MAP_REST_API_KEY")
    lake_bucket = os.environ.get("LAKE")
    if not database_url:
        raise RuntimeError(f"DATABASE_URL이 설정되어 있지 않습니다: {ENV_PATH}")
    if not kakao_api_key:
        raise RuntimeError(f"KAKAO_MAP_REST_API_KEY가 설정되어 있지 않습니다: {ENV_PATH}")
    if not lake_bucket:
        raise RuntimeError(f"LAKE가 설정되어 있지 않습니다: {ENV_PATH}")

    engine = create_engine(database_url, pool_pre_ping=True)

    logger.info("DDL 생성 시작: tb_sgg_master, tb_dong_master")
    metadata.create_all(engine)
    _ensure_updated_at_column(engine, "tb_sgg_master")
    _ensure_updated_at_column(engine, "tb_dong_master")
    logger.info("DDL 준비 완료")

    dim_df = _read_dim_apartment(lake_bucket)
    dim_df["sgg_cd"] = dim_df["sgg_cd"].astype(str).str.zfill(5)

    sgg_source = dim_df[["sgg_cd", "sgg_nm"]].drop_duplicates(subset=["sgg_cd"]).reset_index(drop=True)
    sgg_source = _drop_incomplete_rows(sgg_source, ["sgg_cd", "sgg_nm"], "sgg_source")
    sgg_df = _geocode_incremental(
        sgg_source,
        key_col="sgg_cd",
        cache_path=SGG_COORDS_CSV_PATH,
        build_query=lambda row: f"서울특별시 {row['sgg_nm']}",
        api_key=kakao_api_key,
    )

    sgg_nm_by_cd = dict(zip(sgg_df["sgg_cd"], sgg_df["sgg_nm"]))

    def build_dong_query(row: pd.Series) -> str:
        sgg_nm = sgg_nm_by_cd.get(row["sgg_cd"])
        if sgg_nm is None:
            raise RuntimeError(
                f"dim_apartment의 sgg_cd '{row['sgg_cd']}'에 대응하는 자치구명을 찾을 수 없습니다."
            )
        return f"서울특별시 {sgg_nm} {row['dong_nm']}"

    # dim_apartment의 dong_cd(원천 STDG_CD)는 자치구 안에서만 유일한 5자리 코드라
    # (예: dong_cd=10100이 22개 자치구에서 서로 다른 동을 가리킴), 그대로 UNIQUE 키로
    # 쓰면 서로 다른 자치구의 동이 충돌한다. sgg_cd(5자리)+dong_cd(5자리)를 이어붙인
    # 표준 10자리 법정동코드를 만들어 저장한다 (tb_user.my_dong도 이 10자리 형식이어야 함).
    dong_source = dim_df[["sgg_cd", "dong_cd", "dong_nm"]].copy()
    dong_source = _drop_incomplete_rows(dong_source, ["sgg_cd", "dong_cd", "dong_nm"], "dong_source")
    dong_source["dong_cd"] = dong_source["sgg_cd"] + dong_source["dong_cd"].astype(str).str.zfill(5)
    dong_source = dong_source.drop_duplicates(subset=["dong_cd"]).reset_index(drop=True)
    # dong 자신의 컬럼은 다 채워져 있어도, 그 sgg_cd가 위에서 sgg_source 필터링으로
    # 제외됐을 수 있다 (예: sgg_nm이 비어 있던 sgg_cd). 그런 고아 행도 걸러낸다.
    orphan_mask = ~dong_source["sgg_cd"].isin(sgg_nm_by_cd)
    if orphan_mask.any():
        logger.warning(
            "dong_source: 유효한 자치구 정보가 없는 sgg_cd를 참조하는 행 %d건을 제외합니다: %s",
            orphan_mask.sum(), dong_source[orphan_mask].to_dict(orient="records"),
        )
    dong_source = dong_source[~orphan_mask].reset_index(drop=True)
    dong_df = _geocode_incremental(
        dong_source,
        key_col="dong_cd",
        cache_path=DONG_COORDS_CSV_PATH,
        build_query=build_dong_query,
        api_key=kakao_api_key,
    )

    _upsert_dataframe(
        engine,
        tb_sgg_master,
        sgg_df[["sgg_cd", "sgg_nm", "center_lat", "center_lng"]],
        update_cols=["sgg_nm", "center_lat", "center_lng"],
    )

    # 방금 upsert한 tb_sgg_master의 (id, sgg_cd)를 조회해 dong_df에 FK 값(sgg_id)을 매핑한다.
    sgg_id_map = pd.read_sql("SELECT id AS sgg_id, sgg_cd FROM tb_sgg_master", engine)
    dong_df = dong_df.merge(sgg_id_map, on="sgg_cd", how="left")

    missing = dong_df[dong_df["sgg_id"].isna()]
    if not missing.empty:
        missing_codes = sorted(missing["sgg_cd"].unique())
        raise RuntimeError(f"tb_sgg_master에 없는 sgg_cd가 dim_apartment에 존재합니다: {missing_codes}")
    dong_df["sgg_id"] = dong_df["sgg_id"].astype("int64")

    _upsert_dataframe(
        engine,
        tb_dong_master,
        dong_df[["sgg_id", "sgg_cd", "dong_cd", "dong_nm", "center_lat", "center_lng"]],
        update_cols=["dong_nm", "center_lat", "center_lng"],
    )

    logger.info("자치구/행정동 기준 정보 upsert가 모두 완료되었습니다.")


if __name__ == "__main__":
    main()

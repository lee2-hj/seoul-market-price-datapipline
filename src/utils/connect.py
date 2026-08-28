import duckdb, os
from duckdb import DuckDBPyConnection

# 이미 있는 DuckDB 연결에 MinIO(S3) 접속 설정을 적용한다.
# SET/INSTALL은 몇 번을 다시 실행해도 안전(idempotent)하므로,
# "이 con이 이미 설정됐는지" 따로 확인하지 않고 항상 재적용해서 설정 누락을 원천 차단한다.
def configure_minio(con: DuckDBPyConnection) -> DuckDBPyConnection:
    ENDPOINT = os.environ.get("S3_END_POINT", "")
    ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
    SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
    
    use_ssl = ENDPOINT.startswith("https://")
    endpoint_clean = ENDPOINT.replace("https://", "").replace("http://", "")
    url_style = "vhost" if use_ssl else "path"
    
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{endpoint_clean}';
        SET s3_access_key_id='{ACCESS_KEY}';
        SET s3_secret_access_key='{SECRET_KEY}';
        SET s3_use_ssl={'true' if use_ssl else 'false'};
        SET s3_url_style='{url_style}';
    """)
    return con


#duckdb 서버 연결 함수
def get_duckdb_connect() -> DuckDBPyConnection:
    con = duckdb.connect() # duckdb 서버 연결
    return configure_minio(con) # s3 연결 설정을 적용한 뒤 반환
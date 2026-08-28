import os
import tomllib
from pathlib import Path
from dotenv import load_dotenv

# 프로젝트 루트 디렉터리 (src/config/paths.py 위치 기준: parents[2])
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# pyproject.toml 파일 경로
PYPROJECT_TOML = PROJECT_ROOT / "pyproject.toml"

# TOML 설정 로드
TOML_CONFIG = {}
if PYPROJECT_TOML.exists():
    with open(PYPROJECT_TOML, "rb") as f:
        TOML_CONFIG = tomllib.load(f)

# tool.dataengineer 설정 추출
APP_CONFIG = TOML_CONFIG.get("tool", {}).get("dataengineer", {})

# 주요 경로 정의 (TOML 설정 활용)
PATHS_CONFIG = APP_CONFIG.get("paths", {})
CONFIG_DIR = PROJECT_ROOT / PATHS_CONFIG.get("config_dir", "env")
DATA_DIR = PROJECT_ROOT / PATHS_CONFIG.get("data_dir", "data")
RAW_DATA_DIR = PROJECT_ROOT / PATHS_CONFIG.get("raw_data_dir", "data/raw")
PROCESSED_DATA_DIR = PROJECT_ROOT / PATHS_CONFIG.get("processed_data_dir", "data/processed")
LOGS_DIR = PROJECT_ROOT / PATHS_CONFIG.get("logs_dir", "logs")

SRC_DIR = PROJECT_ROOT / "src"
UTILS_DIR = SRC_DIR / "utils"
DAGS_DIR = PROJECT_ROOT / "airflow"

ENV_FILE = CONFIG_DIR / ".env"

# .env 파일 자동 로드
if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE, override=True)

# TOML 내 duckdb 및 API 설정 동기화
DUCKDB_CONFIG = APP_CONFIG.get("duckdb", {})
for key, env_var in [("s3_endpoint", "S3_END_POINT"), ("s3_access_key", "S3_ACCESS_KEY"), ("s3_secret_key", "S3_SECRET_KEY")]:
    if key in DUCKDB_CONFIG and not os.environ.get(env_var):
        os.environ[env_var] = str(DUCKDB_CONFIG[key])

SEOUL_API_CONFIG = APP_CONFIG.get("api", {}).get("seoul", {})
if SEOUL_API_CONFIG.get("key") and not os.environ.get("KEY"):
    os.environ["KEY"] = str(SEOUL_API_CONFIG["key"])

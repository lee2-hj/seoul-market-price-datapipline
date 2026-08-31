# 📄 paths.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 프로젝트 전역에서 공유하는 경로 상수 및 환경설정 부트스트랩 모듈. `pyproject.toml`의 `[tool.dataengineer]` 섹션을 읽어 `config_dir`/`data_dir`/`raw_data_dir`/`processed_data_dir`/`logs_dir` 등 주요 디렉터리 경로를 절대경로 상수로 정의하고, `.env` 파일을 자동으로 로드하며, TOML에 정의된 DuckDB S3 접속 정보와 서울시 API 키를 환경변수가 비어 있을 때만 보충해주는 역할을 한다. 이 모듈을 import하는 것만으로 프로젝트의 경로/환경변수 설정이 부수효과로 완료된다.
- **현재 구현 상태:** 완료. 다만 이 모듈은 import되는 순간 파일 I/O(`pyproject.toml`, `.env` 읽기)와 `os.environ` 변경이라는 부수효과(side effect)를 전역 스코프에서 즉시 실행한다는 특징이 있다 — 명시적 TODO 주석은 없으나, 이런 import-time 부수효과는 테스트 격리나 재로드 시 예기치 않은 동작을 유발할 수 있는 설계상 특징이다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음(다른 프로젝트 내부 모듈을 import하지 않는 최하위 설정 모듈).
- **외부 패키지:**
  - `python-dotenv` (`load_dotenv`) — `.env` 파일 로드(`override=True`로 기존 환경변수 덮어씀)
  - `tomllib` (표준 라이브러리, Python 3.11+) — `pyproject.toml` 파싱
  - 표준 라이브러리 `os`, `pathlib.Path`

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `PROJECT_ROOT` | 이 파일(`src/config/paths.py`) 위치 기준 `parents[2]`로 계산한 프로젝트 루트 절대경로 | - | `pathlib.Path` |
| `PYPROJECT_TOML` | `PROJECT_ROOT / "pyproject.toml"` 경로 | - | `pathlib.Path` |
| `TOML_CONFIG` | `pyproject.toml`이 존재하면 `tomllib.load`로 파싱한 전체 딕셔너리, 없으면 빈 딕셔너리 | - | `dict` |
| `APP_CONFIG` | `TOML_CONFIG["tool"]["dataengineer"]` (없으면 빈 딕셔너리) | - | `dict` |
| `PATHS_CONFIG` | `APP_CONFIG["paths"]` (없으면 빈 딕셔너리) | - | `dict` |
| `CONFIG_DIR`, `DATA_DIR`, `RAW_DATA_DIR`, `PROCESSED_DATA_DIR`, `LOGS_DIR` | `PATHS_CONFIG`의 각 키(`config_dir`, `data_dir`, `raw_data_dir`, `processed_data_dir`, `logs_dir`) 값을 `PROJECT_ROOT`에 결합, 각각 기본값 `"env"`, `"data"`, `"data/raw"`, `"data/processed"`, `"logs"` | - | `pathlib.Path` 각각 |
| `SRC_DIR`, `UTILS_DIR`, `DAGS_DIR` | `PROJECT_ROOT/src`, `SRC_DIR/utils`, `PROJECT_ROOT/airflow`로 고정 계산(TOML 설정과 무관) | - | `pathlib.Path` 각각 |
| `ENV_FILE` | `CONFIG_DIR / ".env"` | - | `pathlib.Path` |
| (모듈 최상위 실행 코드) | `ENV_FILE`이 존재하면 `load_dotenv(dotenv_path=ENV_FILE, override=True)` 호출 | - | 부수효과(환경변수 로드) |
| `DUCKDB_CONFIG` | `APP_CONFIG["duckdb"]` (없으면 빈 딕셔너리) | - | `dict` |
| (모듈 최상위 실행 코드) | `DUCKDB_CONFIG`의 `s3_endpoint`/`s3_access_key`/`s3_secret_key` 값을, 대응 환경변수(`S3_END_POINT`/`S3_ACCESS_KEY`/`S3_SECRET_KEY`)가 아직 없을 때만 `os.environ`에 설정 | - | 부수효과(환경변수 보충) |
| `SEOUL_API_CONFIG` | `APP_CONFIG["api"]["seoul"]` (없으면 빈 딕셔너리) | - | `dict` |
| (모듈 최상위 실행 코드) | `SEOUL_API_CONFIG["key"]`가 있고 환경변수 `KEY`가 아직 없으면 `os.environ["KEY"]`에 설정 | - | 부수효과(환경변수 보충) |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** N/A — Airflow DAG가 아닌 공용 설정 모듈. 다른 모듈이 `import`하는 시점에 즉시 실행된다.
- **Source (Input):** `pyproject.toml`의 `[tool.dataengineer]` 섹션(`paths`, `duckdb`, `api.seoul` 하위 키), `CONFIG_DIR/.env` 파일, 기존 `os.environ` 값(존재 여부 확인용).
- **Target (Output):** 모듈 레벨 경로 상수(`PROJECT_ROOT`, `DATA_DIR` 등), `os.environ`에 대한 조건부 갱신(`S3_END_POINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `KEY`) — 반환 객체가 아니라 import 시점의 전역 부수효과.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. 파일 위치 기준으로 `PROJECT_ROOT` 계산 → `pyproject.toml` 존재 시 파싱해 `TOML_CONFIG`/`APP_CONFIG`/`PATHS_CONFIG` 구성.
  2. `PATHS_CONFIG`의 값(없으면 하드코딩된 기본 상대경로)을 `PROJECT_ROOT`에 결합해 5개 주요 디렉터리 상수 정의.
  3. `SRC_DIR`/`UTILS_DIR`/`DAGS_DIR`는 TOML 설정과 무관하게 고정 규칙으로 계산.
  4. `ENV_FILE`이 실제로 존재하면 `load_dotenv(..., override=True)`로 `.env` 값을 환경변수에 강제 반영(기존 값도 덮어씀).
  5. `DUCKDB_CONFIG`를 순회하며, 대응 환경변수가 "아직 설정되지 않은 경우에만"(`.env`나 시스템 환경변수에 없을 때) TOML 값을 채워 넣는다 — `.env`/시스템 환경변수가 TOML보다 우선순위가 높다.
  6. `SEOUL_API_CONFIG["key"]`도 동일한 방식(환경변수 `KEY`가 없을 때만)으로 보충.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] `pyproject.toml` 기반 경로/설정 중앙화 및 파일 위치 기준 프로젝트 루트 자동 계산
- [x] `.env` 우선, TOML 값은 폴백(fallback)으로만 사용하는 환경변수 보충 로직(DuckDB S3 설정, 서울시 API 키)
- [ ] [TODO] 이 모듈은 import되는 즉시 파일 I/O 및 `os.environ` 변경을 수행하는 부수효과 기반 설계다 — 유닛 테스트에서 여러 시나리오(다른 `.env`/TOML 값)를 검증하려면 모듈 재로드(`importlib.reload`) 없이는 어렵다는 제약이 있다. 초기화 로직을 명시적 함수(예: `load_config()`)로 감싸는 리팩터링을 고려할 만하다(코드 내 명시된 TODO는 아닌 구조적 개선 제안).
- [ ] [TODO] `pyproject.toml`이나 `[tool.dataengineer]` 섹션이 없어도 예외 없이 빈 딕셔너리로 조용히 폴백하므로, 설정 누락을 조기에 발견하기 어렵다 — 필수 설정 검증 로직 추가를 고려할 만하다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- `load_dotenv(dotenv_path=ENV_FILE, override=True)`는 `override=True`이므로 `.env` 파일의 값이 이미 설정된 시스템 환경변수도 강제로 덮어쓴다 — 배포 환경에서 시스템 환경변수로 값을 주입한 경우 `.env` 파일 내용이 우선 적용되어 의도치 않게 값이 바뀔 수 있다.
- DuckDB S3 설정과 서울시 API 키(`KEY`)는 "환경변수가 비어 있을 때만" TOML 값을로 채우는 반대 방향의 우선순위(환경변수 우선, TOML 폴백)를 갖는다 — `.env` 로드(4번 로직) 이후에 실행되므로, `.env`에 값이 있으면 TOML 값은 무시된다.
- `tomllib`은 Python 3.11 이상 표준 라이브러리이므로, 이 프로젝트는 Python 3.11+ 런타임을 전제로 한다.
- `pyproject.toml`이 존재하지 않거나 `[tool.dataengineer]` 섹션이 없어도 예외 없이 모든 값이 하드코딩된 기본값(`"env"`, `"data"` 등)으로 조용히 폴백되므로, 설정 오류를 즉시 인지하기 어려울 수 있다.

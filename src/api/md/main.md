# 📄 main.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** FastAPI 기반 조회 전용 API 서버. MinIO(S3 호환) LAKE 버킷에 저장된 아파트 Gold 마트(`dm_apt_pyeong_price`: 평형별, `dm_apt_flr_price`: 층수별) 중 `base_date=YYYY-MM-DD/` 파티션 형태로 저장된 최신 날짜 파티션을 자동 탐색하고, 그 안의 Parquet 파일들을 읽어 자치구명(`cgg_nm`)/법정동명(`stdg_nm`)으로 필터링해 반환하는 단일 엔드포인트(`GET /api/apartments/compare`)를 제공한다.
- **현재 구현 상태:** 완료(단일 엔드포인트 기준). 다만 마트가 2종(`pyeong`, `floor`)으로 제한되어 있고, 페이지네이션/캐싱 없이 매 요청마다 S3에서 파일 목록 조회 및 전체 Parquet을 읽어들이는 구조라 확장성 측면에서는 개선 여지가 있다(코드 내 TODO 주석은 없음).

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음(같은 파일 내 `Settings` 클래스로 환경설정을 자체 정의하며, 다른 프로젝트 내부 모듈을 import하지 않음). `PROJECT_ROOT`를 파일 위치 기준(`parents[2]`)으로 계산해 `env/.env`를 직접 참조.
- **외부 패키지:**
  - `fastapi` (`FastAPI`, `HTTPException`, `Query`) — API 앱 및 엔드포인트 정의
  - `pydantic_settings` (`BaseSettings`, `SettingsConfigDict`) — `.env` 기반 환경설정 로드/검증
  - `boto3`, `botocore.client.Config`, `botocore.exceptions` (`BotoCoreError`, `ClientError`) — MinIO(S3 호환) 클라이언트 및 예외 처리
  - `pyarrow`, `pyarrow.parquet` — Parquet 파일 읽기 및 테이블 병합
  - 표준 라이브러리 `json`, `re`, `enum.Enum`, `io.BytesIO`, `pathlib.Path`

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `Settings` (BaseSettings) | `env/.env`에서 `S3_END_POINT`, `LAKE`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`를 로드하는 필수 환경설정 클래스 | `model_config=SettingsConfigDict(env_file=ENV_FILE, extra="ignore")` | 인스턴스화 시 필수 값 누락이면 pydantic 검증 오류 |
| `get_s3_client` | MinIO 접속용 `boto3` S3 클라이언트 생성. 엔드포인트에 `https://` 포함 여부로 SSL 사용 여부를 자동 판단(로컬/배포 양쪽 형식 지원) | 없음(전역 `settings` 사용) | `boto3.client("s3", ...)` (path-style 주소, `signature_version="s3v4"`) |
| `QueryType` (Enum) | 조회 타입 구분 | `PYEONG = "pyeong"`, `FLOOR = "floor"` | - |
| `MART_PREFIX_BY_TYPE` (dict) | `QueryType` → S3 마트 경로 prefix 매핑 | `PYEONG: "mart/dm_apt_pyeong_price/"`, `FLOOR: "mart/dm_apt_flr_price/"` | - |
| `find_latest_partition_prefix` | `mart_prefix` 하위 `base_date=YYYY-MM-DD/` "폴더" 중 날짜 문자열이 가장 큰(최신) 파티션 prefix 반환 | `s3_client`, `bucket: str`, `mart_prefix: str` | `str`(최신 파티션 prefix); 파티션 없으면 `HTTPException(404)` |
| `read_partition_records` | 파티션 내 모든 `*.parquet`(Spark의 여러 part 파일 포함) 읽어 병합 후 dict 리스트로 변환 | `s3_client`, `bucket: str`, `partition_prefix: str` | `list[dict]`; Parquet 파일 없으면 `HTTPException(404)` |
| `app` (FastAPI 인스턴스) | `title="Apartment Mart API"` | - | - |
| `GET /api/apartments/compare` (`compare_apartments`) | 자치구/법정동/조회타입으로 Gold 마트 최신 파티션 데이터를 필터링해 반환 | Query 파라미터: `cgg_nm: str`(필수), `stdg_nm: str`(필수), `query_type: QueryType`(필수) | `list[dict]`(필터링된 레코드); 조건에 맞는 데이터 없으면 `HTTPException(404)`, S3 오류 시 `HTTPException(500)` |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** N/A — Airflow DAG가 아닌 상시 구동 API 서버. `uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`로 실행(파일 상단 docstring에 명시). HTTP 요청이 곧 트리거.
- **Source (Input):** MinIO(S3 호환) LAKE 버킷의 `mart/dm_apt_pyeong_price/base_date=YYYY-MM-DD/*.parquet`, `mart/dm_apt_flr_price/base_date=YYYY-MM-DD/*.parquet`(`build_dong_pyeong_mart.py`가 Spark로 생성한 마트), HTTP 쿼리 파라미터(`cgg_nm`, `stdg_nm`, `query_type`).
- **Target (Output):** HTTP 응답(JSON 배열, 필터링된 레코드) — 별도 DB/스토리지 적재 없음(순수 조회 API).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. 요청 수신 시 `query_type`으로 `MART_PREFIX_BY_TYPE`에서 대상 마트 경로 결정.
  2. `get_s3_client()`로 MinIO 클라이언트 생성.
  3. `find_latest_partition_prefix`로 `Delimiter="/"` 페이지네이션을 통해 `base_date=` 폴더들을 나열하고 문자열 비교로 최신 날짜 파티션 식별.
  4. `read_partition_records`로 해당 파티션의 모든 Parquet part 파일을 읽어 `pyarrow.concat_tables`로 병합 후 pandas DataFrame으로 변환, `to_json(orient="records", date_format="iso")` → `json.loads`를 거쳐 numpy/Timestamp 스칼라를 순수 파이썬 타입으로 안전하게 직렬화.
  5. 전체 레코드 중 `cgg_nm`/`stdg_nm`이 요청 파라미터와 정확히 일치하는 행만 필터링.
  6. 결과가 비어 있으면 404, S3/MinIO 접속 오류(`BotoCoreError`, `ClientError`)는 500으로 변환, 이미 발생한 `HTTPException`은 그대로 재전파.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] `base_date=` 파티션 자동 탐색(최신 날짜 식별) 및 여러 part Parquet 파일 병합 조회
- [x] 로컬(MinIO)/배포(HTTPS) 양쪽 엔드포인트 형식을 모두 지원하는 S3 클라이언트 생성 로직
- [x] numpy/Timestamp 타입의 안전한 JSON 직렬화(`to_json` 경유)
- [ ] [TODO] 매 요청마다 S3 파티션 목록 조회 + 전체 Parquet 로드를 수행하므로, 트래픽이 늘어나면 지연/비용이 증가할 수 있다 — 최신 파티션 캐싱(TTL 기반)이나 응답 캐싱 도입을 고려할 만하다(코드 내 명시된 TODO는 아닌 구조적 개선 제안).
- [ ] [TODO] 현재는 `cgg_nm`/`stdg_nm` 정확히 일치 필터만 지원하고 페이지네이션이 없어, 마트 데이터량이 커지면 응답 크기가 커질 수 있다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- `Settings`는 `env/.env`에 `S3_END_POINT`, `LAKE`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`가 모두 존재해야 하며, 하나라도 없으면 앱 기동 시점에 pydantic 검증 오류로 즉시 실패한다(필수 필드, 기본값 없음).
- `get_s3_client`는 엔드포인트 문자열에 `https://`가 포함되어 있는지로 SSL 여부를 판단한다 — 스킴을 하드코딩하면 배포 환경에서 `http://https://...` 형태의 깨진 URL이 만들어질 수 있다는 점이 코드 주석에 명시되어 있다.
- `find_latest_partition_prefix`는 파티션이 하나도 없으면 `HTTPException(404)`를 던지며, `read_partition_records`도 Parquet 파일이 없으면 동일하게 404를 던진다 — 즉 마트가 아직 한 번도 빌드되지 않았거나 base_date 형식이 어긋나면 조회가 실패한다(`_BASE_DATE_RE = r"^base_date=(\d{4}-\d{2}-\d{2})/$"` 정규식과 정확히 일치해야 함).
- 재시도 정책이나 인증/인가 로직은 이 파일에 없다 — 순수 조회 전용, 무인증 공개 엔드포인트로 구현되어 있다.
- `query_type`은 `pyeong`/`floor` 두 값만 허용되며, 그 외 값은 FastAPI/Pydantic이 자동으로 422 응답을 반환한다(Enum 기반 검증).

# 📄 region_master.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** Iceberg Silver 레이어(`dim_apartment`, MinIO LAKE 버킷)에서 실제 사용 중인 자치구(sgg)/행정동(dong) 코드·명칭 조합을 추출하고, 카카오 로컬 API로 위경도 좌표를 지오코딩한 뒤 MySQL의 `tb_sgg_master`/`tb_dong_master` 기준정보 테이블에 upsert하는 배치 스크립트다. 좌표는 로컬 CSV 캐시(`data/external/tb_sgg_master_with_coords.csv`, `tb_dong_master_with_coords.csv`)에 누적 저장해 재실행 시 카카오 API 재호출을 최소화한다.
- **현재 구현 상태:** 완료. 멱등성(idempotency)을 명시적으로 설계했고(캐시 재사용 + `ON DUPLICATE KEY UPDATE`), 원천 데이터 품질 이슈(빈 값, 자치구 코드 불일치, 지오코딩 실패)에 대한 방어 로직이 갖춰져 있다. 명시적 TODO 주석은 없다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `utils.connect.get_duckdb_connect` — MinIO(S3) httpfs 설정이 적용된 DuckDB 연결을 생성해 `dim_apartment` parquet을 직접 조회하는 데 사용.
- **외부 패키지:**
  - `pandas` — DataFrame 기반 데이터 정제/병합
  - `requests` — 카카오 로컬 API(주소 검색) HTTP 호출
  - `python-dotenv` (`load_dotenv`) — `env/.env` 로드
  - `SQLAlchemy` (`Column`, `Table`, `MetaData`, `create_engine`, `text`, `ForeignKeyConstraint`, `sqlalchemy.dialects.mysql.insert`, `sqlalchemy.sql.func`) — MySQL DDL 정의 및 `INSERT ... ON DUPLICATE KEY UPDATE` upsert
  - 표준 라이브러리 `logging`, `os`, `time`, `pathlib.Path`

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `tb_sgg_master` (Table) | 자치구 기준정보 테이블 정의(SQLAlchemy Core) | `sgg_cd`(UNIQUE, 10자), `sgg_nm`, `center_lat/lng`(Numeric(10,7)), `created_at`/`updated_at`(자동 타임스탬프) | DDL 정의(실행 대상 아님) |
| `tb_dong_master` (Table) | 행정동 기준정보 테이블 정의, `tb_sgg_master.id`를 FK로 참조(`fk_tb_dong_tb_sgg`, `ON DELETE CASCADE`) | `sgg_id`, `sgg_cd`, `dong_cd`(UNIQUE, 10자 표준 법정동코드), `dong_nm`, 좌표 컬럼 | DDL 정의 |
| `_ensure_updated_at_column` | `create_all()`이 건드리지 않는 기존 테이블에 `updated_at` 컬럼이 없으면 `ALTER TABLE`로 추가 | `engine`, `table_name: str` | `None` |
| `GeocodeNotFoundError` (Exception) | 카카오 지오코딩 결과가 없을 때(주소/코드 조합 오류)의 전용 예외 — API/네트워크 오류와 구분해 해당 행만 건너뛰기 위함 | 없음 | 예외 발생 |
| `geocode_address` | 카카오 로컬 API 호출해 (위도, 경도) 반환. HTTP 실패 시 응답 본문 포함 `RuntimeError`, 결과 없음 시 `GeocodeNotFoundError` | `query: str`, `api_key: str`, timeout=10초 | `tuple[float, float]` (center_lat, center_lng) |
| `_geocode_dataframe` | DataFrame의 각 행을 순회하며 지오코딩, 결과 없는 행은 경고 후 제외 | `df`, `build_query: Callable`, `api_key: str`; 호출 간 `time.sleep(0.1)` | 좌표 컬럼이 추가된 `pd.DataFrame` |
| `_drop_incomplete_rows` | 필수 컬럼 중 하나라도 비었으면(NaN/빈 문자열) 해당 행 제외 | `df`, `required_cols: list[str]`, `label: str` | 필터링된 `pd.DataFrame` |
| `_read_dim_apartment` | DuckDB httpfs로 `s3://{LAKE}/dim_apartment/data/*.parquet`에서 `DISTINCT sgg_cd, sgg_nm, dong_cd, dong_nm` 조회 | `lake_bucket: str` | `pd.DataFrame`; 데이터 없으면 `RuntimeError` |
| `_geocode_incremental` | CSV 캐시에 없는 신규 key만 지오코딩해 캐시에 append, 최종 병합 결과 반환. 끝내 좌표 못 구한 행은 제외 | `source_df`, `key_col: str`, `cache_path: Path`, `build_query`, `api_key: str` | 좌표 병합된 `pd.DataFrame` |
| `_upsert_dataframe` | `INSERT ... ON DUPLICATE KEY UPDATE`로 MySQL upsert | `engine`, `table: Table`, `df`, `update_cols: list[str]` | `None`(로그만 남김) |
| `main` | 전체 실행 진입점: env 로드 → DDL 생성 → dim_apartment 조회 → sgg/dong 지오코딩 → upsert | 없음(CLI 인자 없음) | `None`; `__main__`에서 직접 호출 |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** N/A — Airflow DAG가 아닌 수동/CLI 실행 스크립트(`python scripts/region_master.py`). Airflow에 의해 스케줄링되지 않으며, `data_orchestration.py`의 `task_index_apt_name_es` 주석에 따르면 `dim_apartment`에 이미 지역명이 있어 이 스크립트의 선행 실행이 필수는 아니라고 명시되어 있다(MySQL 마스터 테이블을 쓰는 다른 기능을 위한 별도 스크립트).
- **Source (Input):** MinIO LAKE 버킷의 `dim_apartment/data/*.parquet`(Iceberg Silver 테이블), 카카오 로컬 API(`https://dapi.kakao.com/v2/local/search/address.json`), 로컬 좌표 캐시 CSV 2개, 환경변수(`DATABASE_URL`, `KAKAO_MAP_REST_API_KEY`, `LAKE`, S3 관련 값은 `env/.env` 경유).
- **Target (Output):** MySQL `tb_sgg_master`, `tb_dong_master` 테이블(upsert), 로컬 캐시 CSV(`data/external/tb_sgg_master_with_coords.csv`, `tb_dong_master_with_coords.csv`) 갱신.
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `.env` 로드 및 필수 환경변수(`DATABASE_URL`, `KAKAO_MAP_REST_API_KEY`, `LAKE`) 검증, 없으면 즉시 `RuntimeError`.
  2. `metadata.create_all(engine)`으로 두 테이블 DDL 생성(이미 있으면 무시) 후 `_ensure_updated_at_column`으로 구버전 스키마 보정.
  3. `_read_dim_apartment`로 자치구/행정동 조합 조회, `sgg_cd`를 5자리로 zero-fill.
  4. 자치구(sgg) 처리: 결측 행 제거 → `_geocode_incremental`로 캐시 활용 지오코딩(`"서울특별시 {sgg_nm}"` 질의) → `tb_sgg_master` upsert.
  5. 행정동(dong) 처리: `sgg_cd(5자리)+dong_cd(5자리)`로 표준 10자리 법정동코드 생성(원본 `dong_cd`는 자치구 내부에서만 유일하므로 충돌 방지 목적) → 결측/고아(유효하지 않은 sgg_cd 참조) 행 제거 → `_geocode_incremental`로 지오코딩(`"서울특별시 {sgg_nm} {dong_nm}"` 질의, `sgg_nm`을 못 찾으면 `RuntimeError`).
  6. `tb_sgg_master` upsert 후 DB에서 `(id, sgg_cd)`를 다시 조회해 `dong_df`에 FK(`sgg_id`) 매핑, 매핑 실패 행이 있으면 `RuntimeError`로 즉시 중단.
  7. `tb_dong_master` upsert로 종료.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] `dim_apartment` 기반 자치구/행정동 조합 자동 추출 및 표준 10자리 법정동코드 생성
- [x] 카카오 API 호출 최소화를 위한 CSV 캐시 기반 증분 지오코딩(`_geocode_incremental`)
- [x] `ON DUPLICATE KEY UPDATE` 기반 멱등적 upsert 및 `updated_at` 자동 갱신 보정 로직
- [x] 원천 데이터 품질 이슈(결측값, 고아 행, 지오코딩 실패) 방어 로직
- [ ] [TODO] 좌표 캐시가 로컬 파일시스템 CSV(`data/external/*.csv`)에 저장되므로, 컨테이너/여러 실행 환경 간 캐시가 공유되지 않아 매 환경마다 카카오 API를 재호출하게 될 수 있다 — 캐시를 S3/DB 등 공유 저장소로 옮기는 것을 고려할 만하다(코드 내 명시된 TODO는 아니며, 구조상 개선 제안).
- [ ] [TODO] `geocode_address` 호출 간 `time.sleep(0.1)`이 하드코딩되어 있어 API rate limit 정책 변경 시 코드 수정이 필요하다 — 환경변수화하면 유연성이 개선될 것으로 보인다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- `dong_cd`는 원본(STDG_CD) 그대로는 자치구 내부에서만 유일한 5자리 코드다(예: `dong_cd=10100`이 22개 자치구에서 각각 다른 동을 가리킴) — 반드시 `sgg_cd+dong_cd` 10자리 조합으로 UNIQUE 키를 구성해야 하며, 이를 어기면 서로 다른 동이 충돌한다(코드 주석에서 실제 데이터로 검증됨을 명시).
- API 호출 실패(HTTP 오류, 인증/네트워크 문제)는 `RuntimeError`로 즉시 전체 중단되지만, 지오코딩 결과가 단순히 없는 경우(`GeocodeNotFoundError`)는 해당 행만 건너뛰고 계속 진행한다 — 두 실패 유형을 명확히 구분해서 처리한다.
- MySQL upsert는 값이 완전히 동일하면 `rowcount=0`으로 무갱신 처리되어 `updated_at`도 그대로 유지된다(코드 주석에 실제 MySQL 동작으로 검증됨을 명시).
- `dong_source`에서 `sgg_source` 필터링으로 제외된 자치구를 참조하는 "고아" 행은 별도로 걸러내며, 이를 놓치면 이후 FK(`sgg_id`) 매핑 단계에서 `RuntimeError`로 전체 실행이 중단된다.
- 재시도 정책이나 별도의 동시성 제어 로직은 없다 — 단발성 배치 스크립트로 순차 실행을 전제로 설계됨.

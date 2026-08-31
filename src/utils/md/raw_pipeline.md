# 📄 raw_pipeline.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** 공공데이터포털 기반 원본(raw) 수집 파이프라인들이 공통으로 사용하는 유틸리티 모음. DuckDB-S3 연결 보장(`ensure_connection`), 표준 raw 저장 경로 생성(`default_raw_path`), 재시도 HTTP 세션 생성(`create_session_with_retry`), XML 다운로드/파싱(`download_xml`, `parse_items_xml`), 그리고 raw parquet -> DuckDB 테이블 upsert(`get_raw_columns`, `create_table_if_not_exists`, `upsert_from_parquet`)까지, "PK 기반 표준 upsert" 패턴을 따르는 여러 ingestion 모듈(`ProductInfo_raw.py`, `StoreInfo_raw.py` 등)이 공유하는 코드다.
- **현재 구현 상태:** 완료로 보이는 범용 공통 모듈. TODO/FIXME 주석은 없으며 각 함수가 단일 책임을 갖고 docstring으로 동작이 명확히 문서화되어 있다. 다만 이 파일의 XML 기반 패턴(`download_xml`+`parse_items_xml`+PK 기반 upsert)은 `Real_Estate.py`처럼 JSON 응답에 경로 기반 필터를 쓰는 API에는 맞지 않아, `Real_Estate.py`는 이 파일에서 `create_session_with_retry`와 `ensure_connection`만 재사용하고 나머지(다운로드/파싱/upsert) 로직은 자체 구현하고 있다 — 이는 결함이 아니라 API 특성 차이에 따른 의도된 분리로 판단된다.

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `config.paths`(import 시 부수효과로 env/.env 자동 로드 — `KEY`, `RAW` 등 환경변수를 이 import 하나로 보장), `utils.connect`의 `configure_minio`, `get_duckdb_connect`(DuckDB-S3 연결 설정을 이 파일에서 재사용).
- **외부 패키지:** `requests`(HTTP 호출), `requests.adapters.HTTPAdapter` + `urllib3.util.retry.Retry`(재시도 정책), `xml.etree.ElementTree`(XML 파싱), `duckdb`(`DuckDBPyConnection` 타입), 표준 라이브러리 `os`(환경변수 조회).

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `ensure_connection` | con이 None이면 `get_duckdb_connect()`로 새로 생성, 있으면 `configure_minio`로 S3 설정을 재적용(idempotent) — S3 접근 전 항상 먼저 호출하는 공통 진입점 | `con: DuckDBPyConnection\|None` | `DuckDBPyConnection` |
| `default_raw_path` | RAW 버킷 기준 고정 저장 경로 문자열 생성 | `sub_dir: str`, `file_name: str` | `str`(`s3://{RAW}/{sub_dir}/{file_name}`) |
| `create_session_with_retry` | 429/500/502/503/504 응답에 대해 1초→2초→4초 백오프로 최대 3회 재시도하는 `requests.Session` 생성(GET만 허용) | 없음 | `requests.Session` |
| `download_xml` | 재시도 세션으로 지정 URL을 호출해 XML 응답 텍스트 반환. `params`를 requests의 자동 인코딩 대신 직접 쿼리스트링으로 조립(서비스키 재인코딩 방지) | `url: str`, `params: dict` | `str`(XML 텍스트) |
| `parse_items_xml` | 공공데이터포털 공통 응답(XML)에서 `resultCode` 검사(00이 아니면 예외) 후 `<item>` 태그들을 dict 리스트로 변환 | `xml_text: str`, `operation_name: str` | `list[dict]` 또는 `RuntimeError` |
| `get_raw_columns` | raw parquet의 컬럼 목록을 조회하고 지정한 PK 컬럼 존재 여부 검증 | `con`, `raw_path: str`, `pk_col: str` | `list[str]` 또는 `RuntimeError`(PK 없음) |
| `create_table_if_not_exists` | 최초 실행 시 테이블 생성(모든 컬럼 VARCHAR + created_at/updated_at + PK 제약) | `con`, `table_name`, `columns: list[str]`, `pk_col: str` | `None` |
| `upsert_from_parquet` | `pk_col` 기준 `ON CONFLICT DO UPDATE`로 upsert — 신규는 INSERT(created_at/updated_at=now()), 값 변경 시 UPDATE(updated_at만 갱신), 값 동일 시 `WHERE` 조건에서 걸러져 무시 | `con`, `table_name`, `raw_path`, `other_cols: list[str]`, `pk_col: str` | `None` |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** N/A — Airflow DAG가 아닌 공통 유틸리티 모듈. 실제 스케줄은 이 함수들을 사용하는 개별 ingestion DAG/스크립트 쪽에서 결정된다.
- **Source (Input):** 공공데이터포털 XML API(`download_xml`이 호출하는 임의의 `url`+`params`), 그리고 이미 저장된 raw parquet 경로(`get_raw_columns`/`upsert_from_parquet`의 `raw_path`).
- **Target (Output):** 표준 raw 저장 경로 문자열(`default_raw_path` 결과, 실제 파일 쓰기는 이 함수가 하지 않고 호출자가 수행), 그리고 `upsert_from_parquet`이 반영하는 DuckDB 테이블(`table_name`).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. (ingestion 모듈이) `create_session_with_retry` + `download_xml`로 공공데이터포털 API를 호출해 XML 텍스트를 받는다.
  2. `parse_items_xml`로 `resultCode` 검증 후 `<item>` 태그를 dict 리스트로 변환(필드 가공 없이 태그명=컬럼명 그대로 사용).
  3. 호출자가 이 dict 리스트를 parquet으로 저장(이 파일 밖에서 처리)한 뒤, `ensure_connection`으로 S3 설정이 보장된 DuckDB 연결을 확보.
  4. `get_raw_columns`로 저장된 parquet의 컬럼과 PK 존재를 확인 -> `create_table_if_not_exists`로 대상 테이블이 없으면 생성(PK 제약 포함) -> `upsert_from_parquet`으로 PK 기준 upsert(INSERT or UPDATE, 값 무변경 시 스킵) 수행.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] 공공데이터포털 XML 응답 공통 파싱 및 resultCode 기반 오류 처리
- [x] HTTP 레벨(타임아웃/5xx) 자동 재시도 세션(백오프 1→2→4초, 최대 3회)
- [x] PK 컬럼 기준 표준 upsert 패턴(INSERT/UPDATE/무변경 스킵, created_at/updated_at 관리)
- [ ] [TODO] 코드 내 명시된 TODO는 없음. 개선 제안: `create_table_if_not_exists`/`upsert_from_parquet`가 테이블명·컬럼명을 f-string으로 SQL에 직접 삽입하므로, 이 값들이 외부 입력(API 응답 필드명 등)에서 유래할 경우 SQL 인젝션 방지 관점에서 컬럼명 화이트리스트 검증이나 식별자 이스케이프 강화를 고려할 만하다(현재는 내부에서 신뢰된 값만 전달된다는 전제).

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 재시도 정책: HTTP 레벨은 `create_session_with_retry`가 429/500/502/503/504에 대해 GET만 최대 3회, 1초 지수 백오프(1→2→4초)로 자동 재시도. 논리적 오류(resultCode≠00)는 이 파일에서 재시도하지 않고 즉시 `RuntimeError`로 실패시킨다(재시도는 호출자 책임).
- `ensure_connection`은 con이 이미 S3 설정이 되어 있어도 "가정하지 않고" 매번 `configure_minio`를 재적용한다 — 호출자가 미설정 con을 넘기는 실수를 방지하기 위한 방어적 설계.
- `download_xml`은 `requests`의 `params=` 대신 쿼리스트링을 직접 조립한다 — 서비스키가 이미 URL 인코딩된 상태일 때 `requests`가 이를 다시 인코딩(이중 인코딩)해 인증 실패를 일으키는 문제를 피하기 위함.
- `create_table_if_not_exists`는 모든 컬럼을 VARCHAR로 생성한다 — 타입 캐스팅/스키마 검증은 이 파일의 책임이 아니며, 필요 시 Silver 변환 단계에서 처리해야 한다.
- `upsert_from_parquet`의 변경 감지는 `IS DISTINCT FROM`(NULL-safe 비교) 기반이라 NULL 값 변경도 정상적으로 감지된다.

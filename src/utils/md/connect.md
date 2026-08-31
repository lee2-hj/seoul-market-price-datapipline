# 📄 connect.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** DuckDB 연결에 MinIO(S3 호환 스토리지) 접속 설정을 적용하는 공통 유틸리티. 프로젝트 내 모든 DuckDB <-> S3(MinIO) 연결 설정을 이 파일 한 곳에서만 관리하도록 설계되어 있다(`raw_pipeline.py` 등 다른 모듈이 이 파일의 함수를 재사용).
- **현재 구현 상태:** 완료. 파일이 매우 짧고(30줄) 단일 책임(연결 설정)만 수행하며, TODO/FIXME 주석은 없다. 다만 `configure_minio`가 환경변수 부재 시 빈 문자열(`""`)을 그대로 `SET` 문에 사용하므로, 환경변수 미설정 시에도 예외 없이 조용히 잘못된 접속 설정이 적용될 수 있다는 잠재적 위험이 있다(아래 6번 참고).

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** 없음 (다른 내부 모듈을 import하지 않는 최하위 유틸리티).
- **외부 패키지:** `duckdb`(`DuckDBPyConnection` 타입, `duckdb.connect()`), 표준 라이브러리 `os`(환경변수 조회).

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `configure_minio` | 기존 DuckDB 연결에 `httpfs` 확장을 설치/로드하고 `S3_END_POINT`/`S3_ACCESS_KEY`/`S3_SECRET_KEY` 환경변수를 읽어 S3 접속(endpoint, access key, secret key, SSL 여부, URL 스타일)을 SET. SET/INSTALL은 멱등적이라 "이미 설정됐는지" 확인 없이 항상 재적용 | `con: DuckDBPyConnection` | `DuckDBPyConnection`(같은 con에 설정 적용 후 반환) |
| `get_duckdb_connect` | 새 DuckDB 연결을 생성하고 `configure_minio`로 S3 설정을 적용해 반환하는 진입점 함수 | 없음 | `DuckDBPyConnection` |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** N/A — Airflow DAG가 아닌 일반 유틸리티 모듈로, 스케줄/트리거가 없다.
- **Source (Input):** 환경변수 `S3_END_POINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`(모두 `os.environ.get(..., "")`로 조회, 기본값 빈 문자열).
- **Target (Output):** S3 설정이 적용된 `DuckDBPyConnection` 객체(반환값 자체가 산출물이며, 별도 파일/DB 적재는 없음).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. `get_duckdb_connect` 호출 시 `duckdb.connect()`로 인메모리(파일 미지정) DuckDB 연결 생성.
  2. `configure_minio(con)`에 전달되어, `S3_END_POINT`가 `https://`로 시작하는지로 `use_ssl` 판단.
  3. endpoint 문자열에서 `http(s)://` 접두사를 제거한 `endpoint_clean` 생성, `use_ssl` 여부에 따라 `url_style`을 `vhost`(SSL) 또는 `path`(비SSL)로 결정.
  4. `INSTALL httpfs; LOAD httpfs;` 실행 후, `s3_endpoint`/`s3_access_key_id`/`s3_secret_access_key`/`s3_use_ssl`/`s3_url_style` 5개 DuckDB 세션 변수를 `SET`.
  5. 설정이 적용된 동일 `con` 객체를 반환.

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] `httpfs` 확장 설치/로드 및 MinIO(S3 호환) 접속 설정(endpoint/키/SSL/URL 스타일) 적용
- [x] SSL 여부를 endpoint 스킴(`https://`)으로 동적 판단해 로컬(MinIO, HTTP)과 배포(HTTPS) 환경 겸용 지원
- [ ] [TODO] 코드 내 명시된 TODO는 없으나, 개선 제안: `S3_END_POINT`/`S3_ACCESS_KEY`/`S3_SECRET_KEY` 환경변수가 비어 있을 때 예외를 던지지 않고 빈 문자열로 `SET`을 그대로 실행하므로, 설정 누락을 조기에 감지하지 못하고 이후 S3 접근 시점에야 모호한 오류로 드러날 수 있다 — 필수 환경변수 검증(예: 빈 값이면 명시적 에러) 추가를 고려할 만하다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- `configure_minio`는 "이미 설정됐는지" 여부를 확인하지 않고 매번 무조건 재적용한다 — SET/INSTALL이 멱등적이라는 전제 하에 설계된 것으로, 여러 번 호출해도 안전하지만 반대로 매 호출마다 불필요한 `INSTALL/LOAD` 오버헤드가 있을 수 있다.
- 환경변수 3종(`S3_END_POINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`)이 없으면 기본값이 빈 문자열이라, 잘못된 인증 정보로 S3 접속을 시도하게 되고 에러 메시지가 즉시 원인(환경변수 누락)을 가리키지 않을 수 있다.
- SSL 사용 여부(`use_ssl`)와 URL 스타일(`vhost`/`path`)이 endpoint 문자열 접두사만으로 하드코딩 없이 동적으로 결정되므로, 로컬 MinIO(HTTP)와 클라우드 스토리지(HTTPS) 양쪽 환경을 코드 변경 없이 지원한다 — 이 판단 로직을 변경할 때는 두 환경 모두 재검증이 필요하다.
- 이 파일은 재시도 정책이나 예외 처리를 별도로 두지 않는다 — `con.execute()` 실패 시 예외가 그대로 호출자에게 전파된다.

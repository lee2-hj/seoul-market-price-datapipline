# 📄 pipeline_apt_name.py 명세서

## 1. 파일 개요 (Overview)
- **담당 역할:** Gold 레이어의 아파트 단지명 검색 인덱스(Elasticsearch `apt_name`) 적재 파이프라인. S3 Lake(MinIO)의 Iceberg 데이터 파일(`lakehouse.dim_apartment`, `lakehouse.fact_apt_transactions`)을 DuckDB `httpfs`로 직접 조회해 (자치구 x 법정동 x 단지명) 문서에 단지별 최신 실거래 계약일자(`last_deal_date`)와 대표 지번(`mno`/`sno`)을 더해 Elasticsearch `apt_name` 인덱스에 벌크 색인한다. 단지명 검색/자동완성 전용이며, 가격 조회는 여전히 S3 Lake `mart/` 경로의 Parquet을 그대로 사용한다.
- **현재 구현 상태:** 완료. 과거 MySQL `tb_apt_name` upsert 방식에서 Elasticsearch 직접 색인 방식으로 전환된 이력이 문서화되어 있다(부분일치/자동완성에 Elasticsearch가 더 적합하고, `dim_apartment`에 이미 `sgg_nm`/`dong_nm`이 있어 MySQL 마스터 테이블 조인이 불필요했기 때문). `run_apt_name_es_pipeline()`이라는 명확한 진입점을 가지며 Airflow `PythonOperator`/`@task` 연동 스니펫까지 파일 하단에 준비되어 있다. 명시적 TODO/FIXME 주석은 없다. 다만 한국어 형태소 분석(nori) 플러그인이 없어 edge_ngram 기반 부분일치로만 구성했다는 알려진 제약이 주석에 명시되어 있다(6절 참고).

## 2. 의존성 (Dependencies)
- **내부 모듈/공통 라이브러리:** `utils.connect.get_duckdb_connect` - DuckDB 커넥션을 생성하고 MinIO S3 연결 설정(`configure_minio`)까지 적용해 반환하는 공통 유틸(`src/utils/connect.py`).
- **외부 패키지:** `pandas`(DuckDB 쿼리 결과를 `pd.DataFrame`으로 처리, `merge`), `python-dotenv`(`load_dotenv`), `elasticsearch`(`Elasticsearch` 클라이언트), `elasticsearch.helpers.bulk`(벌크 색인). 표준 라이브러리로 `hashlib`(문서 ID 생성용 md5), `logging`, `os`, `pathlib.Path`.

## 3. 핵심 함수, 클래스 및 Task 명세 (Components & Tasks)
| 식별자 (Task ID / Function / Class) | 설명 | 파라미터 / 설정 | 반환값 / Trigger |
| :--- | :--- | :--- | :--- |
| `ES_INDEX_NAME` / `CHUNK_SIZE` | Elasticsearch 인덱스명(`"apt_name"`) 및 벌크 색인 청크 크기(`2000`) 상수. | 상수 | N/A |
| `_INDEX_BODY` | `apt_name` 인덱스 매핑/설정. `apt_name` 필드는 edge_ngram(1~20자) 색인 분석기 + 표준 검색 분석기(과매칭 방지)로 부분일치 검색을 지원, `apt_name.keyword` 서브필드로 정확일치/정렬 지원. `sgg_cd/sgg_nm/dong_cd/dong_nm`은 keyword, `last_deal_date`는 date, `mno/sno`는 keyword(검색 미사용, 저장용). | 딕셔너리 상수 | N/A |
| `_MNO_SNO_MAPPING` | 기존에 이미 존재하는 인덱스에 `mno`/`sno` 필드만 재색인 없이 추가하기 위한 부분 매핑. | 딕셔너리 상수 | N/A |
| `_resolve_es_client()` | 환경변수 `ES_HOST`로 `Elasticsearch` 클라이언트 생성(스킴 없으면 `http://` 붙임). `ES_HOST` 없으면 예외. | 없음 | `Elasticsearch` |
| `_ensure_index(client)` | `apt_name` 인덱스가 없으면 `_INDEX_BODY`로 생성, 있으면 생성은 스킵하되 `_MNO_SNO_MAPPING`을 `put_mapping`으로 항상 보강. | `client: Elasticsearch` | `None` |
| `_read_dim_apartment(lake_bucket)` | DuckDB로 `dim_apartment/data/*.parquet`에서 `(sgg_cd, sgg_nm, dong_cd, dong_nm, apt_name)` 고유 조합 조회. 결과가 비어 있으면 예외. | `lake_bucket: str` | `pd.DataFrame` |
| `_read_last_deal_dates(lake_bucket)` | `fact_apt_transactions/data/**/*.parquet`(`union_by_name=true`로 스키마 진화 컬럼 누락 허용)에서 취소건 제외 후 (sgg_cd, dong_cd, apt_name)별 `MAX(deal_date)` 집계. | `lake_bucket: str` | `pd.DataFrame` |
| `_read_representative_mno_sno(lake_bucket)` | `fact_apt_transactions`에서 (sgg_cd, dong_cd, apt_name)별 최빈 (mno, sno) 조합을 `ROW_NUMBER() OVER(... ORDER BY cnt DESC)`로 선정. | `lake_bucket: str` | `pd.DataFrame` |
| `_make_doc_id(sgg_cd, dong_cd, apt_name)` | `(sgg_cd\|dong_cd\|apt_name)` 문자열의 md5 해시를 Elasticsearch 문서 `_id`로 사용(결정론적, 멱등적 upsert 효과). | `sgg_cd, dong_cd, apt_name: str` | `str`(32자 hex) |
| `_build_actions(df)` | 병합된 DataFrame의 각 레코드를 Elasticsearch bulk `_index`/`_id`/`_source` 액션 딕셔너리로 변환하는 제너레이터. `last_deal_date`는 `pd.Timestamp(...).isoformat()`, `mno`/`sno`는 `pd.isna()` 체크 후 `None` 처리. | `df: pd.DataFrame` | `Generator[dict]` |
| `_bulk_index(client, df)` | `elasticsearch.helpers.bulk`로 `_build_actions(df)`를 청크(`CHUNK_SIZE=2000`) 단위 색인, `raise_on_error=True`. 빈 df면 0 반환. | `client: Elasticsearch, df: pd.DataFrame` | `int`(색인 성공 건수) |
| `run_apt_name_es_pipeline()` | 메인 진입점(Airflow `PythonOperator`/`@task`가 호출). `LAKE` 환경변수 확인 → ES 클라이언트/인덱스 준비 → `dim_df`/`fact_df`(최신계약일)/`mno_sno_df`(대표지번) 읽기 → `merge`(left join, 3-way) → `_bulk_index` 실행 → 결과 요약 dict 반환. | 없음 | `dict`(`dim_apartment_count`, `matched_last_deal_count`, `matched_mno_sno_count`, `indexed_count`) |
| `if __name__ == "__main__":` | CLI 직접 실행 시 `run_apt_name_es_pipeline()` 호출. | 없음 | N/A |

## 4. 데이터 I/O 및 파이프라인/모듈 흐름 (Data Flow & Logic)
- **Schedule / Trigger:** 이 파일 자체에는 DAG/cron 정의 없음. 파일 하단 주석에 Airflow 연동 스니펫이 명시되어 있으며, `task_transform_silver_real_estate`(Silver 적재) 완료 이후에 `PythonOperator` 또는 `@task`로 `run_apt_name_es_pipeline()`을 호출하도록 의존성을 걸도록 안내한다(더 이상 `region_master.py` 선행 실행 불필요). CLI로도 단독 실행 가능(`python src/transformation/gold/pipeline_apt_name.py`).
- **Source (Input):** S3 Lake(MinIO) Iceberg 데이터 파일 - `s3://{LAKE}/dim_apartment/data/*.parquet`, `s3://{LAKE}/fact_apt_transactions/data/**/*.parquet`(DuckDB httpfs로 직접 글롭 조회, Iceberg 메타데이터 카탈로그를 거치지 않음). 환경변수: `S3_END_POINT`/`S3_ACCESS_KEY`/`S3_SECRET_KEY`/`LAKE`(DuckDB의 `configure_minio`가 사용, `utils/connect.py` 참고), `ES_HOST`.
- **Target (Output):** Elasticsearch 인덱스 `apt_name`(문서: `sgg_cd`, `sgg_nm`, `dong_cd`, `dong_nm`, `apt_name`, `last_deal_date`, `mno`, `sno`). RDB(MySQL) 적재는 더 이상 하지 않음(이전 방식에서 제거됨).
- **핵심 파이프라인 흐름 및 처리 로직:**
  1. 환경변수 로드(`env/.env` 또는 `/opt/airflow/project/env/.env`, `override=False`).
  2. `run_apt_name_es_pipeline()` 호출 시 `LAKE` 환경변수 확인(없으면 예외) → `_resolve_es_client()`로 ES 연결 → `_ensure_index()`로 인덱스 존재 보장(신규 생성 또는 `mno`/`sno` 매핑 보강).
  3. `_read_dim_apartment()`로 단지 차원 고유 조합 조회(비어 있으면 예외로 실패).
  4. `_read_last_deal_dates()`로 취소건 제외 최신 계약일자 집계, `_read_representative_mno_sno()`로 최빈 지번 집계(둘 다 `fact_apt_transactions`를 별도로 재조회, `union_by_name=true`로 스키마 진화 컬럼 결측 허용).
  5. `dim_df`를 기준으로 `fact_df`, `mno_sno_df`를 각각 `left join`(pandas `merge`)해 `merged_df` 구성.
  6. `_bulk_index()`로 `_build_actions()`가 생성하는 액션들을 Elasticsearch에 `bulk` 색인(문서 `_id`는 md5 해시로 결정론적 - 재실행 시 upsert 효과).
  7. 처리 건수 요약 dict(`dim_apartment_count`, `matched_last_deal_count`, `matched_mno_sno_count`, `indexed_count`)를 반환(Airflow XCom으로 자동 전달 가능).

## 5. 변경 히스토리 및 작업 항목 (TODOs)
- [x] MySQL `tb_apt_name` upsert 방식에서 Elasticsearch 직접 색인 방식으로 전환 완료(코드 상단 "[변경 이력]" 주석에 명시) - `tb_sgg_master`/`tb_dong_master` 조인이 불필요해져 MySQL 경로를 걷어냄.
- [x] `apt_name` 인덱스에 대해 edge_ngram 기반 부분일치/자동완성 검색과 `keyword` 서브필드(정확일치/정렬)를 함께 구성.
- [x] 기존 인덱스에 안전하게 `mno`/`sno` 필드를 추가하는 점진적 매핑 보강(`_ensure_index`의 `put_mapping` 분기) 구현.
- [ ] [TODO] 코드 주석에 명시: "한국어 형태소 분석(nori)은 Elasticsearch 서버에 analysis-nori 플러그인이 설치되어 있어야 해서(docker-compose 기본 이미지에는 없음) 현재는 플러그인 없이도 동작하는 edge_ngram 기반 부분일치로 구성했다. 추후 nori 플러그인을 설치하면 apt_name analyzer만 nori 기반으로 교체하면 된다." - 검색 품질 개선을 위한 후속 작업으로 명시되어 있음.
- [ ] [TODO] `_bulk_index()`가 `raise_on_error=True`로 예외를 전파하면서도 `errors` 반환값에 대해 `logger.warning`만 남기는 이중적 처리 구조 - `raise_on_error=True`인 경우 부분 실패는 이미 예외로 던져지므로 `if errors:` 분기가 실질적으로 도달하기 어려울 수 있어(elasticsearch-py 동작에 따라 다름) 로직 재검토 여지가 있다.

## 6. 주의사항 및 제약조건 (Constraints & Gotchas)
- 매번 전체 재계산 후 결정론적 `_id`(md5 해시)로 `index` 액션을 실행하는 완전 멱등적(idempotent) 배치이므로 몇 번을 재실행해도 안전하다(MySQL의 `INSERT ... ON DUPLICATE KEY UPDATE`와 동등 효과).
- `_ensure_index()`는 인덱스가 이미 있으면 매핑을 통째로 덮어쓰지 않는다 - Elasticsearch는 기존 필드 매핑을 재색인 없이 변경할 수 없기 때문에, 새 필드(`mno`/`sno`) 추가만 안전하게 허용하고 나머지 매핑 변경은 수동 재색인이 필요하다.
- `fact_apt_transactions`를 DuckDB로 직접 파케이 글롭 조회할 때 `union_by_name=true`를 사용하는 이유는, Iceberg `ADD COLUMNS`(mno/sno 스키마 진화)가 메타데이터만 바꾸고 기존 데이터 파일을 재작성하지 않아 옛 파일에는 물리적으로 그 컬럼이 없기 때문 - 이 옵션 없이 조회하면 스키마 불일치 오류가 날 수 있다.
- 이 파이프라인은 Iceberg 카탈로그(메타데이터)를 거치지 않고 원본 parquet 파일을 직접 글롭 조회하므로, 삭제되었지만 아직 물리적으로 남아있는 파일(예: Iceberg 스냅샷 만료 전 orphan 파일)이 있다면 잘못된 데이터를 읽을 가능성이 이론적으로 있다(단, 코드에 이에 대한 별도 방어 로직은 없음).
- `ES_HOST` 환경변수가 없으면 `RuntimeError`로 즉시 실패하며, `LAKE` 환경변수가 없어도 `RuntimeError`로 즉시 실패한다 - 두 값 모두 필수.
- `dim_apartment`에서 읽은 데이터가 비어 있으면(`_read_dim_apartment`) 예외를 던져 파이프라인을 중단시킨다(Silver 적재가 선행되지 않았거나 실패한 경우를 방어).

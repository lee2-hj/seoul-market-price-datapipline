# dataengineer

데이터 엔지니어링 프로젝트 기본 폴더 구조입니다.

## 폴더 구조

```
.
├── env/                     # 환경별 설정 파일 (dev/staging/prod)
├── data/                   # 로컬 데이터 레이크 (버전관리 대상 아님, .gitignore 처리)
│   ├── raw/                # 원본 데이터 (수정 금지, append-only)
│   ├── interim/            # 처리 중간 산출물 (재생성 가능)
│   ├── processed/          # 최종 가공 데이터 (분석/서빙용)
│   └── external/           # 외부에서 받아온 참조 데이터
├── src/                    # 파이프라인 소스 코드
│   ├── config/             # 설정 로더 (pyproject.toml, .env 읽기)
│   ├── ingestion/          # 데이터 수집 (API, DB, 파일 등)
│   ├── transformation/     # 정제/변환 로직
│   ├── load/                # 적재 (DW, DB, 파일 저장 등)
│   └── utils/               # 공통 유틸 (로깅, 커넥션, 설정 로더 등)
├── sql/                     # SQL 자산
│   ├── models/              # dbt 스타일 변환 모델 / 뷰 정의
│   └── queries/              # 분석/운영용 쿼리
├── airflow/                  # Airflow (공식 docker-compose 스택 전체가 이 폴더에 있음, airflow/README.md 참고)
│   ├── dags/                # 실제 DAG .py 파일 위치
│   └── docker-compose.yaml  # 공식 Airflow 배포판 (+ MinIO 연동, dataeng-net 연결)
├── notebooks/                 # 탐색/실험용 노트북 (프로덕션 코드 아님)
├── tests/
│   ├── unit/                  # 단위 테스트
│   └── integration/           # 파이프라인 통합 테스트
├── scripts/                    # 1회성 운영/배포/백필 스크립트
├── docs/                        # 설계 문서, 데이터 카탈로그, ERD 등
├── logs/                        # 로컬 실행 로그 (버전관리 대상 아님)
├── docker/                      # MinIO, Elasticsearch, Iceberg REST — docker/README.md 참고
├── .env.example                 # 환경변수 템플릿
├── requirements.txt              # 파이썬 의존성
└── .gitignore
```

## 규칙

- `data/`, `logs/` 하위 실제 데이터 파일은 git으로 추적하지 않습니다 (`.gitkeep`으로 폴더 구조만 유지).
- `raw/` 데이터는 원본 그대로 두고 절대 덮어쓰지 않습니다. 가공은 `interim/` → `processed/` 순으로 진행합니다.
- 민감 정보(DB 접속정보, API 키 등)는 `.env` 파일에 두고 `.env.example`만 커밋합니다.
- `env/`는 애플리케이션 환경변수(.env) 보관용이고, `src/config/`는 그 값을 읽어오는 파이썬 로더입니다 — 이름이 비슷하니 혼동하지 마세요.

# Docker (MinIO + Elasticsearch + Kibana + Iceberg)

Airflow는 이 스택에서 분리되어 [`../airflow/docker-compose.yaml`](../airflow/docker-compose.yaml) (공식 Airflow 배포판)에서 별도로 실행됩니다.
두 스택은 `dataeng-net`이라는 외부 Docker 네트워크로 서로 연결되어, Airflow DAG에서 컨테이너 이름(`minio`, `iceberg-rest` 등)으로 이 스택의 서비스에 접근할 수 있습니다.

## 0. 최초 1회: 공용 네트워크 생성

두 스택 중 어느 쪽을 먼저 올리든 상관없도록, 네트워크를 미리 만들어둡니다.

```powershell
docker network create dataeng-net
```

## 1. 설정

```powershell
cd docker
copy .env.example .env
```

`.env`에서 `S3_SECRET_KEY`를 기본값에서 바꿔주세요 (팀원과 공유할 값입니다. MinIO 컨테이너의
루트 비밀번호도 이 값으로 설정됩니다).

## 2. 실행

```powershell
cd docker
docker compose up -d
```

## 3. 서비스 목록 및 접속 (내 PC에서)

| 서비스 | 주소 | 비고 |
|---|---|---|
| MinIO 콘솔 | http://localhost:9001 | `.env`의 S3_ACCESS_KEY/S3_SECRET_KEY |
| MinIO S3 API | http://localhost:9000 | 버킷 `lake`(일반), `warehouse`(Iceberg) 자동 생성 |
| Elasticsearch | http://localhost:9200 | 개발 편의를 위해 보안(xpack.security) 비활성화 |
| Kibana | http://localhost:5601 | Elasticsearch 인덱스 조회/시각화 |
| Iceberg REST 카탈로그 | http://localhost:8181 | 로컬 venv(pyspark, duckdb 등)에서 Iceberg 테이블 카탈로그로 사용 |

로컬 venv에서 이 스택에 접속할 때는 `docker` 네트워크 안이 아니므로 컨테이너 이름(`minio`, `iceberg-rest`) 대신 `localhost`를 사용하고, S3/카탈로그 엔드포인트를 `http://localhost:9000`, `http://localhost:8181`로 설정하세요.

## 4. 팀원이 내 PC의 서비스에 접속하게 하려면

모든 서비스가 컨테이너에서 `0.0.0.0`으로 열려 있어 Docker 쪽 설정은 끝난 상태입니다. 나머지는 Windows 방화벽과 IP 공유입니다.

**1) 내 PC의 IP 확인**
```powershell
ipconfig
```

**2) Windows 방화벽 인바운드 규칙 추가** (관리자 PowerShell에서 1회 실행)
```powershell
New-NetFirewallRule -DisplayName "MinIO API" -Direction Inbound -Protocol TCP -LocalPort 9000 -Action Allow
New-NetFirewallRule -DisplayName "MinIO Console" -Direction Inbound -Protocol TCP -LocalPort 9001 -Action Allow
New-NetFirewallRule -DisplayName "Elasticsearch" -Direction Inbound -Protocol TCP -LocalPort 9200 -Action Allow
New-NetFirewallRule -DisplayName "Kibana" -Direction Inbound -Protocol TCP -LocalPort 5601 -Action Allow
New-NetFirewallRule -DisplayName "Iceberg REST" -Direction Inbound -Protocol TCP -LocalPort 8181 -Action Allow
```
Airflow(8080)를 공유하려면 [`../airflow/README.md`](../airflow/README.md)의 규칙도 함께 추가하세요.

**3) 팀원 접속 정보**: 위 표의 `localhost`를 `<내 PC IP>`로 바꿔서 전달 (예: `http://192.168.0.15:9001`), 계정/토큰은 `.env` 값.

## 5. 주의사항

- 개발/팀 내부용 구성입니다. TLS 없음, Elasticsearch 보안 비활성화, MinIO 루트 계정 사용 — 인터넷에 노출하지 마세요.
- `minio_data`, `es_data`는 named volume이라 `docker compose down`으로 지워지지 않습니다. 완전 초기화하려면 `docker compose down -v`.
- Elasticsearch는 최소 4GB 이상 Docker Desktop 메모리 할당을 권장합니다 (부족하면 부팅 실패).

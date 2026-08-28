# Airflow (공식 docker-compose)

`docker-compose.yaml`은 [Airflow 공식 문서](https://airflow.apache.org/docs/apache-airflow/2.9.3/docker-compose.yaml)에서 받은 원본에 아래 두 가지만 추가한 것입니다:
1. `AIRFLOW_CONN_MINIO_DEFAULT` 환경변수 (MinIO용 S3 커넥션 자동 등록, Connection Id: `minio_default`)
2. 모든 airflow-* 서비스를 외부 네트워크 `dataeng-net`에도 연결 (→ `docker/docker-compose.yml`의 minio, iceberg-rest, elasticsearch를 컨테이너 이름으로 호출 가능)

DAG 파일(.py)은 **`airflow/dags/`** 폴더에 넣으세요 (공식 compose의 기본 레이아웃을 그대로 따른 것입니다).

## 0. 사전 조건

`../docker` 스택과 통신하려면 두 스택이 같은 외부 네트워크를 씁니다. 아직 안 만들었다면:
```powershell
docker network create dataeng-net
```

## 1. 설정

```powershell
cd airflow
copy .env.example .env
```
`.env`에서 `AIRFLOW_CONN_MINIO_DEFAULT`의 계정/비밀번호를 `env/.env`의 `S3_ACCESS_KEY` / `S3_SECRET_KEY`와 동일하게 맞춰주세요.

## 2. 실행

```powershell
cd airflow
docker compose up -d
```
CeleryExecutor 기반이라 Redis/Postgres/webserver/scheduler/worker/triggerer가 함께 뜨며 최초 기동은 다소 오래 걸립니다 (Docker Desktop 메모리 4GB 이상 권장, 공식 가이드 기준).

## 3. 접속

| 서비스 | 주소 | 계정 |
|---|---|---|
| Airflow UI | http://localhost:8080 | `.env`의 `_AIRFLOW_WWW_USER_USERNAME` / `_PASSWORD` (기본 airflow/airflow) |

DAG 안에서 MinIO 접근 예:
```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
hook = S3Hook(aws_conn_id="minio_default")
hook.load_string("hello", key="test.txt", bucket_name="lake")
```

## 4. 팀원 접속시키기

```powershell
New-NetFirewallRule -DisplayName "Airflow UI" -Direction Inbound -Protocol TCP -LocalPort 8080 -Action Allow
```
팀원에게 `http://<내 PC IP>:8080` 공유. 나머지(MinIO 등) 방화벽 규칙은 [`../docker/README.md`](../docker/README.md) 참고.

## 5. 주의사항

- 공식 파일 특성상 `AIRFLOW__CORE__FERNET_KEY`가 빈 값입니다. 여러 명이 같이 쓰거나 오래 운영할 계획이면 Fernet key를 발급해 `.env`에 추가하는 것을 권장합니다 (안 하면 매 재기동시 자동 생성되어 이전에 저장된 커넥션 암호를 못 읽는 문제가 생길 수 있음).
  ```
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```
  발급한 값을 `airflow/docker-compose.yaml`의 `AIRFLOW__CORE__FERNET_KEY: ''` 부분에 채우거나, `.env`에 `AIRFLOW__CORE__FERNET_KEY=...`로 추가하고 compose 파일에서 `${AIRFLOW__CORE__FERNET_KEY}`로 참조하도록 바꿔도 됩니다.
- 개발용 구성입니다 (공식 파일 상단 경고 참고). 프로덕션에는 사용하지 마세요.

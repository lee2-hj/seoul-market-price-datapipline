# Real_Estate_Transform.py 등 Silver 변환 스크립트가 로컬 PySpark(subprocess)로 실행되므로,
# Airflow 워커 자체의 파이썬 환경에 PySpark와 그 실행에 필요한 JDK가 있어야 한다.
FROM apache/airflow:2.9.3
USER root
# openjdk-17-jdk-headless: PySpark 4.0.x가 요구하는 최소 JDK 버전(Spark 4.0은 Java 8/11 지원 종료).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f /usr/bin/javac)")")" /usr/lib/jvm/default-java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER airflow
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
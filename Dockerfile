FROM apache/airflow:2.9.1-python3.10

# root 권한으로 시스템 패키지 설치
USER root

# 시스템 패키지 업데이트 및 필요한 패키지 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client \
        curl \
        vim \
        git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# airflow 사용자로 전환
USER airflow

# requirements.txt 복사 및 Python 패키지 설치
COPY requirements.txt /requirements.txt

# pip 업그레이드 및 Python 패키지 설치 (파일이 존재하고 비어있지 않은 경우에만)
RUN pip install --upgrade pip \
    && if [ -s /requirements.txt ]; then pip install --no-cache-dir -r /requirements.txt; fi

# 작업 디렉토리 설정
WORKDIR /opt/airflow
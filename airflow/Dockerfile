FROM apache/airflow:2.9.1-python3.10

USER root
# ติดตั้ง client ของ Postgres
RUN apt-get update && apt-get install -y \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# ติดตั้ง dependency สำหรับ Airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

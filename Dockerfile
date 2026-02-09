FROM apache/airflow:3.1.7-python3.12

USER root

COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --upgrade pip && \
    pip install --no-cache-dir --upgrade -r requirements.txt


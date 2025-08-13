FROM apache/airflow:2.8.0

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir --disable-pip-version-check --default-timeout=1000 --retries=20 -r /requirements.txt

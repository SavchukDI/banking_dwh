# Версия для чистого Airflow
#FROM apache/airflow:2.10.5-python3.11
#
#COPY airflow/requirements.txt /requirements.txt
#RUN pip install --no-cache-dir -r /requirements.txt
#
# dbt в отдельном виртуальном окружении, чтобы его зависимости не конфликтовали с Airflow
#RUN python -m venv /opt/airflow/dbt_venv \
#    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-clickhouse==1.10.3

FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow/requirements.txt /requirements.txt
# Фиксируем версию Airflow, чтобы pip не обновил его вместе с зависимостями
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

COPY banking_dbt/requirements.txt /dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt
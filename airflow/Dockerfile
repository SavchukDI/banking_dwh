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

# 1. Зависимости Airflow
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# 2. dbt в отдельном venv
COPY banking_dbt/requirements.txt /dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt

# 3. Настройки dbt внутри образа
ENV DBT_PROFILES_DIR=/opt/airflow/banking_dbt \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs

# 4. Код — в последнюю очередь
COPY --chown=airflow:root banking_dbt /opt/airflow/banking_dbt
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
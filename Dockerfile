FROM apache/airflow:2.10.5


RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
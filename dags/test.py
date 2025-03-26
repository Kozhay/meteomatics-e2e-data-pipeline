from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator


schedule_interval = "@daily"
start_date = days_ago(1)
default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id="test",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
) as dag:
     t1 = EmptyOperator(
        task_id="t1"
    )



t1
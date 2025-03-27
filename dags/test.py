from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.meteomatics_pipeline.meteomatics_get_data import fetch_weather_data

schedule_interval = "@daily"
start_date = days_ago(1)
default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

city_list = ["Tallinn, Estonia", "Amsterdam, Netherlands"]

with DAG(
    dag_id="test_weather_taskgroup",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["weather", "test"],
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("extract_weather") as extract_group:
        for city in city_list:
            PythonOperator(
                task_id=f"fetch_weather_{city.lower().replace(',', '').replace(' ', '_')}",
                python_callable=fetch_weather_data,
                op_kwargs={
                    "city_name": city,
                    "start_time": "{{ ds }}T00:00:00Z",
                    "end_time": "{{ macros.ds_add(ds, 3) }}T00:00:00Z",
                },
            )

    end = EmptyOperator(task_id="end")

    start >> extract_group >> end

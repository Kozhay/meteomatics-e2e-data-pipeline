from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


@dag(
    dag_id="sfsdfsdf",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["extract"]
)
def sf_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> end

dag = sf_dag()
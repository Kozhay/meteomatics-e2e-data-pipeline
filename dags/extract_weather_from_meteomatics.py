from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from tasks.meteomatics_pipeline.meteomatics_get_data import WeatherDataFetcher

city_list = ["Tallinn, Estonia", "Amsterdam, Netherlands"]

@task
def fetch(city_name: str, run_time: str) -> dict:
    return WeatherDataFetcher(city_name, run_time).fetch()

@task
def validate(raw: dict) -> dict:
    return WeatherDataFetcher(
        raw["city_name"], raw["run_time"]
    ).validate(raw)

@task
def save(raw: dict) -> str:
    return WeatherDataFetcher(
        raw["city_name"], raw["run_time"]
    ).save_to_json(raw)

@task_group(group_id="extract_weather")
def extract_weather_group():
    for city in city_list:
        slug = city.lower().replace(",", "").replace(" ", "_")
        fetched = fetch.override(task_id=f"get_{slug}")(
            city_name=city,
            run_time="{{ ds }}"
        )
        validated = validate.override(task_id=f"validate_{slug}")(fetched)
        save.override(task_id=f"save_{slug}")(validated)

@dag(
    dag_id="extract_weather_from_meteomatics",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=1,
    tags=["extract"],
)
def weather_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    group = extract_weather_group()
    start >> group >> end

dag = weather_dag()
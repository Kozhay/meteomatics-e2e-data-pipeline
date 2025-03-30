from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from tasks.meteomatics_pipeline.meteomatics_get_data import WeatherDataFetcher

locations_list = ["Tallinn, Estonia", "Amsterdam, Netherlands", "Berlin, Germany"]

@task
def fetch(location_name: str, run_time: str) -> dict:
    return WeatherDataFetcher(location_name, run_time).fetch()

@task
def validate(raw: dict) -> dict:
    return WeatherDataFetcher(
        raw["location_name"], raw["run_time"]
    ).validate(raw)

@task
def save(raw: dict) -> str:
    s3_bucket = "meteomatics-data-raw"
    return WeatherDataFetcher(
        raw["location_name"], raw["run_time"]
    ).save_to_s3_stage(raw, s3_bucket)

@task_group(group_id="extract_weather")
def extract_weather_group():
    for location in locations_list:
        slug = location.lower().replace(",", "").replace(" ", "_")
        fetched = fetch.override(task_id=f"get_{slug}")(
            location_name=location,
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
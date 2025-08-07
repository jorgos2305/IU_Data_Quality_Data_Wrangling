import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from pipelines.openweather import OpeanWeatherClient
from pipelines.alphavantage import AlphaVantageClient
from pipelines.earthquakes import EarthQuakeClient
from datastore.datastore import DataStore

default_args = {
    "owner": "jorgos2305",
    "depends_on_past": False,
    "retries": 0, # to avoid retries in case of timeouts with the APIs
    "retry_delay": timedelta(minutes=5),
}

berlin_tz = pendulum.timezone("Europe/Berlin")
start_date = pendulum.datetime(year=2025, month=8, day=8, hour=0, minute=0, second=0, tz=berlin_tz)

dag_weather = DAG(
    dag_id="open_weather_pipeline",
    default_args={**default_args, "start_date": start_date},
    schedule=timedelta(hours=2),
    catchup=False,
    tags=["weather"]
)

def fetch_weather():
    weather_result = OpeanWeatherClient().fetch()
    DataStore().store("weather", weather_result, "split_on")

PythonOperator(
    task_id="fetch_weather",
    python_callable=fetch_weather,
    dag=dag_weather,
)

dag_daily = DAG(
    dag_id="daily_clients_pipeline",
    default_args={**default_args, "start_date": start_date.add(hours=6)},
    schedule="0 6 * * *",  # daily at 12:00 local time
    catchup=False,
    tags=["earthquake", "stocks"]
)

def fetch_quakes():
    earthquake_result = EarthQuakeClient().fetch()
    DataStore().store("earthquake", earthquake_result, "split_on")

def fetch_stocks():
    stocks_result = AlphaVantageClient().fetch()
    DataStore().store("stocks", stocks_result, "split_on")

PythonOperator(
    task_id="fetch_earthquake",
    python_callable=fetch_quakes,
    dag=dag_daily,
)

PythonOperator(
    task_id="fetch_stocks",
    python_callable=fetch_stocks,
    dag=dag_daily,
)

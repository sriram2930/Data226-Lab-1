"""
Weather ETL DAG
Pulls daily weather for two cities from the Open-Meteo API and loads it
into Snowflake (RAW.WEATHER_DAILY). When it finishes, it triggers the dbt DAG.

Airflow connection used: snowflake_conn
Airflow variables used:  weather_cities (JSON list), weather_past_days
"""

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

API_URL = "https://api.open-meteo.com/v1/forecast"
TARGET_TABLE = "RAW.WEATHER_DAILY"


def get_snowflake_cursor():
    # Uses the Airflow connection instead of hardcoding credentials
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract():
    # City list and number of past days come from Airflow variables
    cities = Variable.get("weather_cities", deserialize_json=True)
    past_days = int(Variable.get("weather_past_days", default_var=60))

    raw_data = []
    for city in cities:
        params = {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
                     "precipitation_sum,wind_speed_10m_max",
            "past_days": past_days,
            "forecast_days": 1,
            "timezone": "auto",
        }
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        raw_data.append({"city": city, "data": response.json()})

    return raw_data


@task
def transform(raw_data):
    # Flatten the API response into one row per city per day
    records = []
    for item in raw_data:
        city = item["city"]
        daily = item["data"]["daily"]

        for i in range(len(daily["time"])):
            records.append((
                city["name"],
                city["latitude"],
                city["longitude"],
                daily["time"][i],
                daily["temperature_2m_max"][i],
                daily["temperature_2m_min"][i],
                daily["temperature_2m_mean"][i],
                daily["precipitation_sum"][i],
                daily["wind_speed_10m_max"][i],
            ))

    return records


@task
def load(records):
    cur = get_snowflake_cursor()

    try:
        # Everything below runs in one transaction
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                CITY            VARCHAR(50)  NOT NULL,
                LATITUDE        FLOAT        NOT NULL,
                LONGITUDE       FLOAT        NOT NULL,
                DATE            DATE         NOT NULL,
                TEMP_MAX        FLOAT,
                TEMP_MIN        FLOAT,
                TEMP_MEAN       FLOAT,
                PRECIPITATION   FLOAT,
                WIND_SPEED_MAX  FLOAT,
                LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (CITY, DATE)
            );
        """)

        # Idempotency: remove any rows for the same city + date range first,
        # so running the DAG twice never creates duplicates
        cities = sorted(set(r[0] for r in records))
        min_date = min(r[3] for r in records)
        max_date = max(r[3] for r in records)

        for city in cities:
            cur.execute(
                f"DELETE FROM {TARGET_TABLE} WHERE CITY = %s AND DATE BETWEEN %s AND %s",
                (city, min_date, max_date),
            )

        cur.executemany(
            f"""INSERT INTO {TARGET_TABLE}
                (CITY, LATITUDE, LONGITUDE, DATE, TEMP_MAX, TEMP_MIN,
                 TEMP_MEAN, PRECIPITATION, WIND_SPEED_MAX)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            records,
        )

        cur.execute("COMMIT;")
        print(f"Loaded {len(records)} rows into {TARGET_TABLE}")

    except Exception as e:
        # If anything fails, undo the whole load and fail the task
        cur.execute("ROLLBACK;")
        print(f"Load failed, rolled back: {e}")
        raise e

    finally:
        cur.close()


with DAG(
    dag_id="weather_etl",
    start_date=datetime(2026, 9, 1),
    schedule="0 6 * * *",   # every day at 6 AM UTC
    catchup=False,
    tags=["ETL", "weather"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
) as dag:

    raw = extract()
    rows = transform(raw)
    loaded = load(rows)

    # Start the dbt DAG only after the ETL finishes successfully
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="weather_dbt",
        wait_for_completion=False,
    )

    loaded >> trigger_dbt

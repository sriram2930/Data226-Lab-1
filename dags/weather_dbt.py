"""
Weather dbt DAG
Runs dbt run -> dbt test -> dbt snapshot on the weather data.
This DAG has no schedule of its own. The weather_etl DAG triggers it
right after the ETL finishes, so dbt always runs after fresh data lands.

Airflow connection used: snowflake_conn (credentials passed to dbt as env vars)
"""

from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"

# Read Snowflake details from the Airflow connection
conn = BaseHook.get_connection("snowflake_conn")
extra = conn.extra_dejson

dbt_env = {
    "DBT_USER": conn.login or "",
    "DBT_PASSWORD": conn.password or "",
    "DBT_ACCOUNT": extra.get("account", ""),
    "DBT_ROLE": extra.get("role", ""),
    "DBT_WAREHOUSE": extra.get("warehouse", ""),
    "DBT_DATABASE": extra.get("database", ""),
}

with DAG(
    dag_id="weather_dbt",
    start_date=datetime(2026, 9, 1),
    schedule=None,          # triggered by weather_etl
    catchup=False,
    tags=["ELT", "dbt", "weather"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        env=dbt_env,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        env=dbt_env,
        append_env=True,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"dbt snapshot --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        env=dbt_env,
        append_env=True,
    )

    dbt_run >> dbt_test >> dbt_snapshot

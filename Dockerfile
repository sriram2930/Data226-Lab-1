FROM apache/airflow:2.10.5

# Snowflake provider for Airflow (pinned to versions that match Airflow 2.10.5)
RUN pip install --no-cache-dir "apache-airflow==2.10.5" apache-airflow-providers-snowflake requests \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

# dbt goes in its own virtual environment so it doesn't clash with Airflow's packages
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-snowflake~=1.9.0"

# Makes the "dbt" command available everywhere in the container
ENV PATH="${PATH}:/home/airflow/dbt_venv/bin"

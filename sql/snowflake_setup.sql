-- Run this once in a Snowflake worksheet before starting Airflow
-- Change USER_DB_YOURNAME to your own database name

CREATE DATABASE IF NOT EXISTS USER_DB_YOURNAME;
USE DATABASE USER_DB_YOURNAME;

CREATE SCHEMA IF NOT EXISTS RAW;        -- raw data loaded by the Airflow ETL
CREATE SCHEMA IF NOT EXISTS ANALYTICS;  -- tables built by dbt
CREATE SCHEMA IF NOT EXISTS SNAPSHOT;   -- dbt snapshot tables

-- Raw daily weather table (the ETL DAG also creates this if it is missing)
CREATE TABLE IF NOT EXISTS RAW.WEATHER_DAILY (
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

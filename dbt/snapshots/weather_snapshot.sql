-- Snapshot: keeps history when a day's weather values change
-- (for example, today's numbers get updated on the next ETL run)

{% snapshot weather_snapshot %}

{{
    config(
        target_schema='SNAPSHOT',
        unique_key='weather_id',
        strategy='check',
        check_cols=['temp_max', 'temp_min', 'temp_mean', 'precipitation']
    )
}}

select * from {{ ref('stg_weather') }}

{% endsnapshot %}

-- Analytics model: daily weather metrics per city
--   temp_7day_avg      = 7-day moving average of mean temperature
--   temp_anomaly       = how far today is from the city's average temperature
--   rainfall_7day      = total rain over the last 7 days
--   dry_spell_length   = number of dry days in a row (rain < 1 mm)

with base as (
    select
        *,
        case when precipitation < 1 then 1 else 0 end as is_dry_day
    from {{ ref('stg_weather') }}
),

-- every wet day starts a new group, so dry days in a row share a group number
dry_groups as (
    select
        *,
        sum(case when is_dry_day = 0 then 1 else 0 end)
            over (partition by city order by weather_date
                  rows between unbounded preceding and current row) as wet_group
    from base
)

select
    weather_id,
    city,
    weather_date,
    temp_max,
    temp_min,
    temp_mean,
    temp_range,
    precipitation,
    wind_speed_max,

    round(avg(temp_mean) over (partition by city order by weather_date
          rows between 6 preceding and current row), 2)            as temp_7day_avg,

    round(temp_mean - avg(temp_mean) over (partition by city), 2)  as temp_anomaly,

    round(sum(precipitation) over (partition by city order by weather_date
          rows between 6 preceding and current row), 2)            as rainfall_7day,

    case
        when is_dry_day = 1 then
            sum(is_dry_day) over (partition by city, wet_group order by weather_date
                                  rows between unbounded preceding and current row)
        else 0
    end                                                            as dry_spell_length

from dry_groups

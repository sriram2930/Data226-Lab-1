-- Staging model: clean up the raw table and add a unique id

select
    city || '_' || to_char(date, 'YYYY-MM-DD') as weather_id,
    city,
    date                                       as weather_date,
    round(temp_max, 1)                         as temp_max,
    round(temp_min, 1)                         as temp_min,
    round(temp_mean, 1)                        as temp_mean,
    round(temp_max - temp_min, 1)              as temp_range,
    coalesce(precipitation, 0)                 as precipitation,
    wind_speed_max,
    loaded_at
from {{ source('raw', 'weather_daily') }}
where temp_mean is not null

{{ config(materialized='view') }}

with air_quality as (
  select
    station_id,
    observed_at,
    max(value_numeric) filter (where indicator_code = 'so2') as so2,
    max(value_numeric) filter (where indicator_code = 'no2') as no2,
    max(value_numeric) filter (where indicator_code = 'co') as co,
    max(value_numeric) filter (where indicator_code = 'o3') as o3,
    max(value_numeric) filter (where indicator_code = 'pm10') as pm10,
    max(value_numeric) filter (where indicator_code = 'pm2_5') as pm2_5,
    max(value_numeric) filter (where indicator_code = 'temp') as temp,
    max(value_numeric) filter (where indicator_code = 'hum') as hum,
    max(value_numeric) filter (where indicator_code = 'rain') as rain,
    max(value_numeric) filter (where indicator_code = 'wd10') as wd10,
    max(value_numeric) filter (where indicator_code = 'ws10') as ws10
  from {{ ref('stg_airviro_measurement') }}
  where source_type = 'air_quality'
  group by station_id, observed_at
)
select
  aq.station_id,
  aq.observed_at,
  dt.date_value,
  dt.year_number,
  dt.month_number,
  dt.month_name,
  dt.day_number,
  dt.day_name,
  dt.day_of_week_number,
  dt.hour_number,
  aq.so2,
  aq.no2,
  aq.co,
  aq.o3,
  aq.pm10,
  aq.pm2_5,
  aq.temp,
  aq.wd10,
  aq.ws10,
  wd.sector_code as wind_sector,
  wd.sector_name as wind_sector_name,
  aq.hum,
  aq.rain
from air_quality as aq
left join {{ ref('dim_datetime_hour') }} as dt
  on dt.observed_at = aq.observed_at
left join {{ ref('dim_wind_direction') }} as wd
  on (
    (wd.wraps_around and (aq.wd10 >= wd.min_degree or aq.wd10 < wd.max_degree))
    or
    ((not wd.wraps_around) and aq.wd10 >= wd.min_degree and aq.wd10 < wd.max_degree)
  )

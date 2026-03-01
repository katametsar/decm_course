{{ config(materialized='table') }}

select distinct
  observed_at,
  observed_at::date as date_value,
  extract(year from observed_at)::integer as year_number,
  extract(quarter from observed_at)::integer as quarter_number,
  extract(month from observed_at)::integer as month_number,
  trim(to_char(observed_at, 'Month')) as month_name,
  extract(day from observed_at)::integer as day_number,
  extract(hour from observed_at)::integer as hour_number,
  extract(week from observed_at)::integer as iso_week_number,
  extract(isodow from observed_at)::integer as day_of_week_number,
  trim(to_char(observed_at, 'Dy')) as day_name
from {{ ref('stg_airviro_measurement') }}


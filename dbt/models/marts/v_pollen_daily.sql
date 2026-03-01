{{ config(materialized='view') }}

select
  station_id,
  observed_at,
  observed_at::date as observed_date,
  indicator_code,
  indicator_name,
  value_numeric
from {{ ref('stg_airviro_measurement') }}
where source_type = 'pollen'


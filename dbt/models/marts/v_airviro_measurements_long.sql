{{ config(materialized='view') }}

select
  source_type,
  station_id,
  observed_at,
  indicator_code,
  indicator_name,
  value_numeric,
  extracted_at
from {{ ref('stg_airviro_measurement') }}


{{ config(materialized='view') }}

select
  source_type,
  station_id,
  observed_at,
  indicator_code,
  indicator_name,
  value_numeric,
  source_row_hash,
  extracted_at
from {{ source('raw', 'airviro_measurement') }}


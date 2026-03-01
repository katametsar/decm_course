{{ config(materialized='table') }}

select distinct
  source_type,
  indicator_code,
  indicator_name
from {{ ref('stg_airviro_measurement') }}


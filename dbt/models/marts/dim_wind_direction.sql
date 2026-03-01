{{ config(materialized='table') }}

select
  cast(sector_id as integer) as sector_id,
  cast(sector_code as text) as sector_code,
  cast(sector_name as text) as sector_name,
  cast(min_degree as double precision) as min_degree,
  cast(max_degree as double precision) as max_degree,
  cast(wraps_around as boolean) as wraps_around
from {{ ref('dim_wind_direction_seed') }}


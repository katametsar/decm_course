-- Fail when source_type contains values outside the course contract.
select distinct
  source_type
from {{ ref('stg_airviro_measurement') }}
where source_type not in ('air_quality', 'pollen')


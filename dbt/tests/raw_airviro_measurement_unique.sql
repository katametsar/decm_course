-- Fail when raw natural keys are duplicated.
select
  source_type,
  station_id,
  observed_at,
  indicator_code,
  count(*) as duplicate_count
from {{ source('raw', 'airviro_measurement') }}
group by source_type, station_id, observed_at, indicator_code
having count(*) > 1


{{ config(
    schema='junk2',
    materialized='view',
    meta = {
      'bigquery_load': 'true'
    }
) }}
select
  fad.partition_date_msk,
  device_id,
  first(device.os_type) as os_type,
  first(device.pref_country) as device_country
from {{ ref('dbt_fact_active_device') }} fad
-- Intentionally forgot to use ref() here in order to ensure
-- sensor dependency still works
join mart.dim_device_min device using (device_id)
where
  effective_ts <= date_add(fad.partition_date_msk, 1)
  and date_add(fad.partition_date_msk, 1) < next_effective_ts
group by fad.partition_date_msk, device_id

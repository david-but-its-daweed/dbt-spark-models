{{ config(
    schema='mart',
    materialized='view',
) }}
WITH store_performance AS (SELECT *, row_number() over (partition by id order by effective_ts desc) as rn
                           FROM {{ source('default', 'sat_store_performance') }})
SELECT store_id,
       created_time,
       updated_time,
       name,
       merchant_id,
       specialization_id,
       CASE WHEN is_top is null THEN false ELSE is_top END     as is_top,
       CASE WHEN is_power is null THEN false ELSE is_power END as is_power,
       is_active,
       enabled_by_merchant,
       enabled_from_merchant,
       enabled_by_joom,
       disabling_reason,
       CASE
           WHEN sp.status = 0 THEN 'Unknown'
           WHEN sp.status = 1 THEN 'Perfect'
           WHEN sp.status = 2 THEN 'Average'
           WHEN sp.status = 3 THEN 'Poor'
           WHEN sp.status = 4 THEN 'Critical' END              as status
FROM {{ source('mongo', 'store') }} ms
         LEFT OUTER JOIN store_performance sp ON ms.store_id = sp.id
WHERE coalesce(sp.rn, 1) = 1
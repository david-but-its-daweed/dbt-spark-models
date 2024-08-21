{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='table'
) }}

SELECT platform,
       table_name,
       to_date(CASE
                WHEN hour(coalesce(start_time, dttm)) >= 22 THEN date_trunc('Day', coalesce(start_time, dttm)) + interval 24 hours
                ELSE date_trunc('Day', coalesce(start_time, dttm))
       END) date,
       min(dttm) fate_table_dttm,
       min(start_time) fate_table_start_time
FROM platform.fact_table_update t
WHERE coalesce(start_time, dttm) > NOW() - interval 91 days
GROUP BY platform, 
         table_name,
         to_date(CASE
                     WHEN hour(coalesce(start_time, dttm)) >= 22 THEN date_trunc('Day', coalesce(start_time, dttm)) + interval 24 hours
                     ELSE date_trunc('Day', coalesce(start_time, dttm))
         END)

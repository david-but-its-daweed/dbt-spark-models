{{ config(
    schema='category_management',
    materialized='table',
    meta = {
      'model_owner' : '@troyanovskaya',
      'team': 'category_management',
      'bigquery_load': 'true',
    }
) }}


-- статусы (see https://www.notion.so/joomteam/53e8529cdc924c64ab7d1a653548f6c2 ):
--   0 = диагностики нет
--   1 = диагностика есть, но неактивна
--   2 = запрещённое состояние, не бывает
--   3 = диагностика активна.
 select
    product_id,
    diag_code
from 
    (
    select 
        *,
         coalesce (lead(event_ts) over (partition by product_id, diag_code, diag_digest order by event_ts asc), "9999-12-31 23:59:59") as next_effective_ts
    from {{ source('product', 'diagnoses_changes') }}
    where true
        and partition_date > "2022-01-11" -- hardcoded; there are no J1178 earlier
        and diag_code  in ('J1178')
    )
    where true
        and next_effective_ts > current_date() + interval 1 day
        and diag_now_state = 3
    group by 1, 2

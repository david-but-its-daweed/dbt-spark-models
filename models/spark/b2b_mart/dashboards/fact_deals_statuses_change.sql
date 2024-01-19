{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}
with 
statuses as 
(select status,
    id as status_int
from {{ ref('key_issue_status')}}),

t as (select distinct 
deal_id, 
status,
min_date,
status_int,
lead(min_date) over (partition by deal_id order by min_date) as next_status_date,
lag(min_date) over (partition by deal_id order by min_date) as prev_status_date,
lead(status) over (partition by deal_id order by min_date) as next_status,
lag(status_int) over (partition by deal_id order by min_date) as prev_status_int,
lead(status_int) over (partition by deal_id order by min_date) as next_status_int,
lag(status) over (partition by deal_id order by min_date) as prev_status
from
(
select distinct 
deal_id, status, status_int, min_date
from
(select distinct
status as current_status,
min(date) over (partition by deal_id, status) as min_date,
deal_id
from
(
select deal_id, millis_to_ts_msk(statuses.ctms) as date, 
statuses.status as status, statuses.moderatorId as owner_id
from
(select entityId as deal_id, explode(statusHistory) as statuses, etms
    from {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}
    where type = 4 
    )
    order by date desc, deal_id
)
) d left join statuses s on s.status_int = d.current_status
)
     )

select * from t

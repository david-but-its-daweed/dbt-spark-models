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
date as min_date,
status_int,
lead(date) over (partition by deal_id order by date) as next_status_date,
lag(date) over (partition by deal_id order by date) as prev_status_date,
lead(status) over (partition by deal_id order by date) as next_status,
lag(status_int) over (partition by deal_id order by date) as prev_status_int,
lead(status_int) over (partition by deal_id order by date) as next_status_int,
lag(status) over (partition by deal_id order by date) as prev_status
from
(
    select distinct 
    deal_id, status, status_int, date
    from
    (select distinct
    status as current_status,
    lead(status) over (partition by deal_id order by date) as lead_status,
    date,
    deal_id
    from
    (
    select deal_id, millis_to_ts_msk(statuses.ctms) as date, 
    statuses.status as status, statuses.moderatorId as owner_id
    from
        (
            select entityId as deal_id, explode(statusHistory) as statuses, etms
            from {{ source('mongo', 'b2b_core_issues_daily_snapshot') }} i
            left join {{ ref('key_issue_type') }} it ON CAST(i.type AS INT) =  CAST(it.id AS INT)
            where it.type like '%CloseTheDeal%'
        )
        order by date desc, deal_id
        )
    ) d left join statuses s on s.status_int = d.current_status
    where status != lead_status or lead_status is null
)
)

select * from t

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


with concats as 
(
select
explode(contacts) as col,
leadId as lead_id
from {{ source('mongo', 'b2b_core_amo_crm_raw_leads_daily_snapshot') }}
),

phones as (
select explode(col.phones) as phone,
       col._id as contact_id
from concats
)

select lead_id,
        col._id as contact_id,
        col.firstName as first_name,
        col.lastName as last_name,
        col.name as name,
        phone
from concats
left join phones on col._id = phones.contact_id

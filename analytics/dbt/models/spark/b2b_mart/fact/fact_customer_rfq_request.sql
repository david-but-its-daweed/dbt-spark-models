{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'false'
    }
) }}


select 
_id as rfq_request_id,
friendlyId as rfq_friendly_id,
crid as customer_request_id,
categories[0] as category_id,
category_name,
isTop as is_top,
descr,
manufacturingDays as manufacturing_days,
qty as quantity,
name as product_name,
plnk as link,
model,
price.amount as price,
price.ccy as currency,
questionnaire,
status,
parentId as parent_id,
millis_to_ts_msk(ctms) as created_time,
millis_to_ts_msk(utms) as updated_time

from {{ ref('scd2_customer_rfq_request_snapshot') }} rfq
left join 
(
select category_id, name as category_name
    from {{ source('mart', 'category_levels') }}
) cat on rfq.categories[0] = cat.category_id
where dbt_valid_to is null

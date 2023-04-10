{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}



with products as 
(select count(distinct _id) as products, merchantId 
from {{ source('mongo', 'b2b_core_published_products_daily_snapshot') }}
group by merchantId
),

merchant_type as (
select distinct _id, type from {{ source('mongo', 'b2b_core_merchant_appendixes_daily_snapshot') }}
),

rfq as (
select count(distinct order_rfq_response_id) as rfq_responses,
merchant_id,
count(distinct rfq_request_id) as rfq_requests
from {{ ref('fact_rfq_response') }}
where next_effective_ts_msk is null
group by merchant_id)

select distinct m._id as merchant_id, companyName as company_name, name, 
type, products,
rfq_responses,
rfq_requests,
DATE(TIMESTAMP_MILLIS(createdTimeMs)) as created_time
from {{ source('b2b_mart', 'dim_merchant') }} m
left join products p on p.merchantId = m._id
left join merchant_type mt on mt._id = m._id
left join rfq r on r.merchant_id = m._id
where next_effective_ts >= '3030-01-01'

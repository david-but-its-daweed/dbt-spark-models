{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


select 
_id as customer_request_id,
categoryId as category_id,
category_name,
millis_to_ts_msk(ctms) as created_time,
dealId as deal_id,
desc,
expectedQuantity as expected_quantity,
link,
model,
pricePerItem.amount as price_per_item,
pricePerItem.ccy as currency,
c.status as price_type,
productName as product_name,
questionnaire,
rejectReason as reject_reason,
k.status,
userId as user_id,
millis_to_ts_msk(utms) as updated_time

from {{ ref('scd2_customer_requests_snapshot') }} rfq
left join 
(
select category_id, name as category_name
    from {{ source('mart', 'category_levels') }}
) cat on rfq.categoryId = cat.category_id
left join {{ ref('key_customer_request_status') }} k on rfq.status = cast(k.id as int)
left join {{ ref('key_calc_price_types') }} c on rfq.priceType = cast(c.id as int)
where dbt_valid_to is null

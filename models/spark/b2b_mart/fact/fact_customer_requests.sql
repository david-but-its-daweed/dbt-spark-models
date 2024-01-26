{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}


WITH country AS (
    SELECT DISTINCT deal_id, country
    FROM {{ ref('fact_deals') }}
)


select 
    _id as customer_request_id,
    categoryId as category_id,
    category_name,
    millis_to_ts_msk(ctms) as created_time,
    dealId as deal_id,
    country,
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
    plannedOfferCost.amount as planned_offer_cost,
    plannedOfferCost.ccy as planned_offer_currency,
    userId as user_id,
    case when request.type = 1 then TRUE else FALSE end as fake_door,
    millis_to_ts_msk(utms) as updated_time,
    TIMESTAMP(dbt_valid_from) as effective_ts_msk,
    TIMESTAMP(dbt_valid_to) as next_effective_ts_msk
from {{ ref('scd2_customer_requests_snapshot') }} request
left join country AS c ON request.dealId = c.deal_id
left join 
(
select category_id, name as category_name
    from {{ source('mart', 'category_levels') }}
) cat on request.categoryId = cat.category_id
left join {{ ref('key_customer_request_status') }} k on request.status = cast(k.id as int)
left join {{ ref('key_calc_price_types') }} c on request.priceType = cast(c.id as int)

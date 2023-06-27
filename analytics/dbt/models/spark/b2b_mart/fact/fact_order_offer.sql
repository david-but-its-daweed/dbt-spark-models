{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}



select 
        op.offer_product_id,
        op.product_id,
        op.offer_id,
        op.trademark,
        op.hs_code,
        op.manufacturer_id,
        op.type as product_type,
        op.disabled,
        op.created_time_msk as order_product_created_time,
        co.customer_request_id,
        co.deal_id,
        co.merchant_id,
        co.offer_type,
        co.order_id,
        co.status as offer_status,
        co.user_id,
        co.created_time as offer_created_time
    from {{ ref('scd2_offer_products_snapshot') }} op
    left join {{ ref('fact_customer_offers') }} co on op.offer_id = co.offer_id
    where dbt_valid_to is null

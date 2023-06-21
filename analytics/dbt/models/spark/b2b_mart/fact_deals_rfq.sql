{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


with offers as (
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
)

select distinct
    rr.rfq_request_id,
    rr.rfq_friendly_id,
    rr.customer_request_id,
    cr.deal_id,
    cr.user_id,
    rr.category_name,
    rr.is_top,
    rr.descr,
    rr.status as rfq_request_status,
    rr.parent_id as rfq_parent_id,
    rr.created_time as rfq_created_time,
    rp.order_rfq_response_id,
    rp.documents_attached,
    rp.created_ts_msk as response_created_ts_msk,
    rp.merchant_id,
    rp.product_id,
    rp.reject_reason,
    rp.reject_reason_group,
    rp.product_in_stock,
    rp.manufacturing_days,
    rp.sent,
    rp.status as response_status,
    co.offer_product_id,
    co.order_product_created_time,
    co.offer_id,
    co.offer_type,
    co.disabled,
    d.deal_type,
    d.estimated_date as deal_estimated_date,
    d.estimated_gmv as deal_estimated_gmv,
    d.interaction_id,
    d.owner_email,
    owner_role,
    deal_name,
    d.source,
    d.type,
    d.campaign,
    d.min_date_payed,
    d.status as deal_status,
    d.min_date as deal_created_time,
    retention,
    grade,
    country
from {{ ref('fact_customer_rfq_request') }} rr 
left join {{ ref('fact_customer_rfq_response') }} rp on rr.rfq_request_id = rp.rfq_request_id
left join offers co on rr.customer_request_id = co.customer_request_id and rp.product_id = co.product_id
left join {{ ref('fact_customer_requests') }} cr on rr.customer_request_id = cr.customer_request_id
left join {{ ref('fact_deals') }} d on cr.deal_id = d.deal_id
where d.partition_date_msk = (select max(partition_date_msk) from {{ ref('fact_deals') }})

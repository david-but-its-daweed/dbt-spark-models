{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}

with psi as (
    select distinct 
        _id, statusId as status_id from {{ source('mongo', 'b2b_core_form_with_status_daily_snapshot') }}
)

SELECT order_product_id,
       product_id,
       attachments,
       currency,
       danger_kind_set,
       deal_id,
       description,
       disabled,
       duty,
       export_type,
       product_friendly_id,
       hs_code,
       link,
       link_to_calculation,
       manufacturer_id,
       merchant_order_id,
       name,
       name_ru,
       packaging,
       prices,
       production_dead_line,
       psi_status_id,
       status_id as psi_status,
       status,
       statuses,
       trademark,
       type,
       variants,
       vat_rate,
       created_ts_msk
from {{ ref('scd2_mongo_order_products') }} t
left join psi on psi_status_id = _id
where TIMESTAMP(dbt_valid_to) IS NULL

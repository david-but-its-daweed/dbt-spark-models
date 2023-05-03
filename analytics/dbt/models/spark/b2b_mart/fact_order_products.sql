{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}


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
       hsCode AS hs_code,
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
       status,
       statuses,
       trademark,
       type,
       variants,
       vat_rate,
       created_ts_msk
from {{ ref('scd2_mongo_order_products') }} t
where TIMESTAMP(dbt_valid_to) IS NULL

{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}


SELECT
    op.offer_product_id,
    op.product_id,
    op.offer_id,
    op.trademark,
    op.hs_code,
    op.manufacturer_id,
    op.name AS product_name,
    op.type AS product_type,
    op.disabled,
    op.is_certification_required,
    op.is_agency_registration_required,
    op.agency_name,
    op.created_time_msk AS offer_product_created_time,
    co.customer_request_id,
    co.deal_id,
    co.merchant_id,
    co.offer_type,
    co.order_id,
    co.status AS offer_status,
    co.user_id,
    co.created_time AS offer_created_time,
    TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
    TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_offer_products_snapshot') }} AS op
LEFT JOIN (
    SELECT DISTINCT *
    FROM {{ ref('fact_customer_offers') }}
    WHERE next_effective_ts_msk IS NULL
) AS co ON op.offer_id = co.offer_id

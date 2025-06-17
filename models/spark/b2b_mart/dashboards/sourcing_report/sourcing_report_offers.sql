{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}

WITH deals AS (
    SELECT DISTINCT deal_id
    FROM {{ ref('sourcing_report_issues') }}
),
     moderator AS (
    SELECT co.offer_id,
           du.email AS offer_created_email
    FROM {{ ref('fact_customer_offers') }} AS co
    LEFT JOIN {{ ref('dim_user_admin') }} AS du ON co.moderator_id = du.admin_id
    WHERE co.next_effective_ts_msk IS NULL
)

  
SELECT dp.deal_id,
       dp.deal_friendly_id,
       dp.user_id,
       dp.country,
       dp.customer_request_id,
       dp.offer_id,
       fop.offer_status,
       dp.offer_product_id,
       dp.product_id,
       dp.order_id,
       fo.created_ts_msk AS order_created_time,
       dp.order_friendly_id,
       dp.merchant_order_id,
       dp.merchant_order_friendly_id,
       dp.merchant_id,
       dp.order_product_id,
       dp.product_friendly_id,
       mod.offer_created_email,
       dp.owner_email, --AS deal_owner_email,
       dp.owner_role, --AS deal_owner_role,
       CASE
           WHEN fop.offer_status = 'rejected' AND dp.order_id IS NOT NULL THEN 'Оффер изначально был в заказе, но его убрали'
           WHEN fop.offer_status = 'approved' AND fop.offer_created_time > fo.created_ts_msk THEN 'Оффер добавили после создания заказа'
       END AS offer_status_manual,
       MAX(CASE WHEN dp.customer_request_id IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY dp.deal_id) AS is_deal_has_customer_request,
       MAX(CASE WHEN dp.order_id IS NOT NULL AND dp.customer_request_id IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY dp.deal_Id) AS is_deal_with_order_without_customer_request
FROM {{ ref('dim_deal_products') }} AS dp
JOIN deals AS d ON dp.deal_id = d.deal_id
LEFT JOIN {{ ref('fact_offer_product') }} AS fop ON fop.next_effective_ts_msk IS NULL AND dp.offer_id = fop.offer_id
LEFT JOIN {{ ref('fact_order') }} AS fo ON fo.next_effective_ts_msk IS NULL AND dp.order_id = fo.order_id
LEFT JOIN moderator AS mod ON dp.offer_id = mod.offer_id

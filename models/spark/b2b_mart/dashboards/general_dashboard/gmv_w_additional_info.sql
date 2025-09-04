{{
  config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@tigran',
      'bigquery_load': 'true'
    },
  )
}}

SELECT
    gmv.order_id,
    d.deal_id,
    d.deal_created_date,
    gmv.user_id,
    gmv.t AS date_msk,
    gmv.country,
    d.deal_status_group,
    d.deal_status,
    d.deal_type,
    d.utm_source,
    d.utm_campaign,
    d.source,
    d.type,
    d.number_user_deal,
    gmv.gmv_initial,
    gmv.initial_gross_profit,
    gmv.final_gross_profit
FROM {{ ref('gmv_by_sources') }} AS gmv
LEFT JOIN {{ ref('fact_deals_with_requests') }} AS d USING (order_id)
UNION ALL
SELECT
    gmv_kz.procurement_order_id AS order_id,
    gmv_kz.deal_id,
    DATE(gmv_kz.created_ts) AS deal_created_date,
    dl.user_id,
    DATE(gmv_kz.sub_status_waiting_for_payment_ts) AS date_msk,
    gmv_kz.country,
    dl.deal_status_group,
    dl.deal_status,
    dl.deal_type,
    dl.utm_source,
    dl.utm_campaign,
    dl.source,
    dl.type,
    dl.number_user_deal,
    gmv_kz.gmv_usd AS gmv_initial,
    0 AS initial_gross_profit,
    0 AS final_gross_profit
FROM {{ ref('purchasing_and_production_report') }} AS gmv_kz
LEFT JOIN {{ ref('fact_deals_with_requests') }} AS dl USING (deal_id)
WHERE
    gmv_kz.country = 'KZ'
    AND (
        (gmv_kz.is_small_batch = 1 AND gmv_kz.sub_status_waiting_for_payment_ts IS NOT NULL)
        OR (gmv_kz.is_small_batch = 0 AND COALESCE(gmv_kz.sub_status_client_payment_received_ts, gmv_kz.sub_status_manufacturing_ts) IS NOT NULL)
    )

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


SELECT
    _id AS shipment_deal_order_id,
    shipmentId AS shipment_order_id,
    dealId AS deal_id,
    hsLastMileOrderID AS hs_last_mile_order_id,
    hsSource AS hs_source,
    country,
    countOrderProductsInDeal,
    operationalProductIds,
    isEmpty AS is_empty,
    isFull AS is_full,
    MILLIS_TO_TS_MSK(ctms) AS created_time,
    MILLIS_TO_TS_MSK(utms) AS updated_time
FROM {{ source('mongo', 'b2b_core_shipment_deals_daily_snapshot') }}

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
    _id AS offer_id,
    brokertablelink AS broker_table_link,
    comment,
    csmrreqid AS customer_request_id,
    dealid AS deal_id,
    merchantid AS merchant_id,
    ot.type AS offer_type,
    orderid AS order_id,
    ko.status,
    userid AS user_id,
    co.originId AS origin_offer_id,
    moderatorId AS moderator_id,
    brokerprops.link AS broker_link,
    millis_to_ts_msk(ctms) AS created_time,
    millis_to_ts_msk(utms) AS updated_time,
    TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
    TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_customer_offers_snapshot') }} AS co
LEFT JOIN {{ ref('key_offer_status') }} AS ko ON co.status = CAST(ko.id AS INT)
LEFT JOIN {{ ref('key_offer_type') }} AS ot ON co.offertype = CAST(ot.id AS INT)

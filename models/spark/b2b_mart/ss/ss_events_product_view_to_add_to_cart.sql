{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH bots AS
  (SELECT device_id,
          max(1) AS bot_flag
   FROM threat.bot_devices_joompro
   WHERE is_device_marked_as_bot
     OR is_retrospectively_detected_bot
   GROUP BY 1),
       pre_data AS
  (SELECT USER['userId'] AS user_id,
              TYPE,
              event_ts_msk,
              cast(event_ts_msk AS DATE) AS event_msk_date,
              bot_flag,
              payload.pageurl,
              payload.pagename,
              lead(TYPE) OVER (PARTITION BY USER['userId'], payload.pageurl
                               ORDER BY event_ts_msk) AS lead_type,
              lead(payload.productId) OVER (PARTITION BY USER['userId'], payload.pageurl
                                            ORDER BY event_ts_msk) AS product_id
   FROM {{ source('b2b_mart', 'device_events') }}  de
   LEFT JOIN bots ON bots.device_id = de.device.id
   WHERE (TYPE IN ('productSelfServiceOpen',
                   'pageView')
          AND payload.pageurl like '%.pro/pt-br/%')),
       VIEWS AS
  (SELECT user_id,
          bot_flag,
          event_ts_msk,
          event_msk_date,
          coalesce(product_id, replace(pageurl, 'https://joom.pro/pt-br/products/', '')) AS product_id
   FROM pre_data
   WHERE pagename = 'product'), wide_data_add_to_cart AS
  (SELECT id AS event_seed,
          USER['userId'] AS user_id,
              TYPE,
              event_ts_msk,
              cast(event_ts_msk AS DATE) AS event_msk_date,
              payload.pageurl,
              payload.alreadyInCart,
              payload.productId product_id,
              explode(payload.variants)
   FROM {{ source('b2b_mart', 'device_events') }}  de
   WHERE  (TYPE IN ('addToCart')
          AND payload.pageurl like '%.pro/pt-br/%')),
                                add_to_card AS
  (SELECT user_id,
          product_id,
          event_msk_date,
          min(event_seed) AS event_seed,
          min(event_ts_msk) AS event_ts_msk,
          sum(col.quantity) AS qty,
          count(DISTINCT col.variantId) cnt_variants
   FROM wide_data_add_to_cart
   GROUP BY 1,
            2,
            3),
                                DATA AS
  (SELECT views.user_id,
          views.bot_flag,
          views.event_ts_msk,
          views.event_msk_date,
          views.product_id,
          event_seed,
          add_to_card.event_ts_msk AS add_to_card_t,
          qty,
          cnt_variants,
          (CASE
               WHEN qty IS NOT NULL THEN 1
               ELSE 0
           END) * (row_number() Over(PARTITION BY event_seed
                                     ORDER BY views.event_ts_msk)-1) check_seed
   FROM VIEWS
   LEFT JOIN add_to_card ON views.user_id = add_to_card.user_id
   AND views.product_id = add_to_card.product_id
   AND add_to_card.event_ts_msk > views.event_ts_msk)
SELECT user_id,
       bot_flag,
       event_ts_msk,
       event_msk_date,
       product_id,
       CASE
           WHEN check_seed = 0 THEN add_to_card_t
       END add_to_card_t,
       CASE
           WHEN check_seed = 0 THEN qty
       END qty,
       CASE
           WHEN check_seed = 0 THEN cnt_variants
       END cnt_variants
FROM DATA

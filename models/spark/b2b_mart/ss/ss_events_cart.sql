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


with first_level AS 
    (SELECT payload,
         payload.userId AS user_id,
         type,
         event_ts_msk,
         payload.actionType AS actionType,
         payload.cartItemIds AS cartItemIds ,
         explode(payload.productItems)
    FROM {{ source('b2b_mart', 'operational_events') }}
    WHERE partition_date > '2024-04-04'
            AND type = 'cartUpdated'
), product_level AS 
    (SELECT user_id,
         type,
         event_ts_msk,
         actionType,
         cartItemIds,
         key AS product_id,
         value.quantityByVariantId,
         explode(value.quantityByVariantId)
    FROM first_level )
SELECT user_id,
         type,
         event_ts_msk,
         cast(event_ts_msk as DATE) as event_msk_date,
         actionType,
         cartItemIds,
         product_id,
         key AS sub_product_id,
         value AS qty
FROM product_level
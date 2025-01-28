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

                 
 Select
    user['userId'] AS user_id,
    de.device.id as device_id,
    type,
    event_ts_msk,
    CAST(event_ts_msk AS DATE) AS event_msk_date,
    payload.pageUrl,
    payload.pageName,
    payload.source,
    payload.isRegistrationCompleted,
    payload.productId as product_id
    from  {{ source('b2b_mart', 'device_events') }} AS de
    where partition_date >= '2024-04-01'
         and  type in ( 'sampleCheckoutOpen',
                        'pageLeave',
                        'userProfileClick',
                        'addToCart',
                        'checkoutStartClick',
                        'checkoutFinishClick'
                        )
                            

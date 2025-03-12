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


WITH bots AS (
    SELECT device_id,
           MAX(1) AS bot_flag
    FROM threat.bot_devices_joompro
    WHERE is_device_marked_as_bot
       OR is_retrospectively_detected_bot
    GROUP BY 1
)


SELECT user['userId'] AS user_id,
       de.device.id AS device_id,
       type,
       event_ts_msk,
       CAST(event_ts_msk AS DATE) AS event_msk_date,
       payload.pageUrl,
       payload.pageName,
       payload.source,
       payload.isRegistrationCompleted,
       payload.productId AS product_id,
       payload.position
FROM {{ source('b2b_mart', 'device_events') }} AS de
LEFT JOIN bots ON de.device.id = bots.device_id
WHERE partition_date >= '2024-04-01'
  AND bot_flag IS NULL
  AND type IN (
        'click',
        'rfqOpen',
        'cartOpen',
        'addToCart',
        'pageLeave',
        'orderOpen',
        'ordersOpen',
        'productClick',
        'categoryOpen',
        'rfqSubmitForm',
        'categoryClick',
        'translateImage',
        'contactUsClick',
        'userProfileClick',
        'removeOneFromCart',
        'sampleCheckoutOpen',
        'checkoutStartClick',
        'checkoutFinishClick',
        'paymentMethodChange',
        'deliveryChannelChange',
        'contactUsRequestSubmit',
        'productVariantsPricesOpen',
        'checkoutCustomizationButtonClick',
        'requestQuoteNowCreateSuccess'
  )

{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@easaltykova',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

WITH sessions AS (
    SELECT 
        device_id,
        session_id,
        session_num,
        session_start,
        (sessions.session_start - interval '2 minute') AS session_start_minus_2,
        session_end,
        source,
        campaign,
        channel_type
    FROM {{ source('onfy', 'sessions')}}
),

devices_mart AS (

SELECT 
    sessions.device_id,
    sessions.session_id,
    sessions.session_num,
    sessions.session_start,
    session_start_minus_2,
    sessions.session_end,
    sessions.source,
    sessions.campaign,
    sessions.channel_type,
    CASE
        WHEN device.app_type = 'WEB' AND device.device_type = 'DESKTOP' THEN 'web_desktop'
        WHEN device.app_type = 'WEB' AND device.device_type in ('PHONE', 'TABLET') THEN 'web_mobile'
        WHEN device.app_type = 'ANDROID' THEN 'android'
        WHEN device.app_type = 'IOS' THEN 'ios'
        ELSE device.app_type || '_' || device.device_type
        END AS app_device_type,
    MIN(order.created) AS min_order_dt
FROM sessions
INNER JOIN {{ source('pharmacy_landing', 'device')}}
    ON device.id = sessions.device_id
LEFT JOIN {{ source('pharmacy_landing', 'order')}}
    ON device.id = order.device_id 
WHERE DATE(session_start) >= '2022-07-01'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

)

--------------------------------------------------------------------------------------------------------------------------
-- pre-SELECTing raw events
--------------------------------------------------------------------------------------------------------------------------

, minimal_involvement_temp AS ( 
    SELECT
      device_id,
      event_ts_cet AS minimal_involvement_dt,
      CASE 
        WHEN type = 'homeOpen' THEN 'main_page'
        WHEN type IN ('search', 'searchServer') THEN 'search'
        WHEN type = 'productOpen' THEN 'product_page'
        WHEN type = 'catalogOpen' THEN 'catalog'
        WHEN type = 'productPreview' AND payload.sourceScreen = 'productPageLanding' THEN 'products_list'
        ELSE null
        END AS minenv_type
    FROM {{ source('onfy_mart', 'device_events')}}
    WHERE type IN ('search', 'searchServer', 'productOpen', 'catalogOpen', 'homeOpen', 'productPreview')
        AND DATE(event_ts_cet) >= '2022-07-01'
)

, minimal_involvement AS (
  SELECT * 
  FROM minimal_involvement_temp
  WHERE minenv_type IS NOT null
)

, add_to_cart AS (
    SELECT
      device_id,
      event_ts_cet AS add_to_cart_dt
    FROM {{ source('onfy_mart', 'device_events')}}
    WHERE type = 'addToCart'
        AND DATE(event_ts_cet) >= '2022-07-01'
)

, cart_open AS (
    SELECT
      device_id,
      event_ts_cet AS cart_open_dt
    FROM {{ source('onfy_mart', 'device_events')}}
    WHERE type = 'cartOpen' 
      AND payload.productIds IS NOT null
      AND DATE(event_ts_cet) >= '2022-07-01'
)

, checkout_open AS (
    SELECT
      device_id,
      event_ts_cet AS checkout_dt
    FROM {{ source('onfy_mart', 'device_events')}}
    WHERE type = 'checkoutConfirmOpen'
        AND DATE(event_ts_cet) >= '2022-07-01'
)

, payment_start AS (
    SELECT
      device_id,
      event_ts_cet AS payment_start_dt
    FROM {{ source('onfy_mart', 'device_events')}}
    WHERE type = 'paymentStart'
        AND DATE(event_ts_cet) >= '2022-07-01'
)


, successful_payment AS (
    SELECT
      device_id,
      event_ts_cet AS payment_dt,
      (event_ts_cet - INTERVAL 2 MINUTE) AS payment_dt_2 
    FROM {{ source('onfy_mart', 'device_events')}}
    WHERE type = 'paymentCompleteServer'
        AND DATE(event_ts_cet) >= '2022-07-01'
)

--------------------------------------------------------------------------------------------------------------------------

, minenv_sessions AS (

SELECT 
    devices_mart.*,
    minimal_involvement_dt,
    minenv_type,
    ROW_NUMBER() OVER(PARTITION BY devices_mart.device_id, session_start ORDER BY minimal_involvement_dt) AS rnk_minenv
FROM devices_mart
LEFT JOIN minimal_involvement 
    ON devices_mart.device_id = minimal_involvement.device_id
    AND minimal_involvement_dt BETWEEN session_start_minus_2 AND session_end
    
)

, add_to_cart_sessions AS (

SELECT 
    minenv_sessions.*,
    add_to_cart_dt,
    ROW_NUMBER() OVER(PARTITION BY minenv_sessions.device_id, session_start ORDER BY add_to_cart_dt) AS rnk_addtocart
FROM minenv_sessions
LEFT JOIN add_to_cart
    ON minenv_sessions.device_id = add_to_cart.device_id
    AND add_to_cart_dt BETWEEN minimal_involvement_dt AND session_end
WHERE rnk_minenv = 1

)

, cart_open_sessions AS (

SELECT
    add_to_cart_sessions.*,
    cart_open_dt,
    ROW_NUMBER() OVER(PARTITION BY add_to_cart_sessions.device_id, session_start ORDER BY cart_open_dt) AS rnk_cartopen
FROM add_to_cart_sessions
LEFT JOIN cart_open
    ON add_to_cart_sessions.device_id = cart_open.device_id
    AND cart_open_dt BETWEEN add_to_cart_dt AND session_end
WHERE rnk_addtocart = 1

)

, checkout_sessions AS (

SELECT
    cart_open_sessions.*,
    checkout_dt,
    ROW_NUMBER() over(PARTITION BY cart_open_sessions.device_id, session_start ORDER BY checkout_dt) AS rnk_checkout
FROM cart_open_sessions 
LEFT JOIN checkout_open 
    ON cart_open_sessions.device_id = checkout_open.device_id
    AND checkout_dt BETWEEN cart_open_dt AND session_end
WHERE rnk_cartopen = 1

)

, payment_start_sessions AS (

SELECT
    checkout_sessions.*,
    payment_start_dt,
    ROW_NUMBER() over(PARTITION BY checkout_sessions.device_id, session_start ORDER BY payment_start_dt) AS rnk_payment_start 
FROM checkout_sessions
LEFT JOIN payment_start 
    ON checkout_sessions.device_id = payment_start.device_id
    AND payment_start_dt BETWEEN checkout_dt AND session_end
WHERE rnk_checkout = 1

)

, successful_payment_sessions as (

SELECT 
    payment_start_sessions.*,
    payment_dt,
    ROW_NUMBER() over(PARTITION BY payment_start_sessions.device_id, session_start ORDER BY payment_dt) AS rnk_payment
FROM payment_start_sessions
LEFT JOIN successful_payment 
    ON payment_start_sessions.device_id = successful_payment.device_id
    AND payment_dt >= session_start
    AND payment_dt_2 <= session_end
WHERE rnk_payment_start = 1

)

SELECT 
    device_id,
    session_id,
    session_num,
    session_start,
    session_end,
    source,
    campaign,
    channel_type,
    app_device_type,
    CASE
        WHEN min_order_dt < payment_dt THEN False
        ELSE True
        END AS is_buyer,
    minenv_type,
    minimal_involvement_dt,
    add_to_cart_dt,
    cart_open_dt,
    checkout_dt,
    payment_start_dt,
    payment_dt
FROM successful_payment_sessions
WHERE rnk_payment = 1

{{ config(
    schema='onfy',
    materialized='table',
    partition_by=['session_start_date'],
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'session_start_date',
      'bigquery_partition_date': '{{ next_ds }}',
      'bigquery_upload_horizon_days': '30',
    }
) }}

WITH interaction_events AS (
    SELECT DISTINCT
        event_ts_cet,
        device_id,
        type,
        NULL AS order_id,
        NULL AS gmv_initial,
        NULL AS gross_profit_initial,
        NULL AS promocode_discount
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        1 = 1
        AND (
            type IN (
                'addToCart', 'cartInitiateCheckout', 'cartOpen', 'catalogOpen',
                'homeOpen', 'paymentComplete', 'paymentStart', 'productOpen', 'search'
            ) -- funnel events
            OR
            type IN (
                'aboutUsOpen', 'adsBannerClick', 'adsBannerShow', 'allowPushPushNotificationInApp',
                'alternativePackagingChoose', 'analogsOpen', 'bannerPageAction', 'bonusesPromoScreenOpen',
                'cartAddToFreeDeliveryClick', 'checkoutConfirmOpen', 'compilationOpen', 'contactsOpen',
                'cookieBannerClick', 'faqSectionExpand', 'fastDeliveryButtonClick', 'filterDiscard',
                'filterDiscardAll', 'filterListChange', 'filterRangeChange', 'getPrescriptionButtonClick',
                'goToCartClick', 'imprintOpen', 'logInOpen', 'logOut', 'orderStatusDetailsClick',
                'orderTrackingOpen', 'ordersListProductsScreenOpen', 'partnersOpen', 'pharmacyStockOpen',
                'privacyOpen', 'productPictureOpen', 'productReviewFormOpen', 'productReviewFormSubmit',
                'productShare', 'profilePageOpen', 'promocodeCopyClick', 'promocodeFieldActivate',
                'promocodeInfoOpen', 'promocodesPageOpen', 'registrationFlowAction', 'removeFromCart',
                'savePaymentMethod', 'searchInputClick', 'termsOpen', 'tileClick', 'verificationCodeValidation'
            ) -- general activity events
        )
    UNION ALL
    SELECT
        transaction_date,
        device_id,
        'purchase_server' AS type,
        order_id,
        SUM(gmv_initial) AS gmv_initial,
        SUM(gross_profit_initial) AS gross_profit_initial,
        SUM(IF(type = 'DISCOUNT', price, 0)) AS promocode_discount
    FROM {{ source('onfy', 'transactions') }}
    WHERE
        1 = 1
        AND currency = 'EUR'
        AND device_id IS NOT NULL
    GROUP BY
        transaction_date,
        device_id,
        order_id

),


source_change_events AS (
    SELECT
        source_dt,
        device_id,
        type,
        next_source_dt,
        IF(LOWER(COALESCE(source_corrected, 'unknown')) NOT IN ('unknown', 'unmarked_facebook_or_instagram', 'e-rezept'), NULL, 1) AS session_starter,
        source_corrected,
        campaign_corrected,
        utm_medium
    FROM {{ source('onfy', 'sources') }}
    WHERE 1 = 1
),


sessions_predata AS (
    SELECT
        event_ts_cet,
        device_id,
        type,
        order_id,
        gmv_initial,
        gross_profit_initial,
        promocode_discount,
        NULL AS source_corrected,
        NULL AS campaign_corrected,
        NULL AS utm_medium,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id ORDER BY event_ts_cet, type) AS next_event_ts_cet,
        NULL AS session_starter
    FROM interaction_events
    UNION ALL
    SELECT
        source_dt,
        device_id,
        type,
        NULL AS order_id,
        NULL AS gmv_initial,
        NULL AS gross_profit_initial,
        NULL AS promocode_discount,
        source_corrected,
        campaign_corrected,
        utm_medium,
        next_source_dt,
        session_starter
    FROM source_change_events

),


first_sessions AS (
    SELECT
        IF(
            ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY event_ts_cet) = 1
            OR LAG(event_ts_cet) OVER (PARTITION BY device_id ORDER BY event_ts_cet, type) + INTERVAL 30 MINUTES < event_ts_cet, 1, session_starter
        ) AS session_starter,
        event_ts_cet,
        device_id,
        type,
        next_event_ts_cet,
        order_id,
        gmv_initial,
        gross_profit_initial,
        promocode_discount,
        source_corrected,
        campaign_corrected,
        utm_medium
    FROM sessions_predata
    ORDER BY
        device_id
),

sessions_calculation AS (
    SELECT
        COUNT(session_starter) OVER (PARTITION BY device_id ORDER BY event_ts_cet) AS session_num,
        *
    FROM first_sessions
    ORDER BY
        device_id,
        event_ts_cet
),

output AS (
    SELECT
        session_num,
        device_id,
        CONCAT(device_id, '-', CAST(FIRST_VALUE(event_ts_cet) AS INT)) AS session_id,
        FIRST_VALUE(source_corrected, TRUE) AS source,
        IF(FIRST_VALUE(source_corrected) IN ('facebook', 'google', 'idealo', 's24', 'billiger', 'tiktok', 'bing', 'criteo', 'apomio', 'geizhals'), 'paid', 'free') AS channel_type,
        FIRST_VALUE(campaign_corrected, TRUE) AS campaign,
        FIRST_VALUE(utm_medium, TRUE) AS medium,
        MIN(event_ts_cet) AS session_start,
        MAX(event_ts_cet) AS session_end,
        CAST(MIN(event_ts_cet) AS DATE) AS session_start_date,
        CAST(MAX(event_ts_cet) AS DATE) AS session_end_date,
        MIN_BY(type, event_ts_cet) AS starting_session_event,
        MAX_BY(type, event_ts_cet) AS ending_session_event,
        COUNT(type) AS events_in_session,
        COUNT(DISTINCT type) AS unique_events_in_session,
        ARRAY_AGG(order_id) AS orders_ids,
        MIN(IF(type = 'purchase_server', event_ts_cet, NULL)) AS first_transaction_date,
        MIN_BY(gmv_initial, IF(type = 'purchase_server', event_ts_cet, NULL)) AS first_transaction_gmv_initial,
        COUNT(order_id) AS orders,
        SUM(gmv_initial) AS gmv_initial,
        SUM(gross_profit_initial) AS gross_profit_initial,
        SUM(promocode_discount) AS promocode_discount
    FROM sessions_calculation
    GROUP BY
        session_num,
        device_id
)

SELECT *
FROM output
DISTRIBUTE BY session_start_date
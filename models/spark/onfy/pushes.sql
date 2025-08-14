{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}


WITH pushes_sent AS (

    SELECT
        userid,
        mailingname AS campaign,
        CASE
            WHEN mailingname LIKE '%items have been added to the cart but not purchased%' THEN TRUE
            WHEN mailingname LIKE '%Registration without order within%' THEN TRUE
            WHEN mailingname LIKE '%Abandoned Category%' THEN TRUE
            WHEN mailingname LIKE '%The total basket price has been discounted%' THEN TRUE
            WHEN mailingname LIKE '%The price of the viewed item has been reduced%' THEN TRUE
            WHEN mailingname LIKE '%items have been viewed but not purchased%' THEN TRUE
            WHEN mailingname LIKE '%Would you like to repeat the experience%' THEN TRUE
            ELSE FALSE
        END AS is_transactional,
        FROM_UTC_TIMESTAMP(datetimeutc, 'Europe/Berlin') AS push_sent_dt,
        COALESCE(
            LEAD(FROM_UTC_TIMESTAMP(datetimeutc, 'Europe/Berlin')) OVER (PARTITION BY userid, mailingname ORDER BY datetimeutc),
            '2100-01-01 00:00:00'
        ) AS next_push_sent_dt
    FROM {{ source('pharmacy', 'mindbox_events') }}
    WHERE
        channelname = 'MobilePush'
        AND mailingaction = 'MailingSend'
        AND userid IS NOT NULL
        AND DATE(datetimeutc) < '2025-07-02'

    UNION ALL

    SELECT
        userId,
        mailingName AS campaign,
        CASE
            WHEN mailingName LIKE '%items have been added to the cart but not purchased%' THEN TRUE
            WHEN mailingName LIKE '%Registration without order within%' THEN TRUE
            WHEN mailingName LIKE '%Abandoned Category%' THEN TRUE
            WHEN mailingName LIKE '%The total basket price has been discounted%' THEN TRUE
            WHEN mailingName LIKE '%The price of the viewed item has been reduced%' THEN TRUE
            WHEN mailingName LIKE '%items have been viewed but not purchased%' THEN TRUE
            WHEN mailingName LIKE '%Would you like to repeat the experience%' THEN TRUE
            ELSE FALSE
        END AS is_transactional,
        FROM_UTC_TIMESTAMP(dateTimeUtc, 'Europe/Berlin') AS push_sent_dt,
        COALESCE(
            LEAD(FROM_UTC_TIMESTAMP(dateTimeUtc, 'Europe/Berlin')) OVER (PARTITION BY userId, mailingName ORDER BY dateTimeUtc),
            '2100-01-01 00:00:00'
        ) AS next_push_sent_dt
    FROM {{ source('pharmacy', 'mindbox_events_v2') }}
    WHERE
        mailingChannel = 'MobilePush'
        AND mailingStatusName = 'Sent'
        AND userId IS NOT NULL
        AND DATE(datetimeutc) >= '2025-07-02'
),

pushes_clicked AS (
    SELECT
        userid,
        mailingname AS campaign,
        FROM_UTC_TIMESTAMP(datetimeutc, 'Europe/Berlin') AS push_clicked_dt
    FROM {{ source('pharmacy', 'mindbox_events') }}
    WHERE
        channelname = 'MobilePush'
        AND mailingaction = 'MailingClick'
        AND userid IS NOT NULL
        AND DATE(datetimeutc) < '2025-07-02'


    UNION ALL

    SELECT
        userId,
        mailingName AS campaign,
        FROM_UTC_TIMESTAMP(dateTimeUtc, 'Europe/Berlin') AS push_clicked_dt
    FROM {{ source('pharmacy', 'mindbox_events_v2') }}
    WHERE
        mailingChannel = 'MobilePush'
        AND mailingStatusName = 'Clicked'
        AND userId IS NOT NULL
        AND DATE(datetimeutc) >= '2025-07-02'
),

pushes AS (

    SELECT
        pushes_sent.*,
        pushes_clicked.push_clicked_dt
    FROM pushes_sent
    LEFT JOIN pushes_clicked
        ON
            pushes_sent.userid = pushes_clicked.userid
            AND pushes_sent.campaign = pushes_clicked.campaign
            AND pushes_sent.push_sent_dt <= pushes_clicked.push_clicked_dt
            AND pushes_sent.next_push_sent_dt > pushes_clicked.push_clicked_dt
),

sessions_orders AS (
    SELECT
        device.user_id,
        sessions.device_id,
        sessions.session_id,
        sessions.session_start,
        sessions.session_end,
        sessions.orders,
        sessions.gmv_initial,
        sessions.gross_profit_initial,
        sessions.promocode_discount
    FROM {{ ref('onfy_sessions') }} AS sessions
    INNER JOIN {{ source('pharmacy_landing', 'device') }} AS device
        ON sessions.device_id = device.id
)

SELECT
    pushes.userid AS user_id,
    pushes.campaign,
    pushes.is_transactional,
    pushes.push_sent_dt,
    pushes.push_clicked_dt,
    COUNT(sessions_orders.session_id) AS sessions,
    SUM(sessions_orders.orders) AS orders,
    SUM(sessions_orders.gmv_initial) AS gmv_initial,
    SUM(sessions_orders.gross_profit_initial) AS gross_profit_initial,
    SUM(sessions_orders.promocode_discount) AS promocode_discount
FROM pushes
LEFT JOIN sessions_orders
    ON
        pushes.userid = sessions_orders.user_id
        AND sessions_orders.session_start BETWEEN pushes.push_clicked_dt AND (pushes.push_clicked_dt + INTERVAL '30 minute')
GROUP BY 1, 2, 3, 4, 5

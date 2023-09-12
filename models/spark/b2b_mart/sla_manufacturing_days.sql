{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

with 
not_jp_users AS (
  SELECT DISTINCT u.user_id, is_joompro_employee
  FROM {{ ref('fact_user_request') }} f
  LEFT JOIN {{ ref('fact_order') }} u ON f.user_id = u.user_id
),

orders AS (
    SELECT
        o.order_id,
        o.created_ts_msk,
        o.friendly_id
    FROM {{ ref('fact_order') }} AS o
    LEFT JOIN not_jp_users AS r ON o.user_id = r.user_id
    WHERE TRUE
        AND o.created_ts_msk >= '2022-05-19'
        AND o.next_effective_ts_msk IS NULL
        AND NOT COALESCE(r.is_joompro_employee, FALSE)
),

admin AS (
    SELECT
        admin_id,
        email
    FROM {{ ref('dim_user_admin') }}
),

stg1 AS (
    SELECT
        o.order_id,
        o.status,
        o.sub_status,
        o.event_ts_msk,
        CASE WHEN MAX(o.certification_final_price) OVER (PARTITION BY o.order_id) IS NOT NULL
            AND MAX(o.certification_final_price) OVER (PARTITION BY o.order_id) > 0 THEN 1 ELSE 0 END AS certified,
        FIRST_VALUE(ao.email) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS owner_moderator_email,
        FIRST_VALUE(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_sub_status,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status ) AS min_sub_status_ts,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status ) AS min_status_ts,
        LEAD(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_sub_status_ts,
        LEAD(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_status,
        LEAD(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_sub_status,
        LAG(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_sub_status_ts,
        LAG(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_status,
        LAG(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_sub_status
    FROM {{ ref('fact_order_change') }} AS o
    LEFT JOIN admin AS ao ON o.owner_moderator_id = ao.admin_id
),

stg2 AS (
    SELECT
        order_id,
        status,
        sub_status,
        event_ts_msk,
        owner_moderator_email,
        current_status,
        current_sub_status,
        certified,
        IF(lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL, TRUE, FALSE) AS flg,
        COALESCE(lead_sub_status_ts, CURRENT_TIMESTAMP()) AS lead_sub_status_ts,
        COALESCE(
            FIRST_VALUE(CASE WHEN lead_status != status THEN lead_sub_status_ts END)
            OVER (PARTITION BY order_id, status ORDER BY lead_status != status, lead_sub_status_ts),
            CURRENT_TIMESTAMP()) AS lead_status_ts,
        FIRST_VALUE(event_ts_msk) OVER (PARTITION BY order_id, status, sub_status
            ORDER BY event_ts_msk) AS first_substatus_event_msk,
        FIRST_VALUE(event_ts_msk) OVER (PARTITION BY order_id, status
            ORDER BY event_ts_msk) AS first_status_event_msk
    FROM stg1
    WHERE (lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL)
        OR (lag_sub_status != sub_status OR lag_status != status OR lag_status IS NULL)
),


merchant_order AS (
    SELECT
        order_id,
        MAX(manufacturing_days) AS manufacturing_days
    FROM {{ ref('fact_merchant_order') }}
    GROUP BY order_id
),

orders_hist AS (
    SELECT
        order_id,
        owner_moderator_email,
        current_status,
        current_sub_status,
        MAX(IF(status = 'manufacturing', DATEDIFF(lead_status_ts, first_status_event_msk), NULL)) AS days_in_manufacturing,
        MAX(IF(sub_status = 'client2BrokerPaymentSent', DATEDIFF(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_client_payment_sent,
        MAX(IF(sub_status = 'brokerPaymentReceived', DATEDIFF(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_broker_payment_received,
        MAX(IF(sub_status = 'broker2JoomSIAPaymentSent', DATEDIFF(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_joom_sia_payment_sent,
        MAX(IF(sub_status = 'joomSIAPaymentReceived', DATEDIFF(lead_sub_status_ts,
            first_substatus_event_msk), NULL)) AS days_in_joom_sia_payment_recieved,
        MAX(IF(status = 'manufacturing', first_status_event_msk, NULL)) AS manufacturing,
        MAX(IF(status = 'manufacturing' AND sub_status = 'client2BrokerPaymentSent',
            first_substatus_event_msk, NULL)) AS client_payment_sent,
        MAX(IF(status = 'manufacturing' AND sub_status = 'brokerPaymentReceived', first_substatus_event_msk, NULL)) AS broker_payment_received,
        MAX(IF(status = 'manufacturing' AND sub_status = 'broker2JoomSIAPaymentSent', first_substatus_event_msk, NULL))
        AS joom_sia_payment_sent,
        MAX(IF(status = 'manufacturing' AND sub_status = 'joomSIAPaymentReceived',
            first_substatus_event_msk, NULL)) AS joom_sia_payment_recieved
    FROM stg2
    WHERE flg = TRUE AND status IN ('manufacturing', 'shipping')
    GROUP BY 1, 2, 3, 4
)

SELECT
    o.order_id,
    o.friendly_id,
    DATE(o.created_ts_msk) AS created_day,
    o.created_ts_msk,
    mo.manufacturing_days,
    oh.current_status,
    oh.current_sub_status,
    oh.owner_moderator_email,
    oh.days_in_manufacturing,
    days_in_client_payment_sent,
    days_in_broker_payment_received,
    days_in_joom_sia_payment_sent,
    days_in_joom_sia_payment_recieved,
    manufacturing,
    client_payment_sent,
    broker_payment_received,
    joom_sia_payment_sent,
    joom_sia_payment_recieved
FROM orders AS o
INNER JOIN orders_hist AS oh ON o.order_id = oh.order_id
LEFT JOIN merchant_order AS mo ON o.order_id = mo.order_id
WHERE oh.manufacturing IS NOT NULL

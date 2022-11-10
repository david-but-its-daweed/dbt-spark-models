{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


WITH requests AS (
    SELECT
        request_id,
        MAX(is_joompro_employee) OVER (PARTITION BY user_id) AS is_joompro_employee
    FROM {{ ref('fact_user_request') }}
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
        FIRST_VALUE(o.total_final_price) OVER (PARTITION BY o.order_id, o.status, o.sub_status ORDER BY o.event_ts_msk) AS final_gmv,
        FIRST_VALUE(ao.email) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS owner_moderator_email,
        FIRST_VALUE(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk DESC) AS current_sub_status,
        MIN(o.event_ts_msk) OVER (PARTITION BY o.order_id, o.status, o.sub_status ) AS min_status_ts,
        LEAD(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_status_ts,
        LEAD(o.status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_status,
        LEAD(o.sub_status) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lead_sub_status,
        LAG(o.event_ts_msk) OVER (PARTITION BY o.order_id ORDER BY o.event_ts_msk) AS lag_status_ts,
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
        final_gmv,
        COALESCE(lead_status_ts, CURRENT_TIMESTAMP()) AS lead_status_ts,
        IF(lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL,
            COALESCE(lead_status_ts, TIMESTAMP(CURRENT_TIMESTAMP())), NULL) IS NOT NULL AS flg,
        FIRST_VALUE(event_ts_msk) OVER (PARTITION BY order_id, status, sub_status
            ORDER BY CASE WHEN IF(lag_sub_status != sub_status OR lag_status != status OR lag_status IS NULL,
                event_ts_msk, NULL) IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk) AS first_substatus_event_msk
    FROM stg1
    WHERE IF(lag_sub_status != sub_status OR lag_status != status OR lag_status IS NULL, event_ts_msk, NULL) IS NOT NULL
        OR IF(lead_sub_status != sub_status OR lead_status != status OR lead_status IS NULL,
            COALESCE(lead_status_ts, CURRENT_TIMESTAMP()), NULL) IS NOT NULL
),

orders_hist AS (
    SELECT
        order_id,
        owner_moderator_email,
        current_status,
        current_sub_status,
        MAX(CASE WHEN current_status = status AND current_sub_status = sub_status THEN final_gmv ELSE 0 END) AS gmv,
        MAX(IF(status = 'selling', first_substatus_event_msk, null)) AS selling_day,
        CASE WHEN MAX(IF(status = 'manufacturing', first_substatus_event_msk, null)) is not null then 1 else 0 end as was_in_manufacturing,
        CASE WHEN 
            MAX(IF(status = 'cancelled', first_substatus_event_msk, null)) > MAX(IF(status = 'manufacturing', first_substatus_event_msk, null))
            THEN 1 
            WHEN MAX(IF(status = 'cancelled', first_substatus_event_msk, null)) <= MAX(IF(status = 'manufacturing', first_substatus_event_msk, null))
            THEN 0 end as cancelled_in_selling,
        MAX(IF(status = 'cancelled', sub_status, null)) AS cancelled_reason,
        MAX(IF(sub_status = 'new', DATE_DIFF(lead_status_ts, first_substatus_event_msk, day), null)) AS days_in_selling_new,
        MAX(IF(sub_status = 'priceEstimation', DATE_DIFF(lead_status_ts, first_substatus_event_msk, day), null)) AS days_in_selling_price_estimation,
        MAX(IF(sub_status = 'negotiation', DATE_DIFF(lead_status_ts, first_substatus_event_msk, day), null)) AS days_in_selling_negotiation,
        MAX(IF(sub_status = 'finalPricing', DATE_DIFF(lead_status_ts, first_substatus_event_msk, day), null)) AS days_in_selling_final_pricing,
        MAX(IF(sub_status = 'signingAndPayment', DATE_DIFF(lead_status_ts, first_substatus_event_msk, day), null)) AS days_in_selling_signing_and_payment
    FROM stg2
    WHERE flg = TRUE
        AND status = 'selling'
    GROUP BY 1, 2, 3, 4
)

SELECT
    oh.order_id,
    DATE(oh.selling_day) AS partition_date,
    oh.gmv,
    oh.was_in_manufacturing,
    oh.cancelled_in_selling,
    oh.cancelled_reason,
    oh.current_status,
    oh.current_sub_status,
    oh.owner_moderator_email,
    oh.days_in_selling_new,
    oh.days_in_selling_price_estimation,
    oh.days_in_selling_negotiation,
    oh.days_in_selling_final_pricing,
    oh.days_in_selling_signing_and_payment
FROM orders_hist AS oh
WHERE oh.selling_day is not null

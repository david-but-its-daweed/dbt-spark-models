 {{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@marksysoev',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

WITH first_purchases AS (
    SELECT DISTINCT
        pharmacy_landing.order.user_email_hash, 
        from_utc_timestamp(
            FIRST_VALUE(pharmacy_landing.order.created) OVER (PARTITION BY pharmacy_landing.order.user_email_hash ORDER BY pharmacy_landing.order.created)
        , 'Europe/Berlin') AS first_purchase_date,
        FIRST_VALUE(pharmacy_landing.order.device_id) OVER (PARTITION BY pharmacy_landing.order.user_email_hash ORDER BY pharmacy_landing.order.created) 
            AS first_purchase_device_id
    FROM 
        {{ source('pharmacy_landing', 'order') }}
),

transactions_gmv AS (
    SELECT
        order_id,
        SUM(gmv_initial) as gmv_initial
    FROM
        {{ source('onfy', 'transactions') }}
    WHERE 
        currency = 'EUR'
    GROUP BY
        order_id
),

base_email AS (
    SELECT DISTINCT
        pharmacy_landing.order.user_email_hash,
        pharmacy_landing.order.id as order_id,
        from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') as order_date_cet,
        DATE_TRUNC('MONTH', from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin')) AS payment_month,
        DATE_TRUNC('MONTH', first_purchases.first_purchase_date) AS cohort_month,
        months_between(
            DATE_TRUNC('MONTH', from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin')),
            DATE_TRUNC('MONTH', first_purchases.first_purchase_date)
        ) AS month_number,
        /*
        CASE 
            WHEN pharmacy_landing.device.app_type = 'WEB' THEN
                CASE 
                    WHEN pharmacy_landing.device.device_type = 'DESKTOP' THEN 'WEB_DESKTOP'
                    ELSE 'WEB_MOBILE'
                END
            WHEN pharmacy_landing.device.app_type IS NOT NULL THEN pharmacy_landing.device.app_type
            ELSE 'Other'
        END AS app_device_type,
        */
        CASE 
            WHEN pharmacy_landing.device.app_type = 'WEB' THEN 'WEB'
            WHEN pharmacy_landing.device.app_type IS NOT NULL THEN 'APP'
            ELSE 'Other'
        END AS platform_type,
        onfy.lndc_user_attribution.source_corrected as traffic_source,
        onfy.lndc_user_attribution.campaign_corrected,
        CASE 
            WHEN from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') < '2023-01-01' 
            THEN
                SUM(pharmacy_landing.order_parcel_item.price * pharmacy_landing.order_parcel_item.quantity) OVER (PARTITION BY pharmacy_landing.order.id)
                + SUM(pharmacy_landing.order_parcel.delivery_price * 1. / COUNT(pharmacy_landing.order_parcel_item.id) OVER (PARTITION BY pharmacy_landing.order_parcel.id) ) OVER (PARTITION BY pharmacy_landing.order.id)
                + pharmacy_landing.order.service_fee_price * 1. / COUNT(pharmacy_landing.order_parcel_item.id) OVER (PARTITION BY pharmacy_landing.order.id) 
            ELSE
                transactions_gmv.gmv_initial
        END as gmv_initial
    FROM 
        {{ source('pharmacy_landing', 'order') }}
        LEFT JOIN first_purchases
            ON first_purchases.user_email_hash = pharmacy_landing.order.user_email_hash
        LEFT JOIN {{ source('pharmacy_landing', 'device') }}
            ON pharmacy_landing.device.id = first_purchases.first_purchase_device_id
        LEFT JOIN {{ source('pharmacy_landing', 'order_parcel') }}
            ON pharmacy_landing.order.id = pharmacy_landing.order_parcel.order_id
        LEFT JOIN {{ source('pharmacy_landing', 'order_parcel_item') }}
            ON pharmacy_landing.order_parcel.id = pharmacy_landing.order_parcel_item.order_parcel_id
            AND pharmacy_landing.order_parcel_item.product_id IS NOT NULL
        LEFT JOIN {{ source('onfy', 'lndc_user_attribution') }}
            ON onfy.lndc_user_attribution.user_email_hash = pharmacy_landing.order.user_email_hash
        LEFT JOIN transactions_gmv
            ON transactions_gmv.order_id = pharmacy_landing.order.id
),

cohort_values AS (
    SELECT
        platform_type,
        traffic_source,
        cohort_month,
        month_number,
        payment_month,
        COUNT(DISTINCT user_email_hash) AS cohort_value,
        SUM(gmv_initial) AS cohort_month_gmv,
        COUNT(DISTINCT order_id) AS cohort_month_orders
    FROM base_email
    GROUP BY 1, 2, 3, 4, 5
    -- total platform
    UNION ALL
    SELECT
        'total' AS platform_type,
        traffic_source,
        cohort_month,
        month_number,
        payment_month,
        COUNT(DISTINCT user_email_hash) AS cohort_value,
        SUM(gmv_initial) AS cohort_month_gmv,
        COUNT(DISTINCT order_id) AS cohort_month_orders
    FROM base_email
    GROUP BY 1, 2, 3, 4, 5
    -- total traffic source
    UNION ALL
    SELECT
        platform_type,
        'total' AS traffic_source,
        cohort_month,
        month_number,
        payment_month,
        COUNT(DISTINCT user_email_hash) AS cohort_value,
        SUM(gmv_initial) AS cohort_month_gmv,
        COUNT(DISTINCT order_id) AS cohort_month_orders
    FROM base_email
    GROUP BY 1, 2, 3, 4, 5
    -- total both
    UNION ALL
    SELECT
        'total' AS platform_type,
        'total' AS traffic_source,
        cohort_month,
        month_number,
        payment_month,
        COUNT(DISTINCT user_email_hash) AS cohort_value,
        SUM(gmv_initial) AS cohort_month_gmv,
        COUNT(DISTINCT order_id) AS cohort_month_orders
    FROM base_email
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    -- cohorts definition
    platform_type,
    traffic_source,
    cohort_month,
    -- months 
    month_number,
    payment_month,
    -- cohort-month values
    cohort_value,
    cohort_month_orders,
    cohort_month_gmv,
    -- cohort values
    SUM( IF(month_number = 0, cohort_value, 0)
    ) OVER (
        PARTITION BY platform_type, traffic_source, cohort_month
    ) AS cohort_size,
    SUM( IF(month_number = 0, cohort_month_gmv, 0)
    ) OVER (
        PARTITION BY platform_type, traffic_source, cohort_month
    ) AS cohort_gmv,
    SUM( IF(month_number = 0, cohort_month_orders, 0)
    ) OVER (
        PARTITION BY platform_type, traffic_source, cohort_month
    ) AS cohort_orders
FROM 
    cohort_values

{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
      'model_owner' : '@helbuk',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'priority_weight': '150',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_known_gaps': [
          '2022-11-04', '2022-11-22', '2022-12-01', '2022-11-14', 
          '2022-11-09', '2022-11-24', '2022-11-03', '2022-11-20', 
          '2022-11-07', '2022-11-06', '2022-11-27', '2022-11-13', 
          '2022-12-22', '2022-11-28', '2022-11-08', '2022-11-15', 
          '2022-11-25', '2022-11-23', '2022-11-21', '2022-11-19', 
          '2022-11-16', '2022-11-18', '2022-11-05', '2022-11-17', 
          '2022-11-10', '2022-11-11', '2022-12-31', '2022-12-26',
          '2022-12-20', '2022-12-17', '2022-12-21', '2022-12-24', 
          '2022-12-25', '2022-12-29', '2022-12-30', '2022-12-15', 
          '2022-12-16', '2022-12-28', '2022-12-19', 
          '2022-12-18', '2022-12-27', '2022-12-23'
      ],
      'bigquery_overwrite': 'true'
    }
) }}



WITH numbered_purchases AS (
    SELECT
        pharmacy_landing.order.id AS order_id,
        RANK() OVER (PARTITION BY pharmacy_landing.order.user_email_hash ORDER BY pharmacy_landing.order.created ASC) AS purchase_num
    FROM
        {{ source('pharmacy_landing', 'order') }}
),


order_data AS (
    SELECT
        pharmacy_landing.order.id AS order_id,
        from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') AS order_created_time_cet,
        numbered_purchases.purchase_num,
        pharmacy_landing.order.user_email_hash,
        onfy.lndc_user_attribution.source_corrected,
        onfy.lndc_user_attribution.campaign_corrected,
        pharmacy_landing.order.device_id,
        CASE
            WHEN pharmacy_landing.device.app_type = 'WEB' THEN
                CASE
                    WHEN pharmacy_landing.device.device_type = 'DESKTOP' THEN 'WEB_DESKTOP'
                    ELSE 'WEB_MOBILE'
                END
            WHEN pharmacy_landing.device.app_type IS NOT NULL THEN pharmacy_landing.device.app_type
            ELSE 'Other'
        END AS app_device_type,
        CASE
            WHEN pharmacy_landing.device.app_type = 'WEB' THEN 'WEB'
            WHEN pharmacy_landing.device.app_type IS NOT NULL THEN 'APP'
            ELSE 'Other'
        END AS platform_type,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'PAYMENT' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS products_price,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'ORDER_REVERSAL' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS products_price_refund,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'ORDER_SHIPMENT' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS delivery,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'ORDER_SHIPMENT_REV' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS delivery_refund,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'DELIVERY_SURCHARGE' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS delivery_surcharge,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'DELIVERY_SURCHARGE_REVERSAL' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS delivery_surcharge_refund,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'SERVICE_FEE' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS service_fee,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'SERVICE_FEE_REVERSAL' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS service_fee_refund,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'DISCOUNT' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS discount,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'DISCOUNT_REVERSAL' THEN onfy_mart.transactions.price ELSE 0 END), 0) AS discount_refund,
        pharmacy_landing.order.payment_method,
        CASE
            WHEN payment_method = 'PAY_PAL' then 0.35
            WHEN payment_method in ('CARD', 'APPLE_PAY')   then 0.05
            WHEN payment_method = 'GIROPAY' then 0.22
            WHEN payment_method = 'SOFORT'  then 0.22
            WHEN payment_method = 'KLARNA' then 0.2
            ELSE 0
        END AS psp_commission_fix,
        CASE
            WHEN payment_method = 'PAY_PAL' then 0.023
            WHEN payment_method in ('CARD', 'APPLE_PAY')    then 0.01
            WHEN payment_method = 'GIROPAY' then 0.0205
            WHEN payment_method = 'SOFORT'  then 0.0205
            WHEN payment_method = 'KLARNA' then 0.0256
            ELSE 0
        END AS psp_commission_perc,
        CASE
            WHEN payment_method = 'PAY_PAL' then 0
            WHEN payment_method in ('CARD', 'APPLE_PAY')    then 0.03
            WHEN payment_method = 'GIROPAY' then 0.02
            WHEN payment_method = 'SOFORT'  then 0.02
            WHEN payment_method = 'KLARNA' then 0.0256
            ELSE 0
        END AS psp_commission_refund_fix,
        CASE
            WHEN payment_method = 'PAY_PAL' then 0.023
            WHEN payment_method in ('CARD', 'APPLE_PAY')    then 0
            WHEN payment_method = 'GIROPAY' then 0
            WHEN payment_method = 'SOFORT'  then 0
            WHEN payment_method = 'KLARNA' then 0.2
            ELSE 0
        END AS psp_commission_refund_perc
    FROM
        {{ source('onfy_mart', 'transactions') }}
        JOIN {{ source('pharmacy_landing', 'order') }}
            ON pharmacy_landing.order.id = onfy_mart.transactions.order_id
        JOIN numbered_purchases
            ON numbered_purchases.order_id = pharmacy_landing.order.id
        LEFT JOIN {{ source('pharmacy_landing', 'device') }}
            ON pharmacy_landing.device.id = pharmacy_landing.order.device_id
        LEFT JOIN {{ ref('lndc_user_attribution') }}
            ON onfy.lndc_user_attribution.user_email_hash = onfy_mart.transactions.user_email_hash
    GROUP BY
        pharmacy_landing.order.id,
        pharmacy_landing.order.created,
        numbered_purchases.purchase_num,
        pharmacy_landing.order.user_email_hash,
        onfy.lndc_user_attribution.source_corrected,
        onfy.lndc_user_attribution.campaign_corrected,
        pharmacy_landing.order.device_id,
        pharmacy_landing.order.device_id,
        app_device_type,
        platform_type,
        pharmacy_landing.order.payment_method
),

turnover_refunds AS (
    SELECT
        order_data.*,
        products_price + delivery + delivery_surcharge - discount + service_fee AS turnover,
        products_price_refund + delivery_refund + delivery_surcharge_refund + service_fee_refund - discount_refund AS refund
    FROM
        order_data
),

parcel_gmv AS (
    SELECT
        order_id,
        order_parcel_id,
        SUM(
            CASE WHEN type IN ('PAYMENT','SERVICE_FEE','ORDER_SHIPMENT','DELIVERY_SURCHARGE') THEN price
                    WHEN type IN ('ORDER_REVERSAL','SERVICE_FEE_REVERSAL','ORDER_SHIPMENT_REV','DELIVERY_SURCHARGE_REVERSAL') THEN -price
                    ELSE 0 END
        ) AS gmv
    FROM {{ source('onfy_mart', 'transactions') }}
    WHERE order_parcel_id IS NOT NULL
    GROUP BY order_id, order_parcel_id
),

order_gmv AS (
    SELECT
        order_id,
        SUM(gmv) AS gmv_total
    FROM parcel_gmv
    GROUP BY order_id
),

psp_initial AS (
    SELECT
        'CHARGE_FEE' AS type,
        t.order_id,
        t.purchase_num,
        t.device_id,
        t.app_device_type,
        t.platform_type,
        p.order_parcel_id,
        st.name AS store_name,
        t.payment_method,
        t.user_email_hash,
        t.source_corrected,
        t.campaign_corrected,
        t.order_created_time_cet,
        t.order_created_time_cet AS transaction_date,
        (psp_commission_fix + psp_commission_perc * turnover) * (p.gmv / og.gmv_total) AS price,
        'EUR' AS currency
    FROM
        turnover_refunds t
        JOIN parcel_gmv p ON t.order_id = p.order_id
        JOIN order_gmv og ON og.order_id = t.order_id
        LEFT JOIN {{ source('pharmacy_landing', 'order_parcel') }} op
            ON t.order_id = op.order_id AND p.order_parcel_id = op.id
        LEFT JOIN {{ source('pharmacy_landing', 'store') }} st
            ON st.id = op.store_id
    WHERE
        t.order_created_time_cet < '2023-07-21'
),

psp_refund AS (
    SELECT
        'REFUND_FEE' AS type,
        t.order_id,
        t.purchase_num,
        t.device_id,
        t.app_device_type,
        t.platform_type,
        p.order_parcel_id,
        st.name AS store_name,
        t.payment_method,
        t.user_email_hash,
        t.source_corrected,
        t.campaign_corrected,
        t.order_created_time_cet,
        t.order_created_time_cet AS transaction_date,
        (psp_commission_refund_fix + psp_commission_refund_perc * refund) * (p.gmv / og.gmv_total) AS price,
        'EUR' AS currency
    FROM
        turnover_refunds t
        JOIN parcel_gmv p
            ON t.order_id = p.order_id
        JOIN order_gmv og
            ON og.order_id = t.order_id
        LEFT JOIN {{ source('pharmacy_landing', 'order_parcel') }} op
            ON t.order_id = op.order_id AND p.order_parcel_id = op.id
        LEFT JOIN {{ source('pharmacy_landing', 'store') }} st
            ON st.id = op.store_id
    WHERE
        t.refund > 0
        AND t.order_created_time_cet < '2023-07-21'
),


transactions_psp AS (

    -- 1) all raw transactions except PSP-fee 
    SELECT
        UPPER(t.type) AS type,
        t.order_id,
        od.purchase_num,
        od.device_id,
        od.app_device_type,
        od.platform_type,
        t.order_parcel_id,
        st.name AS store_name,
        od.payment_method,
        t.user_email_hash,
        od.source_corrected AS source,
        od.campaign_corrected AS campaign,
        od.order_created_time_cet,
        from_utc_timestamp(t.date, 'Europe/Berlin') AS transaction_date,
        t.price,
        t.currency
    FROM {{ source('onfy_mart', 'transactions') }} AS t
    LEFT JOIN order_data AS od
        ON od.order_id = t.order_id
    LEFT JOIN {{ source('pharmacy_landing', 'order_parcel') }} AS op
        ON t.order_parcel_id = op.id
    LEFT JOIN {{ source('pharmacy_landing', 'store') }} AS st
        ON st.id = op.store_id
    WHERE NOT lower(t.type) IN ('charge_fee','refund_fee')

    UNION ALL

    -- 2) Split only PSP-fee rows AFTER cutoff which have no parcel_id
    SELECT
        UPPER(t.type) AS type,
        t.order_id,
        od.purchase_num,
        od.device_id,
        od.app_device_type,
        od.platform_type,
        p.order_parcel_id,
        st.name AS store_name,
        od.payment_method,
        t.user_email_hash,
        od.source_corrected AS source,
        od.campaign_corrected AS campaign,
        od.order_created_time_cet,
        from_utc_timestamp(t.date, 'Europe/Berlin') AS transaction_date,
        t.price * (p.gmv / og.gmv_total) AS price,
        t.currency
    FROM {{ source('onfy_mart', 'transactions') }} AS t
    JOIN parcel_gmv AS p
         ON t.order_id = p.order_id
    JOIN order_gmv AS og
         ON og.order_id = t.order_id
    LEFT JOIN order_data AS od
         ON od.order_id = t.order_id
    LEFT JOIN {{ source('pharmacy_landing', 'order_parcel') }} AS op
         ON p.order_parcel_id = op.id
    LEFT JOIN {{ source('pharmacy_landing', 'store') }} AS st
         ON st.id = op.store_id
    WHERE
        p.order_parcel_id IS NOT NULL
        AND lower(t.type) IN ('charge_fee', 'refund_fee')
        AND from_utc_timestamp(t.date,'Europe/Berlin') >= '2023-07-21'

    -- 3) Before cutoff '2023-07-21'

    UNION ALL

    SELECT * FROM psp_initial
    UNION ALL
    SELECT * FROM psp_refund
),

transactions_eur AS (
    SELECT
        transactions_psp.type,
        transactions_psp.order_id,
        transactions_psp.purchase_num,
        transactions_psp.device_id,
        transactions_psp.app_device_type,
        transactions_psp.platform_type,
        transactions_psp.order_parcel_id,
        transactions_psp.store_name,
        transactions_psp.payment_method,
        transactions_psp.user_email_hash,
        transactions_psp.source,
        transactions_psp.campaign,
        transactions_psp.order_created_time_cet,
        transactions_psp.transaction_date,
        CAST(transactions_psp.transaction_date AS DATE) AS partition_date,
        CAST(price AS float) AS price,
        transactions_psp.currency,
        CAST(
            CASE WHEN transactions_psp.type IN ('PAYMENT', 'SERVICE_FEE', 'ORDER_SHIPMENT', 'DELIVERY_SURCHARGE') THEN transactions_psp.price ELSE 0 END
        AS float) AS gmv_initial,
        CAST(
            CASE
                WHEN transactions_psp.type IN ('PAYMENT', 'SERVICE_FEE', 'ORDER_SHIPMENT', 'DELIVERY_SURCHARGE') THEN transactions_psp.price
                WHEN transactions_psp.type IN ('ORDER_REVERSAL', 'SERVICE_FEE_REVERSAL', 'ORDER_SHIPMENT_REV', 'DELIVERY_SURCHARGE_REVERSAL') THEN -transactions_psp.price
                ELSE 0
            END
        AS float) AS gmv_final,
        CAST(
            CASE
                WHEN transactions_psp.type IN ('COMMISSION', 'SERVICE_FEE', 'DELIVERY_SURCHARGE', 'MEDIA_REVENUE') THEN transactions_psp.price
                WHEN transactions_psp.type IN ('COMMISSION_VAT', 'SERVICE_FEE_VAT', 'DELIVERY_SURCHARGE_VAT', 'DISCOUNT', 'CHARGE_FEE') THEN -transactions_psp.price
                ELSE 0
            END
        AS float) AS gross_profit_initial,
        CAST(
            CASE
                WHEN transactions_psp.type IN ('COMMISSION', 'SERVICE_FEE', 'DELIVERY_SURCHARGE', 'DISCOUNT_REVERSAL', 'COMMISSION_REVERSAL_VAT', 'SERVICE_FEE_REVERSAL_VAT', 'DELIVERY_SURCHARGE_REVERSAL_VAT', 'MEDIA_REVENUE', 'REFUND_FEE') THEN transactions_psp.price
                WHEN transactions_psp.type IN ('COMMISSION_VAT', 'SERVICE_FEE_VAT', 'DELIVERY_SURCHARGE_VAT', 'DISCOUNT', 'COMMISSION_REVERSAL', 'SERVICE_FEE_REVERSAL', 'DELIVERY_SURCHARGE_REVERSAL', 'CHARGE_FEE') THEN -transactions_psp.price
                ELSE 0
            END
        AS float) AS gross_profit_final
    FROM
        transactions_psp
),

transactions_usd AS (
    SELECT DISTINCT
        transactions_eur.type,
        transactions_eur.order_id,
        transactions_eur.purchase_num,
        transactions_eur.device_id,
        transactions_eur.app_device_type,
        transactions_eur.platform_type,
        transactions_eur.order_parcel_id,
        transactions_eur.store_name,
        transactions_eur.payment_method,
        transactions_eur.user_email_hash,
        transactions_eur.source,
        transactions_eur.campaign,
        transactions_eur.order_created_time_cet,
        transactions_eur.transaction_date,
        CAST(transactions_eur.transaction_date AS DATE) AS partition_date,
        transactions_eur.price * eur.rate / usd.rate AS price,
        'USD' AS currency,
        transactions_eur.gmv_initial * eur.rate / usd.rate AS gmv_initial,
        transactions_eur.gmv_final * eur.rate / usd.rate AS gmv_final,
        transactions_eur.gross_profit_initial * eur.rate / usd.rate AS gross_profit_initial,
        transactions_eur.gross_profit_final * eur.rate / usd.rate AS gross_profit_final
    FROM
        transactions_eur
        LEFT JOIN {{ source('mart', 'dim_currency_rate') }} AS eur
            ON eur.effective_date = DATE_TRUNC('DAY', transactions_eur.transaction_date)
            AND eur.currency_code = 'EUR'
        LEFT JOIN {{ source('mart', 'dim_currency_rate') }} AS usd
            ON usd.effective_date = DATE_TRUNC('DAY', transactions_eur.transaction_date)
            AND usd.currency_code = 'USD'
)

SELECT * FROM transactions_eur
    UNION
SELECT * FROM transactions_usd
DISTRIBUTE BY partition_date

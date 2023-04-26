
 {{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

WITH order_data AS (
    SELECT
        pharmacy_landing.order.id as order_id,
        pharmacy_landing.order.user_email_hash,
        from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') as order_created_time_cet,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'PAYMENT' THEN onfy_mart.transactions.price ELSE 0 END), 0) as products_price,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'ORDER_REVERSAL' THEN onfy_mart.transactions.price ELSE 0 END), 0) as products_price_refund,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'ORDER_SHIPMENT' THEN onfy_mart.transactions.price ELSE 0 END), 0) as delivery,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'ORDER_SHIPMENT_REV' THEN onfy_mart.transactions.price ELSE 0 END), 0) as delivery_refund,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'DELIVERY_SURCHARGE' THEN onfy_mart.transactions.price ELSE 0 END), 0) as delivery_surcharge,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'DELIVERY_SURCHARGE_REVERSAL' THEN onfy_mart.transactions.price ELSE 0 END), 0) as delivery_surcharge_refund,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'SERVICE_FEE' THEN onfy_mart.transactions.price ELSE 0 END), 0) as service_fee,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'SERVICE_FEE_REVERSAL' THEN onfy_mart.transactions.price ELSE 0 END), 0) as service_fee_refund,
        COALESCE(SUM(CASE WHEN onfy_mart.transactions.type = 'DISCOUNT' THEN onfy_mart.transactions.price ELSE 0 END), 0) as discount,
        pharmacy_landing.order.payment_method,
        CASE
            WHEN payment_method = 'PAY_PAL' then 0.35
            WHEN payment_method in ('CARD', 'APPLE_PAY')   then 0.05
            WHEN payment_method = 'GIROPAY' then 0.22
            WHEN payment_method = 'SOFORT'  then 0.22
            ELSE 0
        END as psp_commission_fix,
        CASE
            WHEN payment_method = 'PAY_PAL' then 0.023 
            WHEN payment_method in ('CARD', 'APPLE_PAY')    then 0.01
            WHEN payment_method = 'GIROPAY' then 0.0205
            WHEN payment_method = 'SOFORT'  then 0.0205
            ELSE 0
        END AS psp_commission_perc,
        CASE
            WHEN payment_method = 'PAY_PAL' then 0 
            WHEN payment_method in ('CARD', 'APPLE_PAY')    then 0.03
            WHEN payment_method = 'GIROPAY' then 0.02
            WHEN payment_method = 'SOFORT'  then 0.02
            ELSE 0
        END AS psp_commission_refund_fix,
        CASE
            WHEN payment_method = 'PAY_PAL' then 0.023 
            WHEN payment_method in ('CARD', 'APPLE_PAY')    then 0
            WHEN payment_method = 'GIROPAY' then 0
            WHEN payment_method = 'SOFORT'  then 0
            ELSE 0
        END AS psp_commission_refund_perc
    FROM
        {{ source('onfy_mart', 'transactions') }}
        JOIN {{ source('pharmacy_landing', 'order') }}
            ON pharmacy_landing.order.id = onfy_mart.transactions.order_id
    GROUP BY 
        pharmacy_landing.order.id,
        pharmacy_landing.order.user_email_hash,
        pharmacy_landing.order.created,
        pharmacy_landing.order.payment_method
),

gmv_refunds AS (
    SELECT
        order_data.*,
        products_price + delivery + delivery_surcharge - discount + service_fee as gmv_initial,
        products_price_refund + delivery_refund + delivery_surcharge_refund + service_fee_refund as refund
    FROM 
        order_data
),

psp_initial AS (   
    SELECT 
        'PSP_COMMISSION' as type,
        order_id,
        '' as order_parcel_id,
        user_email_hash,
        order_created_time_cet as date,
        psp_commission_fix + psp_commission_perc * gmv_initial as price,
        'EUR' as currency
    FROM 
        gmv_refunds
),

psp_refund AS (
    SELECT 
        'PSP_COMMISSION_REVERSAL' as type,
        order_id,
        '' as order_parcel_id,
        user_email_hash,
        order_created_time_cet as date,
        psp_commission_refund_fix + psp_commission_refund_perc * refund as price,
        'EUR' as currency
    FROM 
        gmv_refunds
    WHERE
        refund > 0
),

transactions_eur AS (
    SELECT * FROM onfy_mart.transactions 
        UNION
    SELECT * FROM psp_initial
        UNION
    SELECT * FROM psp_refund
),

transactions_usd AS (
    SELECT DISTINCT
        transactions_eur.type,
        transactions_eur.order_id,
        transactions_eur.order_parcel_id,
        transactions_eur.user_email_hash,
        transactions_eur.date,
        transactions_eur.price * eur.rate / usd.rate as price,
        'USD' as currency
    FROM
        transactions_eur
        LEFT JOIN {{ source('mart', 'dim_currency_rate') }} eur
            ON eur.effective_date = DATE_TRUNC('DAY', transactions_eur.date)
            AND eur.currency_code = 'EUR'
        LEFT JOIN {{ source('mart', 'dim_currency_rate') }} usd
            ON usd.effective_date = DATE_TRUNC('DAY', transactions_eur.date)
            AND usd.currency_code = 'USD'
)

SELECT * FROM transactions_eur
    UNION
SELECT * FROM transactions_usd

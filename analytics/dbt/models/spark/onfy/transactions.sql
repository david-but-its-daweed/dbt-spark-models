
 {{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

WITH numbered_purchases AS (
    SELECT
        pharmacy_landing.order.id as order_id, 
        rank() over (partition by pharmacy_landing.order.user_email_hash order by pharmacy_landing.order.created asc) as purchase_num
    from 
        {{ source('pharmacy_landing', 'order') }}
),

order_data AS (
    SELECT
        pharmacy_landing.order.id AS order_id,
        from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') as order_created_time_cet,
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
        JOIN numbered_purchases
            ON numbered_purchases.order_id = pharmacy_landing.order.id
        LEFT JOIN {{ source('pharmacy_landing', 'device') }}
            ON pharmacy_landing.device.id = pharmacy_landing.order.device_id
        LEFT JOIN {{ source('onfy', 'lndc_user_attribution') }}
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
        products_price + delivery + delivery_surcharge - discount + service_fee as turnover,
        products_price_refund + delivery_refund + delivery_surcharge_refund + service_fee_refund as refund
    FROM 
        order_data
),

psp_initial AS (   
    SELECT 
        'PSP_COMMISSION' as type,
        order_id,
        purchase_num,
        device_id,
        app_device_type,
        platform_type,
        '' as order_parcel_id,
        '' as store_name,
        user_email_hash,
        source_corrected,
        campaign_corrected,
        order_created_time_cet,
        order_created_time_cet as transaction_date,
        psp_commission_fix + psp_commission_perc * turnover as price,
        'EUR' as currency
    FROM 
        turnover_refunds
),

psp_refund AS (
    SELECT 
        'PSP_COMMISSION_REVERSAL' as type,
        order_id,
        purchase_num,
        device_id,
        app_device_type,
        platform_type,
        '' as order_parcel_id,
        '' as store_name,
        user_email_hash,
        source_corrected,
        campaign_corrected,
        order_created_time_cet,
        order_created_time_cet as transaction_date,
        psp_commission_refund_fix + psp_commission_refund_perc * refund as price,
        'EUR' as currency
    FROM 
        turnover_refunds
    WHERE
        refund > 0
),

transactions_eur AS (
    SELECT DISTINCT
        onfy_mart.transactions.type,
        onfy_mart.transactions.order_id,
        order_data.purchase_num,
        order_data.device_id,
        order_data.app_device_type,
        order_data.platform_type,
        onfy_mart.transactions.order_parcel_id,
        pharmacy_landing.store.name as store_name,
        onfy_mart.transactions.user_email_hash,
        order_data.source_corrected as source,
        order_data.campaign_corrected as campaign,
        order_data.order_created_time_cet,
        onfy_mart.transactions.date as transaction_date,
        onfy_mart.transactions.price,
        onfy_mart.transactions.currency
    FROM 
        {{ source('onfy_mart', 'transactions') }}
        LEFT JOIN order_data
            ON order_data.order_id = onfy_mart.transactions.order_id
        LEFT JOIN {{ source('pharmacy_landing', 'order_parcel') }}
            ON onfy_mart.transactions.order_parcel_id = pharmacy_landing.order_parcel.id
        LEFT JOIN {{ source('pharmacy_landing', 'store') }}
            ON pharmacy_landing.store.id = pharmacy_landing.order_parcel.store_id
        UNION
    SELECT * FROM psp_initial
        UNION
    SELECT * FROM psp_refund
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
        transactions_eur.user_email_hash,
        transactions_eur.source,
        transactions_eur.campaign,
        transactions_eur.order_created_time_cet,
        transactions_eur.transaction_date,
        transactions_eur.price * eur.rate / usd.rate as price,
        'USD' as currency
    FROM
        transactions_eur
        LEFT JOIN {{ source('mart', 'dim_currency_rate') }} eur
            ON eur.effective_date = DATE_TRUNC('DAY', transactions_eur.transaction_date)
            AND eur.currency_code = 'EUR'
        LEFT JOIN {{ source('mart', 'dim_currency_rate') }} usd
            ON usd.effective_date = DATE_TRUNC('DAY', transactions_eur.transaction_date)
            AND usd.currency_code = 'USD'
)

SELECT * FROM transactions_eur
    UNION
SELECT * FROM transactions_usd

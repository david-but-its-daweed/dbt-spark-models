{{
  config(
    schema='mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['order_date_utc'],
    on_schema_change='sync_all_columns',
    meta = {
        'model_owner' : '@leonid.enov',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'order_date_utc',
        'bigquery_overwrite': 'true'
    }
  )
}}

WITH currency_rate AS (
    SELECT
        currency_code,
        rate,
        effective_date,
        next_effective_date
    FROM {{ source('mart', 'dim_pair_currency_rate') }}
    WHERE currency_code_to = 'USD'
),

product_data AS (
    SELECT
        a.product_id,
        b.business_line,
        b.l1_merchant_category_name,
        b.l2_merchant_category_name,
        b.l3_merchant_category_name,
        b.l4_merchant_category_name,
        b.l5_merchant_category_name,
        c.merchant_name,
        c.origin_name,
        CASE
            WHEN c.origin_name IS NULL OR c.origin_name IN ('Chinese', 'USChinese') THEN 'China'
            WHEN c.origin_name IN ('Australian', 'Korean', 'USKorean', 'Indian', 'Japanese', 'Singaporean', 'Thai', 'Turkish') THEN 'Alternative Asia'
            WHEN c.origin_name IN (
                'Austrian', 'Belgian', 'Bulgarian', 'Croatian', 'Cypriot', 'Czech', 'Danish',
                'Dutch', 'Estonian', 'Finnish', 'French', 'German', 'Greek', 'Hungarian', 'Irish',
                'Italian', 'Latvian', 'Lithuanian', 'Luxembourgish', 'Maltese', 'Polish', 'Portuguese',
                'Romanian', 'Slovakian', 'Slovenian', 'Spanish', 'Swiss', 'Swedish'
            ) THEN 'Europe'
            WHEN c.origin_name = 'British' THEN 'GB'
            WHEN c.origin_name = 'American' THEN 'US'
            WHEN c.origin_name = 'Russian' THEN 'RU'
            WHEN c.origin_name = 'Ukrainian' THEN 'UA'
            WHEN c.origin_name = 'Moldovan' THEN 'MD'
            ELSE 'Other'
        END AS origin_group
    FROM {{ source('mart', 'published_products_current') }} AS a
    LEFT JOIN {{ ref('gold_merchant_categories') }} AS b
        ON a.category_id = b.merchant_category_id
    LEFT JOIN {{ ref('gold_merchants') }} AS c
        ON a.merchant_id = c.merchant_id
),

raw AS (
    SELECT
        -- Main order info
        a.ci.t AS order_datetime_utc,
        DATE(a.ci.t) AS order_date_utc,
        a._id AS jms_order_id,
        CASE
            WHEN a.st.st.s[0] = 0 THEN 'created'
            WHEN a.st.st.s[0] = 1 THEN 'fulfilledOnline'
            WHEN a.st.st.s[0] = 2 THEN 'shipped'
            WHEN a.st.st.s[0] = 3 THEN 'cancelledByMerchant'
            WHEN a.st.st.s[0] = 4 THEN 'refunded'
            WHEN a.st.st.s[0] = 5 THEN 'cancelledByJL'
            ELSE 'unknown'
        END AS order_status,
        a.aId AS jms_account_id,
        a.fId AS friendly_order_id,
        a.ogId AS order_group_id,
        a.mId AS merchant_id,
        pd.merchant_name,
        a.sId AS store_id,
        pd.origin_name,
        pd.origin_group,

        -- Marketplace info
        COALESCE(a.src.spd.spMp, a.src.mp) AS marketplace_name,
        a.src.mp AS integrator_name,

        -- Customer Info
        a.ci.a.c AS customer_country_code,
        a.ci.a.st AS customer_state,
        a.ci.a.ct AS customer_city,

        -- Product Info
        pd.business_line,
        pd.l1_merchant_category_name,
        pd.l2_merchant_category_name,
        pd.l3_merchant_category_name,
        pd.l4_merchant_category_name,
        pd.l5_merchant_category_name,
        a.pi.p AS product_id,
        a.pi.v AS variant_id,
        a.pi.q AS product_quantity,
        a.mi.d.ds AS items_discounted, -- Array<struct>

        -- Shipping Info
        a.st.sh.sId AS shipper_id,
        CASE
            WHEN a.pi.sh.t = 0 THEN 'none'
            WHEN a.pi.sh.t = 1 THEN 'JoomLogistics'
            WHEN a.pi.sh.t = 2 THEN 'merchant'
            ELSE 'Other'
        END AS shipping_type,
        a.st.sh.s AS shipper_name,
        CASE
            WHEN a.pi.sh.m = 0 OR a.pi.sh.m IS NULL THEN 'none'
            WHEN a.pi.sh.m = 1 THEN 'home'
            WHEN a.pi.sh.m = 2 THEN 'pickupPoint'
            WHEN a.pi.sh.m = 3 THEN 'lockers'
            ELSE 'Other'
        END AS shipping_method,
        a.pi.sh.edn AS shipping_external_delivery_number,
        a.pi.sh.p.amount * 1e-6 AS shipping_price_amount,
        a.pi.sh.p.ccy AS shipping_price_currency,
        a.st.sh.tn AS tracking_number,
        a.st.sh.oId AS online_order_id,
        a.st.sh.at AS shipping_arrived_time,
        a.st.sh.dt AS shipping_delivered_time,
        a.st.sh.opt.p.amount * 1e-6 AS shipping_option_price_amount, -- check if we need it
        a.st.sh.opt.p.ccy AS shipping_option_price_currency, -- check if we need it
        a.st.sh.opt.whId AS shipping_option_warehouse_id,
        a.st.sh.opt.tvId AS shipping_option_tier_version_id, -- for merchant shipping only
        a.st.sh.ps.sId AS shipping_partner_shipment_id,

        -- Merchant Info
        a.mi.ma * 1e-6 AS marketing_amount, -- кубышка

        a.mi.m.c AS merchant_price_currency,

        a.mi.m.up * 1e-6 AS merchant_item_price, -- merchant item price before discounts
        a.mi.m.us * 1e-6 AS merchant_item_shipping_price, -- Shipping price per item set by merchant

        a.mi.m.up * 1e-6 * a.pi.q AS merchant_list_price, -- merchant order price before all discounts
        a.mi.m.t * 1e-6 AS merchant_sale_price, -- merchant order price after all discounts
        a.mi.m.r * 1e-6 AS merchant_revenue, -- merchant revenue after take rate

        -- Logistics Info
        a.mi.l.p.amount * 1e-6 AS logistics_revenue_amount,
        a.mi.l.p.ccy AS logistics_revenue_currency,

        -- Take rate
        a.mi.tr.j / 100 AS take_rate_joom_share,
        a.mi.tr.p * 1e-6 / 100 AS take_rate_partner_share,
        a.mi.tr.jms * 1e-6 / 100 AS take_rate_jms_commission_share,

        -- Customer price and gmv
        a.mi.cp.c AS customer_currency,
        a.mi.cp.iPrice * 1e-6 AS customer_item_price_initial_local_currency,
        a.mi.cp.iVat * 1e-6 AS customer_item_vat_initial_local_currency,
        a.mi.cp.tPrice * 1e-6 AS customer_gmv_initial_local_currency,
        a.mi.cp.tVat * 1e-6 AS customer_vat_initial_local_currency,

        -- Customer price and gmv usd
        a.mi.c.gmv * 1e-6 AS gmv_wo_vat_usd,
        a.mi.cp.iPrice * 1e-6 * cr.rate AS customer_item_price_initial_usd,
        a.mi.cp.iVat * 1e-6 * cr.rate AS customer_item_vat_initial_usd,
        a.mi.cp.tPrice * 1e-6 * cr.rate AS customer_gmv_initial_usd,
        a.mi.cp.tVat * 1e-6 * cr.rate AS customer_vat_initial_usd,
        COALESCE((a.mi.cp.tPrice * 1e-6 * cr.rate) + (a.mi.cp.tVat * 1e-6 * cr.rate), a.mi.c.gmv * 1e-6) AS gmv_initial, -- logic is relevant untill customer gmv is ready for all MP

        -- Refund Information
        a.st.ref.f AS refund_fraction,
        IF(a.st.ref.cr IS NOT NULL, 1, NULL) AS is_customer_refund,
        IF(a.st.ref.mr IS NOT NULL, 1, NULL) AS is_merchant_refund,
        CASE
            WHEN a.st.ref.cr IS NULL AND a.st.ref.mr IS NULL THEN NULL
            -- customer refund reasons
            WHEN a.st.ref.cr = 0 THEN NULL
            WHEN a.st.ref.cr = 1 THEN 'cancelledByCustomer'
            WHEN a.st.ref.cr = 2 THEN 'notDelivered'
            WHEN a.st.ref.cr = 3 THEN 'emptyPackage'
            WHEN a.st.ref.cr = 4 THEN 'badQuality'
            WHEN a.st.ref.cr = 5 THEN 'damaged'
            WHEN a.st.ref.cr = 6 THEN 'wrongProduct'
            WHEN a.st.ref.cr = 7 THEN 'wrongQuantity'
            WHEN a.st.ref.cr = 8 THEN 'wrongSize'
            WHEN a.st.ref.cr = 9 THEN 'wrongColor'
            WHEN a.st.ref.cr = 10 THEN 'minorProblems'
            WHEN a.st.ref.cr = 11 THEN 'differentFromImages'
            WHEN a.st.ref.cr = 12 THEN 'other'
            WHEN a.st.ref.cr = 13 THEN 'customerFraud'
            WHEN a.st.ref.cr = 14 THEN 'counterfeit'
            WHEN a.st.ref.cr = 15 THEN 'sentBackToMerchant'
            WHEN a.st.ref.cr = 16 THEN 'returnedToMerchantByShipper'
            WHEN a.st.ref.cr = 17 THEN 'misleadingInformation'
            WHEN a.st.ref.cr = 18 THEN 'paidByJoom'
            WHEN a.st.ref.cr = 19 THEN 'fulfillByJoomUnavailable'
            WHEN a.st.ref.cr = 20 THEN 'approveRejected'
            WHEN a.st.ref.cr = 21 THEN 'productBanned'
            WHEN a.st.ref.cr = 22 THEN 'packageIncomplete'
            WHEN a.st.ref.cr = 23 THEN 'notAsDescribed'
            WHEN a.st.ref.cr = 24 THEN 'missingProofOfDelivery'
            WHEN a.st.ref.cr = 25 THEN 'deliveredToWrongAddress'
            WHEN a.st.ref.cr = 26 THEN 'turnedBackByPostOffice'
            WHEN a.st.ref.cr = 27 THEN 'bannedByCustoms'
            WHEN a.st.ref.cr = 28 THEN 'incorrectConsolidation'
            WHEN a.st.ref.cr = 29 THEN 'freeItemCancel'
            WHEN a.st.ref.cr = 30 THEN 'groupPurchaseExpired'
            WHEN a.st.ref.cr = 31 THEN 'returnedToMerchantInOrigin'
            WHEN a.st.ref.cr = 32 THEN 'vatOnDelivery'
            WHEN a.st.ref.cr = 33 THEN 'shippingUnavailable'
            WHEN a.st.ref.cr = 34 THEN 'threeForTwoCancel'
            WHEN a.st.ref.cr = 35 THEN 'reseller'
            WHEN a.st.ref.cr = 36 THEN 'bmplCancel'
            WHEN a.st.ref.cr = 37 THEN 'couponAllOrderCancelBehaviour'
            WHEN a.st.ref.cr = 38 THEN 'minOrderPriceAllOrderCancel'
            -- merchant refund reasons
            WHEN a.st.ref.mr = 0 THEN NULL
            WHEN a.st.ref.mr = 1 THEN 'unableToFulfill'
            WHEN a.st.ref.mr = 2 THEN 'outOfStock'
            WHEN a.st.ref.mr = 3 THEN 'wrongAddress'
            WHEN a.st.ref.mr = 4 THEN 'notShippedOnTime or pickupProviderFault' -- need to align with gold.orders
            ELSE 'other'
        END AS detailed_refund_reason,
        a.st.ref.cb AS refund_created_by
    FROM {{ source('mongo', 'order_jms_orders_daily_snapshot') }} AS a
    LEFT JOIN currency_rate AS cr
        ON
            a.mi.cp.c = cr.currency_code
            AND a.ci.t > cr.effective_date
            AND a.ci.t <= cr.next_effective_date
    LEFT JOIN product_data AS pd ON
        a.pi.p = pd.product_id
)

SELECT *
FROM raw
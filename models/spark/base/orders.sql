{{
  config(
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'day',
      'priority_weight': '1000',
      'bigquery_upload_horizon_days': '230',
    },
    alias='orders',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['day'],
    on_schema_change='sync_all_columns',
  )
}}

WITH product_order_number AS (
    SELECT
        order_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_time_utc) AS product_order_number
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE product_id IS NOT null
),

orders_ext0 AS (
    SELECT
        so.partition_date AS day,
        so.created_time_utc,
        so.device_id,
        so.real_user_id,
        so.user_id,
        so.order_id,
        so.friendly_order_id,
        so.order_group_id,
        so.merchant_id,
        so.store_id,
        so.product_quantity,
        so.psp,
        so.product_id,
        so.product_variant_id,
        so.category_id,
        so.refund_reason,
        TO_DATE(CAST(CAST(so.refund_time_utc AS INT) + 3 * 3600 AS TIMESTAMP)) AS refund_date_msk,
        UPPER(so.shipping_country) AS country,
        LOWER(so.os_type) AS platform,
        COALESCE(so.last_context.name, 'unknown') AS last_context,
        ROUND(so.gmv_initial, 3) AS gmv_initial,
        ROUND(so.gmv_final, 3) AS gmv_final,
        ROUND(so.refund, 3) AS gmv_refunded,
        ROUND(so.ecgp_initial, 3) AS ecgp_initial,
        ROUND(so.ecgp_final, 3) AS ecgp_final,
        ROUND(so.order_gross_profit_final, 3) AS order_gross_profit_final,
        ROUND(so.merchant_revenue_initial, 3) AS merchant_revenue_initial,
        ROUND(so.order_gross_profit_final_estimated, 3) AS order_gross_profit_final_estimated,
        ROUND(so.merchant_revenue_final, 3) AS merchant_revenue_final,
        ROUND(so.gmv_initial / so.product_quantity, 3) AS item_gmv,
        ROUND(so.gmv_initial / so.product_quantity, 0) AS item_gmv_rounded,
        ROUND(so.merchant_revenue_initial / so.product_quantity, 0) AS item_merchant_revenue_initial_rounded,
        ROUND(so.logistics_price_initial, 3) AS logistics_price_initial,
        ROUND(so.merchant_revenue_initial / so.product_quantity, 3) AS item_merchant_revenue_initial,
        ROUND(so.logistics_price_initial / so.product_quantity, 3) AS item_logistics_price_initial,
        ROUND(so.merchant_sale_price, 3) AS merchant_sale_price,
        ROUND(so.merchant_list_price, 3) AS merchant_list_price,
        ROUND(so.extra_charge, 3) AS extra_charge,
        IF(so.gmv_initial = 0, null, ROUND(so.ecgp_initial / so.gmv_initial, 3)) AS ecgp_in_gmv,
        ROUND(so.psp_initial, 3) AS psp_initial,
        ROUND(so.psp_refund_fee, 3) AS psp_refund_fee,
        ROUND(so.psp_chargeback_fee, 3) AS psp_chargeback_fee,
        so.estimated_delivery_min_days,
        so.estimated_delivery_max_days,
        so.customer_refund_reason IS NOT null AND so.customer_refund_reason IN (2, 5, 16, 24, 25, 26, 28) AS is_not_delivered_refund,
        so.customer_refund_reason IS NOT null AND so.customer_refund_reason IN (3, 4, 6, 7, 8, 9, 10, 11, 14, 17, 21, 22, 23, 27) AS is_quality_refund,
        -- note: this one can break if is_finalized changes after 230 days after order is created
        COALESCE((
            so.customer_refund_reason IS NOT null
            OR so.refund_reason IS NOT null
            OR so.delivered_time_utc IS NOT null
            OR DATEDIFF(CURRENT_DATE(), CAST(so.created_time_utc AS DATE)) > so.warranty_duration_max_days
        ), false) AS is_finalized,
        so.used_coupon_id,
        so.used_coupon_id IS NOT null AS with_coupon,
        so.coupon AS coupon_discount,
        so.points_initial > 0 AS with_points,
        so.points_initial,
        so.points_final,
        so.discounts,
        COALESCE(so.jl_shipping_type_initial, 'offline') AS shipping_type,
        TO_DATE(so.review_time_utc) AS review_day,
        so.review_stars,
        so.review_has_text,
        so.review_media_count,
        so.review_image_count,
        so.customer_refund_reason,
        COALESCE(
            DATE_FORMAT(TO_DATE(so.partition_date), 'yyyy-MM') = DATE_FORMAT(TO_DATE(so.real_user_join_ts_msk), 'yyyy-MM'),
            false
        ) AS is_join_month_order,

        COALESCE(
            so.rating_counts.count_1_star
            + so.rating_counts.count_2_star
            + so.rating_counts.count_3_star
            + so.rating_counts.count_4_star
            + so.rating_counts.count_5_star, 0
        ) AS number_of_reviews,
        COALESCE(
            so.rating_counts.count_1_star * 1
            + so.rating_counts.count_2_star * 2
            + so.rating_counts.count_3_star * 3
            + so.rating_counts.count_4_star * 4
            + so.rating_counts.count_5_star * 5, 0
        ) AS total_product_rating,

        COALESCE(
            (so.review_stars IN (1, 2) AND DATEDIFF(so.review_time_utc, so.created_time_utc) <= 80)
            OR (
                so.customer_refund_reason IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 17, 21, 22, 23, 27)
                AND DATEDIFF(so.refund_time_utc, so.created_time_utc) <= 80
            ), false
        ) AS is_negative_feedback,
        CASE
            WHEN so.customer_refund_reason IS null OR so.customer_refund_reason = 0 THEN 'none'
            WHEN so.customer_refund_reason = 1 THEN 'cancelledByCustomer'
            WHEN so.customer_refund_reason = 2 THEN 'notDelivered'
            WHEN so.customer_refund_reason = 3 THEN 'emptyPackage'
            WHEN so.customer_refund_reason = 4 THEN 'badQuality'
            WHEN so.customer_refund_reason = 5 THEN 'damaged'
            WHEN so.customer_refund_reason = 6 THEN 'wrongProduct'
            WHEN so.customer_refund_reason = 7 THEN 'wrongQuantity'
            WHEN so.customer_refund_reason = 8 THEN 'wrongSize'
            WHEN so.customer_refund_reason = 9 THEN 'wrongColor'
            WHEN so.customer_refund_reason = 10 THEN 'minorProblems'
            WHEN so.customer_refund_reason = 11 THEN 'differentFromImages'
            WHEN so.customer_refund_reason = 12 THEN 'other'
            WHEN so.customer_refund_reason = 13 THEN 'customerFraud'
            WHEN so.customer_refund_reason = 14 THEN 'counterfeit'
            WHEN so.customer_refund_reason = 15 THEN 'sentBackToMerchant'
            WHEN so.customer_refund_reason = 16 THEN 'returnedToMerchantByShipper'
            WHEN so.customer_refund_reason = 17 THEN 'misleadingInformation'
            WHEN so.customer_refund_reason = 18 THEN 'paidByJoom'
            WHEN so.customer_refund_reason = 19 THEN 'fulfillByJoomUnavailable'
            WHEN so.customer_refund_reason = 20 THEN 'approveRejected'
            WHEN so.customer_refund_reason = 21 THEN 'productBanned'
            WHEN so.customer_refund_reason = 22 THEN 'packageIncomplete'
            WHEN so.customer_refund_reason = 23 THEN 'notAsDescribed'
            WHEN so.customer_refund_reason = 24 THEN 'missingProofOfDelivery'
            WHEN so.customer_refund_reason = 25 THEN 'deliveredToWrongAddress'
            WHEN so.customer_refund_reason = 26 THEN 'turnedBackByPostOffice'
            WHEN so.customer_refund_reason = 27 THEN 'bannedByCustoms'
            WHEN so.customer_refund_reason = 28 THEN 'incorrectConsolidation'
            WHEN so.customer_refund_reason = 29 THEN 'freeItemCancel'
            WHEN so.customer_refund_reason = 30 THEN 'groupPurchaseExpired'
            WHEN so.customer_refund_reason = 31 THEN 'returnedToMerchantInOrigin'
            WHEN so.customer_refund_reason = 32 THEN 'vatOnDelivery'
            WHEN so.customer_refund_reason = 33 THEN 'shippingUnavailable'
            ELSE 'Unknown'
        END AS detailed_refund_reason,
        so.gmv_initial <= 0.05 AS is_influencer_order,
        pon.product_order_number
    FROM {{ source('mart', 'star_order_2020') }} AS so
    LEFT JOIN product_order_number AS pon USING (order_id)
    WHERE
        true
        AND NOT (so.refund_reason = 'fraud' AND so.refund_reason IS NOT null)
        {% if is_incremental() %}
            AND so.partition_date >= DATE '{{ var("start_date_ymd") }}' - INTERVAL 230 DAYS
        {% endif %}
),

logistics_orders AS (
    SELECT
        order_id,
        tracking_delivered_time_utc
    FROM {{ source('logistics_mart', 'fact_order') }}
),

support_tickets AS (
    SELECT
        order_id,
        MAX(ticket_id) AS ticket_id
    FROM {{ ref('joom_babylone_tickets') }}
    GROUP BY order_id
),

orders_ext1 AS (
    SELECT
        o.day,
        o.created_time_utc,
        o.device_id,
        o.real_user_id,
        o.user_id,
        o.order_id,
        o.friendly_order_id,
        o.order_group_id,
        o.merchant_id,
        o.store_id,
        o.product_quantity,
        o.psp,
        o.product_id,
        o.product_variant_id,
        o.category_id,
        o.refund_reason,
        o.refund_date_msk,
        o.country,
        o.platform,
        o.last_context,
        o.gmv_initial,
        o.gmv_final,
        o.gmv_refunded,
        o.ecgp_initial,
        o.ecgp_final,
        o.order_gross_profit_final,
        o.merchant_revenue_initial,
        o.order_gross_profit_final_estimated,
        o.merchant_revenue_final,
        o.item_gmv,
        o.item_gmv_rounded,
        o.item_merchant_revenue_initial_rounded,
        o.logistics_price_initial,
        o.item_merchant_revenue_initial,
        o.item_logistics_price_initial,
        o.merchant_sale_price,
        o.merchant_list_price,
        o.extra_charge,
        o.ecgp_in_gmv,
        o.psp_initial,
        o.psp_refund_fee,
        o.psp_chargeback_fee,
        o.estimated_delivery_min_days,
        o.estimated_delivery_max_days,
        o.is_not_delivered_refund,
        o.is_quality_refund,
        o.used_coupon_id,
        o.with_coupon,
        o.coupon_discount,
        o.with_points,
        o.points_initial,
        o.points_final,
        o.discounts,
        o.shipping_type,
        o.review_day,
        o.review_stars,
        o.review_has_text,
        o.review_media_count,
        o.review_image_count,
        o.customer_refund_reason,
        o.is_join_month_order,
        o.number_of_reviews,
        o.is_negative_feedback,
        o.detailed_refund_reason,
        o.is_influencer_order,

        IF(o.number_of_reviews > 0, ROUND(o.total_product_rating / o.number_of_reviews, 1), null) AS product_rating, -- рейтинг товара на момент покупки
        COALESCE(o.number_of_reviews >= 15, false) AS is_product_with_stable_rating, -- был ли рейтинг стабильным на момент покупки
        o.product_order_number, -- номер покупки товара
        COALESCE(b.ticket_id, c.ticket_id) AS support_ticket_id,
        IF(l.tracking_delivered_time_utc IS NOT null, true, o.is_finalized) AS is_finalized

    FROM orders_ext0 AS o
    -- Не менять порядок джойнов - джойны по order_id должны идти подряд, чтобы избежать лишних шафлов
    LEFT JOIN logistics_orders AS l USING (order_id)
    -- note: this one can break if support_tickets arrives after 230 days after order is created
    -- добавляем id тикета сапорта, если есть 
    LEFT JOIN support_tickets AS b USING (order_id)
    LEFT JOIN support_tickets AS c ON o.friendly_order_id = c.order_id
),

orders_ext2 AS (
    -- добавляем информацию, новый пользователь или нет из таблицы active_devices
    SELECT
        a.day,
        a.created_time_utc,
        a.device_id,
        a.real_user_id,
        a.user_id,
        a.order_id,
        a.friendly_order_id,
        a.order_group_id,
        a.merchant_id,
        a.store_id,
        a.product_quantity,
        a.psp,
        a.product_id,
        a.product_variant_id,
        a.category_id,
        a.refund_reason,
        a.refund_date_msk,
        a.country,
        a.platform,
        a.last_context,
        a.gmv_initial,
        a.gmv_final,
        a.gmv_refunded,
        a.ecgp_initial,
        a.ecgp_final,
        a.order_gross_profit_final,
        a.merchant_revenue_initial,
        a.order_gross_profit_final_estimated,
        a.merchant_revenue_final,
        a.item_gmv,
        a.item_gmv_rounded,
        a.item_merchant_revenue_initial_rounded,
        a.logistics_price_initial,
        a.item_merchant_revenue_initial,
        a.item_logistics_price_initial,
        a.merchant_sale_price,
        a.merchant_list_price,
        a.extra_charge,
        a.ecgp_in_gmv,
        a.psp_initial,
        a.psp_refund_fee,
        a.psp_chargeback_fee,
        a.estimated_delivery_min_days,
        a.estimated_delivery_max_days,
        a.is_not_delivered_refund,
        a.is_quality_refund,
        a.used_coupon_id,
        a.with_coupon,
        a.coupon_discount,
        a.with_points,
        a.points_initial,
        a.points_final,
        a.discounts,
        a.shipping_type,
        a.review_day,
        a.review_stars,
        a.review_has_text,
        a.review_media_count,
        a.review_image_count,
        a.customer_refund_reason,
        a.is_join_month_order,
        a.number_of_reviews,
        a.is_negative_feedback,
        a.detailed_refund_reason,
        a.is_influencer_order,
        a.product_rating,
        a.is_product_with_stable_rating,
        a.product_order_number,
        a.support_ticket_id,

        a.is_finalized,
        COALESCE(b.is_new_user, false) AS is_new_user
    FROM orders_ext1 AS a
    LEFT JOIN {{ ref('active_devices') }} AS b ON a.device_id = b.device_id AND a.day = b.day
)

SELECT *
FROM orders_ext2
DISTRIBUTE BY day
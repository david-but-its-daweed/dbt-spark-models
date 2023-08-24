{{
  config(
    partition_by=['day'],
    alias='orders',
    file_format='delta',
    incremental_strategy='append',
    materialized='incremental',
  )
}}

WITH product_numbers AS (
    SELECT
        order_id,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_time_utc) AS product_order_number
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE
        TRUE
        AND NOT (refund_reason IN ('fraud', 'cancelled_by_customer') AND refund_reason IS NOT NULL)
),

orders_ext0 AS (
    SELECT
        partition_date AS day,
        created_time_utc,
        device_id,
        real_user_id,
        user_id,
        order_id,
        friendly_order_id,
        order_group_id,
        merchant_id,
        store_id,
        product_quantity,
        psp,
        product_id,
        product_variant_id,
        category_id,
        refund_reason,
        TO_DATE(CAST(CAST(refund_time_utc AS INT) + 3 * 3600 AS TIMESTAMP)) AS refund_date_msk,
        UPPER(shipping_country) AS country,
        LOWER(os_type) AS platform,
        COALESCE(last_context.name, 'unknown') AS last_context,
        ROUND(gmv_initial, 3) AS gmv_initial,
        ROUND(gmv_final, 3) AS gmv_final,
        ROUND(refund, 3) AS gmv_refunded,
        ROUND(ecgp_initial, 3) AS ecgp_initial,
        ROUND(ecgp_final, 3) AS ecgp_final,
        ROUND(order_gross_profit_final, 3) AS order_gross_profit_final,
        ROUND(merchant_revenue_initial, 3) AS merchant_revenue_initial,
        ROUND(order_gross_profit_final_estimated, 3) AS order_gross_profit_final_estimated,
        ROUND(merchant_revenue_final, 3) AS merchant_revenue_final,
        ROUND(gmv_initial / product_quantity, 3) AS item_gmv,
        ROUND(gmv_initial / product_quantity, 0) AS item_gmv_rounded,
        ROUND(merchant_revenue_initial / product_quantity, 0) AS item_merchant_revenue_initial_rounded,
        ROUND(logistics_price_initial, 3) AS logistics_price_initial,
        ROUND(merchant_revenue_initial / product_quantity, 3) AS item_merchant_revenue_initial,
        ROUND(logistics_price_initial / product_quantity, 3) AS item_logistics_price_initial,
        ROUND(merchant_sale_price, 3) AS merchant_sale_price,
        ROUND(merchant_list_price, 3) AS merchant_list_price,
        ROUND(extra_charge, 3) AS extra_charge,
        IF(gmv_initial = 0, NULL, ROUND(ecgp_initial / gmv_initial, 3)) AS ecgp_in_gmv,
        ROUND(psp_initial, 3) AS psp_initial,
        ROUND(psp_refund_fee, 3) AS psp_refund_fee,
        ROUND(psp_chargeback_fee, 3) AS psp_chargeback_fee,
        estimated_delivery_min_days,
        estimated_delivery_max_days,
        customer_refund_reason IS NOT NULL AND customer_refund_reason IN (2, 5, 16, 24, 25, 26, 28) AS is_not_delivered_refund,
        customer_refund_reason IS NOT NULL AND customer_refund_reason IN (3, 4, 6, 7, 8, 9, 10, 11, 14, 17, 21, 22, 23, 27) AS is_quality_refund,
        COALESCE((
            customer_refund_reason IS NOT NULL
            OR refund_reason IS NOT NULL
            OR delivered_time_utc IS NOT NULL
            OR DATEDIFF(CURRENT_DATE(), CAST(created_time_utc AS DATE)) > warranty_duration_max_days
        ), FALSE) AS is_finalized,
        used_coupon_id,
        used_coupon_id IS NOT NULL AS with_coupon,
        coupon AS coupon_discount,
        points_initial > 0 AS with_points,
        points_initial,
        points_final,
        discounts,
        COALESCE(jl_shipping_type_initial, 'offline') AS shipping_type,
        TO_DATE(review_time_utc) AS review_day,
        review_stars,
        review_has_text,
        review_media_count,
        review_image_count,
        customer_refund_reason,
        COALESCE(
            DATE_FORMAT(TO_DATE(partition_date), 'yyyy-MM') = DATE_FORMAT(TO_DATE(real_user_join_ts_msk), 'yyyy-MM'),
            FALSE
        ) AS is_join_month_order,

        COALESCE(
            rating_counts.count_1_star
            + rating_counts.count_2_star
            + rating_counts.count_3_star
            + rating_counts.count_4_star
            + rating_counts.count_5_star, 0
        ) AS number_of_reviews,
        COALESCE(
            rating_counts.count_1_star * 1
            + rating_counts.count_2_star * 2
            + rating_counts.count_3_star * 3
            + rating_counts.count_4_star * 4
            + rating_counts.count_5_star * 5, 0
        ) AS total_product_rating,

        COALESCE(
            (review_stars IN (1, 2) AND DATEDIFF(review_time_utc, created_time_utc) <= 80)
            OR (
                customer_refund_reason IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 17, 21, 22, 23, 27)
                AND DATEDIFF(refund_time_utc, created_time_utc) <= 80
            ), FALSE
        ) AS is_negative_feedback,
        CASE
            WHEN customer_refund_reason IS NULL OR customer_refund_reason = 0 THEN 'none'
            WHEN customer_refund_reason = 1 THEN 'cancelledByCustomer'
            WHEN customer_refund_reason = 2 THEN 'notDelivered'
            WHEN customer_refund_reason = 3 THEN 'emptyPackage'
            WHEN customer_refund_reason = 4 THEN 'badQuality'
            WHEN customer_refund_reason = 5 THEN 'damaged'
            WHEN customer_refund_reason = 6 THEN 'wrongProduct'
            WHEN customer_refund_reason = 7 THEN 'wrongQuantity'
            WHEN customer_refund_reason = 8 THEN 'wrongSize'
            WHEN customer_refund_reason = 9 THEN 'wrongColor'
            WHEN customer_refund_reason = 10 THEN 'minorProblems'
            WHEN customer_refund_reason = 11 THEN 'differentFromImages'
            WHEN customer_refund_reason = 12 THEN 'other'
            WHEN customer_refund_reason = 13 THEN 'customerFraud'
            WHEN customer_refund_reason = 14 THEN 'counterfeit'
            WHEN customer_refund_reason = 15 THEN 'sentBackToMerchant'
            WHEN customer_refund_reason = 16 THEN 'returnedToMerchantByShipper'
            WHEN customer_refund_reason = 17 THEN 'misleadingInformation'
            WHEN customer_refund_reason = 18 THEN 'paidByJoom'
            WHEN customer_refund_reason = 19 THEN 'fulfillByJoomUnavailable'
            WHEN customer_refund_reason = 20 THEN 'approveRejected'
            WHEN customer_refund_reason = 21 THEN 'productBanned'
            WHEN customer_refund_reason = 22 THEN 'packageIncomplete'
            WHEN customer_refund_reason = 23 THEN 'notAsDescribed'
            WHEN customer_refund_reason = 24 THEN 'missingProofOfDelivery'
            WHEN customer_refund_reason = 25 THEN 'deliveredToWrongAddress'
            WHEN customer_refund_reason = 26 THEN 'turnedBackByPostOffice'
            WHEN customer_refund_reason = 27 THEN 'bannedByCustoms'
            WHEN customer_refund_reason = 28 THEN 'incorrectConsolidation'
            WHEN customer_refund_reason = 29 THEN 'freeItemCancel'
            WHEN customer_refund_reason = 30 THEN 'groupPurchaseExpired'
            WHEN customer_refund_reason = 31 THEN 'returnedToMerchantInOrigin'
            WHEN customer_refund_reason = 32 THEN 'vatOnDelivery'
            WHEN customer_refund_reason = 33 THEN 'shippingUnavailable'
            ELSE 'Unknown'
        END AS detailed_refund_reason,
        gmv_initial <= 0.05 AS is_influencer_order
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE
        TRUE
        AND NOT (refund_reason IN ('fraud', 'cancelled_by_customer') AND refund_reason IS NOT NULL)
    {% if is_incremental() %}
        AND DATEDIFF(CURRENT_DATE(), date) < 181
    {% elif target.name != 'prod' %}
        AND DATEDIFF(CURRENT_DATE(), date) < 181
    {% endif %}
),

orders_ext1 AS (
    SELECT
        o.*,
        n.product_order_number
    FROM orders_ext0 AS o
    LEFT JOIN product_numbers AS n USING (order_id, product_id)
),

logistics_orders AS (
    SELECT
        order_id,
        tracking_delivered_time_utc
    FROM {{ source('logistics_mart', 'fact_order') }}
),

orders_ext2 AS (
    SELECT
        day,
        created_time_utc,
        device_id,
        real_user_id,
        user_id,
        order_id,
        friendly_order_id,
        order_group_id,
        merchant_id,
        store_id,
        product_quantity,
        psp,
        product_id,
        product_variant_id,
        category_id,
        refund_reason,
        refund_date_msk,
        country,
        platform,
        last_context,
        gmv_initial,
        gmv_final,
        gmv_refunded,
        ecgp_initial,
        ecgp_final,
        order_gross_profit_final,
        merchant_revenue_initial,
        order_gross_profit_final_estimated,
        merchant_revenue_final,
        item_gmv,
        item_gmv_rounded,
        item_merchant_revenue_initial_rounded,
        logistics_price_initial,
        item_merchant_revenue_initial,
        item_logistics_price_initial,
        merchant_sale_price,
        merchant_list_price,
        extra_charge,
        ecgp_in_gmv,
        psp_initial,
        psp_refund_fee,
        psp_chargeback_fee,
        estimated_delivery_min_days,
        estimated_delivery_max_days,
        is_not_delivered_refund,
        is_quality_refund,
        is_finalized,
        used_coupon_id,
        with_coupon,
        coupon_discount,
        with_points,
        points_initial,
        points_final,
        discounts,
        shipping_type,
        review_day,
        review_stars,
        review_has_text,
        review_media_count,
        review_image_count,
        customer_refund_reason,
        is_join_month_order,
        number_of_reviews,
        is_negative_feedback,
        detailed_refund_reason,
        is_influencer_order,

        IF(number_of_reviews > 0, ROUND(total_product_rating / number_of_reviews, 1), NULL) AS product_rating, -- рейтинг товара на момент покупки
        COALESCE(number_of_reviews >= 15, FALSE) AS is_product_with_stable_rating, -- был ли рейтинг стабильным на момент покупки
        product_order_number -- номер покупки товара
    FROM orders_ext1
),

support_tickets AS (
    SELECT
        order_id,
        MAX(ticket_id) AS ticket_id
    FROM {{ ref('joom_babylone_tickets') }}
    GROUP BY order_id
),

orders_ext3 AS (
    -- добавляем id тикета сапорта, если есть 
    SELECT
        a.*,
        COALESCE(b.ticket_id, c.ticket_id) AS support_ticket_id
    FROM orders_ext2 AS a
    LEFT JOIN support_tickets AS b ON a.order_id = b.order_id
    LEFT JOIN support_tickets AS c ON a.friendly_order_id = c.order_id
),


orders_ext4 AS (
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

        IF(l.tracking_delivered_time_utc IS NOT NULL, TRUE, a.is_finalized) AS is_finalized,
        COALESCE(b.is_new_user, FALSE) AS is_new_user
    FROM orders_ext3 AS a
    LEFT JOIN {{ ref('active_devices') }} AS b ON a.device_id = b.device_id AND a.day = b.day
    LEFT JOIN logistics_orders AS l USING (order_id)
)

SELECT * FROM orders_ext4
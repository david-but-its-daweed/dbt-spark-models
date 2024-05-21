{{
  config(
    materialized='incremental',
    alias='orders',
    file_format='parquet',
    schema='gold',
    incremental_strategy='insert_overwrite',
    partition_by=['month'],
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'order_date_msk',
        'bigquery_upload_horizon_days': '230',
        'priority_weight': '1000',
    },
  )
}}

-- todo: calculate what period we should take for incremental update to get the correct result

WITH numbers AS (
    SELECT
        order_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_time_utc) AS product_orders_number, -- номер покупки товара
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY created_time_utc) AS device_orders_number, -- номер покупки девайса
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_time_utc) AS user_orders_number, -- номер покупки пользователя
        ROW_NUMBER() OVER (PARTITION BY real_user_id ORDER BY created_time_utc) AS real_user_orders_number -- номер покупки пользователя (real_id)
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE NOT(refund_reason = 'fraud' AND refund_reason IS NOT NULL)
),

orders_ext0 AS (
    SELECT
        order_id,
        friendly_order_id,
        order_group_id,

        device_id,
        real_user_id,
        user_id,

        partition_date AS order_date_msk,
        created_time_utc AS order_datetime_utc,

        IF(legal_entity = 'jmt', 'JMT', 'Joom') AS legal_entity,
        CASE
            WHEN partition_date < '2023-07-01' OR app_entity = 'joom'
                THEN 'Joom'
            WHEN app_entity = 'joom_geek'
                THEN 'Joom Geek'
            WHEN app_entity = 'cool_be'
                THEN 'CoolBe'
            WHEN app_entity = 'cool_be_com'
                THEN 'CoolBeCom'
            WHEN app_entity = 'cool_be_trending'
                THEN 'CoolBe Trending'
        END AS app_entity,
        merchant_id,
        store_id,
        product_id,
        product_variant_id,
        category_id AS merchant_category_id,

        UPPER(shipping_country) AS country_code,
        currency AS currency_code,
        LOWER(os_type) AS platform,
        COALESCE(last_context.name, 'unknown') AS last_context,
        normalized_contexts AS contexts,
        COALESCE(
            (
                customer_refund_reason IS NOT NULL
                OR refund_reason IS NOT NULL
                OR delivered_time_utc IS NOT NULL
                OR DATEDIFF(CURRENT_DATE(), CAST(created_time_utc AS DATE)) > warranty_duration_max_days
            ),
            FALSE
        ) AS is_finalized,

        product_quantity,
        ROUND(gmv_initial, 3) AS gmv_initial,
        ROUND(gmv_final, 3) AS gmv_final,
        ROUND(refund, 3) AS gmv_refunded,
        ROUND(amount_currency, 3) AS gmv_initial_in_local_currency,
        psp AS psp_name,
        ROUND(psp_initial, 3) AS psp_initial,
        ROUND(psp_final, 3) AS psp_final,
        ROUND(jl_cost_final_estimated, 3) AS jl_cost_final_estimated,
        ROUND(order_gross_profit_final, 3) AS order_gross_profit_final,
        ROUND(order_gross_profit_final_estimated, 3) AS order_gross_profit_final_estimated,
        ROUND(ecgp_initial, 3) AS ecgp_initial,
        ROUND(ecgp_final, 3) AS ecgp_final,
        ROUND(merchant_revenue_initial, 3) AS merchant_revenue_initial,
        ROUND(merchant_revenue_final, 3) AS merchant_revenue_final,
        ROUND(merchant_sale_price, 3) AS merchant_sale_price,
        ROUND(merchant_list_price, 3) AS merchant_list_price,
        ROUND(merchant_revenue_initial / product_quantity, 3) AS item_merchant_revenue_initial,
        ROUND(logistics_price_initial, 3) AS logistics_price_initial,
        ROUND(logistics_price_initial / product_quantity, 3) AS item_logistics_price_initial,
        ROUND(vat_markup, 3) AS vat_markup,
        ROUND(merchant_sale_price - merchant_revenue_initial, 3) AS marketplace_commission_initial,
        ROUND(logistics_extra_charge, 3) AS jl_markup,
        used_coupon_id IS NOT NULL AS is_with_coupon,
        ROUND(coupon, 3) AS coupon_discount,
        used_coupon_id,
        points_initial > 0 AS is_with_points,
        points_initial,
        points_final,
        COALESCE(ARRAY_CONTAINS(discounts.type, 'specialPriceFinal'), FALSE) AS is_with_special_price,
        ROUND(COALESCE(FILTER(discounts, x -> x.type = 'specialPriceFinal')[0].amount * 1e6, 0), 3) AS special_price_discount,
        discounts,

        refund_reason IS NOT NULL AS is_refunded,
        refund_reason,
        CASE
            WHEN refund_reason IS NOT NULL AND (customer_refund_reason IS NULL AND merchant_refund_reason IS NULL) THEN 'Unknown'
            WHEN refund_reason IS NULL AND (customer_refund_reason IS NULL AND merchant_refund_reason IS NULL) THEN NULL

            WHEN customer_refund_reason = 0 THEN NULL
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
            WHEN customer_refund_reason = 34 THEN 'threeForTwoCancel'
            WHEN customer_refund_reason = 35 THEN 'reseller'
            WHEN customer_refund_reason = 36 THEN 'bmplCancel'
            WHEN customer_refund_reason = 37 THEN 'couponAllOrderCancelBehaviour'
            WHEN customer_refund_reason = 38 THEN 'minOrderPriceAllOrderCancel'

            WHEN merchant_refund_reason = 0 THEN NULL
            WHEN merchant_refund_reason = 1 THEN 'unableToFulfill'
            WHEN merchant_refund_reason = 2 THEN 'outOfStock'
            WHEN merchant_refund_reason = 3 THEN 'wrongAddress'
            WHEN merchant_refund_reason = 4 THEN 'notShippedOnTime'
        END AS detailed_refund_reason,
        TO_DATE(refund_time_utc + INTERVAL 3 HOUR) AS refund_date_msk,
        customer_refund_reason IS NOT NULL AND customer_refund_reason IN (2, 5, 16, 24, 25, 26, 28) AS is_not_delivered_refund,
        customer_refund_reason IS NOT NULL AND customer_refund_reason IN (3, 4, 6, 7, 8, 9, 10, 11, 14, 17, 21, 22, 23, 27) AS is_quality_refund,

        COALESCE(refund_reason IN ('cancelled_by_merchant', 'other'), FALSE) AS is_canceled_by_merchant,

        COALESCE(jl_shipping_type_initial, 'offline') AS shipping_type_initial,
        estimated_delivery_min_days,
        estimated_delivery_max_days,

        TO_DATE(review_time_utc + INTERVAL 3 HOUR) AS review_date_msk,
        review_stars,
        review_has_text AS is_review_with_text,
        review_media_count,
        review_image_count,
        COALESCE(
            rating_counts.count_1_star
            + rating_counts.count_2_star
            + rating_counts.count_3_star
            + rating_counts.count_4_star
            + rating_counts.count_5_star,
            0
        ) AS number_of_reviews,
        COALESCE(
            rating_counts.count_1_star * 1
            + rating_counts.count_2_star * 2
            + rating_counts.count_3_star * 3
            + rating_counts.count_4_star * 4
            + rating_counts.count_5_star * 5,
            0
        ) AS total_product_rating,

        COALESCE(
            (
                (review_stars IS NOT NULL AND review_stars IN (1, 2))
                AND DATEDIFF(review_time_utc, created_time_utc) <= 80
            )
            OR
            (
                (customer_refund_reason IS NOT NULL AND customer_refund_reason IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 17, 21, 22, 23, 27))
                AND DATEDIFF(refund_time_utc, created_time_utc) <= 80
            ),
            FALSE
        ) AS is_negative_feedback

    FROM {{ source('mart', 'star_order_2020') }}
    WHERE
        NOT(refund_reason = 'fraud' AND refund_reason IS NOT NULL)
        {% if is_incremental() %}
            AND partition_date >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
        {% endif %}
),

logistics_orders AS (
    SELECT
        order_id,
        delivery_duration_by_tracking,
        delivery_duration_by_user,
        tracking_delivered_datetime_utc,
        jl_consolidation_profit_final,
        is_delivered_by_jl
    FROM {{ ref('gold_logistics_orders') }}
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
        o.*,
        n.product_orders_number,
        n.device_orders_number,
        n.user_orders_number,
        n.real_user_orders_number
    FROM orders_ext0 AS o
    LEFT JOIN numbers AS n USING (order_id)
),

orders_ext2 AS (
    SELECT
        order_id,
        friendly_order_id,
        order_group_id,
        device_id,
        real_user_id,
        user_id,
        order_date_msk,
        order_datetime_utc,
        legal_entity,
        app_entity,
        merchant_id,
        store_id,
        product_id,
        product_variant_id,
        merchant_category_id,
        country_code,
        currency_code,
        platform,
        last_context,
        contexts,
        is_finalized,
        product_quantity,
        ROUND((gmv_initial + coupon_discount + points_initial + special_price_discount) / product_quantity, 3) AS item_price,
        gmv_initial,
        gmv_final,
        gmv_refunded,
        gmv_initial_in_local_currency,
        psp_name,
        psp_initial,
        psp_final,
        jl_cost_final_estimated,
        order_gross_profit_final,
        order_gross_profit_final_estimated,
        ecgp_initial,
        ecgp_final,
        merchant_revenue_initial,
        merchant_revenue_final,
        merchant_sale_price,
        merchant_list_price,
        item_merchant_revenue_initial,
        logistics_price_initial,
        item_logistics_price_initial,
        vat_markup,
        marketplace_commission_initial,
        ROUND(gmv_initial + coupon_discount + points_initial + special_price_discount - logistics_price_initial - jl_markup - merchant_sale_price - vat_markup, 3) AS jm_markup,
        jl_markup,
        is_with_coupon,
        coupon_discount,
        used_coupon_id,
        is_with_points,
        points_initial,
        points_final,
        is_with_special_price,
        special_price_discount,
        discounts,
        is_refunded,
        refund_reason,
        detailed_refund_reason,
        refund_date_msk,
        is_not_delivered_refund,
        is_quality_refund,
        is_canceled_by_merchant,
        shipping_type_initial,
        estimated_delivery_min_days,
        estimated_delivery_max_days,
        review_date_msk,
        review_stars,
        is_review_with_text,
        review_media_count,
        review_image_count,
        number_of_reviews,
        is_negative_feedback,

        IF(number_of_reviews > 0, ROUND(total_product_rating / number_of_reviews, 1), NULL) AS product_rating, -- рейтинг товара на момент покупки
        product_orders_number, -- номер покупки товара
        device_orders_number, -- номер покупки девайса
        user_orders_number, -- номер покупки пользователя
        real_user_orders_number -- номер покупки пользователя (real_id)
    FROM orders_ext1
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

active_devices AS (
    SELECT
        device_id,
        day AS order_date_msk,
        is_new_user,
        join_day
    FROM {{ ref('active_devices') }}
    {% if is_incremental() %}
        WHERE month >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
    {% endif %}
),

orders_ext4 AS (
    -- добавляем информацию, новый девайс или нет из таблицы active_devices
    -- добавляем регион
    -- добавляем блогеров
    SELECT
        a.*,
        COALESCE(c.top_country_code, 'Other') AS top_country_code,
        COALESCE(c.country_priority_type, 'Other') AS country_priority_type,
        COALESCE(c.region_name, 'Other') AS region_name,
        COALESCE(b.is_new_user, FALSE) AS is_new_device,
        DATEDIFF(a.order_date_msk, b.join_day) AS device_lifetime,
        IF(d.user_id IS NOT NULL, d.blogger_type, IF(a.gmv_initial = 0 AND a.points_initial >= 100, 'probably', 'not')) AS blogger_type
    FROM orders_ext3 AS a
    LEFT JOIN active_devices AS b USING (device_id, order_date_msk)
    LEFT JOIN {{ ref('gold_countries') }} AS c USING (country_code)
    LEFT JOIN {{ ref('bloggers') }} AS d USING (user_id)
),

orders_ext5 AS (
    -- добавляем логистику
    -- добавляем origin_name
    SELECT
        a.order_id,
        a.friendly_order_id,
        a.order_group_id,
        a.device_id,
        a.real_user_id,
        a.user_id,
        a.order_date_msk,
        a.order_datetime_utc,
        a.legal_entity,
        a.app_entity,
        a.merchant_id,
        a.store_id,
        a.product_id,
        a.product_variant_id,
        a.merchant_category_id,
        a.country_code,
        a.currency_code,
        a.platform,
        a.last_context,
        a.contexts,
        a.product_quantity,
        a.item_price,
        a.gmv_initial,
        a.gmv_final,
        a.gmv_refunded,
        a.gmv_initial_in_local_currency,
        a.psp_name,
        a.psp_initial,
        a.psp_final,
        a.jl_cost_final_estimated,
        a.order_gross_profit_final,
        a.order_gross_profit_final_estimated,
        a.ecgp_initial,
        a.ecgp_final,
        a.merchant_revenue_initial,
        a.merchant_revenue_final,
        a.merchant_sale_price,
        a.merchant_list_price,
        a.item_merchant_revenue_initial,
        a.logistics_price_initial,
        a.item_logistics_price_initial,
        a.vat_markup,
        a.marketplace_commission_initial,
        a.jm_markup,
        a.jl_markup,
        a.is_with_coupon,
        a.coupon_discount,
        a.used_coupon_id,
        a.is_with_points,
        a.points_initial,
        a.points_final,
        a.is_with_special_price,
        a.special_price_discount,
        a.discounts,
        a.is_refunded,
        a.refund_reason,
        a.detailed_refund_reason,
        a.refund_date_msk,
        a.is_not_delivered_refund,
        a.is_quality_refund,
        a.is_canceled_by_merchant,
        a.shipping_type_initial,
        a.estimated_delivery_min_days,
        a.estimated_delivery_max_days,
        a.review_date_msk,
        a.review_stars,
        a.is_review_with_text,
        a.review_media_count,
        a.review_image_count,
        a.number_of_reviews,
        a.is_negative_feedback,
        a.product_rating,
        a.product_orders_number,
        a.device_orders_number,
        a.user_orders_number,
        a.real_user_orders_number,
        a.support_ticket_id,
        a.top_country_code,
        a.country_priority_type,
        a.region_name,
        a.is_new_device,
        a.device_lifetime,
        a.blogger_type,

        IF(b.tracking_delivered_datetime_utc IS NOT NULL, TRUE, a.is_finalized) AS is_finalized,

        b.delivery_duration_by_tracking,
        b.delivery_duration_by_user,
        COALESCE(b.delivery_duration_by_tracking IS NOT NULL OR b.delivery_duration_by_user IS NOT NULL, FALSE) AS is_delivered,
        b.jl_consolidation_profit_final,
        b.is_delivered_by_jl,
        m.origin_name
    FROM orders_ext4 AS a
    LEFT JOIN logistics_orders AS b USING (order_id)
    LEFT JOIN {{ ref('gold_merchants') }} AS m USING (merchant_id)
),

orders_ext6 AS (
    -- добавляем сегмент пользователя
    SELECT
        a.*,
        COALESCE(b.user_segment, 'Non-buyers') AS real_user_segment
    FROM orders_ext5 AS a
    LEFT JOIN {{ ref('user_segments') }} AS b
        ON
            a.real_user_id = b.real_user_id
            AND a.order_date_msk >= TO_DATE(b.effective_ts)
            AND a.order_date_msk <= TO_DATE(b.next_effective_ts)
),

orders_ext7 AS (
    -- добавляем бизнес-линию
    SELECT
        a.*,
        b.business_line
    FROM orders_ext6 AS a
    LEFT JOIN {{ ref('gold_merchant_categories') }} AS b USING (merchant_category_id)
)

SELECT
    order_id,
    friendly_order_id,
    order_group_id,

    device_id,
    real_user_id,
    user_id,

    order_date_msk,
    order_datetime_utc,

    legal_entity,
    app_entity,
    merchant_id,
    store_id,
    product_id,
    product_variant_id,
    merchant_category_id,
    business_line,

    country_code,
    top_country_code,
    country_priority_type,
    region_name,
    origin_name,
    platform,
    last_context,
    contexts,
    is_new_device,
    real_user_segment,
    blogger_type,
    product_orders_number,
    device_orders_number,
    user_orders_number,
    real_user_orders_number,
    device_lifetime,

    product_quantity,
    item_price,
    gmv_initial,
    gmv_final,
    gmv_refunded,
    currency_code,
    gmv_initial_in_local_currency,
    psp_name,
    psp_initial,
    psp_final,
    jl_cost_final_estimated,
    order_gross_profit_final,
    order_gross_profit_final_estimated,
    ecgp_initial,
    ecgp_final,
    merchant_revenue_initial,
    merchant_revenue_final,
    merchant_sale_price,
    merchant_list_price,
    item_merchant_revenue_initial,
    logistics_price_initial,
    item_logistics_price_initial,
    jl_consolidation_profit_final,
    is_delivered_by_jl,
    vat_markup,
    marketplace_commission_initial,
    jm_markup,
    jl_markup,

    is_with_coupon,
    coupon_discount,
    used_coupon_id,
    is_with_points,
    points_initial,
    points_final,
    is_with_special_price,
    special_price_discount,
    discounts,

    is_refunded,
    refund_reason,
    detailed_refund_reason,
    refund_date_msk,
    is_not_delivered_refund,
    is_quality_refund,
    is_finalized,

    is_canceled_by_merchant,

    support_ticket_id IS NOT NULL AS is_with_support_ticket,
    support_ticket_id,

    shipping_type_initial,
    estimated_delivery_min_days,
    estimated_delivery_max_days,
    is_delivered,
    delivery_duration_by_user,
    delivery_duration_by_tracking,

    review_date_msk,
    review_stars,
    is_review_with_text,
    review_media_count,
    review_image_count,
    number_of_reviews,
    product_rating,
    is_negative_feedback,
    TRUNC(order_date_msk, 'MM') AS month

FROM orders_ext7

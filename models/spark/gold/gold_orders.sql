{{
  config(
    materialized='incremental',
    alias='orders',
    file_format='parquet',
    schema='gold',
    incremental_strategy='insert_overwrite',
    partition_by=['order_month_msk'],
    on_schema_change='sync_all_columns',
    meta = {
        'model_owner' : '@analytics.duty',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'order_date_msk',
        'bigquery_upload_horizon_days': '230',
        'priority_weight': '1000',
        'full_reload_on': '6',
    }
  )
}}

WITH device_orders_number AS (
    SELECT
        order_id,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY created_time_utc) AS device_orders_number -- номер покупки девайса
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE NOT (refund_reason = 'fraud' AND refund_reason IS NOT NULL) AND device_id IS NOT NULL
),

product_orders_number AS (
    SELECT
        order_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_time_utc) AS product_orders_number -- номер покупки товара
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE NOT (refund_reason = 'fraud' AND refund_reason IS NOT NULL) AND product_id IS NOT NULL
),

user_orders_number AS (
    SELECT
        order_id,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_time_utc) AS user_orders_number -- номер покупки пользователя
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE NOT (refund_reason = 'fraud' AND refund_reason IS NOT NULL) AND user_id IS NOT NULL
),

real_user_orders_number AS (
    SELECT
        order_id,
        ROW_NUMBER() OVER (PARTITION BY real_user_id ORDER BY created_time_utc) AS real_user_orders_number -- номер покупки пользователя
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE NOT (refund_reason = 'fraud' AND refund_reason IS NOT NULL) AND real_user_id IS NOT NULL
),

numbers AS (
    SELECT
        order_id,
        product_orders_number.product_orders_number, -- номер покупки товара
        device_orders_number.device_orders_number, -- номер покупки девайса
        user_orders_number.user_orders_number, -- номер покупки пользователя
        real_user_orders_number.real_user_orders_number -- номер покупки пользователя (real_id)
    FROM
        device_orders_number
    LEFT JOIN product_orders_number USING (order_id)
    LEFT JOIN user_orders_number USING (order_id)
    LEFT JOIN real_user_orders_number USING (order_id)
),

merchant_order_notes AS (
    SELECT _id AS merchant_order_id
    FROM {{ source('mongo', 'merchant_order_order_metric_notes_daily_snapshot') }}
    WHERE metrics['cancelRate']['ignored']['value'] = TRUE
),

pickup_fault_cancelled_orders AS (
    -- отобрали заказы, у которых отмена произошла из-за потери посылки пикап провайдером 
    SELECT source.id AS order_id
    FROM {{ source('mongo', 'merchant_order') }} AS mo
    INNER JOIN merchant_order_notes AS mon ON mo.order_id = mon.merchant_order_id
    WHERE
        mo.status = 'cancelledByMerchant'
        AND refund.merchant_reason = 'notShippedOnTime'
        AND source.kind = 'joom'
),

orders_ext0 AS (
    SELECT
        ord.order_id,
        ord.friendly_order_id,
        ord.order_group_id,

        ord.device_id,
        ord.real_user_id,
        ord.user_id,

        ord.partition_date AS order_date_msk,
        ord.created_time_utc AS order_datetime_utc,

        IF(ord.legal_entity = 'jmt', 'JMT', 'SIA') AS legal_entity,
        CASE WHEN ord.partition_date < '2023-07-01' THEN 'joom' ELSE ord.app_entity_group END AS app_entity_group,
        CASE WHEN ord.partition_date < '2023-07-01' THEN 'joom' ELSE ord.app_entity END AS app_entity,
        ord.custom_domain,
        ord.merchant_id,
        ord.store_id,
        ord.product_id,
        ord.product_variant_id,
        ord.category_id AS merchant_category_id,

        UPPER(ord.shipping_country) AS country_code,
        ord.currency AS currency_code,
        LOWER(ord.os_type) AS platform,
        COALESCE(last_context.name, 'unknown') AS last_context,
        ord.normalized_contexts AS contexts,
        COALESCE(
            (
                ord.customer_refund_reason IS NOT NULL
                OR ord.refund_reason IS NOT NULL
                OR ord.delivered_time_utc IS NOT NULL
                OR DATEDIFF(CURRENT_DATE(), CAST(ord.created_time_utc AS DATE)) > ord.warranty_duration_max_days
            ),
            FALSE
        ) AS is_finalized,

        ord.product_quantity,
        ROUND(ord.gmv_initial, 3) AS gmv_initial,
        ROUND(ord.gmv_final, 3) AS gmv_final,
        ROUND(ord.refund, 3) AS gmv_refunded,
        ROUND(ord.amount_currency, 3) AS gmv_initial_in_local_currency,
        ord.psp AS psp_name,
        ROUND(ord.psp_initial, 3) AS psp_initial,
        ROUND(ord.psp_final, 3) AS psp_final,
        ROUND(ord.jl_cost_final_estimated, 3) AS jl_cost_final_estimated,
        ROUND(ord.order_gross_profit_final, 3) AS order_gross_profit_final,
        ROUND(ord.order_gross_profit_final_estimated, 3) AS order_gross_profit_final_estimated,
        ROUND(ord.ecgp_initial, 3) AS ecgp_initial,
        ROUND(ord.ecgp_final, 3) AS ecgp_final,
        ROUND(ord.merchant_revenue_initial, 3) AS merchant_revenue_initial,
        ROUND(ord.merchant_revenue_final, 3) AS merchant_revenue_final,
        ROUND(ord.merchant_sale_price, 3) AS merchant_sale_price,
        ROUND(ord.merchant_list_price, 3) AS merchant_list_price,
        ROUND(ord.merchant_revenue_initial / ord.product_quantity, 3) AS item_merchant_revenue_initial,
        ROUND(ord.logistics_price_initial, 3) AS logistics_price_initial,
        ROUND(ord.logistics_price_initial / ord.product_quantity, 3) AS item_logistics_price_initial,
        ROUND(ord.vat_markup, 3) AS vat_markup,
        ROUND(ord.merchant_sale_price - ord.merchant_revenue_initial, 3) AS marketplace_commission_initial,
        ROUND(ord.logistics_extra_charge, 3) AS jl_markup,
        ord.used_coupon_id IS NOT NULL AS is_with_coupon,
        ROUND(ord.coupon, 3) AS coupon_discount,
        ord.used_coupon_id,
        ord.points_initial > 0 AS is_with_points,
        ord.points_initial,
        ord.points_final,
        COALESCE(ARRAY_CONTAINS(ord.discounts.type, 'specialPriceFinal'), FALSE) AS is_with_special_price,
        ROUND(COALESCE (FILTER(ord.discounts, x -> x.type = 'specialPriceFinal')[0].amount * 1e6, 0), 3) AS special_price_discount,
        ROUND(COALESCE (FILTER(ord.discounts, x -> x.type = 'specialPrice')[0].amount * 1e6, 0), 3) AS special_price_potential_discount,
        ord.discounts,
        ord.is_1688_product,

        ord.refund_reason IS NOT NULL AS is_refunded,
        ord.refund_reason,
        CASE
            WHEN ord.refund_reason IS NOT NULL AND (ord.customer_refund_reason IS NULL AND ord.merchant_refund_reason IS NULL) THEN 'Unknown'
            WHEN ord.refund_reason IS NULL AND (ord.customer_refund_reason IS NULL AND ord.merchant_refund_reason IS NULL) THEN NULL

            WHEN ord.customer_refund_reason = 0 THEN NULL
            WHEN ord.customer_refund_reason = 1 THEN 'cancelledByCustomer'
            WHEN ord.customer_refund_reason = 2 THEN 'notDelivered'
            WHEN ord.customer_refund_reason = 3 THEN 'emptyPackage'
            WHEN ord.customer_refund_reason = 4 THEN 'badQuality'
            WHEN ord.customer_refund_reason = 5 THEN 'damaged'
            WHEN ord.customer_refund_reason = 6 THEN 'wrongProduct'
            WHEN ord.customer_refund_reason = 7 THEN 'wrongQuantity'
            WHEN ord.customer_refund_reason = 8 THEN 'wrongSize'
            WHEN ord.customer_refund_reason = 9 THEN 'wrongColor'
            WHEN ord.customer_refund_reason = 10 THEN 'minorProblems'
            WHEN ord.customer_refund_reason = 11 THEN 'differentFromImages'
            WHEN ord.customer_refund_reason = 12 THEN 'other'
            WHEN ord.customer_refund_reason = 13 THEN 'customerFraud'
            WHEN ord.customer_refund_reason = 14 THEN 'counterfeit'
            WHEN ord.customer_refund_reason = 15 THEN 'sentBackToMerchant'
            WHEN ord.customer_refund_reason = 16 THEN 'returnedToMerchantByShipper'
            WHEN ord.customer_refund_reason = 17 THEN 'misleadingInformation'
            WHEN ord.customer_refund_reason = 18 THEN 'paidByJoom'
            WHEN ord.customer_refund_reason = 19 THEN 'fulfillByJoomUnavailable'
            WHEN ord.customer_refund_reason = 20 THEN 'approveRejected'
            WHEN ord.customer_refund_reason = 21 THEN 'productBanned'
            WHEN ord.customer_refund_reason = 22 THEN 'packageIncomplete'
            WHEN ord.customer_refund_reason = 23 THEN 'notAsDescribed'
            WHEN ord.customer_refund_reason = 24 THEN 'missingProofOfDelivery'
            WHEN ord.customer_refund_reason = 25 THEN 'deliveredToWrongAddress'
            WHEN ord.customer_refund_reason = 26 THEN 'turnedBackByPostOffice'
            WHEN ord.customer_refund_reason = 27 THEN 'bannedByCustoms'
            WHEN ord.customer_refund_reason = 28 THEN 'incorrectConsolidation'
            WHEN ord.customer_refund_reason = 29 THEN 'freeItemCancel'
            WHEN ord.customer_refund_reason = 30 THEN 'groupPurchaseExpired'
            WHEN ord.customer_refund_reason = 31 THEN 'returnedToMerchantInOrigin'
            WHEN ord.customer_refund_reason = 32 THEN 'vatOnDelivery'
            WHEN ord.customer_refund_reason = 33 THEN 'shippingUnavailable'
            WHEN ord.customer_refund_reason = 34 THEN 'threeForTwoCancel'
            WHEN ord.customer_refund_reason = 35 THEN 'reseller'
            WHEN ord.customer_refund_reason = 36 THEN 'bmplCancel'
            WHEN ord.customer_refund_reason = 37 THEN 'couponAllOrderCancelBehaviour'
            WHEN ord.customer_refund_reason = 38 THEN 'minOrderPriceAllOrderCancel'

            WHEN ord.merchant_refund_reason = 0 THEN NULL
            WHEN ord.merchant_refund_reason = 1 THEN 'unableToFulfill'
            WHEN ord.merchant_refund_reason = 2 THEN 'outOfStock'
            WHEN ord.merchant_refund_reason = 3 THEN 'wrongAddress'
            WHEN ord.merchant_refund_reason = 4 AND c.order_id IS NULL THEN 'notShippedOnTime'
            WHEN ord.merchant_refund_reason = 4 AND c.order_id IS NOT NULL THEN 'pickupProviderFault' -- заказы, потерянные пикап провайдером
        END AS detailed_refund_reason,
        TO_DATE(ord.refund_time_utc + INTERVAL 3 HOUR) AS refund_date_msk,
        ord.customer_refund_reason IS NOT NULL AND ord.customer_refund_reason IN (2, 5, 16, 24, 25, 26, 28) AS is_not_delivered_refund,
        ord.customer_refund_reason IS NOT NULL AND ord.customer_refund_reason IN (3, 4, 6, 7, 8, 9, 10, 11, 14, 17, 21, 22, 23, 27) AS is_quality_refund,

        COALESCE(ord.refund_reason IN ('cancelled_by_merchant', 'other'), FALSE) AS is_canceled_by_merchant,

        COALESCE(ord.jl_shipping_type_initial, 'offline') AS shipping_type_initial,
        ord.estimated_delivery_min_days,
        ord.estimated_delivery_max_days,

        TO_DATE(ord.review_time_utc + INTERVAL 3 HOUR) AS review_date_msk,
        ord.review_stars,
        ord.review_has_text AS is_review_with_text,
        ord.review_media_count,
        ord.review_image_count,
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
                (ord.review_stars IS NOT NULL AND ord.review_stars IN (1, 2))
                AND DATEDIFF(ord.review_time_utc, ord.created_time_utc) <= 80
            )
            OR
            (
                (ord.customer_refund_reason IS NOT NULL AND ord.customer_refund_reason IN (3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 17, 21, 22, 23, 27))
                AND DATEDIFF(ord.refund_time_utc, ord.created_time_utc) <= 80
            ),
            FALSE
        ) AS is_negative_feedback

    FROM {{ source('mart', 'star_order_2020') }} AS ord
    LEFT JOIN pickup_fault_cancelled_orders AS c USING (order_id)
    WHERE
        NOT (ord.refund_reason = 'fraud' AND ord.refund_reason IS NOT NULL)
        {% if is_incremental() %}
            AND ord.partition_date >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
        {% endif %}
),

logistics_orders AS (
    SELECT
        order_id,
        delivery_duration_by_tracking,
        delivery_duration_by_user,
        tracking_delivered_datetime_utc,
        jl_consolidation_profit_final,
        is_delivered_by_jl,
        is_fbj
    FROM {{ ref('gold_logistics_orders') }}
),

support_tickets AS (
    SELECT
        order_id,
        MAX(ticket_id) AS ticket_id
    FROM {{ ref('joom_babylone_tickets') }}
    GROUP BY order_id
),

merchant_fulfill AS (
    SELECT
        friendly_id AS friendly_order_id,
        MAX(mo.cft) AS order_commited_merchant_fulfillment_days,
        ROUND(MAX(mod.aft / 1000 / 60 / 60 / 24), 3) AS order_merchant_fulfillment_days_estimated
    FROM {{ source('mongo', 'merchant_order') }} AS mo
    LEFT JOIN {{ source('merchant', 'order_data') }} AS mod USING (friendly_id)
    WHERE
        (mod.aft IS NOT NULL OR mo.cft IS NOT NULL)
        {% if is_incremental() %}
            AND DATE(mo.created_time_utc) >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
        {% endif %}
    GROUP BY 1
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
        app_entity_group,
        custom_domain,
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
        special_price_potential_discount,
        discounts,
        is_1688_product,

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
        WHERE month_msk >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
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
        a.app_entity_group,
        a.custom_domain,
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
        a.special_price_potential_discount,
        a.discounts,
        a.is_1688_product,
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
        b.is_fbj,
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
    -- добавляем бизнес-линию, order_commited_merchant_fulfillment_days и order_merchant_fulfillment_days_estimated
    SELECT
        a.*,
        b.business_line,
        c.order_merchant_fulfillment_days_estimated,
        c.order_commited_merchant_fulfillment_days
    FROM orders_ext6 AS a
    LEFT JOIN {{ ref('gold_merchant_categories') }} AS b USING (merchant_category_id)
    LEFT JOIN merchant_fulfill AS c USING (friendly_order_id)
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
    app_entity_group,
    CASE
        WHEN UPPER(app_entity) = 'SHOPY' THEN custom_domain
    END AS shopy_blogger_domain,
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
    is_fbj,
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
    special_price_potential_discount,
    discounts,
    is_1688_product,

    is_refunded,
    refund_reason,
    detailed_refund_reason,
    refund_date_msk,
    is_not_delivered_refund,
    is_quality_refund,
    is_finalized,

    is_canceled_by_merchant,
    order_merchant_fulfillment_days_estimated,
    order_commited_merchant_fulfillment_days,

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
    TRUNC(order_date_msk, 'MM') AS order_month_msk

FROM orders_ext7
DISTRIBUTE BY order_month_msk, ABS(HASH(order_id)) % 10
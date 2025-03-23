{{
  config(
    materialized='incremental',
    alias='fbj_variant_statistics',
    file_format='parquet',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    },
    on_schema_change='append_new_columns'
  )
}}

WITH calendar AS ( -- делаем календарь, который по сути нужен только для ручных репленишментов
    {% if is_incremental() %}
        SELECT DATE '{{ var("start_date_ymd") }}' AS dt -- +1 день потому что в таблице st хранится current_date а в start_date_ymd предыдущий день
    --select current_date as dt -- +1 день потому что в таблице st хранится current_date а в start_date_ymd предыдущий день
    {% else %}
    select EXPLODE(SEQUENCE(to_date('2024-07-01'),  DATE '{{ var("start_date_ymd") }}', INTERVAL 1 DAY)) AS dt
    --select EXPLODE(SEQUENCE(to_date('2025-03-20'), to_date(CURRENT_DATE()), INTERVAL 1 DAY)) AS dt
{% endif %}
),

cal_x_stock AS ( -- отбираем варианты которые находятся на складе
    -- 1
    SELECT
        c.dt AS partition_date,
        st.product_variant_id AS variant_id,
        st.product_id,
        st.logistics_product_id,
        st.product_dimensions.h AS product_dimensions_h,
        st.product_dimensions.l AS product_dimensions_l,
        st.product_dimensions.w AS product_dimensions_w,
        st.number_of_products_in_stock,
        st.number_of_products_in_pending_stock
    FROM calendar AS c
    INNER JOIN models.fbj_product_stocks AS st
        ON
            1 = 1
            AND st.partition_date - INTERVAL 1 DAY = c.dt
),

cal_x_demand AS ( -- отбираем варианты которые включены в деманд системе
    -- 2
    SELECT
        c.dt AS partition_date,
        pe.payload.skuId AS variant_id,
        COALESCE(pe.product_id, pe.payload.productId) AS product_id,
        FIRST_VALUE(pe.payload.result) AS last_demand_status
    FROM calendar AS c
    INNER JOIN mart.product_events AS pe
        ON
            1 = 1
            AND c.dt = pe.partition_date
            AND pe.type = 'fbjProcessingResult'
    GROUP BY
        c.dt,
        pe.payload.skuId,
        COALESCE(pe.product_id, pe.payload.productId)
),

cal_x_repl AS ( -- отбираем варианты которые созданы или едут в репленишментах
    -- 3
    SELECT DISTINCT
        c.dt AS partition_date,
        fmr.variant_id,
        fmr.product_id
    FROM calendar AS c
    INNER JOIN category_management.fbj_merchant_replenishments AS fmr  -- чтобы учесть ручные репленишменты
        ON
            1 = 1
            AND c.dt BETWEEN fmr.partition_date AND CASE
                WHEN fmr.current_status IN ('1. Pending Approve', '2. Pending Inbound', '3. Pending Shipping', '4. Shipped by Merchant')
                    THEN CAST('5999-12-31' AS DATE) -- то есть просто берем все строчки у которых такие статусы
                WHEN fmr.current_status IN ('5. Action Required', '6. On Review', '7. Completed')
                    THEN DATE(fmr.completed_dt)
                WHEN fmr.current_status IN ('8. Canceled by Joom', '8. Canceled by Merchant')
                    THEN DATE(fmr.last_updated_at)
            END
),

base AS (
    SELECT
        COALESCE(cal_x_stock.partition_date, cal_x_demand.partition_date, cal_x_repl.partition_date) AS partition_date,
        COALESCE(cal_x_stock.variant_id, cal_x_demand.variant_id, cal_x_repl.variant_id) AS variant_id,
        COALESCE(cal_x_stock.product_id, cal_x_demand.product_id, cal_x_repl.product_id) AS product_id,
        cal_x_stock.logistics_product_id,
        cal_x_stock.product_dimensions_h,
        cal_x_stock.product_dimensions_l,
        cal_x_stock.product_dimensions_w,
        COALESCE(cal_x_stock.number_of_products_in_stock, 0) AS number_in_stock, -- на конец дня
        COALESCE(cal_x_stock.number_of_products_in_pending_stock, 0) AS number_in_pending_stock,
        IF(cal_x_demand.variant_id IS NULL, 0, 1) AS enabled_flg,
        cal_x_demand.last_demand_status
    FROM cal_x_stock
    FULL JOIN cal_x_demand
        ON
            1 = 1
            AND cal_x_stock.partition_date = cal_x_demand.partition_date
            AND cal_x_stock.variant_id = cal_x_demand.variant_id
    FULL JOIN cal_x_repl
        ON
            1 = 1
            AND cal_x_repl.partition_date = COALESCE(cal_x_stock.partition_date, cal_x_demand.partition_date) -- джоин на коалеск чтобы не дублить строки тут
            AND cal_x_repl.variant_id = COALESCE(cal_x_stock.variant_id, cal_x_demand.variant_id)
),

-- заказы
-- фильтруем ли тут как-нибудь заказы? отмененные/рефанднутые?
variants_orders AS (
    SELECT
        b.partition_date, -- бизнес дата
        b.variant_id,
        b.product_id,
        b.number_in_stock,
        b.enabled_flg,
        b.last_demand_status,
        -- o_1
        COUNT(IF(CAST(o_30.order_datetime_utc AS DATE) = b.partition_date, o_30.order_id, NULL)) AS orders_cnt,
        COUNT(IF(CAST(o_30.order_datetime_utc AS DATE) = b.partition_date AND o_30.is_fbj, o_30.order_id, NULL)) AS fbj_orders_cnt,
        SUM(IF(CAST(o_30.order_datetime_utc AS DATE) = b.partition_date, o_30.product_quantity, 0)) AS quantity_cnt,
        SUM(IF(CAST(o_30.order_datetime_utc AS DATE) = b.partition_date AND o_30.is_fbj, o_30.product_quantity, 0)) AS fbj_quantity_cnt,
        -- o_7
        COUNT(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 6 DAY) AND b.partition_date, o_30.order_id, NULL)) AS orders_7_cnt,
        COUNT(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 6 DAY) AND b.partition_date AND o_30.is_fbj, o_30.order_id, NULL)) AS fbj_orders_7_cnt,
        SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 6 DAY) AND b.partition_date, o_30.product_quantity, 0)) AS quantity_7_cnt,
        SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 6 DAY) AND b.partition_date AND o_30.is_fbj, o_30.product_quantity, 0)) AS fbj_quantity_7_cnt,
        b.number_in_stock / SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 6 DAY) AND b.partition_date, o_30.product_quantity, 0)) * 7 AS to_7,
        -- o_14
        COUNT(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 13 DAY) AND b.partition_date, o_30.order_id, NULL)) AS orders_14_cnt,
        COUNT(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 13 DAY) AND b.partition_date AND o_30.is_fbj, o_30.order_id, NULL)) AS fbj_orders_14_cnt,
        SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 13 DAY) AND b.partition_date, o_30.product_quantity, 0)) AS quantity_14_cnt,
        SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 13 DAY) AND b.partition_date AND o_30.is_fbj, o_30.product_quantity, 0)) AS fbj_quantity_14_cnt,
        b.number_in_stock / SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 13 DAY) AND b.partition_date, o_30.product_quantity, 0)) * 14 AS to_14,
        -- o_30
        COUNT(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 29 DAY) AND b.partition_date, o_30.order_id, NULL)) AS orders_30_cnt,
        COUNT(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 29 DAY) AND b.partition_date AND o_30.is_fbj, o_30.order_id, NULL)) AS fbj_orders_30_cnt,
        SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 29 DAY) AND b.partition_date, o_30.product_quantity, 0)) AS quantity_30_cnt,
        SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 29 DAY) AND b.partition_date AND o_30.is_fbj, o_30.product_quantity, 0)) AS fbj_quantity_30_cnt,
        b.number_in_stock / SUM(IF(CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 29 DAY) AND b.partition_date, o_30.product_quantity, 0)) * 30 AS to_30
    FROM base AS b
    LEFT JOIN {{ ref('gold_orders') }} AS o_30
        ON
            1 = 1
            AND CAST(o_30.order_datetime_utc AS DATE) BETWEEN (b.partition_date - INTERVAL 29 DAY) AND b.partition_date -- 30 прошлых дней
            AND b.variant_id = o_30.product_variant_id
    GROUP BY
        b.partition_date,
        b.variant_id,
        b.product_id,
        b.number_in_stock,
        b.enabled_flg,
        b.last_demand_status
),

variant_repl_status AS (
    SELECT
        b.partition_date,
        b.variant_id,
        fmr.merchant_id,
        -- статусы по репленишментам
        --
        -- для каждого варианта считаем метрики: 
        -- сколько было айтемов заказано в репленишментах в этот день
        -- сколько в этот день везут (репл создан раньше и сейчас статус какой-то из 2/3/4/5/6)
        -- сколько завершено в этот день
        -- закомпличено в этот день
        -- отменено джумом 
        -- отменено мерчантом 
        -- когда последний раз был отмена джумом, отмена мерчантом, завершение
        -- когда было последний раз создание репленишмента
        -- доля отмен среди всех репленишментов
        -- скорость завершения для варианта
        --
        -- сколько айтемов было заказано в текущий день
        SUM(IF(fmr.partition_date = b.partition_date, fmr.requested_count, 0)) AS qty_created,
        -- сколько айтемов в репленишментах созданных до текущего дня сейчас находятся в разных статусах
        SUM(IF(fmr.last_updated_at = fmr.2_pending_inbound_dt, fmr.requested_count, 0)) AS 2_qty_in_pending_inbound,
        SUM(IF(fmr.last_updated_at = fmr.3_pending_shipping_dt, fmr.requested_count, 0)) AS 3_qty_in_pending_shipping,
        SUM(IF(fmr.last_updated_at = fmr.4_shipped_dt, fmr.requested_count, 0)) AS 4_qty_in_shipped_by_merchant,
        SUM(IF(fmr.last_updated_at = fmr.5_action_required_dt, fmr.requested_count, 0)) AS 5_qty_in_action_required,
        SUM(IF(fmr.last_updated_at = fmr.6_on_review_dt, fmr.requested_count, 0)) AS 6_qty_on_review,
        -- сколько завершено в этот день или отменено джумом или мерчантом
        -- если дата последнего обновления = дата расчета и текущий статус Completed
        SUM(IF((DATE(fmr.last_updated_at) = b.partition_date) AND (fmr.current_status = '7. Completed'), fmr.requested_count, 0)) AS qty_completed,
        -- если дата последнего обновления = дата расчета и текущий статус Canceled by joom 
        SUM(IF((DATE(fmr.last_updated_at) = b.partition_date) AND (fmr.current_status = '8. Canceled by Joom'), fmr.requested_count, 0)) AS qty_canceled_by_joom,
        -- если дата последнего обновления = дата расчета и текущий статус Canceled by merchant
        SUM(IF((DATE(fmr.last_updated_at) = b.partition_date) AND (fmr.current_status = '8. Canceled by Merchant'), fmr.requested_count, 0)) AS qty_canceled_by_merchant,
        -- 7, 8 статус со временем не переходит в другой, поэтому можно посмотреть когда был последний раз такой статус среди репленишментов созданых ранее и это и будет последненяя дата
        MAX(IF(fmr.current_status = '8. Canceled by Joom', fmr.last_updated_at, NULL)) AS last_canceled_by_joom_at,
        MAX(IF(fmr.current_status = '8. Canceled by Merchant', fmr.last_updated_at, NULL)) AS last_canceled_by_merch_at,
        MAX(IF(fmr.current_status = '7. Completed', fmr.last_updated_at, NULL)) AS last_completed_at,
        --
        MAX(fmr.created_at) AS last_replenishment_created_at, -- когда был создан последний репленишмент
        -- 8 статус со временем не переходит в другой, поэтому можно посмотреть когда был последний раз такой статус среди репленишментов созданых ранее и это и будет последненяя дата
        COALESCE(COUNT(IF(fmr.current_status IN ('8. Canceled by Joom', '8. Canceled by Merchant'), fmr.replenishment_id, NULL)), 0) / COUNT(fmr.replenishment_id) AS variant_cancel_rate,
        --
        AVG(fmr.create_to_complete_days) AS avg_complete_days_var,
        CASE
            WHEN qty_created > 0 THEN 'Has new replenishment' -- если сегодня был создан репленишмент
            WHEN 2_qty_in_pending_inbound > 0 OR 3_qty_in_pending_shipping > 0 OR 4_qty_in_shipped_by_merchant > 0 THEN 'In Progress' -- если сегодня не было создано,  но есть в разных статусах
            -- сегодня не было, в статусах 2,3,4 нет но была отмена или завершение
            WHEN last_canceled_by_merch_at = GREATEST(last_canceled_by_merch_at, last_canceled_by_joom_at, last_completed_at) THEN 'Canceled by Merch'
            WHEN last_canceled_by_joom_at = GREATEST(last_canceled_by_merch_at, last_canceled_by_joom_at, last_completed_at) THEN 'Canceled by Joom'
            WHEN last_completed_at = GREATEST(last_canceled_by_merch_at, last_canceled_by_joom_at, last_completed_at) THEN 'Completed'
            ELSE 'Other'
        END AS last_replenishment_status
    FROM base AS b
    LEFT JOIN {{ ref('fbj_merchant_replenishments') }} AS fmr --category_management.fbj_merchant_replenishments AS fmr
        ON
            1 = 1
            AND fmr.partition_date <= b.partition_date -- все репленишменты созданные до текущей даты 
            AND b.variant_id = fmr.variant_id
    GROUP BY
        b.partition_date,
        b.variant_id,
        fmr.merchant_id
),

var_kam AS (
    SELECT
        fmr.partition_date,
        fmr.variant_id,
        MAX(fmr.merchant_name) AS merchant_name,
        MAX(fmr.main_merchant_name) AS main_merchant_name,
        MAX(fmr.kam) AS kam
    FROM {{ ref('fbj_merchant_replenishments') }} AS fmr --category_management.fbj_merchant_replenishments AS fmr
    GROUP BY
        fmr.partition_date,
        fmr.variant_id
),

--платность хранения
paid_storage_pre AS (
    SELECT
        b.partition_date,
        b.variant_id,
        b.logistics_product_id,
        b.number_in_stock,
        SUM(IF(lr_v3_ds.source IN (1, 2) AND CAST(lr_s_ds.ct AS DATE) = b.partition_date, lr_s_ds.s, 0)) AS repl_amount, -- accepted_day
        SUM(IF(lr_v3_ds.source IN (1, 2) AND CAST(lr_s_ds.ct AS DATE) BETWEEN (b.partition_date - INTERVAL 29 DAY) AND b.partition_date, lr_s_ds.s, 0)) AS relp_amount_30d,
        SUM(IF(lr_v3_ds.source IN (1, 2) AND CAST(lr_s_ds.ct AS DATE) BETWEEN (b.partition_date - INTERVAL 59 DAY) AND b.partition_date, lr_s_ds.s, 0)) AS relp_amount_60d,
        SUM(IF(lr_v3_ds.source IN (1, 2) AND CAST(lr_s_ds.ct AS DATE) BETWEEN (b.partition_date - INTERVAL 89 DAY) AND b.partition_date, lr_s_ds.s, 0)) AS relp_amount_90d,
        SUM(IF(lr_v3_ds.source IN (1, 2) AND CAST(lr_s_ds.ct AS DATE) BETWEEN (b.partition_date - INTERVAL 119 DAY) AND b.partition_date, lr_s_ds.s, 0)) AS relp_amount_120d,
        SUM(IF(lr_v3_ds.source IN (1, 2) AND CAST(lr_s_ds.ct AS DATE) BETWEEN (b.partition_date - INTERVAL 179 DAY) AND b.partition_date, lr_s_ds.s, 0)) AS relp_amount_180d,
        SUM(IF(lr_v3_ds.source IN (1, 2) AND CAST(lr_s_ds.ct AS DATE) BETWEEN (b.partition_date - INTERVAL 359 DAY) AND b.partition_date, lr_s_ds.s, 0)) AS relp_amount_360d,
        SUM(IF(lr_v3_ds.source IN (1, 2), lr_s_ds.s, 0)) AS relp_amount_all_d
    FROM base AS b
    LEFT JOIN {{ source('mongo', 'logistics_replenishments_stock_daily_snapshot') }} AS lr_s_ds --mongo.logistics_replenishments_stock_daily_snapshot as lr_s_ds
        ON
            1 = 1
            AND b.logistics_product_id = lr_s_ds.pid
            AND CAST(lr_s_ds.ct AS DATE) <= b.partition_date  -- ввозы репленишментов на склад созданные до вчера включительно
    LEFT JOIN {{ source('mongo', 'logistics_replenishments_v3_daily_snapshot') }} AS lr_v3_ds --mongo.logistics_replenishments_v3_daily_snapshot as lr_v3_ds
        ON
            1 = 1
            AND lr_v3_ds._id = lr_s_ds.rid -- ReplenishmentID
            AND lr_v3_ds.source IN (1, 2) -- только мерчантские поставки (не складские накладные)
    GROUP BY
        b.partition_date,
        b.variant_id,
        b.logistics_product_id,
        b.number_in_stock
),

paid_storage AS (
    SELECT
        partition_date,
        variant_id,
        --
        relp_amount_30d,
        relp_amount_60d,
        relp_amount_90d,
        relp_amount_120d,
        relp_amount_180d,
        relp_amount_360d,
        relp_amount_all_d,
        --
        LEAST(number_in_stock, relp_amount_30d) AS ps_lt30,
        LEAST(GREATEST(number_in_stock - relp_amount_30d, 0), relp_amount_60d - relp_amount_30d) AS ps_lt60,
        LEAST(GREATEST(number_in_stock - relp_amount_60d, 0), relp_amount_90d - relp_amount_60d) AS ps_lt90,
        LEAST(GREATEST(number_in_stock - relp_amount_90d, 0), relp_amount_120d - relp_amount_90d) AS ps_lt120,
        LEAST(GREATEST(number_in_stock - relp_amount_120d, 0), relp_amount_180d - relp_amount_120d) AS ps_lt180,
        LEAST(GREATEST(number_in_stock - relp_amount_180d, 0), relp_amount_360d - relp_amount_180d) AS ps_lt360,
        LEAST(GREATEST(number_in_stock - relp_amount_360d, 0), relp_amount_all_d - relp_amount_360d) AS ps_gt360
    FROM paid_storage_pre
),

-- промо и скидка продукта
product_promo AS (
    SELECT
        b.partition_date,
        b.product_id,
        MAX(p.discount) AS discount,
        COUNT(p.promo_id) AS promos
    FROM base AS b
    LEFT JOIN {{ source('mart','promotions') }} AS p --mart.promotions as p
        ON
            1 = 1
            AND b.product_id = p.product_id
            AND CAST(b.partition_date AS TIMESTAMP) BETWEEN p.promo_start_time_utc AND p.promo_end_time_utc
    GROUP BY
        b.partition_date,
        b.product_id
),

--joom select продукты
product_joom_select (
    SELECT DISTINCT
        b.partition_date,
        b.product_id
    FROM base AS b
    INNER JOIN {{ source('goods', 'product_labels') }} AS pl --goods.product_labels as pl
        ON
            1 = 1
            AND b.product_id = pl.product_id
            AND b.partition_date = pl.partition_date
            AND pl.label = 'joom_select'
),

-- first_time_in_stock
first_time_in_stock_pre AS (
    SELECT
        EXPLODE(statusHistory) AS sh_raw,
        boxes
    FROM {{ source('mongo', 'logistics_replenishments_v3_daily_snapshot') }} --mongo.logistics_replenishments_v3_daily_snapshot 
),

first_time_in_stock_pre2 AS (
    SELECT
        sh_raw.uTm AS completed_at,
        EXPLODE(boxes) AS boxes_raw
    FROM first_time_in_stock_pre
    WHERE
        1 = 1
        AND sh_raw.status = 30 -- completed?
),

first_time_in_stock_pre3 AS (
    SELECT
        EXPLODE(boxes_raw.stocks) AS stock,
        completed_at
    FROM first_time_in_stock_pre2
),

first_time_in_stock AS (
    SELECT
        stock.extid AS variant_id,
        MIN(completed_at) AS first_time_in_stock
    FROM first_time_in_stock_pre3
    GROUP BY
        stock.extid
),

--paid stock factors
variant_public AS (
    SELECT
        b.partition_date,
        b.variant_id,
        MAX(pv.public) AS is_variant_public
    FROM base AS b
    LEFT JOIN {{ source('mart', 'dim_published_variant_min') }} AS pv --mart.dim_published_variant_min as pv
        ON
            1 = 1
            AND b.variant_id = pv.variant_id
            AND CAST(b.partition_date AS TIMESTAMP) BETWEEN pv.effective_ts AND pv.next_effective_ts
    GROUP BY
        b.partition_date,
        b.variant_id
),

product_public AS (
    SELECT
        b.partition_date,
        b.product_id,
        MAX(pv.public) AS is_product_public
    FROM base AS b
    LEFT JOIN {{ source('mart', 'dim_published_product_min') }} AS pv --mart.dim_published_product_min as pv
        ON
            1 = 1
            AND b.product_id = pv.product_id
            AND CAST(b.partition_date AS TIMESTAMP) BETWEEN pv.effective_ts AND pv.next_effective_ts
    GROUP BY
        b.partition_date,
        b.product_id
),

product_best AS (
    SELECT
        b.partition_date,
        b.product_id,
        SUM(c.open_count) AS opens,
        SUM(c.preview_count) AS previews
    FROM base AS b
    LEFT JOIN {{ source('platform', 'context_product_counters_v5') }} AS c  --mart.context_product_counters_v5 as c
        ON
            1 = 1
            AND b.product_id = c.product_id
            AND b.partition_date = c.partition_date
            AND c.context_name = 'best'
    GROUP BY
        b.partition_date,
        b.product_id

),

product_tier AS (
    SELECT
        b.partition_date,
        b.product_id,
        MAX(pt.tier) AS tier
    FROM base AS b
    LEFT JOIN {{ source('goods', 'product_tiers') }} AS pt --goods.product_tiers as pt
        ON
            1 = 1
            AND b.product_id = pt.product_id
            AND b.partition_date = pt.partition_date
    GROUP BY
        b.partition_date,
        b.product_id
)

SELECT
    b.partition_date,
    b.variant_id,
    b.product_id,
    b.logistics_product_id,
    vrs.merchant_id,
    b.product_dimensions_h,
    b.product_dimensions_l,
    b.product_dimensions_w,
    b.number_in_stock,
    b.number_in_pending_stock,
    b.enabled_flg,
    b.last_demand_status,
    -- o_1
    vo.orders_cnt,
    vo.fbj_orders_cnt,
    vo.quantity_cnt,
    vo.fbj_quantity_cnt,
    -- o_7
    vo.orders_7_cnt,
    vo.fbj_orders_7_cnt,
    vo.quantity_7_cnt,
    vo.fbj_quantity_7_cnt,
    vo.to_7,
    -- o_14
    vo.orders_14_cnt,
    vo.fbj_orders_14_cnt,
    vo.quantity_14_cnt,
    vo.fbj_quantity_14_cnt,
    vo.to_14,
    -- o_30
    vo.orders_30_cnt,
    vo.fbj_orders_30_cnt,
    vo.quantity_30_cnt,
    vo.fbj_quantity_30_cnt,
    vo.to_30,
    --
    vrs.qty_created,
    vrs.2_qty_in_pending_inbound,
    vrs.3_qty_in_pending_shipping,
    vrs.4_qty_in_shipped_by_merchant,
    vrs.5_qty_in_action_required,
    vrs.6_qty_on_review,
    vrs.qty_completed,
    vrs.qty_canceled_by_joom,
    vrs.qty_canceled_by_merchant,
    -- variant replenishment last ... 
    vrs.last_canceled_by_joom_at,
    vrs.last_canceled_by_merch_at,
    vrs.last_completed_at,
    vrs.last_replenishment_created_at,
    vrs.last_replenishment_status,
    --
    vrs.variant_cancel_rate,
    vrs.avg_complete_days_var,
    --paid storage
    ps.relp_amount_30d,
    ps.relp_amount_60d,
    ps.relp_amount_90d,
    ps.relp_amount_120d,
    ps.relp_amount_180d,
    ps.relp_amount_360d,
    ps.relp_amount_all_d,
    --
    ps.ps_lt30,
    ps.ps_lt60,
    ps.ps_lt90,
    ps.ps_lt120,
    ps.ps_lt180,
    ps.ps_lt360,
    ps.ps_gt360,
    --
    pp.discount,
    IF(pp.promos > 0, 1, 0) AS is_product_in_promo,
    --
    IF(pjs.product_id IS NOT NULL, 1, 0) AS is_product_joom_select,
    --
    ftis.first_time_in_stock,
    CASE
        WHEN b.number_in_stock > 0 THEN 'IN_STOCK'
        WHEN ftis.first_time_in_stock IS NULL THEN 'NEW'
        WHEN b.enabled_flg = 0 THEN 'RUN_OUT_DISABLED'
        ELSE 'OOS'
    END AS stock_status,
    --
    vp.is_variant_public,
    p_pub.is_product_public,
    p_best.opens,
    p_best.previews,
    p_tier.tier,
    --
    gp.product_name,
    gp.business_line,
    gp.l1_merchant_category_id,
    gp.l1_merchant_category_name,
    gp.l2_merchant_category_id,
    gp.l2_merchant_category_name,
    --
    vk.merchant_name,
    vk.main_merchant_name,
    vk.kam
FROM base AS b
LEFT JOIN {{ ref('gold_products') }} AS gp --gold.products as gp -- данные только на текущий момент
    ON
        1 = 1
        AND b.product_id = gp.product_id
LEFT JOIN variants_orders AS vo
    ON
        1 = 1
        AND b.partition_date = vo.partition_date
        AND b.variant_id = vo.variant_id
LEFT JOIN variant_repl_status AS vrs
    ON
        1 = 1
        AND b.partition_date = vrs.partition_date
        AND b.variant_id = vrs.variant_id
LEFT JOIN var_kam AS vk
    ON
        1 = 1
        AND b.variant_id = vk.variant_id
        AND b.partition_date = vk.partition_date
LEFT JOIN paid_storage AS ps
    ON
        1 = 1
        AND b.partition_date = ps.partition_date
        AND b.variant_id = ps.variant_id
LEFT JOIN product_promo AS pp
    ON
        1 = 1
        AND b.partition_date = pp.partition_date
        AND b.product_id = pp.product_id
LEFT JOIN product_joom_select AS pjs
    ON
        1 = 1
        AND b.partition_date = pjs.partition_date
        AND b.product_id = pjs.product_id
LEFT JOIN first_time_in_stock AS ftis
    ON
        1 = 1
        AND b.variant_id = ftis.variant_id
LEFT JOIN variant_public AS vp
    ON
        1 = 1
        AND b.partition_date = vp.partition_date
        AND b.variant_id = vp.variant_id
LEFT JOIN product_public AS p_pub
    ON
        1 = 1
        AND b.partition_date = p_pub.partition_date
        AND b.product_id = p_pub.product_id
LEFT JOIN product_best AS p_best
    ON
        1 = 1
        AND b.partition_date = p_best.partition_date
        AND b.product_id = p_best.product_id
LEFT JOIN product_tier AS p_tier
    ON
        1 = 1
        AND b.partition_date = p_tier.partition_date
        AND b.product_id = p_tier.product_id

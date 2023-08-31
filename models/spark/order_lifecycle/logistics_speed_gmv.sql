{{ config(
     schema='order_lifecycle',
     materialized='table',
     partition_by=['week_date'],
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'bigquery_partitioning_date_column': 'week_date',
       'alerts_channel': "#olc_dbt_alerts",
       'bigquery_fail_on_missing_partitions': 'false'
     }
 ) }}

WITH regions AS (
    SELECT
        region,
        EXPLODE(country_codes) AS country
    FROM mart.dim_region
    WHERE
        next_effective_ts >= "3000-01-01"
    UNION
    SELECT
        "DACH" AS region,
        EXPLODE(ARRAY("DE", "AU", "CH")) AS country
    UNION
    SELECT
        "DACHFR" AS region,
        EXPLODE(ARRAY("DE", "AU", "CH", "FR")) AS country
),

filter_logistics_mart AS (
    SELECT
        fo.order_id,
        fo.gmv_initial AS order_gmv,
        r.region,
        fo.order_created_date_utc,
        DATE_TRUNC("week", TO_TIMESTAMP(fo.order_created_date_utc)) AS order_created_week,
        TO_TIMESTAMP(fo.tracking_destination_country_time_utc) AS destination_country_date,
        DATE_TRUNC("week", TO_TIMESTAMP(fo.tracking_destination_country_time_utc)) AS destination_country_week,
        fo.shipping_type,
        ROUND(fo.delivery_duration_tracking) AS delivery_duration_tracking,
        ROUND(DATEDIFF(fo.tracking_destination_country_time_utc, fo.order_created_time_utc)) AS delivery_duration_destination,
        ROUND(DATEDIFF(fo.tracking_issuing_point_time_utc, fo.tracking_destination_country_time_utc)) AS delivery_duration_from_destination,
        --заказы для которых сможем посчитать скорость
        IF(fo.tracking_destination_country_time_utc IS NOT NULL AND (fo.refund_type IS NULL OR fo.refund_type != "not_delivered") OR fo.delivery_duration_tracking IS NOT NULL, 1, 0) AS is_data_exist,
        IF(fo.refund_type = "not_delivered", 1, 0) AS is_refunded_not_deliv
    FROM
        {{ source('logistics_mart', 'fact_order') }} AS fo
    LEFT JOIN
        regions AS r
        USING (country)
    WHERE
        (
            fo.refund_type IS NULL
            OR fo.refund_type NOT IN ("cancelled_by_customer", "cancelled_by_merchant")
        )
        AND fo.order_created_date_utc >= "2020-01-01"
        AND fo.origin_country = "CN"
        AND fo.shipping_type IN ("RM", "SRM", "NRM")
),

t1 AS ( -- считаем avg_delivery_duration_from_destination - средняя длительнось внутри страны для рм
    SELECT
        region,
        destination_country_week,
        ROUND(AVG(delivery_duration_from_destination)) AS avg_delivery_duration_from_destination
    FROM filter_logistics_mart
    WHERE
        destination_country_week IS NOT NULL
        AND delivery_duration_from_destination IS NOT NULL
    GROUP BY 1, 2
),

data_stat AS (-- считаем долю gmv заказов, по которым можем посчитать длительность
    SELECT
        order_created_week,
        region,
        AVG(is_data_exist) AS share_data_exist,
        AVG(IF(is_data_exist = 0 AND is_refunded_not_deliv = 0, 1, 0)) AS share_not_data_not_refunded,
        AVG(IF(is_data_exist = 0 AND is_refunded_not_deliv = 1, 1, 0)) AS share_not_data_refunded
    FROM
        filter_logistics_mart
    GROUP BY 1, 2
),

existed_data AS (-- считаем general_duration - для рм по трекингу, для нрм складываем длительность до страны назначения со средней длительности внутри страны для рм для дня доставки в страну
    SELECT
        l.*,
        t1.avg_delivery_duration_from_destination,
        COALESCE(l.delivery_duration_tracking, l.delivery_duration_destination + t1.avg_delivery_duration_from_destination) AS general_duration
    FROM
        filter_logistics_mart AS l
    LEFT JOIN
        t1
        ON
            l.region = t1.region
            AND l.destination_country_week = t1.destination_country_week
    WHERE
        l.is_data_exist = 1
),

orders_by_delivery_days AS (
    SELECT
        region,
        order_created_date_utc,
        general_duration,
        COUNT(order_id) AS order_count,
        SUM(order_gmv) AS order_gmv,
        SUM(is_refunded_not_deliv) AS refund_deliv_count,
        SUM(is_refunded_not_deliv * order_gmv) AS refund_deliv_gmv
    FROM existed_data
    GROUP BY 1, 2, 3
),

add_orders_before_dd AS (
    SELECT
        region,
        order_created_date_utc,
        general_duration,
        SUM(order_gmv) OVER (PARTITION BY region, order_created_date_utc ORDER BY general_duration) AS order_gmv_before_dd,
        SUM(order_gmv) OVER (PARTITION BY region, order_created_date_utc) AS total_order_gmv,
        SUM(refund_deliv_gmv) OVER (PARTITION BY region, order_created_date_utc) AS total_refund_deliv_gmv
    FROM orders_by_delivery_days
),

add_perc AS (
    SELECT
        region,
        order_created_date_utc,
        SUM(total_refund_deliv_gmv) AS total_refund_deliv_gmv,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.05, general_duration, NULL)) AS perc_5_delivered,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.1, general_duration, NULL)) AS perc_10_delivered,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.25, general_duration, NULL)) AS perc_25_delivered,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.5, general_duration, NULL)) AS perc_50_delivered,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.8, general_duration, NULL)) AS perc_80_delivered,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.9, general_duration, NULL)) AS perc_90_delivered,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.95, general_duration, NULL)) AS perc_95_delivered,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 0.99, general_duration, NULL)) AS perc_99_delivered,
        MIN(IF(order_gmv_before_dd * 1.0 / total_order_gmv >= 1.0, general_duration, NULL)) AS perc_100_delivered
    FROM add_orders_before_dd
    GROUP BY 1, 2
)


SELECT
    add_perc.region,
    DATE(DATE_TRUNC("week", add_perc.order_created_date_utc)) AS week_date,
    ROUND(AVG(add_perc.perc_5_delivered)) AS perc_5_delivered,
    ROUND(AVG(add_perc.perc_10_delivered)) AS perc_10_delivered,
    ROUND(AVG(add_perc.perc_25_delivered)) AS perc_25_delivered,
    ROUND(AVG(add_perc.perc_50_delivered)) AS perc_50_delivered,
    ROUND(AVG(add_perc.perc_80_delivered)) AS perc_80_delivered,
    ROUND(AVG(add_perc.perc_90_delivered)) AS perc_90_delivered,
    ROUND(AVG(add_perc.perc_95_delivered)) AS perc_95_delivered,
    ROUND(AVG(add_perc.perc_99_delivered)) AS perc_99_delivered,
    ROUND(AVG(add_perc.perc_100_delivered)) AS perc_100_delivered,
    MIN(data_stat.share_data_exist) AS share_data_exist,
    MIN(data_stat.share_not_data_not_refunded) AS share_not_data_not_refunded,
    MIN(data_stat.share_not_data_refunded) AS share_not_data_refunded
FROM add_perc
LEFT JOIN data_stat
    ON
        add_perc.region = data_stat.region
        AND DATE_TRUNC("week", add_perc.order_created_date_utc) = data_stat.order_created_week
GROUP BY 1, 2
ORDER BY 2

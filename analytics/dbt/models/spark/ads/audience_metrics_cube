{{ config(
    schema='ads',
    materialized='incremental',
    partition_by=['partition_date'],
    file_format='delta',
    meta = {
      'team': 'ads',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

WITH regions AS (

  SELECT
    EXPLODE(country_codes) AS country,
    region
  FROM {{ source('mart', 'dim_region') }}

), installs AS (

  SELECT 
    join_date,
    device_id,
    source,
    partner_id
  FROM {{ source('ads', 'ads_install') }}
  WHERE a = 42
      {% if is_incremental() %}
    AND join_date >= DATE('{{ VAR("start_date_ymd") }}') - INTERVAL 120 DAY
    AND join_date < DATE('{{ VAR("end_date_ymd") }}')
      {% else %}
    AND date >= DATE('2019-12-25')
      {% endif %}

), activity AS (

  SELECT DISTINCT
    device_id,
    date_msk AS partition_date
  FROM {{ source('mart', 'star_active_device') }}
  WHERE a = 42
      {% if is_incremental() %}
    AND date_msk >= DATE('{{ VAR("start_date_ymd") }}') - INTERVAL 120 DAY
    AND date_msk < DATE('{{ VAR("end_date_ymd") }}
      {% else %}
    AND date_msk >= DATE('2019-12-25')
      {% endif %}

), dimentions AS (

  SELECT
    star_active_device.date_msk AS partition_date,
    star_active_device.device_id,
    IF(star_active_device.date_msk = join_date, 1, 0) AS is_new,
    DATEDIFF(star_active_device.date_msk, join_date) / 30.42 AS user_age,
    COALESCE(star_active_device.os_type, "unknown") AS os_type,
    COALESCE(star_active_device.country, "unknown") AS country,
    join_date,
    CASE
        WHEN source = "other" THEN COALESCE(installs.partner_id, 'unknown')
        ELSE source
    END AS source_extended
  FROM {{ source('mart', 'star_active_device') }} AS star_active_device
    JOIN installs ON star_active_device.device_id = installs.device_id
  WHERE a = 42
      {% if is_incremental() %}
    AND star_active_device.date_msk >= DATE('{{ VAR("start_date_ymd") }}') - INTERVAL 120 DAY
    AND star_active_device.date_msk < DATE('{{ VAR("end_date_ymd") }}')
      {% else %}
    AND star_active_device.date_msk >= DATE('2019-12-25')
      {% endif %}
      
), orders AS (
  
  SELECT
    device_id,
    DATE(created_time_utc + INTERVAL 3 HOURS) AS order_date,
    COUNT(DISTINCT order_group_id) AS orders_num,
    SUM(gmv_initial) AS gmv,
    SUM(order_gross_profit_final_estimated) AS profit
  FROM {{ source('mart', 'star_order_2020') }}
  GROUP BY 1, 2

), total_orders AS (
  
  SELECT
    device_id,
    SUM(orders_num) AS orders_total,
    SUM(gmv) AS gmv_total,
    SUM(profit) AS profit_total
  FROM orders
  GROUP BY 1

), events AS (

  SELECT DISTINCT
    device_id,
    partition_date,
    1 AS is_product_open
  FROM {{ source('recom', 'context_device_counters_v5') }}
  WHERE type = "productOpen"
      {% if is_incremental() %}
    AND partition_date >= DATE('{{ VAR("start_date_ymd") }}') - INTERVAL 120 DAY
    AND partition_date < DATE('{{ VAR("end_date_ymd") }}')
      {% else %}
    AND partition_date >= DATE('2019-12-25')
      {% endif %}
  

), payments_events AS (

  SELECT
    device_id,
    date AS event_date,
    MAX(is_product_to_cart) AS is_product_to_cart,
    MAX(is_checkout_start) AS is_checkout_start,
    MAX(is_address_save) AS is_address_save,
    MAX(is_checkout_delivery_select) AS is_checkout_delivery_select,
    MAX(is_checkout_pmt_method_select) AS is_checkout_pmt_method_select,
    MAX(is_pmt_start) AS is_pmt_start,
    MAX(is_pmt_success) AS is_pmt_success
  FROM {{ source('payments', 'checkout_data') }}
  WHERE a = 42
      {% if is_incremental() %}
    AND date >= DATE('{{ VAR("start_date_ymd") }}') - INTERVAL 120 DAY
    AND date < DATE('{{ VAR("end_date_ymd") }}')
      {% else %}
    AND date >= DATE('2019-12-25')
      {% endif %}
  GROUP BY 1, 2

), dataset AS (

  SELECT 
      dimentions.partition_date,
      dimentions.device_id,
      user_age,
      is_new,
      os_type,
      country,
      source_extended,
      orders_total,
      COALESCE(orders_num, 0) AS orders_num,
      CASE
        WHEN orders_total IS NULL THEN "1. 0"
        WHEN orders_total = 1 THEN "2. 1"
        WHEN orders_total BETWEEN 2 AND 5 THEN "3. 2-5"
        WHEN orders_total BETWEEN 6 AND 10 THEN "4. 6-10"
        WHEN orders_total BETWEEN 11 AND 100 THEN "5. 11-100"
        ELSE "6. >100"
      END AS orders_total_category,
      gmv_total,
      COALESCE(gmv, 0) AS gmv,
      CASE
        WHEN gmv_total IS NULL THEN "1. 0"
        WHEN gmv_total <= 10 THEN "2. 0-10$$"
        WHEN gmv_total <= 100 THEN "3. 10-100$$"
        ELSE "4. >100$$"
      END AS gmv_total_category,
      profit_total,
      COALESCE(profit, 0) AS profit,
      COALESCE(is_product_open, 0) AS is_product_open,
      COALESCE(is_product_to_cart, 0) AS is_product_to_cart,
      COALESCE(is_checkout_start, 0) AS is_checkout_start,
      COALESCE(is_address_save, 0) AS is_address_save,
      COALESCE(is_checkout_delivery_select, 0) AS is_checkout_delivery_select,
      COALESCE(is_checkout_pmt_method_select, 0) AS is_checkout_pmt_method_select,
      COALESCE(is_pmt_start, 0) AS is_pmt_start,
      COALESCE(is_pmt_success, 0) AS is_pmt_success,
      IF(gmv IS NOT NULL, 1, 0) AS is_product_purchase,
      MAX(IF(activity.partition_date <= DATE_ADD(dimentions.partition_date, 1), 1, 0)) AS is_returned_1d,
      MAX(IF(activity.partition_date <= DATE_ADD(dimentions.partition_date, 7), 1, 0)) AS is_returned_7d,
      MAX(IF(activity.partition_date BETWEEN DATE_ADD(dimentions.partition_date, 7) AND DATE_ADD(dimentions.partition_date, 13), 1, 0)) AS is_returned_1w,
      MAX(IF(activity.partition_date BETWEEN DATE_ADD(dimentions.partition_date, 30) AND DATE_ADD(dimentions.partition_date, 59), 1, 0)) AS is_returned_1m,
      MAX(IF(activity.partition_date BETWEEN DATE_ADD(dimentions.partition_date, 90) AND DATE_ADD(dimentions.partition_date, 119), 1, 0)) AS is_returned_3m,
      IF(MAX(IF(activity.partition_date <= DATE_ADD(dimentions.partition_date, 5), 1, 0)) = 1, 0, 1) AS is_bounced_5d,
      IF(MAX(IF(activity.partition_date <= DATE_ADD(dimentions.partition_date, 30), 1, 0)) = 1, 0, 1) AS is_bounced_30d,
      IF(MAX(IF(activity.partition_date <= DATE_ADD(dimentions.partition_date, 90), 1, 0)) = 1, 0, 1) AS is_bounced_90d
  FROM dimentions
  LEFT JOIN orders ON dimentions.device_id = orders.device_id
    AND orders.order_date = dimentions.partition_date
  LEFT JOIN total_orders ON dimentions.device_id = total_orders.device_id
  LEFT JOIN events ON dimentions.device_id = events.device_id
    AND dimentions.partition_date = events.partition_date
  LEFT JOIN payments_events ON dimentions.device_id = payments_events.device_id
    AND dimentions.partition_date = payments_events.event_date
  LEFT JOIN activity ON dimentions.device_id = activity.device_id
    AND activity.partition_date > dimentions.partition_date
    AND activity.partition_date <= DATE_ADD(dimentions.partition_date, 120)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24  
  ORDER BY 1

), dataset_orders AS (

    SELECT 
        partition_date,
        dataset.device_id AS device_id,
        user_age,
        is_new,
        os_type,
        country,
        source_extended,
        orders_total,
        dataset.orders_num AS orders_num,
        orders_total_category,
        gmv_total,
        dataset.gmv AS gmv,
        gmv_total_category,
        profit_total,
        dataset.profit AS profit,
        is_product_open,
        is_product_to_cart,
        is_checkout_start,
        is_address_save,
        is_checkout_delivery_select,
        is_checkout_pmt_method_select,
        is_pmt_start,
        is_pmt_success,
        is_product_purchase,
        is_returned_1d,
        is_returned_7d,
        is_returned_1w,
        is_returned_1m,
        is_returned_3m,
        is_bounced_5d,
        is_bounced_30d,
        is_bounced_90d,
        SUM(IF(orders.order_date <= DATE_ADD(dataset.partition_date, 7), orders.gmv, 0)) as gmv_7d,
        SUM(IF(orders.order_date <= DATE_ADD(dataset.partition_date, 30), orders.gmv, 0)) as gmv_30d,
        SUM(IF(orders.order_date <= DATE_ADD(dataset.partition_date, 7), orders.profit, 0)) as profit_7d,
        SUM(IF(orders.order_date <= DATE_ADD(dataset.partition_date, 30), orders.profit, 0)) as profit_30d    
    FROM dataset
    LEFT JOIN orders ON dataset.device_id = orders.device_id
        AND orders.order_date > dataset.partition_date
        AND orders.order_date <= DATE_ADD(dataset.partition_date, 30)
    WHERE partition_date IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32

), cube AS (

    SELECT  
        partition_date,
        is_new,
        os_type,
        country,
        source_extended,
        orders_total_category,
        gmv_total_category,
        COUNT(*) AS dau,
        SUM(user_age) AS user_age,
        SUM(is_product_open) AS open_users,
        SUM(is_product_to_cart) AS to_cart_users,
        SUM(is_checkout_start) AS checkout_start_users,
        SUM(is_address_save) AS address_save_users,
        SUM(is_checkout_delivery_select) AS checkout_delivery_users,
        SUM(is_checkout_pmt_method_select) AS checkout_pmt_users,
        SUM(is_pmt_start) AS pmt_start_users,
        SUM(is_pmt_success) AS pmt_success_users,
        SUM(is_product_purchase) AS purchase_users,  
        SUM(is_returned_1d) AS retention_1d_users,
        SUM(is_returned_7d) AS retention_7d_users,
        SUM(is_returned_1w) AS retention_1w_users,
        SUM(is_returned_1m) AS retention_1m_users,
        SUM(is_returned_3m) AS retention_3m_users,
        SUM(is_bounced_5d) AS bounce_5d_users,
        SUM(is_bounced_30d) AS bounce_30d_users,
        SUM(is_bounced_90d) AS bounce_90d_users,
        SUM(orders_num) AS orders,
        SUM(gmv) AS gmv,
        SUM(profit) AS profit,
        SUM(gmv_7d) AS gmv_7d,
        SUM(gmv_30d) AS gmv_30d,
        SUM(profit_7d) AS profit_7d,
        SUM(profit_30d) AS profit_30d
    FROM dataset_orders
    GROUP BY partition_date, is_new, os_type, country, source_extended, orders_total_category, gmv_total_category WITH CUBE

)

SELECT         
    partition_date,
    is_new,
    os_type,
    region,
    country,
    source_extended,
    orders_total_category,
    gmv_total_category,
    dau,
    user_age,
    open_users,
    to_cart_users,
    checkout_start_users,
    address_save_users,
    checkout_delivery_users,
    checkout_pmt_users,
    pmt_start_users,
    pmt_success_users,
    purchase_users,  
    retention_1d_users,
    retention_7d_users,
    retention_1w_users,
    retention_1m_users,
    retention_3m_users,
    bounce_5d_users,
    bounce_30d_users,
    bounce_90d_users,
    orders,
    gmv,
    profit,
    gmv_7d,
    gmv_30d,
    profit_7d,
    profit_30d
FROM cube
JOIN regions ON UPPER(regions.country) = UPPER(cube.country)
WHERE partition_date IS NOT NULL

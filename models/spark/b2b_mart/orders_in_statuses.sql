{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

WITH not_jp_users AS (
  SELECT DISTINCT u.user_id, u.order_id
  FROM {{ ref('fact_user_request') }} f
  LEFT JOIN {{ ref('fact_order') }} u ON f.user_id = u.user_id
  WHERE is_joompro_employee != TRUE or is_joompro_employee IS NULL
),

order_v2_mongo AS
(
    SELECT fo.order_id AS order_id,
        fo.user_id,
        DATE(fo.min_manufactured_ts_msk) AS manufactured_date
    FROM {{ ref('fact_order') }} AS fo
    INNER JOIN not_jp_users AS u ON fo.user_id = u.user_id
    WHERE fo.last_order_status < 60
        AND fo.last_order_status >= 10
        AND fo.next_effective_ts_msk IS NULL
        AND fo.min_manufactured_ts_msk IS NOT NULL
),

orders AS
(       SELECT 
      manufactured_date AS t,
        CASE WHEN manufactured_date = first_order_date THEN 'first order' ELSE 'repeated order' END AS repeated_order,
        COUNT(DISTINCT order_id) AS orders,
        SUM(total_confirmed_price)  AS gmv_initial,
        SUM(initial_gross_profit)  AS initial_gross_profit,
        SUM(final_gross_profit)  AS final_gross_profit,
        status, sub_status,
        current_status, current_sub_status
        FROM
( SELECT DISTINCT manufactured_date,
        first_order_date,
        order_id,
        total_confirmed_price,
        initial_gross_profit,
        final_gross_profit,
        status, sub_status,
        current_status, current_sub_status
        FROM
(
    SELECT  
        DATE(MIN(p.event_ts_msk) OVER (PARTITION BY p.order_id, status, sub_status)) AS manufactured_date,
        DATE(MIN(p.event_ts_msk) OVER (PARTITION BY u.user_id, status, sub_status)) AS first_order_date,
        p.order_id,
        FIRST_VALUE(total_confirmed_price) OVER (PARTITION BY p.order_id, status, sub_status ORDER BY p.event_ts_msk DESC) AS total_confirmed_price,
        FIRST_VALUE(final_gross_profit) OVER (PARTITION BY p.order_id, status, sub_status ORDER BY p.event_ts_msk DESC) AS final_gross_profit,
        FIRST_VALUE(initial_gross_profit) OVER (PARTITION BY p.order_id, status, sub_status ORDER BY p.event_ts_msk DESC) AS initial_gross_profit,
        FIRST_VALUE(status) OVER (PARTITION BY p.order_id ORDER BY p.event_ts_msk DESC) AS current_status,
        FIRST_VALUE(sub_status) OVER (PARTITION BY p.order_id ORDER BY p.event_ts_msk DESC) AS current_sub_status,
        status, sub_status
    FROM {{ ref('fact_order_change') }} AS p
    INNER JOIN order_v2_mongo AS u ON p.order_id = u.order_id
    WHERE p.order_id NOT IN ('6294f3dd4c428b23cd6f2547')
)
)
GROUP BY manufactured_date,
        CASE WHEN manufactured_date = first_order_date THEN 'first order' ELSE 'repeated order' END,
        status, sub_status, current_status, current_sub_status
)

SELECT  t,
        repeated_order,
        status, sub_status,
        current_status, current_sub_status,
        SUM(orders) AS orders,
        SUM(gmv_initial)  AS gmv_initial,
        SUM(initial_gross_profit)  AS initial_gross_profit,
        SUM(final_gross_profit)  AS final_gross_profit
FROM (
    SELECT * from orders
)
WHERE (gmv_initial > 0 or initial_gross_profit > 0 or final_gross_profit > 0)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1

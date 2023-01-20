{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


with not_jp_users AS (
  SELECT DISTINCT user_id
  FROM {{ ref('fact_user_request') }}
  WHERE is_joompro_employee != TRUE or is_joompro_employee IS NULL
),

order_v2_mongo AS
(
    SELECT fo.order_id AS order_id,
        fo.user_id,
        DATE(fo.min_manufactured_ts_msk) AS manufactured_date,
        MIN(DATE(fo.min_manufactured_ts_msk)) OVER (PARTITION BY fo.user_id) AS min_manufactured_date
    FROM {{ ref('fact_order') }} AS fo
    INNER JOIN not_jp_users AS u ON fo.user_id = u.user_id
    WHERE fo.last_order_status < 60
        AND fo.last_order_status >= 10
        AND fo.next_effective_ts_msk IS NULL
        AND fo.min_manufactured_ts_msk IS NOT NULL
),

 order_v2 AS
(
    SELECT order_id,
        total_confirmed_price,
        final_gross_profit,
        initial_gross_profit
        from
    (SELECT order_id,
        total_confirmed_price,
        final_gross_profit,
        initial_gross_profit,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts_msk DESC) as rn
    FROM {{ (ref'fact_order_change') }}
    )
    WHERE rn = 1
),

sources AS (
  select distinct
        utm_campaign,
        utm_source,
        utm_medium,
        source, 
        type,
        campaign,
        order_id
    FROM {{ ref('fact_interactions') }}
),

after_second_qrt_new_order AS
(
    SELECT  manufactured_date AS t,
        p.order_id,
        SUM(total_confirmed_price) AS gmv_initial,
        SUM(initial_gross_profit) AS initial_gross_profit,
        SUM(final_gross_profit) AS final_gross_profit,
        utm_campaign,
        utm_source,
        utm_medium,
        source, 
        type,
        campaign,
        user_id,
        CASE WHEN
            o.min_manufactured_date < o.manufactured_date THEN 'first order'
            ELSE 'repeated order'
        END AS first_order
    FROM order_v2 AS p
    INNER JOIN order_v2_mongo AS o ON  p.order_id = o.order_id
    LEFT JOIN sources AS s on p.order_id = s.order_id
    WHERE p.order_id NOT IN ('6294f3dd4c428b23cd6f2547')
    GROUP BY 
      manufactured_date, 
      p.order_id,
      utm_campaign,
      utm_source,
      utm_medium,
      source, 
      type,
      campaign,
      CASE WHEN
            o.min_manufactured_date < o.manufactured_date THEN 'first order'
            ELSE 'repeated order'
      END,
      user_id
),

users as (
    select distinct user_id, day, client
    from 
    (select a.user_id, day, CASE WHEN SUM(CASE WHEN t > add_months(day, -6)
                AND t <= day
                THEN gmv_initial ELSE 0 END) OVER (PARTITION BY user_id, day) > 100000 THEN 'big client'
            WHEN SUM(CASE WHEN t > add_months(day, -6)
                AND t <= day
                THEN gmv_initial ELSE 0 END) OVER (PARTITION BY user_id, day) > 30000 THEN 'medium client' 
            ELSE 'small client' END as client
    from
    (select distinct order_id, user_id, gmv_initial, t, 1 as for_join from after_second_qrt_new_order) a
    left join  (SELECT
        explode(sequence(to_date('2022-06-01'), to_date(CURRENT_DATE()), interval 1 day)) as day,
        1 AS for_join
        ) as d on a.for_join = d.for_join
     )
)

SELECT a.*, client, CASE WHEN SUM(CASE WHEN t > add_months(current_date(), -6)
                AND t <= current_date()
                THEN gmv_initial ELSE 0 END) OVER (PARTITION BY a.user_id) > 100000 THEN 'big client'
             WHEN SUM(CASE WHEN t > add_months(current_date(), -6)
                AND t <= current_date()
                THEN gmv_initial ELSE 0 END) OVER (PARTITION BY a.user_id) > 30000 THEN 'medium client' 
             ELSE 'small client' END as current_client
    FROM after_second_qrt_new_order as a
    left join users on a.user_id = users.user_id and a.t = users.day
WHERE gmv_initial > 0

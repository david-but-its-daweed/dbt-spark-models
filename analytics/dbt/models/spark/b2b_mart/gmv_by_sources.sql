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
  WHERE is_joompro_employee = TRUE
),

users_owner as (
select user_id, day, min(owner_moderator_id) as owner_moderator_id
from 
(
select user_id, owner_moderator_id,
explode(sequence(to_date(date_from), to_date(date_to), interval 1 day)) as day
from
(
select
user_id, date(event_ts_msk) as date_from, 
coalesce(date(lead(event_ts_msk) over (partition by user_id order by event_ts_msk)), current_date()) as date_to,
owner_moderator_id
from
(
select user_id, event_ts_msk,
coalesce(lead(owner_moderator_id) over (partition by user_id order by event_ts_msk), '0') as next_owner,
owner_moderator_id
from {{ ref('fact_order_change') }}
where owner_moderator_id is not null)
where owner_moderator_id != next_owner or next_owner is null
)
)
group by user_id, day
),

admin AS (
    SELECT
        admin_id,
        a.email,
        s.role as owner_role
    FROM {{ ref('dim_user_admin') }} a
    LEFT JOIN {{ ref('support_roles') }} s on a.email = s.email
),

order_v2_mongo AS
(
    SELECT fo.order_id AS order_id,
        fo.user_id,
        DATE(fo.min_manufactured_ts_msk) AS manufactured_date,
        MIN(DATE(fo.min_manufactured_ts_msk)) OVER (PARTITION BY fo.user_id) AS min_manufactured_date
    FROM {{ ref('fact_order') }} AS fo
    LEFT JOIN not_jp_users AS u ON fo.user_id = u.user_id
    WHERE fo.last_order_status < 60
        AND u.user_id IS NULL
        AND fo.last_order_status >= 10
        AND fo.next_effective_ts_msk IS NULL
        AND fo.min_manufactured_ts_msk IS NOT NULL
),

 order_v2 AS
(
    SELECT order_id,
        total_confirmed_price,
        final_gross_profit,
        initial_gross_profit,
        owner_moderator_id
        from
    (SELECT order_id,
        total_confirmed_price,
        final_gross_profit,
        initial_gross_profit,
        owner_moderator_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts_msk DESC) as rn
    FROM {{ ref('fact_order_change') }}
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
        a.email as owner_email,
        a.owner_role,
        CASE WHEN
            o.min_manufactured_date < o.manufactured_date THEN 'first order'
            ELSE 'repeated order'
        END AS first_order
    FROM order_v2 AS p
    INNER JOIN order_v2_mongo AS o ON  p.order_id = o.order_id
    LEFT JOIN sources AS s on p.order_id = s.order_id
    LEFT JOIN admin AS a on p.owner_moderator_id = a.admin_id
    WHERE p.order_id NOT IN ('6294f3dd4c428b23cd6f2547', '63fe06635ff04955459c3482')
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
      user_id,
      a.email,
      a.owner_role
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
        explode(sequence(to_date('2022-03-01'), to_date(CURRENT_DATE()), interval 1 day)) as day,
        1 AS for_join
        ) as d on a.for_join = d.for_join
     )
)

SELECT 
    a.t, 
    a.order_id,
    a.gmv_initial,
    a.initial_gross_profit,
    a.final_gross_profit,
    a.utm_campaign,
    a.utm_source,
    a.utm_medium,
    a.source,
    a.type,
    a.campaign,
    a.user_id, 
    ad.email as owner_email,
    ad.owner_role as owner_role,
    a.first_order,
    client, CASE WHEN SUM(CASE WHEN t > add_months(current_date(), -6)
                AND t <= current_date()
                THEN gmv_initial ELSE 0 END) OVER (PARTITION BY a.user_id) > 100000 THEN 'big client'
             WHEN SUM(CASE WHEN t > add_months(current_date(), -6)
                AND t <= current_date()
                THEN gmv_initial ELSE 0 END) OVER (PARTITION BY a.user_id) > 30000 THEN 'medium client' 
             ELSE 'small client' END as current_client
    FROM after_second_qrt_new_order as a
    left join users on a.user_id = users.user_id and a.t = users.day
    left join users_owner uo on a.user_id = uo.user_id and a.t = uo.day
    left join admin ad on uo.owner_moderator_id = ad.admin_id
WHERE gmv_initial > 0

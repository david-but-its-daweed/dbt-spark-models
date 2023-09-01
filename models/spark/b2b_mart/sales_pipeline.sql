{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2023-06-24', '2023-06-23', '2023-06-27']
    }
) }}
    
WITH 
customer_plans AS (
    SELECT
        user_id,
        MAX(case when DATE(quarter) = DATE(DATE_TRUNC('QUARTER', partition_date_msk))
        then CAST(plan AS INT) ELSE 0 END) AS plan,
        MAX(case when DATE(quarter) = DATE(DATE_TRUNC('QUARTER', partition_date_msk) - INTERVAL 3 MONTH)
        then CAST(plan AS INT) ELSE 0 END) AS plan_last_quarter
    FROM {{ ref('customer_plans_daily_snapshot') }}
    WHERE
        partition_date_msk = (
            SELECT MAX(partition_date_msk) FROM {{ ref('customer_plans_daily_snapshot') }}
        )
        GROUP by user_id
),

user_admin_plans AS (
    SELECT
        moderator_id,
        MAX(case when DATE(quarter) = DATE(DATE_TRUNC('QUARTER', partition_date_msk))
        then CAST(plan AS INT) ELSE 0 END) AS plan,
        MAX(case when DATE(quarter) = DATE(DATE_TRUNC('QUARTER', partition_date_msk) - INTERVAL 3 MONTH)
        then CAST(plan AS INT) ELSE 0 END) AS plan_last_quarter
    FROM {{ ref('user_admin_plans') }}
    WHERE
        partition_date_msk = (
            SELECT MAX(partition_date_msk) FROM {{ ref('user_admin_plans') }}
        )
     GROUP by moderator_id
),

gmv_user as (
    select user_id,
    sum(gmv_initial) as gmv_year,
    sum(case when date_payed >= date('{{ var("start_date_ymd") }}') - interval 3 month then gmv_initial else 0 end) as gmv_quarter
    from (
        select distinct
            t as date_payed, 
            g.order_id,
            g.gmv_initial,
            g.initial_gross_profit,
            g.final_gross_profit,
            g.owner_email,
            g.owner_role,
            user_id
        FROM {{ ref('gmv_by_sources') }} g
        )
    where date_payed >= date('{{ var("start_date_ymd") }}') - interval 1 year
    group by user_id
),

customers AS (
    select distinct
    c.user_id,
    c.created_ts_msk,
    c.country,
    c.conversion_status,
    c.tracked,
    c.validation_status,
    c.reject_reason,
    c.owner_id,
    c.owner_email,
    c.owner_role,
    c.last_name, 
    c.first_name,
    c.company_name,
    c.volume_from,
    c.volume_to,
    coalesce(c.grade, "unknown") as grade,
    coalesce(c.grade_probability, "unknown") as grade_probability,
    gu.gmv_year,
    gu.gmv_quarter,
    c.amo_crm_id, 
    c.amo_id, 
    c.invited_by_promo
    from {{ ref('fact_customers') }} as c
    left join gmv_user gu on gu.user_id = c.user_id
),

gmv AS (
    SELECT
        user_id,
        SUM(CASE WHEN t >= DATE(DATE_TRUNC('QUARTER', CURRENT_DATE())) THEN gmv_initial ELSE 0 END) AS gmv_fact,
        SUM(CASE WHEN t >= DATE(DATE_TRUNC('QUARTER', CURRENT_DATE()) - INTERVAL 3 MONTH)
            AND t < DATE(DATE_TRUNC('QUARTER', CURRENT_DATE())) THEN gmv_initial ELSE 0 END) AS gmv_fact_last_quarter
    FROM {{ ref('gmv_by_sources') }}
    GROUP BY
        user_id
),

deals AS (
    SELECT DISTINCT
        user_id,
        deal_id,
        deal_type,
        estimated_date,
        estimated_gmv,
        owner_email,
        deal_name,
        case when status_int >= 10 and status_int <= 50 then 'Upside'
        when status_int = 60 then 'Forecast'
        when status_int >= 70 and status_int <= 80 then 'Commited' end as status
    FROM {{ ref('fact_deals') }}
    WHERE next_effective_ts_msk IS NULL
),


forecast_user AS (
    SELECT
        user_id,
        sum(case when status = 'Upside' then estimated_gmv else 0 end) as upside,
        sum(case when status = 'Forecast' then estimated_gmv else 0 end) as forecast,
        sum(case when status = 'Commited' then estimated_gmv else 0 end) as commited
    FROM deals
    GROUP BY user_id
),

final_users AS (
    SELECT
        owner_email,
        owner_role,
        c.user_id,
        country,
        conversion_status,
        coalesce(tracked, false) as tracked,
        validation_status,
        reject_reason,
        last_name,
        first_name,
        company_name,
        volume_from,
        volume_to,
        grade,
        grade_probability,
        gmv_year,
        gmv_quarter,
        g.gmv_fact_last_quarter AS fact_last_quarter,
        COALESCE(cp.plan_last_quarter, 0) AS plan_last_quarter,
        coalesce(d.upside, 0) as upside,
        coalesce(d.forecast, 0) as forecast,
        coalesce(d.commited, 0) as commited,
        coalesce(g.gmv_fact, 0) AS fact,
        COALESCE(cp.plan, 0) AS plan,
        COALESCE(cp.plan, 0) > 0 as predicted,
        'clients' AS type
    FROM customers AS c
    LEFT JOIN gmv AS g ON c.user_id = g.user_id
    LEFT JOIN forecast_user AS d ON c.user_id = d.user_id
    LEFT JOIN customer_plans AS cp ON c.user_id = cp.user_id
),

owners AS (
    SELECT DISTINCT
        admin_id,
        email
    FROM {{ ref('dim_user_admin') }}
),

kam_country AS (
    SELECT 
        owner_email,
        FIRST_VALUE(country) OVER (PARTITION BY owner_email ORDER BY country DESC) AS country
    FROM
    (
    SELECT
        owner_email,
        country,
        COUNT(DISTINCT user_id) AS users
    FROM final_users
    WHERE country IS NOT NULL
    GROUP BY owner_email,
        country
    )
),

forecast_kam AS (
    SELECT
        owner_email,
        owner_role,
        sum(upside) as upside,
        sum(forecast) as forecast,
        sum(commited) as commited,
        plan > 0 as predicted
    FROM final_users
    GROUP BY
        owner_email,
        owner_role,
        plan > 0
),

fact_kam AS (
    SELECT
        g.owner_email,
        SUM(case when plan > 0 then fact_last_quarter else 0 end) AS fact_last_quarter,
        SUM(case when plan > 0 then plan_last_quarter else 0 end) AS plan_last_quarter,
        SUM(case when plan > 0 then fact else 0 end) AS fact,
        SUM(case when plan > 0 then plan else 0 end) AS plan,
        true as predicted
    FROM final_users AS g
    GROUP BY g.owner_email
    UNION ALL
        SELECT
        g.owner_email,
        SUM(case when g.plan = 0 OR g.plan_last_quarter IS NULL then g.fact_last_quarter else 0 end) AS fact_last_quarter,
        MAX(case when g.plan = 0 OR g.plan_last_quarter IS NULL then aup.plan_last_quarter else 0 end) AS plan_last_quarter,
        SUM(case when g.plan = 0 OR g.plan IS NULL then g.fact else 0 end) AS fact,
        MAX(case when g.plan = 0 OR g.plan IS NULL then aup.plan else 0 end) AS plan,
        false as predicted
    FROM final_users AS g
    left join owners as o on g.owner_email = o.email
    left join user_admin_plans as aup on aup.moderator_id = o.admin_id
    GROUP BY g.owner_email
),

final_kam AS (
    SELECT
        c.owner_email,
        c.owner_role,
        '' as user_id,
        kc.country as country,
        '' as conversion_status,
        true as tracked,
        '' as validation_status,
        '' as reject_reason,
        '' as last_name,
        '' as first_name,
        '' as company_name,
        '' as volume_from,
        '' as volume_to,
        '' as grade,
        '' as grade_probability,
        0 as gmv_year,
        0 as gmv_quarter,
        f.fact_last_quarter AS fact_last_quarter,
        COALESCE(f.plan_last_quarter, 0) AS plan_last_quarter,
        c.upside,
        c.forecast,
        c.commited,
        coalesce(f.fact, 0) AS fact,
        COALESCE(f.plan, 0) AS plan,
        COALESCE(f.predicted, false) as predicted,
        'KAM' AS type

    FROM forecast_kam AS c
    LEFT JOIN fact_kam AS f ON f.owner_email = c.owner_email 
        and COALESCE(c.predicted, false) = COALESCE(f.predicted, false)
    LEFT JOIN kam_country AS kc ON c.owner_email = kc.owner_email 
)


SELECT DISTINCT
    *,
    date'{{ var("start_date_ymd") }}' AS partition_date_msk
FROM (
    SELECT *
    FROM final_users
    UNION ALL
    SELECT *
    FROM final_kam
)
WHERE owner_email IS NOT NULL

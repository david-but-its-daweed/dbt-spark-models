{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk'
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
            SELECT MAX(partition_date_msk) FROM {{ ref('customer_plans_daily_snapshot') }}
        )
     GROUP by moderator_id
),

customers AS (
    SELECT
        user_id,
        validation_status,
        reject_reason,
        owner_email,
        owner_role,
        company_name,
        volume_from,
        volume_to,
        grade,
        grade_probability,
        MAX(gmv_year) AS gmv_year,
        MAX(gmv_quarter) AS gmv_quarter
    FROM {{ ref('users_daily_table') }}
    WHERE
        partition_date_msk = (
            SELECT MAX(partition_date_msk) FROM {{ ref('users_daily_table') }}
        )
        AND owner_email IS NOT NULL AND admin_status = 'current admin'
    GROUP BY
        user_id,
        validation_status,
        reject_reason,
        owner_email,
        owner_role,
        company_name,
        volume_from,
        volume_to,
        grade,
        grade_probability
),

gmv AS (
    SELECT
        user_id,
        owner_email,
        owner_role,
        COALESCE(grade, 'unknown') AS grade,
        SUM(CASE WHEN date_payed >= DATE(DATE_TRUNC('QUARTER', partition_date_msk)) THEN gmv_user_admin ELSE 0 END) AS gmv_fact,
        SUM(CASE WHEN date_payed >= DATE(DATE_TRUNC('QUARTER', partition_date_msk) - INTERVAL 3 MONTH) THEN gmv_user_admin ELSE 0 END) AS gmv_fact_last_quarter
    FROM {{ ref('users_daily_table') }}
    WHERE
        partition_date_msk = (
            SELECT MAX(partition_date_msk) FROM {{ ref('users_daily_table') }}
        )
        AND owner_email IS NOT NULL
    GROUP BY
        user_id,
        owner_email,
        owner_role,
        COALESCE(grade, 'unknown')
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
        gmv_initial,
        case when status_int >= 10 and status_int <= 50 then 'Upsale'
        when status_int >= 60 and status_int <= 70 then 'Forecast'
        when status_int = 80 then 'Commited' end as status
    FROM {{ ref('fact_deals') }}
    WHERE partition_date_msk = (SELECT MAX(partition_date_msk) FROM {{ ref('fact_deals') }})
),


forecast_user AS (
    SELECT
        user_id,
        owner_email,
        sum(case when status = 'Upsale' then estimated_gmv else 0 end) as upsale,
        sum(case when status = 'Forecast' then estimated_gmv else 0 end) as forecast,
        sum(case when status = 'Commited' then estimated_gmv else 0 end) as commited
    FROM deals
    GROUP BY user_id, owner_email
),

final_users AS (
    SELECT
        c.owner_email,
        c.owner_role,
        c.user_id,
        c.company_name,
        c.grade,
        c.grade_probability,
        c.gmv_year,
        c.gmv_quarter,
        c.validation_status,
        c.reject_reason,
        g.gmv_fact_last_quarter AS fact_last_quarter,
        COALESCE(cp.plan_last_quarter, 0) AS plan_last_quarter,
        d.upsale,
        d.forecast,
        d.commited,
        g.gmv_fact AS fact,
        COALESCE(cp.plan, 0) AS plan,
        plan > 0 as predicted,
        'clients' AS type
    FROM customers AS c
    LEFT JOIN gmv AS g ON c.user_id = g.user_id AND c.owner_email = g.owner_email
    LEFT JOIN forecast_user AS d ON c.user_id = d.user_id
    LEFT JOIN customer_plans AS cp ON c.user_id = cp.user_id
),

owners AS (
    SELECT DISTINCT
        admin_id,
        email
    FROM {{ ref('dim_user_admin') }}
),

forecast_kam AS (
    SELECT
        owner_email,
        owner_role,
        sum(upsale) as upsale,
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
        SUM(case when plan_last_quarter > 0 then fact_last_quarter else 0 end) AS fact_last_quarter,
        SUM(case when plan_last_quarter > 0 then plan_last_quarter else 0 end) AS plan_last_quarter,
        SUM(case when plan > 0 then fact else 0 end) AS fact,
        SUM(case when plan > 0 then plan else 0 end) AS plan,
        true as predicted
    FROM final_users AS g
    GROUP BY g.owner_email
    UNION ALL
        SELECT
        g.owner_email,
        SUM(case when g.plan_last_quarter = 0 OR g.plan_last_quarter IS NULL then g.fact_last_quarter else 0 end) AS fact_last_quarter,
        MAX(case when g.plan_last_quarter = 0 OR g.plan_last_quarter IS NULL then aup.plan_last_quarter else 0 end) AS plan_last_quarter,
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
        '' AS user_id,
        '' AS company_name,
        '' AS grade,
        '' AS grade_probability,
        NULL AS gmv_year,
        NULL AS gmv_quarter,
        '' AS validation_status,
        '' AS reject_reason,
        f.fact_last_quarter AS fact_last_quarter,
        COALESCE(f.plan_last_quarter, 0) AS plan_last_quarter,
        c.upsale,
        c.forecast,
        c.commited,
        f.fact AS fact,
        COALESCE(f.plan, 0) AS plan,
        plan > 0 as predicted,
        'KAM' AS type

    FROM forecast_kam AS c
    LEFT JOIN fact_kam AS f ON f.owner_email = c.owner_email and c.predicted = f.predicted
)


SELECT
    *,
    CURRENT_DATE() AS partition_date_msk
FROM (
    SELECT *
    FROM final_users
    UNION ALL
    SELECT *
    FROM final_kam
)
WHERE owner_email IS NOT NULL

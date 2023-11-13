{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2023-03-01', '2023-11-02', '2023-11-05', '2023-11-04', '2023-11-03']
    }
) }}


WITH gmv AS (
    SELECT DISTINCT
        t AS date_payed,
        g.order_id,
        g.gmv_initial,
        g.initial_gross_profit,
        g.final_gross_profit,
        g.owner_email,
        g.owner_role,
        user_id
    FROM {{ ref('gmv_by_sources') }} AS g
),

gmv_user AS (
    SELECT
        user_id,
        SUM(gmv_initial) AS gmv_year,
        SUM(CASE
            WHEN date_payed >= DATE('{{ var("start_date_ymd") }}') - INTERVAL 3 MONTH
                THEN gmv_initial
            ELSE 0
        END) AS gmv_quarter
    FROM gmv
    WHERE date_payed >= DATE('{{ var("start_date_ymd") }}') - INTERVAL 1 YEAR
    GROUP BY user_id
),

gmv_user_admin AS (
    SELECT
        date_payed,
        user_id,
        owner_email,
        owner_role,
        SUM(gmv_initial) AS gmv_user_admin
    FROM gmv
    GROUP BY
        date_payed,
        user_id,
        owner_email,
        owner_role
),

admin AS (
    SELECT
        admin_id,
        email,
        role AS owner_role
    FROM {{ ref('dim_user_admin') }}
),

users AS (
    SELECT
        du.user_id,
        key_validation_status.status AS validation_status,
        reject_reason.reason AS reject_reason,
        du.owner_id
    FROM {{ ref('dim_user') }} AS du
    LEFT JOIN {{ ref('key_validation_status') }} ON key_validation_status.id = validation_status
    LEFT JOIN {{ ref('key_validation_reject_reason') }} AS reject_reason ON reject_reason.id = du.reject_reason
    WHERE du.next_effective_ts_msk IS NULL
),


customers AS (
    SELECT DISTINCT
        user_id,
        c.company_name,
        volume_from,
        volume_to,
        grade,
        grade_probability
    FROM {{ ref('fact_customers') }} AS c
),

current_admin AS (
    SELECT
        gua.date_payed,
        u.user_id,
        u.validation_status,
        u.reject_reason,
        a.email AS owner_email,
        a.owner_role,
        c.company_name AS company_name,
        c.volume_from,
        c.volume_to,
        c.grade,
        c.grade_probability,
        MAX(gu.gmv_year) AS gmv_year,
        MAX(gu.gmv_quarter) AS gmv_quarter,
        MAX(gua.gmv_user_admin) AS gmv_user_admin,
        'current admin' AS admin_status
    FROM users AS u
    LEFT JOIN admin AS a ON u.owner_id = a.admin_id
    LEFT JOIN customers AS c ON u.user_id = c.user_id
    LEFT JOIN gmv_user AS gu ON gu.user_id = u.user_id
    LEFT JOIN gmv_user_admin AS gua ON u.user_id = gua.user_id AND a.email = gua.owner_email
    GROUP BY
        gua.date_payed,
        u.user_id,
        u.validation_status,
        u.reject_reason,
        a.email,
        a.owner_role,
        c.company_name,
        c.volume_from,
        c.volume_to,
        c.grade,
        c.grade_probability
),
 
past_admin AS (
    SELECT
        gua.date_payed,
        u.user_id,
        u.validation_status,
        u.reject_reason,
        gua.owner_email,
        gua.owner_role,
        c.company_name AS company_name,
        c.volume_from,
        c.volume_to,
        c.grade,
        c.grade_probability,
        MAX(gu.gmv_year) AS gmv_year,
        MAX(gu.gmv_quarter) AS gmv_quarter,
        MAX(gua.gmv_user_admin) AS gmv_user_admin,
        'past admin' AS admin_status
    FROM gmv_user_admin AS gua
    LEFT JOIN users AS u ON u.user_id = gua.user_id
    LEFT JOIN admin AS a ON u.owner_id = a.admin_id
    LEFT JOIN customers AS c ON u.user_id = c.user_id
    LEFT JOIN gmv_user AS gu ON gu.user_id = u.user_id
    WHERE a.email != gua.owner_email
    GROUP BY
        gua.date_payed,
        u.user_id,
        u.validation_status,
        u.reject_reason,
        gua.owner_email,
        gua.owner_role,
        c.company_name,
        c.volume_from,
        c.volume_to,
        c.grade,
        c.grade_probability
)

SELECT *, DATE('{{ var("start_date_ymd") }}') AS partition_date_msk FROM current_admin
UNION ALL
SELECT *, DATE('{{ var("start_date_ymd") }}') AS partition_date_msk FROM past_admin

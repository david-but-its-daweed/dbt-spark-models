{{
    config(
        materialized='table',
        alias='active_real_users',
        schema='gold',
        clustered_by=['real_user_id'],
        file_format='parquet',
        meta = {
            'model_owner' : '@general_analytics',
            'bigquery_load': 'true',
            'bigquery_partitioning_date_column': 'date_msk',
            'bigquery_check_counts_max_diff_fraction': '0.01',
            'bigquery_upload_horizon_days': '230',
            'priority_weight': '1000',
        }
    )
}}

WITH real_users_0 AS (
    SELECT
        date_msk,
        real_user_id,
        country_code,
        app_language,
        legal_entity,
        app_entity,
        MIN(MIN(join_date_msk)) OVER (PARTITION BY real_user_id) AS join_date_msk,
        FIRST_VALUE(real_user_segment) AS real_user_segment,
        MIN(MIN(join_date_msk)) OVER (PARTITION BY real_user_id) = date_msk AS is_new_real_user,
        DATEDIFF(date_msk, MIN(MIN(join_date_msk)) OVER (PARTITION BY real_user_id)) AS real_user_lifetime,
        SUM(gmv_per_day_initial) AS gmv_per_day_initial,
        SUM(gmv_per_day_final) AS gmv_per_day_final,
        SUM(order_gross_profit_per_day_final_estimated) AS order_gross_profit_per_day_final_estimated,
        SUM(order_gross_profit_per_day_final) AS order_gross_profit_per_day_final,
        SUM(ecgp_per_day_initial) AS ecgp_per_day_initial,
        SUM(ecgp_per_day_final) AS ecgp_per_day_final,
        SUM(number_of_orders) AS number_of_orders,
        MAX(is_payer) AS is_payer,
        MAX(is_converted) AS is_converted,

        -- столбцы нужны для определения страны/платформы пользователя. Отдаем приоритет стране, где у пользователя больше gmv за дату
        SUM(SUM(gmv_per_day_initial)) OVER (PARTITION BY real_user_id, date_msk, country_code) AS gmv_initial_per_country_code,
        SUM(SUM(gmv_per_day_initial)) OVER (PARTITION BY real_user_id, date_msk, app_language) AS gmv_initial_per_app_language,
        SUM(SUM(gmv_per_day_initial)) OVER (PARTITION BY real_user_id, date_msk, legal_entity) AS gmv_initial_per_legal_entity,
        SUM(SUM(gmv_per_day_initial)) OVER (PARTITION BY real_user_id, date_msk, app_entity) AS gmv_initial_per_app_entity
    FROM {{ ref('gold_active_devices') }}
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- определям страну/платформу пользователя на основе gmv
adjusted_slices AS (
    SELECT DISTINCT
        real_user_id,
        date_msk,
        FIRST_VALUE(country_code) OVER (PARTITION BY real_user_id, date_msk ORDER BY gmv_initial_per_country_code DESC, country_code ASC) AS country_code_based_on_gmv_initial,
        FIRST_VALUE(app_language) OVER (PARTITION BY real_user_id, date_msk ORDER BY gmv_initial_per_app_language DESC, app_language ASC) AS app_language_based_on_gmv_initial,
        FIRST_VALUE(legal_entity) OVER (PARTITION BY real_user_id, date_msk ORDER BY gmv_initial_per_legal_entity DESC, legal_entity ASC) AS legal_entity_based_on_gmv_initial,
        FIRST_VALUE(app_entity) OVER (PARTITION BY real_user_id, date_msk ORDER BY gmv_initial_per_app_entity DESC, app_entity ASC) AS app_entity_based_on_gmv_initial
    FROM real_users_0
),

real_users_1 AS (
    SELECT
        date_msk,
        real_user_id,
        FIRST_VALUE(country_code) AS country_code,
        FIRST_VALUE(app_language) AS app_language,
        FIRST_VALUE(legal_entity) AS legal_entity,
        FIRST_VALUE(app_entity) AS app_entity,
        MIN(MIN(join_date_msk)) OVER (PARTITION BY real_user_id) AS join_date_msk,
        FIRST_VALUE(real_user_segment) AS real_user_segment,
        MIN(MIN(join_date_msk)) OVER (PARTITION BY real_user_id) = date_msk AS is_new_real_user,
        DATEDIFF(date_msk, MIN(MIN(join_date_msk)) OVER (PARTITION BY real_user_id)) AS real_user_lifetime,
        SUM(gmv_per_day_initial) AS gmv_per_day_initial,
        SUM(gmv_per_day_final) AS gmv_per_day_final,
        SUM(order_gross_profit_per_day_final_estimated) AS order_gross_profit_per_day_final_estimated,
        SUM(order_gross_profit_per_day_final) AS order_gross_profit_per_day_final,
        SUM(ecgp_per_day_initial) AS ecgp_per_day_initial,
        SUM(ecgp_per_day_final) AS ecgp_per_day_final,
        SUM(number_of_orders) AS number_of_orders,
        MAX(is_payer) AS is_payer,
        MAX(is_converted) AS is_converted
    FROM real_users_0
    GROUP BY 1, 2
),

real_users_2 AS (
    SELECT
        *,
        LAG(date_msk) OVER (PARTITION BY real_user_id ORDER BY date_msk) AS prev_date_msk, -- смотрим на предыдущий день активности
        LEAD(date_msk) OVER (PARTITION BY real_user_id ORDER BY date_msk) AS next_date_msk  -- смотрим на следующий  день активности
    FROM real_users_1
),

real_users_3 AS (
    -- добавляем группы по активности и времени жизни
    SELECT
        *,
        CASE
            WHEN is_new_real_user THEN 'new'
            WHEN prev_date_msk_lag BETWEEN 1 AND 28 THEN 'regular'
            ELSE 'reactivated'
        END AS previous_activity_real_user_group -- тип по последней активности устройства
    FROM (
        SELECT
            -- для пользователей без предыдущего дня, смотрим нет ли join date (иногда join date есть, а активности в этот день нет)
            *,
            IF(a_l = 0, real_user_lifetime, a_l) AS prev_date_msk_lag,
            DATEDIFF(next_date_msk, date_msk) AS next_date_msk_lag
        FROM (
            SELECT
                *,
                COALESCE(DATEDIFF(date_msk, prev_date_msk), 0) AS a_l -- заменяем нули
            FROM real_users_2
        )
    )
),

real_users_4 AS (
    SELECT
        *,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 1,
            (COUNT(*) OVER (PARTITION BY real_user_id ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) > 0,
            NULL
        ) AS is_rd1,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 3,
            (COUNT(*) OVER (PARTITION BY real_user_id ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 3 FOLLOWING AND 3 FOLLOWING)) > 0,
            NULL
        ) AS is_rd3,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 7,
            (COUNT(*) OVER (PARTITION BY real_user_id ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 7 FOLLOWING AND 7 FOLLOWING)) > 0,
            NULL
        ) AS is_rd7,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 14,
            (COUNT(*) OVER (PARTITION BY real_user_id ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 14 FOLLOWING AND 14 FOLLOWING)) > 0,
            NULL
        ) AS is_rd14,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 7,
            (COUNT(*) OVER (PARTITION BY real_user_id ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 1 FOLLOWING AND 7 FOLLOWING)) > 0,
            NULL
        ) AS is_rw1,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 14,
            (COUNT(*) OVER (PARTITION BY real_user_id ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 8 FOLLOWING AND 14 FOLLOWING)) > 0,
            NULL
        ) AS is_rw2,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 21,
            (COUNT(*) OVER (PARTITION BY real_user_id ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 15 FOLLOWING AND 21 FOLLOWING)) > 0,
            NULL
        ) AS is_rw3,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 28,
            (COUNT(*) OVER (PARTITION BY real_user_id ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 22 FOLLOWING AND 28 FOLLOWING)) > 0,
            NULL
        ) AS is_rw4,
        IF(
            DATEDIFF(CURRENT_DATE(), date_msk) >= 14,
            (next_date_msk_lag > 14 OR (next_date_msk_lag IS NULL AND DATEDIFF(CURRENT_DATE(), date_msk) >= 14)),
            NULL
        ) AS is_churned_14,
        IF(
            DATEDIFF(CURRENT_DATE(), date_msk) >= 28,
            (next_date_msk_lag > 28 OR (next_date_msk_lag IS NULL AND DATEDIFF(CURRENT_DATE(), date_msk) >= 28)),
            NULL
        ) AS is_churned_28
    FROM real_users_3
),

real_users_5 AS (
    SELECT
        real_users.date_msk,
        real_users.real_user_id,
        COALESCE(adjusted_slices.country_code_based_on_gmv_initial, real_users.country_code) AS country_code,
        COALESCE(adjusted_slices.app_language_based_on_gmv_initial, real_users.app_language) AS app_language,
        COALESCE(adjusted_slices.legal_entity_based_on_gmv_initial, real_users.legal_entity) AS legal_entity,
        COALESCE(adjusted_slices.app_entity_based_on_gmv_initial, real_users.app_entity) AS app_entity,
        real_users.join_date_msk,
        real_users.real_user_segment,
        real_users.is_new_real_user,
        real_users.real_user_lifetime,
        real_users.previous_activity_real_user_group,
        real_users.prev_date_msk_lag,
        real_users.next_date_msk_lag,
        real_users.gmv_per_day_initial,
        real_users.gmv_per_day_final,
        real_users.order_gross_profit_per_day_final_estimated,
        real_users.order_gross_profit_per_day_final,
        real_users.ecgp_per_day_initial,
        real_users.ecgp_per_day_final,
        real_users.number_of_orders,
        real_users.is_payer,
        real_users.is_converted,
        real_users.is_rd1,
        real_users.is_rd3,
        real_users.is_rd7,
        real_users.is_rd14,
        real_users.is_rw1,
        real_users.is_rw2,
        real_users.is_rw3,
        real_users.is_rw4,
        real_users.is_churned_14,
        real_users.is_churned_28
    FROM real_users_4 AS real_users
    LEFT JOIN adjusted_slices USING (real_user_id, date_msk)
)

SELECT
    real_users.date_msk,
    real_users.real_user_id,
    real_users.country_code,
    COALESCE(countries.top_country_code, 'Other') AS top_country_code,
    COALESCE(countries.region_name, 'Other') AS region_name,
    COALESCE(countries.country_priority_type, 'Other') AS country_priority_type,
    real_users.app_language,
    real_users.legal_entity,
    real_users.app_entity,
    real_users.join_date_msk,
    real_users.real_user_segment,
    real_users.is_new_real_user,
    real_users.real_user_lifetime,
    real_users.previous_activity_real_user_group,
    real_users.prev_date_msk_lag,
    real_users.next_date_msk_lag,
    real_users.gmv_per_day_initial,
    real_users.gmv_per_day_final,
    real_users.order_gross_profit_per_day_final_estimated,
    real_users.order_gross_profit_per_day_final,
    real_users.ecgp_per_day_initial,
    real_users.ecgp_per_day_final,
    real_users.number_of_orders,
    real_users.is_payer,
    real_users.is_converted,
    real_users.is_rd1,
    real_users.is_rd3,
    real_users.is_rd7,
    real_users.is_rd14,
    real_users.is_rw1,
    real_users.is_rw2,
    real_users.is_rw3,
    real_users.is_rw4,
    real_users.is_churned_14,
    real_users.is_churned_28
FROM real_users_5 AS real_users
LEFT JOIN {{ ref('gold_countries') }} AS countries USING (country_code)

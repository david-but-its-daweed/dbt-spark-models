{% macro gold_active_users_and_devices_with_ephemeral(device_or_user_id) %}



{% if  device_or_user_id == 'device_id' %}
    {% set naming_field = 'device' %}

    {{
        config(
        materialized='incremental',
        alias='active_devices_with_ephemeral',
        file_format='parquet',
        schema='gold',
        meta = {
            'model_owner' : '@analytics.duty',
            'bigquery_load': 'true',
            'bigquery_partitioning_date_column': 'date_msk',
            'bigquery_upload_horizon_days': '230',
            'bigquery_override_dataset_id': 'gold',
            'priority_weight': '1000',
            'full_reload_on': '6',
        },
        incremental_strategy='insert_overwrite',
        partition_by=['month_msk'],
    )
    }}
{% elif device_or_user_id == 'user_id' %}
    {% set naming_field = 'user' %}

    {{
        config(
        materialized='incremental',
        alias='active_users_with_ephemeral',
        file_format='parquet',
        schema='gold',
        meta = {
            'model_owner' : '@analytics.duty',
            'bigquery_load': 'true',
            'bigquery_partitioning_date_column': 'date_msk',
            'bigquery_upload_horizon_days': '230',
            'bigquery_override_dataset_id': 'gold',
            'priority_weight': '1000',
            'full_reload_on': '6',
        },
        incremental_strategy='insert_overwrite',
        partition_by=['month_msk'],
    )
    }}
{% endif %}


WITH
uniq_regions AS (
    SELECT * FROM {{ ref('gold_regions') }} WHERE is_uniq = TRUE
),

first_order_dates AS (
    SELECT
        {{ device_or_user_id }},
        MIN(order_date_msk) AS dt
    FROM {{ ref('gold_orders') }}
    GROUP BY 1
),

orders_ext1 AS (
    SELECT
        {{ device_or_user_id }},
        order_date_msk AS date_msk,
        country_code,
        platform,
        SUM(gmv_initial) AS gmv_per_day_initial,
        SUM(gmv_final) AS gmv_per_day_final,
        SUM(order_gross_profit_final_estimated) AS order_gross_profit_per_day_final_estimated,
        SUM(order_gross_profit_final) AS order_gross_profit_per_day_final,
        SUM(ecgp_initial) AS ecgp_per_day_initial,
        SUM(ecgp_final) AS ecgp_per_day_final,
        COUNT(order_id) AS number_of_orders,
        COUNT(order_id) > 0 AS is_converted,

        -- столбцы нужны для определения страны/платформы пользователя. Отдаем приоритет стране, где у пользователя больше gmv за дату
        SUM(SUM(gmv_initial)) OVER(PARTITION BY {{ device_or_user_id }}, order_date_msk, country_code) AS gmv_initial_per_country_code,
        SUM(SUM(gmv_initial)) OVER(PARTITION BY {{ device_or_user_id }}, order_date_msk, platform) AS gmv_initial_per_platform
    FROM {{ ref('gold_orders') }}

    {% if is_incremental() %}
        WHERE order_month_msk >= trunc(date'{{ var("start_date_ymd") }}' - interval 200 days, 'MM')
    {% elif device_or_user_id == 'device_id' %}
        WHERE order_date_msk >= '2018-04-15' -- до 2018-04-15 пустые device_id
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

orders_ext2 AS (
    SELECT
        {{ device_or_user_id }},
        date_msk,
        SUM(gmv_per_day_initial) AS gmv_per_day_initial,
        SUM(gmv_per_day_final) AS gmv_per_day_final,
        SUM(order_gross_profit_per_day_final_estimated) AS order_gross_profit_per_day_final_estimated,
        SUM(order_gross_profit_per_day_final) AS order_gross_profit_per_day_final,
        SUM(ecgp_per_day_initial) AS ecgp_per_day_initial,
        SUM(ecgp_per_day_final) AS ecgp_per_day_final,
        SUM(number_of_orders) AS number_of_orders,
        MAX(is_converted) AS is_converted
    FROM orders_ext1
    GROUP BY 1, 2
),

-- определям страну/платформу пользователя на основе gmv
adjusted_slices AS (
    SELECT DISTINCT
        {{ device_or_user_id }},
        date_msk AS day,
        FIRST_VALUE(country_code) OVER(PARTITION BY {{ device_or_user_id }}, date_msk ORDER BY gmv_initial_per_country_code DESC, country_code) AS country_code_based_on_gmv_initial,
        FIRST_VALUE(platform) OVER(PARTITION BY {{ device_or_user_id }}, date_msk ORDER BY gmv_initial_per_platform DESC, platform) AS platform_based_on_gmv_initial
    FROM orders_ext1
),

active_devices_ext0 AS (
    -- добавляем предыдущий день активности и другие св-ва, для которых требуется вся история
    select
        {{ device_or_user_id }},
        day,
        LAG(day) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY day) AS prev_date_msk, -- смотрим на предыдущий день активности
        LEAD(day) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY day) AS next_date_msk, -- смотрим на следующий день активности
        MIN(is_ephemeral) OVER(PARTITION BY {{ device_or_user_id }}) AS min_is_ephemeral

    {% if device_or_user_id == 'user_id' %}
        FROM {{ ref('active_users') }}
    {% else %}
        FROM {{ ref('active_devices') }}
    {% endif %}
),

active_devices_ext1 AS (
    SELECT
        main.{{ device_or_user_id }},
        main.day AS date_msk,
        main.real_user_id,
        main.join_day AS join_date_msk,
        main.legal_entity,
        {% if device_or_user_id == 'device_id' %}
            main.app_entity,
            main.app_entity_group,
            main.shopy_blogger_domain,
            main.is_product_opened,
            main.is_product_added_to_cart,
            main.is_product_purchased,
            main.is_product_to_favourites,
            main.is_cart_opened,
            main.is_checkout_started,
            main.is_checkout_payment_method_selected,
            main.is_checkout_delivery_selected,
        {% endif %}
        COALESCE(adjusted_slices.country_code_based_on_gmv_initial, main.country) AS country_code,
        main.app_language,
        COALESCE(adjusted_slices.platform_based_on_gmv_initial, main.platform) AS platform,
        main.os_version,
        main.app_version,
        main.is_ephemeral,
        DATEDIFF(main.day, main.join_day) AS {{ naming_field }}_lifetime,

        aux.prev_date_msk,
        aux.next_date_msk,
        aux.min_is_ephemeral,

        main.day = main.join_day AS is_new_{{ naming_field }}

    {% if device_or_user_id == 'user_id' %}
        FROM {{ ref('active_users') }} AS main
    {% else %}
        FROM {{ ref('active_devices') }} AS main
    {% endif %}
    JOIN active_devices_ext0 AS aux USING ({{ device_or_user_id}}, day)
    LEFT JOIN adjusted_slices USING({{ device_or_user_id }}, day)

    {% if is_incremental() %}
        WHERE month_msk >= trunc(date'{{ var("start_date_ymd") }}' - interval 200 days, 'MM')
    {% endif %}
),

active_devices_ext2 AS (
    -- добавляем группы по активности и времени жизни
    SELECT
        *,
        CASE
            WHEN is_new_{{ naming_field }} THEN 'new'
            WHEN prev_date_msk_lag BETWEEN 1 AND 28 THEN 'regular'
            ELSE 'reactivated'
        END AS previous_activity_device_group -- тип по последней активности устройства
    FROM (
        SELECT
            -- для пользователей без предыдущего дня, смотрим нет ли join date (иногда join date есть, а активности в этот день нет)
            *,
            IF(a_l = 0, {{ naming_field }}_lifetime, a_l) AS prev_date_msk_lag,
            DATEDIFF(next_date_msk, date_msk) AS next_date_msk_lag
        FROM (
            SELECT
                *,
                COALESCE(DATEDIFF(date_msk, prev_date_msk), 0) AS a_l -- заменяем нули
            FROM active_devices_ext1
        )
    )
),

active_devices_ext3 AS (
    -- джоиним заказы к дням активности
    SELECT
        a.{{ device_or_user_id }},
        a.date_msk,
        a.real_user_id,
        a.country_code,
        a.platform,
        a.legal_entity,
        {% if device_or_user_id == 'device_id' %}
            a.app_entity,
            a.app_entity_group,
            a.shopy_blogger_domain,
            a.is_product_opened,
            a.is_product_added_to_cart,
            a.is_product_purchased,
            a.is_product_to_favourites,
            a.is_cart_opened,
            a.is_checkout_started,
            a.is_checkout_payment_method_selected,
            a.is_checkout_delivery_selected,
        {% endif %}
        a.app_language,
        a.is_new_{{ naming_field }},
        a.join_date_msk,
        a.{{ naming_field }}_lifetime,
        a.prev_date_msk_lag,
        a.next_date_msk_lag,
        a.previous_activity_device_group,
        a.min_is_ephemeral AS is_ephemeral_{{ naming_field }},

        COALESCE(b.gmv_per_day_initial, 0) AS gmv_per_day_initial,
        COALESCE(b.gmv_per_day_final, 0) AS gmv_per_day_final,
        COALESCE(b.order_gross_profit_per_day_final_estimated, 0) AS order_gross_profit_per_day_final_estimated,
        COALESCE(b.order_gross_profit_per_day_final, 0) AS order_gross_profit_per_day_final,
        COALESCE(b.ecgp_per_day_initial, 0) AS ecgp_per_day_initial,
        COALESCE(b.ecgp_per_day_final, 0) AS ecgp_per_day_final,
        COALESCE(b.number_of_orders, 0) AS number_of_orders,

        COALESCE(a.date_msk >= f.dt, false) AS is_payer,

        COALESCE(b.is_converted, false) AS is_converted
    FROM active_devices_ext2 AS a
    LEFT JOIN orders_ext2 AS b USING ({{ device_or_user_id }}, date_msk)
    LEFT JOIN first_order_dates f USING ({{ device_or_user_id }})
),

active_devices_ext4 AS (
    SELECT
        *,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 1,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) > 0,
            NULL
        ) AS is_rd1,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 3,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 3 FOLLOWING AND 3 FOLLOWING)) > 0,
            NULL
        ) AS is_rd3,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 7,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 7 FOLLOWING AND 7 FOLLOWING)) > 0,
            NULL
        ) AS is_rd7,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 14,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 14 FOLLOWING AND 14 FOLLOWING)) > 0,
            NULL
        ) AS is_rd14,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 7,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 1 FOLLOWING AND 7 FOLLOWING)) > 0,
            NULL
        ) AS is_rw1,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 14,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 8 FOLLOWING AND 14 FOLLOWING)) > 0,
            NULL
        ) AS is_rw2,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 21,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 15 FOLLOWING AND 21 FOLLOWING)) > 0,
            NULL
        ) AS is_rw3,
        IF(
            DATEDIFF(CURRENT_DATE() - 1, date_msk) >= 28,
            (COUNT(*) OVER (PARTITION BY {{ device_or_user_id }} ORDER BY UNIX_DATE(date_msk) RANGE BETWEEN 22 FOLLOWING AND 28 FOLLOWING)) > 0,
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
    FROM active_devices_ext3
),

active_devices_ext5 AS (
    -- добавляем основной регион
    SELECT
        a.*,
        COALESCE(c.top_country_code, 'Other') AS top_country_code,
        COALESCE(c.country_priority_type, 'Other') AS country_priority_type,
        COALESCE(b.region_name, 'Other') AS region_name
    FROM active_devices_ext4 AS a
    LEFT JOIN uniq_regions AS b USING(country_code)
    LEFT JOIN {{ ref('gold_countries') }} c USING(country_code)
),

active_devices_ext6 AS (
    -- добавляем сегмент пользователя
    SELECT
        a.*,
        COALESCE(b.user_segment, 'Non-buyers') AS real_user_segment
    FROM active_devices_ext5 AS a
    LEFT JOIN {{ ref('user_segments') }} AS b
        ON
            a.real_user_id = b.real_user_id
            AND a.date_msk >= TO_DATE(b.effective_ts)
            AND a.date_msk <= TO_DATE(b.next_effective_ts)
)

SELECT
    date_msk,

    {{ device_or_user_id }},
    real_user_id,

    country_code,
    top_country_code,
    country_priority_type,
    region_name,
    app_language,
    platform,
    legal_entity,
    {% if device_or_user_id == 'device_id' %}
        app_entity,
        app_entity_group,
        shopy_blogger_domain,
        is_product_opened,
        is_product_added_to_cart,
        is_product_purchased,
        is_product_to_favourites,
        is_cart_opened,
        is_checkout_started,
        is_checkout_payment_method_selected,
        is_checkout_delivery_selected,
    {% endif %}
    join_date_msk,
    real_user_segment,
    is_new_{{ naming_field }},
    is_ephemeral_{{ naming_field }},
    {{ naming_field }}_lifetime,

    previous_activity_device_group,
    prev_date_msk_lag,
    next_date_msk_lag,
    gmv_per_day_initial,
    gmv_per_day_final,
    order_gross_profit_per_day_final_estimated,
    order_gross_profit_per_day_final,
    ecgp_per_day_initial,
    ecgp_per_day_final,
    number_of_orders,

    is_payer,
    is_converted,
    is_rd1,
    is_rd3,
    is_rd7,
    is_rd14,
    is_rw1,
    is_rw2,
    is_rw3,
    is_rw4,
    is_churned_14,
    is_churned_28,
    trunc(date_msk, 'MM') AS month_msk
FROM active_devices_ext6
DISTRIBUTE by month_msk, abs(hash({{ device_or_user_id }})) % 10

{% endmacro %}
